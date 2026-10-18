import hashlib
import os
import shutil
from functools import partial
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from braincore_memory import migrate
from braincore_memory.config import Settings, get_settings
from braincore_memory.embedding import EmbeddingResult
from braincore_memory.models import Scope, SearchRequest
from braincore_memory.service import _incompatible_vector_query, _search_filter, search_memories

from .test_search_performance import (
    _borrow_connection,
    _search_fixture_principal,
    _seed_search_memory,
)
from .test_search_performance import (
    search_database as _search_database,
)

search_database = _search_database


def test_embedding_coverage_upgrade_and_rerun_preserve_existing_memories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires the verified disposable integration database")
    dsn = get_settings().database_url.get_secret_value()
    database = "braincore_coverage_test_" + uuid4().hex
    source_migrations = migrate.migration_root()
    predecessor = tmp_path / "predecessor-migrations"
    predecessor.mkdir()
    for migration in source_migrations.glob("*.sql"):
        if migration.name < "033_embedding_coverage.sql":
            shutil.copyfile(migration, predecessor / migration.name)
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as owner:
        assert owner.info.dbname == "braincore_test"
        owner.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            upgrade_dsn = make_conninfo(dsn, dbname=database)
            with monkeypatch.context() as environment:
                environment.setenv("BRAINCORE_DATABASE_URL", upgrade_dsn)
                environment.setattr(migrate, "migration_root", lambda: predecessor)
                get_settings.cache_clear()
                migrate.apply_migrations()
                with psycopg.connect(upgrade_dsn, row_factory=dict_row) as target:
                    tenant = target.execute(
                        "INSERT INTO braincore.tenants(name) VALUES('upgrade-test') RETURNING id"
                    ).fetchone()
                    assert tenant is not None
                    target.execute(
                        """INSERT INTO braincore.memories
                           (tenant_id,content,dedupe_key,user_id,scope)
                           VALUES(%s,'Preserved pending source',%s,'alice',
                                  '{"user_id":"alice"}')""",
                        (tenant["id"], hashlib.sha256(b"synthetic upgrade fixture").digest()),
                    )
                    before = target.execute(
                        "SELECT row_to_json(m)::jsonb AS memory FROM braincore.memories m"
                    ).fetchall()
                    target.commit()
                    assert target.execute(
                        "SELECT to_regclass('braincore.memories_embedding_missing_idx') AS index"
                    ).fetchone() == {"index": None}
                    environment.setattr(migrate, "migration_root", lambda: source_migrations)
                    migrate.apply_migrations()
                    migrate.apply_migrations()
                    assert (
                        target.execute(
                            "SELECT row_to_json(m)::jsonb AS memory FROM braincore.memories m"
                        ).fetchall()
                        == before
                    )
                    assert target.execute(
                        """SELECT count(*) AS total FROM braincore.schema_migrations
                           WHERE version='033_embedding_coverage.sql'"""
                    ).fetchone() == {"total": 1}
                    assert target.execute(
                        "SELECT to_regclass('braincore.memories_embedding_missing_idx') AS index"
                    ).fetchone() == {"index": "braincore.memories_embedding_missing_idx"}
        finally:
            get_settings.cache_clear()
            owner.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


@pytest.mark.parametrize("embedding_state", ["pending", "failed", "ready"])
async def test_missing_applicable_embeddings_disclose_incomplete_vector_coverage(
    search_database: AsyncConnection[dict[str, object]], embedding_state: str
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact", search_full_text_mode="canonical")
        compatible = await _seed_search_memory(conn, actor, settings)
        missing = await _seed_search_memory(conn, actor, settings, embedding_state=embedding_state)
        await conn.execute("UPDATE braincore.memories SET embedding=NULL WHERE id=%s", (missing,))
        outcome = await search_memories(
            partial(_borrow_connection, conn),
            actor,
            SearchRequest(query="synthetic", scope=Scope(user_id="alice")),
            EmbeddingResult([1.0] + [0.0] * 383, "ok"),
            settings,
        )
        assert outcome.degraded
        assert outcome.payload["streams"]["vector"]["state"] == "ok"
        assert "missing" in outcome.payload["streams"]["vector"]["warning"]
        assert compatible in [row["id"] for row in outcome.payload["results"]]


@pytest.mark.parametrize(
    "excluded",
    [
        "tenant",
        "scope",
        "suppressed",
        "expired",
        "future",
        "namespace",
        "metadata",
        "type",
        "not_applicable",
    ],
)
async def test_coverage_never_discloses_embeddings_outside_search_eligibility(
    search_database: AsyncConnection[dict[str, object]], excluded: str
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        body = SearchRequest(
            query="synthetic",
            scope=Scope(user_id="alice"),
            reference_namespace_prefix=["facts"],
            metadata_filter={"kind": "eligible"},
            object_type="memory",
        )
        overrides: dict[str, Any] = {"embedding_state": "pending"}
        if excluded == "tenant":
            overrides["tenant_id"] = (await _search_fixture_principal(conn)).tenant_id
        elif excluded == "scope":
            overrides["user_id"] = "other"
        elif excluded == "suppressed":
            overrides["state"] = "suppressed"
        elif excluded == "expired":
            overrides["expired"] = True
        elif excluded == "future":
            overrides["days_from_now"] = 1
        elif excluded == "namespace":
            overrides["namespace"] = ["other"]
        elif excluded == "metadata":
            overrides["metadata"] = {"kind": "excluded"}
        elif excluded == "type":
            overrides["object_type"] = "episode"
        memory_id = await _seed_search_memory(conn, actor, settings, **overrides)
        if excluded == "not_applicable":
            await conn.execute(
                """UPDATE braincore.memories SET content=' ',embedding=NULL,
                   embedding_model=NULL,embedding_preprocessing_revision=NULL,
                   embedding_state='not_applicable' WHERE id=%s""",
                (memory_id,),
            )
        where, values = _search_filter(actor, body)
        query, parameters = _incompatible_vector_query(where, values, settings)
        assert await (await conn.execute(query, parameters)).fetchone() == {"incompatible": False}


async def test_coverage_tracks_model_revision_and_missing_rows_independently(
    search_database: AsyncConnection[dict[str, object]],
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        where, values = _search_filter(actor, SearchRequest(query="synthetic"))
        query, parameters = _incompatible_vector_query(where, values, settings)
        for identity in (
            {"embedding_model": "other-model"},
            {"embedding_preprocessing_revision": "other-revision"},
            {"embedding_model": None},
            {"embedding_preprocessing_revision": None},
        ):
            memory_id = await _seed_search_memory(conn, actor, settings, **identity)
            assert await (await conn.execute(query, parameters)).fetchone() == {
                "incompatible": True
            }
            await conn.execute("DELETE FROM braincore.memories WHERE id=%s", (memory_id,))
        memory_id = await _seed_search_memory(conn, actor, settings, embedding_state="failed")
        assert await (await conn.execute(query, parameters)).fetchone() == {"incompatible": True}
        await conn.execute(
            "UPDATE braincore.memories SET embedding_state='ready' WHERE id=%s", (memory_id,)
        )
        assert await (await conn.execute(query, parameters)).fetchone() == {"incompatible": False}

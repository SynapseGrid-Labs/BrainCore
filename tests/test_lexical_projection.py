import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.errors import InsufficientPrivilege, LockNotAvailable
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from braincore_memory.auth import Principal
from braincore_memory.config import get_settings
from braincore_memory.lexical_projection import backfill_batch, backfill_projection
from braincore_memory.migrate import migration_root
from braincore_memory.models import Scope, SearchRequest
from braincore_memory.service import _search_filter


@pytest.fixture
def lexical_database() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires the disposable integration database")
    dsn = get_settings().database_url.get_secret_value()
    database = "braincore_lexical_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as admin:
        assert admin.execute("SELECT current_database() AS name").fetchone()["name"] == (
            "braincore_test"
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            with psycopg.connect(
                make_conninfo(dsn, dbname=database), autocommit=True, row_factory=dict_row
            ) as conn:
                for path in sorted(migration_root().glob("*.sql")):
                    with conn.transaction():
                        conn.execute(path.read_text())
                yield conn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def _tenant(conn: psycopg.Connection[dict[str, Any]]) -> UUID:
    row = conn.execute(
        "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id", (uuid4().hex,)
    ).fetchone()
    assert row is not None
    return UUID(str(row["id"]))


def _seed(conn: psycopg.Connection[dict[str, Any]], tenant: UUID, **overrides: Any) -> UUID:
    memory_id = overrides.pop("id", uuid4())
    fields = {
        "id": memory_id,
        "tenant_id": tenant,
        "content": "alpha beta useful evidence",
        "dedupe_key": hashlib.sha256(memory_id.bytes).digest(),
        "user_id": "alice",
        "scope": Jsonb({"user_id": overrides.get("user_id", "alice")}),
        "reference_namespace": ["facts", "search"],
        "reference_key": str(memory_id),
        "metadata": Jsonb({"kind": "eligible"}),
        "state": "active",
        "valid_from": datetime(2000, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2000, 1, 1, tzinfo=UTC),
        "legacy_scope_path": "project/literal_%/nested",
        **overrides,
    }
    conn.execute(
        sql.SQL("INSERT INTO braincore.memories ({}) VALUES ({})").format(
            sql.SQL(",").join(sql.Identifier(field) for field in fields),
            sql.SQL(",").join(sql.Placeholder() for _ in fields),
        ),
        list(fields.values()),
    )
    return memory_id


def _ordered_fts(
    conn: psycopg.Connection[dict[str, Any]], actor: Principal, body: SearchRequest, table: str
) -> list[UUID]:
    where, values = _search_filter(actor, body)
    rows = conn.execute(
        sql.SQL("""SELECT m.id FROM {} m WHERE {} AND m.legacy_fts_eligible
          AND m.search_vector @@ plainto_tsquery('english',%s)
          ORDER BY ts_rank_cd(m.search_vector,plainto_tsquery('english',%s)) DESC,
                   m.updated_at DESC,m.id LIMIT %s""").format(
            sql.Identifier("braincore", table), where
        ),
        [*values, body.query, body.query, (body.limit + body.offset) * 3],
    ).fetchall()
    return [UUID(str(row["id"])) for row in rows]


def test_projection_matches_exact_rank_and_all_filters(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    actor = Principal(uuid4(), tenant, frozenset({"read"}), {"user_id": "alice"})
    winners = [_seed(conn, tenant), _seed(conn, tenant)]
    for override in (
        {"tenant_id": other},
        {"user_id": "bob"},
        {"state": "suppressed"},
        {"state": "retired"},
        {"valid_until": datetime(2021, 1, 1, tzinfo=UTC)},
        {"valid_from": datetime(2100, 1, 1, tzinfo=UTC)},
        {"object_type": "entity"},
        {"metadata": Jsonb({"kind": "excluded"})},
        {"reference_namespace": ["other"]},
        {"legacy_scope_path": "project/literal_XX/nested"},
        {"legacy_fts_eligible": False},
    ):
        _seed(conn, tenant, **override)
    assert backfill_batch(conn)["ready"] is False
    assert backfill_batch(conn)["ready"] is True
    strict = SearchRequest(
        query="alpha beta",
        scope=Scope(user_id="alice"),
        legacy_scope_prefix="project/literal_%",
        reference_namespace_prefix=["facts"],
        metadata_filter={"kind": "eligible"},
        limit=10,
    )
    assert _ordered_fts(conn, actor, strict, "memories_lexical") == sorted(winners)
    for body in (
        strict,
        SearchRequest(query="alpha beta", object_type="entity", limit=10),
        SearchRequest(query="alpha beta", include_inactive=True, limit=10),
        SearchRequest(query="alpha beta", as_of=datetime(2020, 1, 1, tzinfo=UTC), limit=10),
        SearchRequest(
            query="alpha beta",
            include_inactive=True,
            as_of=datetime(2020, 1, 1, tzinfo=UTC),
            limit=10,
        ),
        SearchRequest(query="alpha beta", limit=1, offset=2),
    ):
        assert _ordered_fts(conn, actor, body, "memories_lexical") == _ordered_fts(
            conn, actor, body, "memories"
        )


def test_transactional_corrections_lifecycle_and_embedding_only_updates(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    tenant = _tenant(conn)
    memory = _seed(conn, tenant)
    original = conn.execute(
        "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (memory,)
    ).fetchone()
    conn.execute("UPDATE braincore.memories SET embedding_state='failed' WHERE id=%s", (memory,))
    assert (
        conn.execute(
            "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (memory,)
        ).fetchone()
        == original
    )
    conn.execute(
        "UPDATE braincore.memories SET embedding_state='pending',updated_at=now() WHERE id=%s",
        (memory,),
    )
    assert (
        conn.execute(
            "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (memory,)
        ).fetchone()
        != original
    )
    with conn.transaction(force_rollback=True):
        conn.execute(
            "UPDATE braincore.memories SET content='corrected gamma',state='suppressed' "
            "WHERE id=%s",
            (memory,),
        )
        row = conn.execute(
            """SELECT state,search_vector @@ plainto_tsquery('english','gamma') AS corrected
               FROM braincore.memories_lexical WHERE id=%s""",
            (memory,),
        ).fetchone()
        assert row == {"state": "suppressed", "corrected": True}
    row = conn.execute(
        "SELECT state FROM braincore.memories_lexical WHERE id=%s", (memory,)
    ).fetchone()
    assert row == {"state": "active"}
    conn.execute("DELETE FROM braincore.memories WHERE id=%s", (memory,))
    assert (
        conn.execute("SELECT id FROM braincore.memories_lexical WHERE id=%s", (memory,)).fetchone()
        is None
    )


def test_backfill_resumes_commits_checkpoints_and_preserves_live_writes(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    tenant = _tenant(conn)
    # Simulate historical records predating the projection trigger.
    conn.execute("ALTER TABLE braincore.memories DISABLE TRIGGER memories_lexical_sync")
    historical = [_seed(conn, tenant) for _ in range(3)]
    conn.execute("ALTER TABLE braincore.memories ENABLE TRIGGER memories_lexical_sync")
    before = conn.execute(
        "SELECT ready FROM braincore.lexical_projection_state WHERE singleton=true"
    ).fetchone()
    assert before == {"ready": False}
    first = backfill_projection(
        make_conninfo(get_settings().database_url.get_secret_value(), dbname=conn.info.dbname),
        batch_size=1,
        max_batches=1,
    )
    assert first["scanned_items"] == 1 and first["ready"] is False
    assert first["backfill_after_id"] == min(historical)
    # New rows behind the cursor and updates to already copied rows remain current.
    new = _seed(conn, tenant, id=UUID(int=1), content="live gamma")
    conn.execute(
        "UPDATE braincore.memories SET content='corrected gamma' WHERE id=%s",
        (min(historical),),
    )
    result = backfill_projection(
        make_conninfo(get_settings().database_url.get_secret_value(), dbname=conn.info.dbname),
        batch_size=1,
    )
    assert result["ready"] is True and result["rows_processed"] == 3
    assert backfill_batch(conn)["scanned_items"] == 0
    for memory in [*historical, new]:
        row = conn.execute(
            """SELECT l.search_vector=m.search_vector AS matches
               FROM braincore.memories m JOIN braincore.memories_lexical l USING(id,tenant_id)
               WHERE m.id=%s""",
            (memory,),
        ).fetchone()
        assert row == {"matches": True}


def test_backfill_locks_source_and_rolls_back_with_checkpoint(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    conn.execute("ALTER TABLE braincore.memories DISABLE TRIGGER memories_lexical_sync")
    memory = _seed(conn, _tenant(conn))
    conn.execute("ALTER TABLE braincore.memories ENABLE TRIGGER memories_lexical_sync")
    with psycopg.connect(
        make_conninfo(get_settings().database_url.get_secret_value(), dbname=conn.info.dbname),
        autocommit=True,
        row_factory=dict_row,
    ) as writer:
        writer.execute("SET lock_timeout='100ms'")
        with conn.transaction(force_rollback=True):
            assert backfill_batch(conn, batch_size=1)["scanned_items"] == 1
            # A competing canonical writer cannot race the locked source copy.
            with pytest.raises(LockNotAvailable):
                writer.execute(
                    "UPDATE braincore.memories SET content='concurrent gamma' WHERE id=%s",
                    (memory,),
                )
        assert conn.execute(
            "SELECT backfill_after_id,rows_processed FROM braincore.lexical_projection_state"
        ).fetchone() == {"backfill_after_id": None, "rows_processed": 0}
        assert (
            conn.execute(
                "SELECT id FROM braincore.memories_lexical WHERE id=%s", (memory,)
            ).fetchone()
            is None
        )
        writer.execute(
            "UPDATE braincore.memories SET content='concurrent gamma' WHERE id=%s", (memory,)
        )
        assert backfill_batch(conn, batch_size=1)["scanned_items"] == 1
        assert conn.execute(
            """SELECT l.search_vector=m.search_vector AS matches
               FROM braincore.memories m JOIN braincore.memories_lexical l USING(id,tenant_id)
               WHERE m.id=%s""",
            (memory,),
        ).fetchone() == {"matches": True}


def test_incomplete_backfill_never_sets_ready(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    memory = _seed(conn, _tenant(conn))
    backfill_batch(conn)
    conn.execute("DELETE FROM braincore.memories_lexical WHERE id=%s", (memory,))
    with pytest.raises(RuntimeError, match="incomplete"):
        backfill_batch(conn)
    assert conn.execute(
        "SELECT ready FROM braincore.lexical_projection_state WHERE singleton=true"
    ).fetchone() == {"ready": False}


def test_trigger_maintains_projection_for_restricted_canonical_writer(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    memory = _seed(conn, _tenant(conn))
    role = "lexical_writer_" + uuid4().hex
    conn.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
    try:
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA braincore TO {}").format(sql.Identifier(role)))
        conn.execute(
            sql.SQL("GRANT SELECT,UPDATE ON braincore.memories TO {}").format(sql.Identifier(role))
        )
        with conn.transaction():
            conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            conn.execute(
                "UPDATE braincore.memories SET content='restricted gamma' WHERE id=%s", (memory,)
            )
            with pytest.raises(InsufficientPrivilege), conn.transaction():
                conn.execute("DELETE FROM braincore.memories_lexical WHERE id=%s", (memory,))
        assert conn.execute(
            """SELECT search_vector @@ plainto_tsquery('english','gamma') AS matches
               FROM braincore.memories_lexical WHERE id=%s""",
            (memory,),
        ).fetchone() == {"matches": True}
    finally:
        conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_security_definer_trigger_rejects_a_crafted_table(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    conn.execute("CREATE TEMP TABLE lexical_spoof(id uuid)")
    conn.execute(
        """CREATE TRIGGER spoof_sync AFTER INSERT ON lexical_spoof FOR EACH ROW
           EXECUTE FUNCTION braincore.sync_memory_lexical_projection()"""
    )
    with pytest.raises(InsufficientPrivilege, match="canonical memories"), conn.transaction():
        conn.execute("INSERT INTO lexical_spoof VALUES(%s)", (uuid4(),))


def test_projection_follows_canonical_identity_changes(
    lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    original = _seed(conn, tenant)
    changed = uuid4()
    conn.execute(
        "UPDATE braincore.memories SET id=%s,tenant_id=%s WHERE id=%s",
        (changed, other, original),
    )
    assert (
        conn.execute(
            "SELECT id FROM braincore.memories_lexical WHERE id=%s", (original,)
        ).fetchone()
        is None
    )
    assert conn.execute(
        "SELECT tenant_id FROM braincore.memories_lexical WHERE id=%s", (changed,)
    ).fetchone() == {"tenant_id": other}


@pytest.mark.parametrize("batch_size", [0, -1, 10_001])
def test_invalid_batch_size_is_rejected_without_connecting(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        backfill_projection("unused", batch_size=batch_size)


def test_invalid_batch_limit_is_rejected_without_connecting() -> None:
    with pytest.raises(ValueError, match="max_batches"):
        backfill_projection("unused", max_batches=0)

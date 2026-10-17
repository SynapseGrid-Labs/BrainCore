import asyncio
import hashlib
import json
import logging
import math
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from psycopg import AsyncConnection, Error, sql
from psycopg.errors import InvalidParameterValue, QueryCanceled, UndefinedTable
from psycopg.rows import dict_row
from starlette.requests import Request

from braincore_memory import api as memory_api
from braincore_memory import service as memory_service
from braincore_memory.auth import Principal
from braincore_memory.config import Settings
from braincore_memory.embedding import EmbeddingResult
from braincore_memory.models import MemoryReference, RememberRequest, Scope, SearchRequest
from braincore_memory.service import (
    _configure_iterative_search,
    _incompatible_vector_query,
    _search_filter,
    _vector_query,
    check_search_capability,
    remember_one,
    search_memories,
)

from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source


class Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    async def fetchall(self) -> list[dict[str, object]]:
        return self.rows

    async def fetchone(self) -> dict[str, object] | None:
        return self.rows[0] if self.rows else None


class ScriptedConnection:
    def __init__(self, responses: list[list[dict[str, object]] | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Any, int]] = []
        self.transaction_depth = 0
        self.rollbacks = 0
        self.events: list[str] = []
        self.connection_depth = 0
        self.coverage_calls: list[tuple[str, Any, int]] = []
        self.projection_response: list[dict[str, object]] | Exception = [{"ready": True}]
        self.projection_checks = 0
        self.coverage_response: list[dict[str, object]] | Exception = [{"incompatible": False}]

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection[dict[str, object]]]:
        self.connection_depth += 1
        try:
            yield self.connection
        finally:
            self.connection_depth -= 1

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        self.transaction_depth += 1
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            raise
        finally:
            self.transaction_depth -= 1

    async def execute(self, query: sql.Composed | str, values: Any = None) -> Cursor:
        rendered = query if isinstance(query, str) else query.as_string()
        if "FROM braincore.lexical_projection_state" in rendered:
            self.projection_checks += 1
            if isinstance(self.projection_response, Exception):
                raise self.projection_response
            return Cursor(self.projection_response)
        if "AS incompatible" in rendered:
            self.coverage_calls.append((rendered, values, self.transaction_depth))
            self.events.append("coverage")
            if isinstance(self.coverage_response, Exception):
                raise self.coverage_response
            return Cursor(self.coverage_response)
        self.calls.append((rendered, values, self.transaction_depth))
        self.events.append("full_text" if "plainto_tsquery" in rendered else "database")
        if not self.responses:
            raise AssertionError("unexpected database call")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return Cursor(response)

    @property
    def connection(self) -> AsyncConnection[dict[str, object]]:
        return cast(AsyncConnection[dict[str, object]], self)


def principal() -> Principal:
    return Principal(uuid4(), uuid4(), frozenset({"read"}), {"user_id": "alice"})


def embedded() -> EmbeddingResult:
    return EmbeddingResult([1.0] + [0.0] * 383, "ok")


async def test_ranks_uuid_candidates_before_hydrating_only_the_requested_page() -> None:
    first, second, third = uuid4(), uuid4(), uuid4()
    conn = ScriptedConnection(
        [
            [{"id": first}, {"id": second}, {"id": third}],
            [{"id": second}, {"id": third}, {"id": first}],
            [{"id": first, "content": "Chosen memory", "metadata": {"kind": "fact"}}],
        ]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="chosen", limit=1, offset=1),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"] == [
        {
            "id": first,
            "content": "Chosen memory",
            "metadata": {"kind": "fact"},
            "score": pytest.approx(1 / 61 + 1 / 63),
        }
    ]
    for query, _, _ in conn.calls[:2]:
        assert "m.content" not in query
        assert "memory_evidence" not in query
    hydration_query, hydration_values, _ = conn.calls[2]
    assert "m.content" in hydration_query
    assert "unnest(%s::uuid[]) WITH ORDINALITY" in hydration_query
    assert "ORDER BY ranked.position" in hydration_query
    assert "LIMIT %s OFFSET %s" in hydration_query
    assert hydration_values[0] == [second, first, third]
    assert hydration_values[-2:] == [1, 1]
    assert all(isinstance(value, UUID) for value in hydration_values[0])
    assert set(outcome.timings_ms) == {
        "eligibility",
        "connection_wait",
        "coverage",
        "full_text",
        "embedding_wait",
        "vector",
        "fusion",
        "hydration",
        "total",
    }
    assert all(value >= 0 for value in outcome.timings_ms.values())
    assert outcome.timings_ms["total"] >= outcome.timings_ms["hydration"]
    assert not outcome.degraded
    assert not conn.responses


async def test_eligibility_is_parameterized_before_limit_and_rechecked_on_hydration() -> None:
    memory_id = uuid4()
    actor = principal()
    conn = ScriptedConnection([[{"id": memory_id}], [{"id": memory_id}], []])
    body = SearchRequest(
        query="source",
        scope=Scope(user_id="alice", project_id="project"),
        reference_namespace_prefix=["notes", "daily"],
        metadata_filter={"kind": "preference"},
        limit=1,
    )
    settings = Settings(search_vector_mode="exact")
    outcome = await search_memories(conn.connect, actor, body, embedded(), settings)
    assert outcome.payload["results"] == []  # Winner became ineligible before hydration.
    for query, values, _ in conn.calls:
        before_limit = query.split("LIMIT")[0]
        assert "m.tenant_id = %s" in before_limit
        assert 'm."user_id" = %s' in before_limit
        assert 'm."project_id" = %s' in before_limit
        assert "m.reference_namespace[1:cardinality(%s::text[])] = %s::text[]" in before_limit
        assert "m.metadata @> %s::jsonb" in before_limit
        assert "m.state = 'active'" in before_limit
        assert "m.valid_from <= statement_timestamp()" in before_limit
        assert "m.valid_until > statement_timestamp()" in before_limit
        assert actor.tenant_id in values
        assert "alice" in values
        assert "project" in values
    vector_query, vector_values, _ = conn.calls[1]
    assert "WITH eligible AS MATERIALIZED" in vector_query
    assert "m.embedding_state = 'ready'" in vector_query
    assert "m.embedding_model = %s" in vector_query
    assert "m.embedding_preprocessing_revision = %s" in vector_query
    assert settings.embedding_model in vector_values
    assert settings.embedding_preprocessing_revision in vector_values


async def test_scope_mismatch_is_rejected_before_any_database_read() -> None:
    conn = ScriptedConnection([])
    with pytest.raises(HTTPException) as error:
        await search_memories(
            conn.connect,
            principal(),
            SearchRequest(query="secret", scope=Scope(user_id="bob")),
            embedded(),
            Settings(search_vector_mode="exact"),
        )
    assert error.value.status_code == 403
    assert conn.calls == []


@pytest.mark.parametrize("include_inactive", [False, True])
async def test_listing_uses_authenticated_scope_and_keeps_public_shape(
    include_inactive: bool,
) -> None:
    memory = {"id": uuid4(), "content": "Listed memory", "score": 1.0}
    conn = ScriptedConnection([[memory]])
    actor = principal()
    outcome = await search_memories(
        conn.connect,
        actor,
        SearchRequest(limit=2, offset=4, include_inactive=include_inactive),
        None,
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"] == [memory]
    assert outcome.payload["streams"]["listing"]["state"] == "ok"
    assert outcome.payload["streams"]["vector"]["state"] == "disabled"
    query, values, _ = conn.calls[0]
    assert values == [actor.tenant_id, "alice", 2, 4]
    assert ("m.state = 'active'" in query) is not include_inactive
    assert ("m.valid_until > statement_timestamp()" in query) is not include_inactive
    assert not outcome.degraded


async def test_full_text_work_precedes_waiting_for_the_embedding() -> None:
    conn = ScriptedConnection([[], []])

    async def delayed_embedding() -> EmbeddingResult:
        assert conn.connection_depth == 0
        assert conn.transaction_depth == 0
        conn.events.append("embedding_awaited")
        return embedded()

    await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="overlap"),
        delayed_embedding(),
        Settings(search_vector_mode="exact"),
    )
    assert conn.events == ["full_text", "embedding_awaited", "coverage", "database"]


async def test_full_text_failure_is_contained_and_disclosed() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection(
        [Error("private database details"), [{"id": memory_id}], [{"id": memory_id}]]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["full_text"]["state"] == "failed"
    assert outcome.payload["streams"]["vector"]["state"] == "ok"
    assert len(outcome.payload["warnings"]) == 1
    assert "private" not in str(outcome.payload)
    assert conn.rollbacks == 1
    assert outcome.degraded


async def test_vector_failure_preserves_full_text_results_and_discloses_degradation() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection(
        [[{"id": memory_id}], Error("private database details"), [{"id": memory_id}]]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert outcome.payload["warnings"] == ["Vector search unavailable; this stream was skipped."]
    assert "private" not in str(outcome.payload)
    assert conn.rollbacks == 1
    assert outcome.degraded


async def test_failed_embedding_skips_vector_sql() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection([[{"id": memory_id}], [{"id": memory_id}]])
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        EmbeddingResult(None, "failed", "Embedder unavailable; vector search was skipped."),
        Settings(search_vector_mode="exact"),
    )
    assert all("<=>" not in query for query, _, _ in conn.calls)
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert outcome.degraded


async def test_iterative_settings_are_transaction_local_and_ordering_can_use_hnsw() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection(
        [[], [{"extversion": "0.8.5"}], [], [{"id": memory_id}], [{"id": memory_id}]]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(
            search_vector_mode="iterative_hnsw",
            search_hnsw_ef_search=80,
            search_hnsw_max_scan_tuples=12000,
            search_hnsw_scan_mem_multiplier=2,
        ),
    )
    settings_query, settings_values, depth = conn.calls[2]
    assert "set_config('hnsw.iterative_scan', 'strict_order', true)" in settings_query
    assert settings_query.count(", true)") == 4
    assert settings_values == ("80", "12000", "2.0")
    assert depth == 2
    vector_query, _, depth = conn.calls[3]
    assert "WITH nearest AS MATERIALIZED" in vector_query
    assert "ORDER BY m.embedding <=> %s::vector\n            LIMIT" in vector_query
    assert "m.tenant_id = %s" in vector_query.split("LIMIT")[0]
    assert "m.embedding_state = 'ready'" in vector_query.split("LIMIT")[0]
    assert depth == 2
    assert not outcome.degraded


@pytest.mark.parametrize("version", ["0.7.4", "unknown", None])
async def test_unsupported_iterative_scans_preserve_fts_without_exact_retry(
    version: str | None,
) -> None:
    memory_id = uuid4()
    version_rows = [{"extversion": version}] if version is not None else []
    conn = ScriptedConnection([[{"id": memory_id}], version_rows, [{"id": memory_id}]])
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="iterative_hnsw"),
    )
    assert not any("WITH eligible AS MATERIALIZED" in query for query, _, _ in conn.calls)
    assert not any("set_config" in query for query, _, _ in conn.calls)
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert outcome.payload["warnings"] == ["Vector search unavailable; this stream was skipped."]
    assert conn.rollbacks == 1
    assert outcome.degraded


async def test_ann_statement_timeout_is_not_retried_as_an_expensive_exact_scan() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection(
        [
            [{"id": memory_id}],
            [{"extversion": "0.8.5"}],
            [],
            QueryCanceled("timeout"),
            [{"id": memory_id}],
        ]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="iterative_hnsw"),
    )
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert not any("WITH eligible AS MATERIALIZED" in query for query, _, _ in conn.calls)
    assert outcome.degraded


async def test_iterative_setting_rejection_preserves_fts_without_exact_retry() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection(
        [
            [{"id": memory_id}],
            [{"extversion": "0.8.5"}],
            InvalidParameterValue("unsupported setting"),
            [{"id": memory_id}],
        ]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="iterative_hnsw"),
    )
    assert not any("WITH eligible AS MATERIALIZED" in query for query, _, _ in conn.calls)
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert conn.rollbacks == 1
    assert outcome.degraded


async def test_iterative_schema_errors_are_disclosed_without_repeating_the_query() -> None:
    conn = ScriptedConnection([[], [{"extversion": "0.8.5"}], [], UndefinedTable("missing")])
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="iterative_hnsw"),
    )
    assert outcome.payload["results"] == []
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert not any("WITH eligible AS MATERIALIZED" in query for query, _, _ in conn.calls)
    assert outcome.degraded


@pytest.mark.parametrize("changed", [False, True])
async def test_reference_content_corrections_clear_vectors_and_preserve_audits(
    changed: bool,
) -> None:
    memory_id = uuid4()
    actor = principal()
    existing = {
        "id": memory_id,
        "content": "Original fact",
        "object_type": "memory",
        "legacy_source_id": None,
        "legacy_attributes_ready": False,
        "project_id": None,
        "user_id": "alice",
        "agent_id": None,
        "run_id": None,
    }
    responses: list[list[dict[str, object]] | Exception] = [[{"id": uuid4()}], [], [existing]]
    if changed:
        responses.extend([[], [], []])  # Dedupe, prior revision, correction revision.
    responses.extend([[], [], [], [{"id": uuid4()}], [], []])
    conn = ScriptedConnection(responses)
    saved_id = await remember_one(
        conn.connection,
        actor,
        uuid4(),
        0,
        RememberRequest(
            content="Corrected fact" if changed else "Original fact",
            scope=Scope(user_id="alice"),
            reference=MemoryReference(namespace=["facts"], key="one"),
        ),
    )
    assert saved_id == memory_id
    update, parameters, _ = next(
        call for call in conn.calls if "UPDATE braincore.memories" in call[0]
    )
    assert "embedding=CASE WHEN %s THEN NULL ELSE embedding END" in update
    assert "embedding_model=CASE WHEN %s THEN NULL ELSE embedding_model END" in update
    assert "embedding_preprocessing_revision=CASE WHEN %s THEN NULL" in update
    assert parameters[-5:-2] == (changed, changed, changed)
    assert parameters[-2:] == (memory_id, actor.tenant_id)
    assert any("external reference update" in query for query, _, _ in conn.calls) is changed
    assert any("INSERT INTO braincore.memory_evidence" in query for query, _, _ in conn.calls)
    assert any("INSERT INTO braincore.audit_events" in query for query, _, _ in conn.calls)
    assert not conn.responses


@asynccontextmanager
async def _borrow_connection(
    conn: AsyncConnection[dict[str, object]],
) -> AsyncIterator[AsyncConnection[dict[str, object]]]:
    yield conn


@pytest.fixture
async def search_database() -> AsyncIterator[AsyncConnection[dict[str, object]]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("set BRAINCORE_E2E=1 with a disposable database for SQL integration tests")
    conn = await AsyncConnection.connect(
        Settings(search_vector_mode="exact").database_url.get_secret_value(),
        row_factory=dict_row,
        autocommit=True,
    )
    async with conn:
        yield conn


@pytest.fixture
async def typed_search_database(
    typed_source: dict[str, Any],
) -> AsyncIterator[AsyncConnection[dict[str, object]]]:
    # Each generic-plan corpus has its own index population. Shared test rows
    # and rolled-back HNSW entries must not define this controlled fixture.
    conn = await AsyncConnection.connect(typed_source["dsn"], row_factory=dict_row, autocommit=True)
    async with conn:
        yield conn


async def _search_fixture_principal(conn: AsyncConnection[dict[str, object]]) -> Principal:
    tenant = await (
        await conn.execute(
            "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id",
            (f"search-fixture-{uuid4()}",),
        )
    ).fetchone()
    assert tenant is not None
    return Principal(uuid4(), UUID(str(tenant["id"])), frozenset({"read"}), {"user_id": "alice"})


async def _seed_search_memory(
    conn: AsyncConnection[dict[str, object]],
    actor: Principal,
    settings: Settings,
    **overrides: Any,
) -> UUID:
    memory_id = uuid4()
    fields = {
        "tenant_id": actor.tenant_id,
        "user_id": "alice",
        "state": "active",
        "days_from_now": -1,
        "expired": False,
        "vector": [1.0] + [0.0] * 383,
        "embedding_state": "ready",
        "embedding_model": settings.embedding_model,
        "embedding_preprocessing_revision": settings.embedding_preprocessing_revision,
        "namespace": ["facts", "search"],
        "metadata": {"kind": "eligible"},
        "object_type": "memory",
        **overrides,
    }
    vector = f"[{','.join(str(value) for value in fields['vector'])}]"
    await conn.execute(
        """INSERT INTO braincore.memories
           (id,tenant_id,content,dedupe_key,user_id,scope,state,valid_from,valid_until,
            embedding,embedding_state,embedding_model,embedding_preprocessing_revision,
            reference_namespace,reference_key,metadata,object_type)
           VALUES (%s,%s,'synthetic vector fixture',%s,%s,jsonb_build_object('user_id',%s::text),
                   %s,now() + (%s * interval '1 day'),
                   CASE WHEN %s THEN now()-interval '1 second' ELSE NULL END,
                   %s::vector,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
        (
            memory_id,
            fields["tenant_id"],
            hashlib.sha256(memory_id.bytes).digest(),
            fields["user_id"],
            fields["user_id"],
            fields["state"],
            fields["days_from_now"],
            fields["expired"],
            vector,
            fields["embedding_state"],
            fields["embedding_model"],
            fields["embedding_preprocessing_revision"],
            fields["namespace"],
            str(memory_id),
            json.dumps(fields["metadata"]),
            fields["object_type"],
        ),
    )
    return memory_id


async def test_sql_modes_filter_ineligible_neighbors_before_limit(
    search_database: AsyncConnection[dict[str, object]],
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        other = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        winner = await _seed_search_memory(conn, actor, settings, vector=[0.8, 0.6] + [0.0] * 382)
        variants = [
            {"tenant_id": other.tenant_id},
            {"user_id": "bob"},
            {"state": "suppressed"},
            {"state": "retired"},
            {"expired": True},
            {"days_from_now": 1},
            {"embedding_state": "pending"},
            {"embedding_state": "failed"},
            {"embedding_model": "other-model"},
            {"embedding_preprocessing_revision": "other-revision"},
            {"embedding_model": None},
            {"namespace": ["other"]},
            {"metadata": {"kind": "excluded"}},
        ]
        for variant in variants:
            await _seed_search_memory(conn, actor, settings, **variant)
        body = SearchRequest(
            query="unmatchedquerytoken",
            scope=Scope(user_id="alice"),
            reference_namespace_prefix=["facts", "search"],
            metadata_filter={"kind": "eligible"},
            limit=1,
        )
        for mode in ("planner", "exact", "iterative_hnsw"):
            outcome = await search_memories(
                partial(_borrow_connection, conn),
                actor,
                body,
                embedded(),
                settings.model_copy(update={"search_vector_mode": mode}),
            )
            assert [row["id"] for row in outcome.payload["results"]] == [winner], mode
            assert "incompatible or unidentified" in outcome.payload["warnings"][0], mode
            assert outcome.degraded, mode


def _plan_nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    return [node, *(child for plan in node.get("Plans", []) for child in _plan_nodes(plan))]


async def test_sql_exact_path_is_structurally_exact_and_ann_query_can_use_hnsw(
    search_database: AsyncConnection[dict[str, object]],
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="iterative_hnsw")
        await _seed_search_memory(conn, actor, settings)
        body = SearchRequest(query="unmatchedquerytoken", limit=1)
        where, values = _search_filter(actor, body)
        vector_literal = f"[{','.join(str(value) for value in embedded().vector or [])}]"
        await _configure_iterative_search(conn, settings)
        # This is a plan eligibility check, not a natural-plan performance claim.
        await conn.execute("SET LOCAL enable_seqscan = off")
        await conn.execute("SET LOCAL enable_bitmapscan = off")
        await conn.execute("SET LOCAL enable_sort = off")
        for mode in ("exact", "iterative_hnsw", "planner"):
            query, parameters = _vector_query(where, values, vector_literal, 3, settings, mode=mode)
            explained = await (
                await conn.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + query, parameters)
            ).fetchone()
            assert explained is not None
            plan = cast(list[dict[str, Any]], explained["QUERY PLAN"])[0]["Plan"]
            nodes = _plan_nodes(plan)
            hnsw_indexes = {"memories_embedding_idx"} | {
                "memories_embedding_" + kind + "_idx"
                for kind in ("entity", "fact", "memory", "segment", "episode")
            }
            uses_hnsw = any(node.get("Index Name") in hnsw_indexes for node in nodes)
            assert uses_hnsw is (mode != "exact")
            if mode == "exact":
                assert any(node["Node Type"] == "CTE Scan" for node in nodes)


@pytest.mark.parametrize("ef_search", [None, 81])
async def test_sql_iterative_settings_reset_when_request_transaction_commits(
    search_database: AsyncConnection[dict[str, object]],
    ef_search: int | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BRAINCORE_SEARCH_HNSW_EF_SEARCH", raising=False)
    settings = (
        Settings(_env_file=None)
        if ef_search is None
        else Settings(_env_file=None, search_hnsw_ef_search=ef_search)
    )
    expected_ef_search = 1000 if ef_search is None else ef_search
    conn = search_database
    await conn.execute("SELECT '[1,0,0]'::vector")  # Load the extension's GUC definitions.
    settings_query = """SELECT current_setting('hnsw.iterative_scan') AS iterative_scan,
                               current_setting('hnsw.ef_search') AS ef_search,
                               current_setting('hnsw.max_scan_tuples') AS max_scan_tuples,
                               current_setting('hnsw.scan_mem_multiplier') AS scan_mem_multiplier"""
    baseline = await (await conn.execute(settings_query)).fetchone()
    async with conn.transaction():
        outcome = await search_memories(
            partial(_borrow_connection, conn),
            principal(),  # Nonexistent tenant: this request writes no fixture data.
            SearchRequest(query="unmatchedquerytoken"),
            embedded(),
            settings,
        )
        during = await (await conn.execute(settings_query)).fetchone()
        assert during is not None
        assert during["iterative_scan"] == "strict_order"
        assert during["ef_search"] == str(expected_ef_search)
        assert not outcome.degraded
    after = await (await conn.execute(settings_query)).fetchone()
    assert after == baseline


async def test_hydration_can_refill_from_bounded_ranked_reserves() -> None:
    first, second, third = uuid4(), uuid4(), uuid4()
    conn = ScriptedConnection(
        [
            [{"id": first}, {"id": second}, {"id": third}],
            [{"id": second, "content": "Reserve one"}, {"id": third, "content": "Reserve two"}],
        ]
    )
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact", limit=2),
        EmbeddingResult(None, "failed", "Vector stream unavailable."),
        Settings(search_vector_mode="exact"),
    )
    assert [row["id"] for row in outcome.payload["results"]] == [second, third]
    assert [row["score"] for row in outcome.payload["results"]] == [1 / 62, 1 / 63]
    query, values, _ = conn.calls[-1]
    assert values[0] == [first, second, third]
    assert values[-2:] == [2, 0]
    assert query.index("WHERE") < query.index("LIMIT %s OFFSET %s")


async def test_sql_hydration_refills_and_paginates_after_candidate_suppression(
    search_database: AsyncConnection[dict[str, object]], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        ids = sorted([await _seed_search_memory(conn, actor, settings) for _ in range(5)])
        candidate_ids = memory_service._candidate_ids

        async def suppress_after_candidates(
            connection: AsyncConnection[dict[str, object]],
            query: sql.Composed,
            values: list[object],
        ) -> list[UUID]:
            candidates = await candidate_ids(connection, query, values)
            await connection.execute(
                "UPDATE braincore.memories SET state='suppressed' WHERE tenant_id=%s AND id=%s",
                (actor.tenant_id, ids[1]),
            )
            return candidates

        monkeypatch.setattr(memory_service, "_candidate_ids", suppress_after_candidates)
        outcome = await search_memories(
            partial(_borrow_connection, conn),
            actor,
            SearchRequest(query="synthetic", limit=2, offset=1),
            EmbeddingResult(None, "failed", "Vector stream unavailable."),
            settings,
        )
        assert [row["id"] for row in outcome.payload["results"]] == ids[2:4]
        assert [row["score"] for row in outcome.payload["results"]] == [1 / 63, 1 / 64]


async def test_incompatible_coverage_is_disclosed_without_losing_compatible_results() -> None:
    memory_id = uuid4()
    actor = principal()
    conn = ScriptedConnection([[], [{"id": memory_id}], [{"id": memory_id}]])
    conn.coverage_response = [{"incompatible": True}]
    outcome = await search_memories(
        conn.connect,
        actor,
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "ok"
    assert "incompatible or unidentified" in outcome.payload["streams"]["vector"]["warning"]
    assert outcome.degraded
    assert outcome.timings_ms["coverage"] >= 0
    query, parameters, _ = conn.coverage_calls[0]
    assert query.count("UNION ALL") == 5
    assert parameters.count(actor.tenant_id) == 6
    assert parameters.count("alice") == 6
    for branch in query.split("UNION ALL"):
        assert "m.tenant_id = %s" in branch
        assert 'm."user_id" = %s' in branch
        assert "m.state = 'active'" in branch
        assert "m.valid_until > statement_timestamp()" in branch
        assert "m.embedding_state = 'ready'" in branch


async def test_failed_coverage_verification_skips_vector_without_claiming_complete_search() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection([[{"id": memory_id}], [{"id": memory_id}]])
    conn.coverage_response = UndefinedTable("private schema details")
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fact"),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.payload["results"][0]["id"] == memory_id
    assert outcome.payload["streams"]["vector"]["state"] == "failed"
    assert outcome.payload["warnings"] == [
        "Embedding coverage could not be verified; vector search was skipped."
    ]
    assert "private" not in str(outcome.payload)
    assert not any("<=>" in query for query, _, _ in conn.calls)
    assert outcome.degraded


async def test_sql_hydration_rechecks_expiry_at_the_current_statement_time(
    search_database: AsyncConnection[dict[str, object]], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        ids = sorted([await _seed_search_memory(conn, actor, settings) for _ in range(2)])
        candidate_ids = memory_service._candidate_ids

        async def expire_after_candidates(
            connection: AsyncConnection[dict[str, object]],
            query: sql.Composed,
            values: list[object],
        ) -> list[UUID]:
            candidates = await candidate_ids(connection, query, values)
            await connection.execute(
                """UPDATE braincore.memories SET valid_until=statement_timestamp()
                   WHERE tenant_id=%s AND id=%s""",
                (actor.tenant_id, ids[0]),
            )
            return candidates

        monkeypatch.setattr(memory_service, "_candidate_ids", expire_after_candidates)
        outcome = await search_memories(
            partial(_borrow_connection, conn),
            actor,
            SearchRequest(query="synthetic", limit=1),
            EmbeddingResult(None, "failed", "Vector stream unavailable."),
            settings,
        )
        assert [row["id"] for row in outcome.payload["results"]] == [ids[1]]


async def test_sql_healthy_coverage_uses_empty_index_ranges_without_scanning_the_corpus(
    search_database: AsyncConnection[dict[str, object]],
) -> None:
    conn = search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="exact")
        seed = await _seed_search_memory(conn, actor, settings)
        await conn.execute(
            """INSERT INTO braincore.memories
               (tenant_id,content,dedupe_key,user_id,scope,embedding,embedding_state,
                embedding_model,embedding_preprocessing_revision)
               SELECT tenant_id,'coverage index fixture',
                      sha256(convert_to('coverage-' || fixture::text,'UTF8')),
                      user_id,scope,embedding,embedding_state,
                      embedding_model,embedding_preprocessing_revision
               FROM braincore.memories CROSS JOIN generate_series(1,2048) fixture
               WHERE id=%s""",
            (seed,),
        )
        await conn.execute("ANALYZE braincore.memories")
        where, values = _search_filter(actor, SearchRequest(query="fixture"))
        query, parameters = _incompatible_vector_query(where, values, settings)
        covered = await (await conn.execute(query, parameters)).fetchone()
        assert covered == {"incompatible": False}
        explained = await (
            await conn.execute(
                sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + query, parameters
            )
        ).fetchone()
        assert explained is not None
        plan = cast(list[dict[str, Any]], explained["QUERY PLAN"])[0]["Plan"]
        nodes = _plan_nodes(plan)
        assert not any(node["Node Type"] == "Seq Scan" for node in nodes)
        index_nodes = [node for node in nodes if node.get("Index Name")]
        assert len(index_nodes) == 6
        for node in index_nodes:
            assert node["Index Name"] == "memories_embedding_identity_idx"
            assert node["Actual Rows"] == 0
            assert node.get("Rows Removed by Filter", 0) == 0


@pytest.mark.parametrize("configured", [None, "planner", "iterative_hnsw"])
async def test_shipped_default_and_planner_alias_use_filtered_index_order(
    configured: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BRAINCORE_SEARCH_VECTOR_MODE", raising=False)
    monkeypatch.delenv("BRAINCORE_SEARCH_HNSW_EF_SEARCH", raising=False)
    settings = (
        Settings(_env_file=None)
        if configured is None
        else Settings(_env_file=None, search_vector_mode=configured)
    )
    assert settings.search_vector_mode == "iterative_hnsw"
    assert settings.search_hnsw_ef_search == 1000
    memory_id = uuid4()
    conn = ScriptedConnection(
        [[], [{"extversion": "0.8.1"}], [], [{"id": memory_id}], [{"id": memory_id}]]
    )
    actor = principal()
    body = SearchRequest(query="fact", object_type="fact", legacy_scope_prefix="notes%", limit=1)
    outcome = await search_memories(conn.connect, actor, body, embedded(), settings)
    settings_query, settings_values, _ = conn.calls[2]
    assert "set_config('hnsw.ef_search', %s, true)" in settings_query
    assert settings_values[0] == "1000"
    query, values, _ = conn.calls[3]
    inner, outer = query.split("SELECT id FROM nearest")
    assert (
        "ORDER BY (CASE WHEN m.object_type = 'fact' THEN m.embedding ELSE NULL END)"
        "::vector(384) <=> %s::vector\n            LIMIT %s"
    ) in inner
    assert "AND m.object_type = 'fact'" in inner
    assert "updated_at DESC" not in inner
    assert "ORDER BY distance, updated_at DESC, id" in outer
    assert "m.tenant_id = %s" in inner
    assert 'm."user_id" = %s' in inner
    assert "m.object_type = %s" in inner
    assert "m.legacy_scope_path LIKE %s" in inner
    assert actor.tenant_id in values
    assert "notes\\%%" in values
    assert outcome.payload["results"][0]["id"] == memory_id
    assert not outcome.degraded


async def test_failed_stage_observer_survives_cancellation_without_query_data() -> None:
    stages = []
    conn = ScriptedConnection([])

    async def blocked_candidate_ids(*args: object) -> list[UUID]:
        raise asyncio.CancelledError

    original = memory_service._candidate_ids
    memory_service._candidate_ids = blocked_candidate_ids
    try:
        with pytest.raises(asyncio.CancelledError):
            await search_memories(
                conn.connect,
                principal(),
                SearchRequest(query="private query"),
                embedded(),
                Settings(search_vector_mode="exact"),
                stage_observer=stages.append,
            )
    finally:
        memory_service._candidate_ids = original
    assert stages == ["eligibility", "connection_wait", "full_text"]
    assert "private" not in str(stages)


async def test_readiness_capability_requires_iterative_support_unless_exact() -> None:
    conn = ScriptedConnection([[{"extversion": "0.7.4"}]])
    with pytest.raises(Error):
        await check_search_capability(
            conn.connection, Settings(search_vector_mode="iterative_hnsw")
        )
    conn = ScriptedConnection([])
    await check_search_capability(conn.connection, Settings(search_vector_mode="exact"))
    assert not conn.calls


async def test_readiness_rejects_unsupported_normal_search_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection([[], [{"extversion": "0.7.4"}]])
    monkeypatch.setattr(memory_api, "connection", conn.connect)
    monkeypatch.setattr(memory_api, "get_settings", lambda: Settings(search_vector_mode="planner"))
    with pytest.raises(HTTPException) as error:
        await memory_api.readiness(principal())
    assert error.value.status_code == 503
    assert error.value.detail == "iterative_vector_search_unavailable"


async def test_aborted_search_logs_stage_and_correlation_without_private_text(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fake_embedding(*args: object, **kwargs: object) -> EmbeddingResult:
        return embedded()

    async def cancelled_search(*args: object, **kwargs: Any) -> Any:
        kwargs["stage_observer"]("full_text")
        raise asyncio.CancelledError

    monkeypatch.setattr(memory_api, "embed_one", fake_embedding)
    monkeypatch.setattr(memory_service, "search_memories", cancelled_search)
    monkeypatch.setattr(memory_api, "get_settings", lambda: Settings(search_vector_mode="planner"))
    request = Request({"type": "http"})
    request.state.request_id = "req_test_cancellation"
    with caplog.at_level(logging.WARNING), pytest.raises(asyncio.CancelledError):
        await memory_api.search(SearchRequest(query="private search text"), request, principal())
    event = json.loads(caplog.records[-1].message)
    assert event["event"] == "braincore_search_aborted"
    assert event["request_id"] == "req_test_cancellation"
    assert event["stage"] == "full_text"
    assert event["stage_elapsed_ms"] >= 0
    assert event["vector_mode"] == "iterative_hnsw"
    assert "private search text" not in caplog.text


async def test_unready_lexical_projection_is_disclosed_without_canonical_retry() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection([[{"id": memory_id}], [{"id": memory_id}]])
    conn.projection_response = [{"ready": False}]
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fixture", limit=1),
        embedded(),
        Settings(search_vector_mode="exact"),
    )
    assert outcome.degraded
    assert outcome.payload["streams"]["full_text"]["state"] == "failed"
    assert outcome.payload["streams"]["vector"]["state"] == "ok"
    assert conn.projection_checks == 1
    assert not any("plainto_tsquery" in query for query, _, _ in conn.calls)
    assert outcome.payload["results"][0]["id"] == memory_id


async def test_canonical_lexical_mode_is_explicit_and_skips_projection_gate() -> None:
    memory_id = uuid4()
    conn = ScriptedConnection([[{"id": memory_id}], [], [{"id": memory_id}]])
    conn.projection_response = UndefinedTable("missing projection")
    outcome = await search_memories(
        conn.connect,
        principal(),
        SearchRequest(query="fixture", limit=1),
        embedded(),
        Settings(search_vector_mode="exact", search_full_text_mode="canonical"),
    )
    assert not outcome.degraded
    assert conn.projection_checks == 0
    assert 'braincore."memories" m' in conn.calls[0][0]


@pytest.mark.parametrize("ready", [False, None])
async def test_readiness_rejects_incomplete_full_text_projection(
    ready: bool | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection([[]])
    conn.projection_response = [] if ready is None else [{"ready": ready}]
    monkeypatch.setattr(memory_api, "connection", conn.connect)
    monkeypatch.setattr(memory_api, "get_settings", lambda: Settings(search_vector_mode="exact"))
    with pytest.raises(HTTPException) as error:
        await memory_api.readiness(principal())
    assert error.value.status_code == 503
    assert error.value.detail == "full_text_projection_unavailable"


def test_projection_query_preserves_all_filters_and_exact_rank_before_limit() -> None:
    actor = principal()
    body = SearchRequest(
        query="fixture",
        object_type="fact",
        legacy_scope_prefix="notes_%",
        metadata_filter={"kind": "value"},
        reference_namespace_prefix=["root"],
    )
    where, values = memory_service._search_filter(actor, body)
    projected, projected_values = memory_service._full_text_query(
        where, values, body.query, 30, Settings()
    )
    canonical, canonical_values = memory_service._full_text_query(
        where,
        values,
        body.query,
        30,
        Settings(search_full_text_mode="canonical"),
    )
    text = projected.as_string()
    assert 'braincore."memories_lexical" m' in text
    assert text.replace('"memories_lexical"', '"memories"') == canonical.as_string()
    assert projected_values == canonical_values
    assert (
        text.index("m.legacy_scope_path LIKE")
        < text.index("ORDER BY ts_rank_cd")
        < text.index("LIMIT %s")
    )
    assert "m.metadata @>" in text and "m.valid_until" in text
    assert "m.legacy_fts_eligible" in text
    assert "m.embedding" not in text and "m.content" not in text


@pytest.mark.parametrize("kind", [None, "entity", "segment"])
async def test_sql_native_typed_lanes_generic_plan_preserves_merge_and_identity_filters(
    typed_search_database: AsyncConnection[dict[str, object]], kind: str | None
) -> None:
    conn = typed_search_database
    async with conn.transaction(force_rollback=True):
        actor = await _search_fixture_principal(conn)
        settings = Settings(search_vector_mode="iterative_hnsw")
        other = await _search_fixture_principal(conn)
        for i, object_type in enumerate(("entity", "fact", "memory", "segment", "episode")):
            for j in range(2):
                angle = 0.1 + i * 0.1 + j * 0.02
                await _seed_search_memory(
                    conn,
                    actor,
                    settings,
                    object_type=object_type,
                    vector=[math.cos(angle), math.sin(angle)] + [0.0] * 382,
                )
            await _seed_search_memory(conn, other, settings, object_type=object_type)
            for fields in (
                {"embedding_model": "different-model"},
                {"embedding_preprocessing_revision": "different-revision"},
                {"embedding_state": "pending"},
                {"state": "suppressed"},
                {"user_id": "different-user"},
                {"expired": True},
            ):
                await _seed_search_memory(conn, actor, settings, object_type=object_type, **fields)
        body = SearchRequest(query="unmatchedquerytoken", object_type=kind, limit=4)
        where, values = _search_filter(actor, body)
        vector_literal = "[" + ",".join(str(x) for x in embedded().vector or []) + "]"
        exact, exact_values = _vector_query(
            where, values, vector_literal, 4, settings, mode="exact", object_type=kind
        )
        expected = [row["id"] for row in await (await conn.execute(exact, exact_values)).fetchall()]
        await _configure_iterative_search(conn, settings)
        await conn.execute("SET LOCAL plan_cache_mode=force_generic_plan")
        await conn.execute("SET LOCAL enable_seqscan=off")
        await conn.execute("SET LOCAL enable_bitmapscan=off")
        await conn.execute("SET LOCAL enable_sort=off")
        await conn.execute("ANALYZE braincore.memories")  # Disposable fixture only.
        statement, parameters = _vector_query(
            where, values, vector_literal, 4, settings, mode="iterative_hnsw", object_type=kind
        )
        parts = statement.as_string().split("%s")
        prepared = "".join(
            part + (f"${i + 1}" if i < len(parts) - 1 else "") for i, part in enumerate(parts)
        )
        name = sql.Identifier("native_typed_" + uuid4().hex)
        await conn.execute(sql.SQL("PREPARE {} AS {}").format(name, sql.SQL(prepared)))
        try:
            execution = sql.SQL("EXECUTE {} ({})").format(
                name, sql.SQL(",").join(sql.Literal(value) for value in parameters)
            )
            explained = await (
                await conn.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + execution)
            ).fetchone()
            assert explained is not None
            nodes = _plan_nodes(cast(list[dict[str, Any]], explained["QUERY PLAN"])[0]["Plan"])
            indexes = {node["Index Name"] for node in nodes if "Index Name" in node}
            if kind is None:
                assert "memories_embedding_idx" in indexes
                assert "CASE WHEN" not in statement.as_string()
            else:
                assert "memories_embedding_" + kind + "_idx" in indexes
                assert "memories_embedding_idx" not in indexes
                assert "::vector(384) <=>" in statement.as_string()
            actual = [row["id"] for row in await (await conn.execute(execution)).fetchall()]
            assert actual == expected and len(actual) == (4 if kind is None else 2)
        finally:
            await conn.execute(sql.SQL("DEALLOCATE {}").format(name))


def test_native_vector_type_literal_refuses_untrusted_type() -> None:
    with pytest.raises(ValueError, match="Unknown vector object type"):
        _vector_query(
            sql.SQL("true"),
            [],
            "[1]",
            3,
            Settings(),
            mode="iterative_hnsw",
            object_type="segment' OR true --",
        )

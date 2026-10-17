import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest
from fastapi import HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from braincore_memory.auth import Principal, key_digest
from braincore_memory.config import get_settings
from braincore_memory.legacy_clients import mutate_legacy_object
from braincore_memory.legacy_lexical_projection import backfill_projection
from braincore_memory.legacy_writes import mutate_control
from braincore_memory.lexical_projection import backfill_projection as backfill_native
from braincore_memory.maintenance import (
    _STALE_KEYS,
    StaleDemotionRequest,
    demote_stale_page,
    maintenance_status,
)
from braincore_memory.migrate import bootstrap_admin

from .conftest import auth_headers


@pytest.fixture
def typed_source() -> Iterator[dict[str, Any]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires a disposable sibling database")
    dsn = get_settings().database_url.get_secret_value()
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        assert conn.info.dbname == "braincore_test" or conn.info.dbname.startswith(
            "braincore_maintenance_test_"
        )
        # The standard E2E database or explicitly owned UUID sibling is disposable.
        conn.execute("TRUNCATE braincore.tenants CASCADE")
        bootstrap_admin()
        conn.execute("INSERT INTO braincore.tenants(name) VALUES('alpha'),('beta')")
        principal = _principal(conn)
        _seed(conn, principal, kind="fact", scope="project/initial", identity=UUID(int=2**126 + 1))
        _seed(conn, principal, kind="fact", scope="project/initial", identity=UUID(int=2**126 + 2))
        beta = conn.execute("SELECT id FROM braincore.tenants WHERE name='beta'").fetchone()["id"]
        _seed(
            conn,
            Principal(principal.key_id, beta, principal.roles),
            kind="fact",
            scope="project/initial",
        )
        backfill_native(dsn, batch_size=2)
        yield {"target": conn, "dsn": dsn}


def _principal(conn: psycopg.Connection[Any], scope: dict[str, str] | None = None) -> Principal:
    tenant = conn.execute(
        "SELECT id AS tenant_id FROM braincore.tenants WHERE name='alpha'"
    ).fetchone()["tenant_id"]
    key = conn.execute(
        "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope) "
        "VALUES(%s,%s,%s,%s,ARRAY['read','write','admin'],%s) RETURNING id",
        (
            tenant,
            uuid4().hex,
            uuid4().hex,
            key_digest(uuid4().hex),
            None,
        ),
    ).fetchone()["id"]
    return Principal(key, tenant, frozenset({"admin", "read", "write"}), scope)


def _prepare(fixture: dict[str, Any]) -> tuple[psycopg.Connection[Any], Principal]:
    conn = fixture["target"]
    backfill_projection(fixture["dsn"], batch_size=2)
    return conn, _principal(conn)


def _seed(
    conn: psycopg.Connection[Any],
    principal: Principal,
    *,
    kind: str = "memory",
    identity: UUID | None = None,
    scope: str = "project/nightly",
    attrs: dict[str, Any] | None = None,
) -> UUID:
    identity = identity or uuid4()
    conn.execute(
        """INSERT INTO braincore.memories(id,tenant_id,object_type,content,dedupe_key,
        project_id,scope,legacy_attributes,legacy_attributes_ready,legacy_content_hash)
        VALUES(%s,%s,%s,'Unchanged content',%s,%s,%s,%s,true,
               public.digest('Unchanged content','sha256'))""",
        (
            identity,
            principal.tenant_id,
            kind,
            hashlib.sha256(identity.bytes).digest(),
            scope,
            Jsonb({"project_id": scope}),
            Jsonb(
                {
                    "title": "Unchanged",
                    "narrative": "content",
                    "lifecycle_state": "published",
                    "governance_status": "active",
                    "last_supported_at": "2000-01-01T00:00:00Z",
                    **(attrs or {}),
                }
            ),
        ),
    )
    return identity


def _body(conn: psycopg.Connection[Any], **fields: Any) -> StaleDemotionRequest:
    return StaleDemotionRequest(
        run_id=fields.pop("run_id", uuid4()),
        destination_database=conn.info.dbname,
        **fields,
    )


def test_status_checks_native_schema_manifest_and_primary_tenant(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    result = maintenance_status(conn, principal, include_stats=True)
    assert result["schema_ready"], result["issues"]
    assert result["database"] == conn.info.dbname
    assert result["tenant"] == "alpha"
    assert result["object_counts"]["fact"] == 2  # beta source fact is excluded
    assert result["embedding_identity"]["model"] == get_settings().embedding_model
    # Deliberately corrupt this fixture only; schema changes must not leak.
    with conn.transaction(force_rollback=True):
        # A named relation is insufficient: removing its unique contract fails readiness.
        conn.execute(
            "ALTER TABLE braincore.legacy_client_artifact_ids "
            "DROP CONSTRAINT legacy_client_artifact_ids_tenant_id_source_key_key"
        )
        result = maintenance_status(conn, principal)
        assert not result["schema_ready"]
        assert "unique:legacy_client_artifact_ids:tenant_id,source_key" in result["issues"]
        conn.execute(
            "UPDATE braincore.schema_migrations SET checksum='modified' "
            "WHERE version=(SELECT min(version) FROM braincore.schema_migrations)"
        )
        assert any(
            x.startswith("migration:") for x in maintenance_status(conn, principal)["issues"]
        )
    assert maintenance_status(conn, principal)["schema_ready"]


def test_stale_page_atomic_retry_boundary_and_vector_preservation(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    run_id = uuid4()
    _seed(
        conn, principal, identity=UUID(int=1), attrs={"last_supported_at": "2100-01-01T00:00:00Z"}
    )
    init = demote_stale_page(conn, principal, _body(conn, run_id=run_id, limit=1), "start")
    cutoff = datetime.fromisoformat(init["cutoff"])
    at = _seed(
        conn,
        principal,
        identity=UUID(int=2**128 - 4),
        attrs={"last_supported_at": cutoff.isoformat()},
    )
    before = _seed(
        conn,
        principal,
        identity=UUID(int=2**128 - 3),
        attrs={"last_supported_at": (cutoff - timedelta(microseconds=1)).isoformat()},
    )
    unknown = _seed(
        conn, principal, identity=UUID(int=2**128 - 2), attrs={"last_supported_at": None}
    )
    draft = _seed(
        conn, principal, identity=UUID(int=2**128 - 1), attrs={"lifecycle_state": "draft"}
    )
    conn.execute("UPDATE braincore.memories SET state='suppressed' WHERE id=%s", (draft,))
    vector = "[" + ",".join(["1"] + ["0"] * 383) + "]"
    conn.execute(
        "UPDATE braincore.memories SET embedding=%s::vector,embedding_state='ready' WHERE id=%s",
        (vector, before),
    )
    original = conn.execute(
        "SELECT content,embedding::text AS embedding FROM braincore.memories WHERE id=%s", (before,)
    ).fetchone()
    body = _body(conn, run_id=run_id, after_id=UUID(init["next_cursor"]))
    result = demote_stale_page(conn, principal, body, "boundary")
    assert result["cutoff"] == init["cutoff"]
    assert result["demoted"] == 1
    assert demote_stale_page(conn, principal, body, "boundary") == result
    assert demote_stale_page(conn, principal, _body(conn, run_id=run_id, limit=1), "start") == init
    row = conn.execute(
        "SELECT content,embedding::text AS embedding,state FROM braincore.memories WHERE id=%s",
        (before,),
    ).fetchone()
    assert row["state"] == "suppressed"
    assert row["content"] == original["content"] and row["embedding"] == original["embedding"]
    assert all(
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (i,)).fetchone()["state"]
        == "active"
        for i in (at, unknown)
    )
    assert (
        conn.execute(
            "SELECT count(*) AS n FROM braincore.audit_events "
            "WHERE target_id=%s AND action='legacy_client.upsert'",
            (before,),
        ).fetchone()["n"]
        == 1
    )
    with pytest.raises(ValueError, match="idempotency_conflict"):
        demote_stale_page(conn, principal, body.model_copy(update={"limit": 2}), "boundary")


def test_stale_milestone_current_override_and_scope(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    memory = _seed(conn, principal)
    fact = _seed(conn, principal, kind="fact", scope="project/denied", attrs={"priority": 1})
    mutate_control(
        conn,
        principal,
        object_type="memory_support",
        object_id=uuid4(),
        fields={"memory_id": str(memory), "fact_id": str(fact)},
        idempotency_key="support",
    )
    protected = demote_stale_page(conn, principal, _body(conn), "milestone")
    assert protected["demoted"] == 0
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (memory,)).fetchone()[
            "state"
        ]
        == "active"
    )
    # Current override, not old imported attributes, determines reinforcement.
    mutate_legacy_object(
        conn,
        principal,
        object_type="memory",
        object_id=memory,
        fields={"last_supported_at": datetime.now(UTC).isoformat()},
        idempotency_key="reinforce",
    )
    conn.execute("UPDATE braincore.legacy_client_controls SET deleted=true")
    assert demote_stale_page(conn, principal, _body(conn), "reinforced")["demoted"] == 0
    denied = _seed(conn, principal, scope="project/denied")
    scoped = _principal(conn, {"project_id": "project/nightly"})
    with pytest.raises(HTTPException, match="maintenance_unscoped_admin_required"):
        demote_stale_page(conn, scoped, _body(conn), "scoped")
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (denied,)).fetchone()[
            "state"
        ]
        == "active"
    )
    assert maintenance_status(conn, scoped, include_stats=True)["object_counts"] == {
        "memory": 1,
    }


def test_destination_and_auth_denial_have_no_effects(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    _seed(conn, principal)
    before = conn.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"]
    with pytest.raises(HTTPException, match="maintenance_destination_mismatch"):
        demote_stale_page(
            conn,
            principal,
            _body(conn).model_copy(update={"destination_database": "elsewhere"}),
            "wrong-destination",
        )
    reader = Principal(principal.key_id, principal.tenant_id, frozenset({"read"}))
    with pytest.raises(HTTPException, match="permission_denied"):
        demote_stale_page(conn, reader, _body(conn), "reader")
    with pytest.raises(HTTPException, match="permission_denied"):
        maintenance_status(conn, reader)
    assert (
        conn.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"] == before
    )
    assert (
        conn.execute("SELECT count(*) AS n FROM braincore.mutation_receipts").fetchone()["n"] == 0
    )


def test_unchanged_uuid_pages_progress_and_keep_fixed_cutoff(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    conn.execute(
        """INSERT INTO braincore.memories(id,tenant_id,object_type,content,dedupe_key,
        project_id,scope,legacy_attributes,legacy_attributes_ready)
        SELECT lpad(to_hex(i),32,'0')::uuid,%s,'memory','unchanged',
        public.digest(i::text,'sha256'),'project/nightly','{"project_id":"project/nightly"}',
        '{"last_supported_at":"2100-01-01T00:00:00Z"}',true
        FROM generate_series(1,2001) i""",
        (principal.tenant_id,),
    )
    final = _seed(conn, principal, identity=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"))
    body = _body(conn)
    pages = []
    while True:
        result = demote_stale_page(conn, principal, body, "page-" + str(len(pages)))
        pages.append(result)
        assert result["scanned"] <= 1000
        if result["exhausted"]:
            break
        body = body.model_copy(update={"after_id": UUID(result["next_cursor"])})
        assert len(pages) <= 4
    assert pages[0]["demoted"] == pages[1]["demoted"] == 0
    assert sum(x["demoted"] for x in pages) == 1
    assert sum(x["scanned"] for x in pages) == 2002  # mixed-type facts never enter the pages
    assert len({x["cutoff"] for x in pages}) == 1
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (final,)).fetchone()[
            "state"
        ]
        == "suppressed"
    )
    # Narrow physical key selection carries a plan Limit before subtype data.
    plan = conn.execute(
        "EXPLAIN (FORMAT JSON) " + _STALE_KEYS,
        (principal.tenant_id, None, None, None, None, 2),
    ).fetchone()["QUERY PLAN"][0]["Plan"]
    assert plan["Node Type"] == "Limit" and plan["Plan Rows"] <= 2
    with conn.transaction():
        conn.execute("SET LOCAL enable_seqscan=off")
        predicate_plan = conn.execute(
            "EXPLAIN (FORMAT JSON) SELECT id FROM braincore.memories "
            "WHERE tenant_id=%s AND object_type='memory'",
            (principal.tenant_id,),
        ).fetchone()["QUERY PLAN"][0]["Plan"]

    def plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get("Plans", []):
            yield from plan_nodes(child)

    indexed = [
        n
        for n in plan_nodes(predicate_plan)
        if n.get("Index Name") in {"memories_legacy_scope_idx", "memories_compat_project_scope_idx"}
    ]
    assert indexed
    assert "tenant_id =" in indexed[0]["Index Cond"]
    assert "object_type =" in indexed[0]["Index Cond"]


def test_late_invalid_subtype_rolls_back_page_and_run_receipt(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    good = _seed(conn, principal, identity=UUID(int=100))
    _seed(conn, principal, identity=UUID(int=200), attrs={"last_supported_at": "invalid timestamp"})
    before = conn.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"]
    with pytest.raises(psycopg.errors.InvalidDatetimeFormat):
        demote_stale_page(conn, principal, _body(conn), "invalid-later")
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (good,)).fetchone()[
            "state"
        ]
        == "active"
    )
    assert (
        conn.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"] == before
    )
    assert (
        conn.execute("SELECT count(*) AS n FROM braincore.mutation_receipts").fetchone()["n"] == 0
    )


@pytest.mark.asyncio
async def test_maintenance_http_auth_and_destination(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/admin/maintenance/status")
    assert response.status_code == 401
    response = await client.get("/v1/admin/maintenance/status", headers=auth_headers())
    assert response.status_code == 200
    with psycopg.connect(get_settings().database_url.get_secret_value()) as connection:
        assert response.json()["database"] == connection.info.dbname
    response = await client.post(
        "/v1/admin/maintenance/stale:demote",
        json={"run_id": str(uuid4()), "destination_database": "wrong"},
        headers=auth_headers(idempotency="wrong-maintenance-" + uuid4().hex),
    )
    assert response.status_code == 409
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row
    ) as conn:
        tenant = conn.execute("SELECT id FROM braincore.tenants WHERE name='default'").fetchone()[
            "id"
        ]
        raw = "bck_read_" + uuid4().hex
        conn.execute(
            "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope) "
            "VALUES(%s,'readonly-maintenance',%s,%s,ARRAY['read'],%s)",
            (tenant, raw, key_digest(raw), Jsonb({"user_id": "readonly"})),
        )
    assert (
        await client.get("/v1/admin/maintenance/status", headers=auth_headers(raw))
    ).status_code == 403
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        conn.execute(
            "UPDATE braincore.api_keys SET revoked_at=now() WHERE digest=%s", (key_digest(raw),)
        )
    assert (
        await client.get("/v1/admin/maintenance/status", headers=auth_headers(raw))
    ).status_code == 401


def test_parent_lookup_index_contract_and_strict_cursor(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    with conn.transaction():
        # The tiny fixture can otherwise prefer a heap scan. This proves that
        # the query's exact parent predicate is supported by the new index.
        conn.execute("SET LOCAL enable_seqscan=off")
        plan = conn.execute(
            "EXPLAIN (FORMAT JSON) SELECT control_id FROM braincore.legacy_client_control_refs "
            "WHERE field='memory_id' AND memory_id=%s",
            (uuid4(),),
        ).fetchone()["QUERY PLAN"][0]["Plan"]

    def nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    indexed = [
        x
        for x in nodes(plan)
        if x.get("Index Name") == "legacy_client_control_refs_memory_parent_idx"
    ]
    assert indexed and "memory_id =" in indexed[0]["Index Cond"]
    _seed(conn, principal, identity=UUID(int=1))
    _seed(conn, principal, identity=UUID(int=2))
    run_id = uuid4()
    first = demote_stale_page(conn, principal, _body(conn, run_id=run_id, limit=1), "cursor-first")
    with pytest.raises(HTTPException) as rejected:
        demote_stale_page(conn, principal, _body(conn, run_id=run_id, limit=1), "cursor-rewind")
    assert rejected.value.detail == "maintenance_cursor_conflict"
    second_body = _body(conn, run_id=run_id, after_id=UUID(first["next_cursor"]), limit=1)
    second = demote_stale_page(conn, principal, second_body, "cursor-second")
    assert (
        demote_stale_page(conn, principal, _body(conn, run_id=run_id, limit=1), "cursor-first")
        == first
    )
    with pytest.raises(HTTPException):
        demote_stale_page(conn, principal, second_body, "cursor-repeated-new-key")
    assert second["after_id"] == first["next_cursor"]
    with conn.transaction(force_rollback=True):
        conn.execute("DROP INDEX braincore.legacy_client_control_refs_memory_parent_idx")
        status = maintenance_status(conn, principal)
        assert "partial_index:legacy_client_control_refs_memory_parent_idx" in status["issues"]
    assert maintenance_status(conn, principal)["schema_ready"]


def test_incomplete_support_is_not_demoted(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    memory = _seed(conn, principal, identity=UUID(int=2))
    source, native = uuid4(), uuid4()
    conn.execute(
        "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) "
        "VALUES(%s,'fixture','{}')",
        (source,),
    )
    payload, key = Jsonb({"fixture": True}), Jsonb({"support_id": str(native)})
    conn.execute(
        "INSERT INTO braincore.legacy_record_versions"
        "(source_id,source_table,record_key,payload_hash,payload) "
        "VALUES(%s,'memory_support',%s,public.digest(%s::jsonb::text,'sha256'),%s)",
        (source, key, payload, payload),
    )
    conn.execute(
        "INSERT INTO braincore.legacy_memory_support(source_id,record_key,native_id,tenant_id,"
        "native_memory_id,imported_payload_hash) "
        "VALUES(%s,%s,%s,%s,%s,public.digest(%s::jsonb::text,'sha256'))",
        (source, key, native, principal.tenant_id, memory, payload),
    )
    with pytest.raises(HTTPException) as rejected:
        demote_stale_page(conn, principal, _body(conn), "incomplete-support")
    assert rejected.value.detail == "maintenance_support_not_ready"
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (memory,)).fetchone()[
            "state"
        ]
        == "active"
    )
    assert (
        conn.execute(
            "SELECT 1 FROM braincore.mutation_receipts WHERE key='incomplete-support'"
        ).fetchone()
        is None
    )


def _imported_support(
    conn: psycopg.Connection[Any],
    principal: Principal,
    memory: UUID,
    *,
    fact: UUID | None = None,
    episode: UUID | None = None,
) -> UUID:
    source, native = uuid4(), uuid4()
    conn.execute(
        "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) "
        "VALUES(%s,'fixture','{}')",
        (source,),
    )
    payload, key = Jsonb({"fixture": True}), Jsonb({"support_id": str(native)})
    conn.execute(
        "INSERT INTO braincore.legacy_record_versions"
        "(source_id,source_table,record_key,payload_hash,payload) "
        "VALUES(%s,'memory_support',%s,public.digest(%s::jsonb::text,'sha256'),%s)",
        (source, key, payload, payload),
    )
    conn.execute(
        "INSERT INTO braincore.legacy_memory_support(source_id,record_key,native_id,tenant_id,"
        "native_memory_id,native_fact_id,native_episode_id,attributes,imported_payload_hash) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,'{}',public.digest(%s::jsonb::text,'sha256'))",
        (source, key, native, principal.tenant_id, memory, fact, episode, payload),
    )
    return source


def test_valid_episode_only_support_does_not_count_as_fact_milestone(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    memory, episode = _seed(conn, principal), _seed(conn, principal, kind="episode")
    _imported_support(conn, principal, memory, episode=episode)
    result = demote_stale_page(conn, principal, _body(conn), "episode-only")
    assert result["demoted"] == 1
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (memory,)).fetchone()[
            "state"
        ]
        == "suppressed"
    )


def test_suppressed_published_and_unready_rows_remain_untouched(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    memory = _seed(conn, principal)
    conn.execute("UPDATE braincore.memories SET state='suppressed' WHERE id=%s", (memory,))
    result = demote_stale_page(conn, principal, _body(conn), "suppressed-published")
    assert result["demoted"] == 0
    assert (
        conn.execute(
            "SELECT legacy_attributes FROM braincore.memories WHERE id=%s", (memory,)
        ).fetchone()["legacy_attributes"]["lifecycle_state"]
        == "published"
    )
    assert (
        conn.execute(
            "SELECT 1 FROM braincore.audit_events WHERE target_id=%s "
            "AND action='legacy_client.upsert'",
            (memory,),
        ).fetchone()
        is None
    )


def test_unknown_active_imported_fact_blocks_stale_demotion(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    memory, fact = _seed(conn, principal), _seed(conn, principal, kind="fact")
    source = _imported_support(conn, principal, memory, fact=fact)
    conn.execute(
        "UPDATE braincore.memories SET legacy_source_id=%s,legacy_record_key=%s,"
        "legacy_external_id=%s,legacy_attributes_ready=false WHERE id=%s",
        (source, Jsonb({"fact_id": str(fact)}), fact, fact),
    )
    with pytest.raises(HTTPException) as rejected:
        demote_stale_page(conn, principal, _body(conn), "unknown-active-fact")
    assert rejected.value.detail == "maintenance_fact_subtype_not_ready"
    assert (
        conn.execute("SELECT state FROM braincore.memories WHERE id=%s", (memory,)).fetchone()[
            "state"
        ]
        == "active"
    )
    assert conn.execute("SELECT 1 FROM braincore.mutation_receipts").fetchone() is None
    # Inactive facts were absent from the old compatibility view and do not
    # block a valid stale memory merely because their subtype is unready.
    conn.execute("UPDATE braincore.memories SET state='retired' WHERE id=%s", (fact,))
    assert demote_stale_page(conn, principal, _body(conn), "inactive-fact")["demoted"] == 1


def test_alias_resolver_wrong_native_receipt_rolls_back_all_effects(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    collider = _seed(
        conn, principal, identity=UUID(int=100), attrs={"last_supported_at": "2100-01-01T00:00:00Z"}
    )
    selected = _seed(conn, principal, identity=UUID(int=200))
    # A retained alias can collide with a different row's native UUID. Exact
    # native hydration must be independent of whichever alias row fetchone sees.
    conn.execute(
        "INSERT INTO braincore.legacy_client_ids(memory_id,tenant_id,object_type,external_id) "
        "VALUES(%s,%s,'memory',%s)",
        (collider, principal.tenant_id, selected),
    )
    with conn.transaction():
        conn.execute("SET LOCAL enable_indexscan=off")
        conn.execute("SET LOCAL enable_bitmapscan=off")
        with pytest.raises(HTTPException) as rejected:
            demote_stale_page(conn, principal, _body(conn), "alias-collision")
        assert rejected.value.detail == "maintenance_alias_collision"
    for identity in (collider, selected):
        assert (
            conn.execute(
                "SELECT state FROM braincore.memories WHERE id=%s", (identity,)
            ).fetchone()["state"]
            == "active"
        )
    assert conn.execute("SELECT 1 FROM braincore.mutation_receipts").fetchone() is None
    assert conn.execute("SELECT 1 FROM braincore.audit_events").fetchone() is None
    assert conn.execute("SELECT 1 FROM braincore.legacy_client_attributes").fetchone() is None


def test_exact_optional_embedding_checks_cover_null_identity_blank_and_jobs(
    typed_source: dict[str, Any],
) -> None:
    conn, principal = _prepare(typed_source)
    settings = get_settings()
    vector = "[" + ",".join(["1"] + ["0"] * 383) + "]"
    conn.execute(
        "UPDATE braincore.memories SET embedding=%s::vector,embedding_state='ready',"
        "embedding_model=%s,embedding_preprocessing_revision=%s WHERE tenant_id=%s",
        (
            vector,
            settings.embedding_model,
            settings.embedding_preprocessing_revision,
            principal.tenant_id,
        ),
    )
    assert "embedding_checks" not in maintenance_status(conn, principal)
    assert maintenance_status(conn, principal, embedding_checks=True)["embedding_checks"] == {
        "eligible_refresh_exists": False,
        "active_embedding_jobs_exists": False,
    }
    memory = _seed(conn, principal)
    conn.execute(
        "UPDATE braincore.memories SET embedding_state='ready',embedding_model=%s,"
        "embedding_preprocessing_revision=%s WHERE id=%s",
        (settings.embedding_model, settings.embedding_preprocessing_revision, memory),
    )
    assert (
        maintenance_status(conn, principal, embedding_checks=True)["embedding_checks"][
            "eligible_refresh_exists"
        ]
        is True
    )  # ready state and compatible identity do not hide a NULL vector
    conn.execute(
        "UPDATE braincore.memories SET embedding=%s::vector,embedding_model='other' WHERE id=%s",
        (vector, memory),
    )
    assert (
        maintenance_status(conn, principal, embedding_checks=True)["embedding_checks"][
            "eligible_refresh_exists"
        ]
        is True
    )
    conn.execute(
        "UPDATE braincore.memories SET content='   ',embedding=NULL,embedding_state='pending' "
        "WHERE id=%s",
        (memory,),
    )
    assert (
        maintenance_status(conn, principal, embedding_checks=True)["embedding_checks"][
            "eligible_refresh_exists"
        ]
        is False
    )
    operation = conn.execute(
        "INSERT INTO braincore.operations(tenant_id,actor_key_id,kind) "
        "VALUES(%s,%s,'embedding_refresh') RETURNING id",
        (principal.tenant_id, principal.key_id),
    ).fetchone()["id"]
    item = conn.execute(
        "INSERT INTO braincore.operation_items(operation_id,tenant_id,item_index) "
        "VALUES(%s,%s,0) RETURNING id",
        (operation, principal.tenant_id),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO braincore.jobs(tenant_id,operation_id,operation_item_id,kind,payload,"
        "available_at) VALUES(%s,%s,%s,'embed_memory','{}',now()+interval '1 hour')",
        (principal.tenant_id, operation, item),
    )
    assert (
        maintenance_status(conn, principal, embedding_checks=True)["embedding_checks"][
            "active_embedding_jobs_exists"
        ]
        is True
    )  # queued delayed retry is still an active job


def test_embedding_checks_reject_scoped_completion_claim(typed_source: dict[str, Any]) -> None:
    conn, principal = _prepare(typed_source)
    scoped = Principal(
        principal.key_id, principal.tenant_id, principal.roles, {"project_id": "project/nightly"}
    )
    with pytest.raises(HTTPException) as denied:
        maintenance_status(conn, scoped, embedding_checks=True)
    assert denied.value.detail == "maintenance_unscoped_admin_required"

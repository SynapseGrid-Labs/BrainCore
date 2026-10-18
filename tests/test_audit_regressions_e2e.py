"""Authenticated write/read regressions; run only against disposable E2E fixtures."""

import hashlib
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from braincore_memory import AsyncBrainCore, BrainCore
from braincore_memory.auth import Principal, key_digest
from braincore_memory.config import get_settings
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.legacy_clients import configure_legacy_read
from braincore_memory.legacy_relationships import CORE_TYPES, auxiliary_relationships
from braincore_memory.legacy_search import configure_authenticated_readers

from .conftest import auth_headers
from .test_audit_regressions import LIVE_RELATIONSHIPS, fields_for
from .test_legacy_search import _credential


@pytest.fixture
async def writer(client: httpx.AsyncClient) -> tuple[str, Principal]:
    key = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=str(uuid4())),
        json={
            "name": "audit regression",
            "roles": ["read", "write"],
            "scope": {"user_id": "alice"},
        },
    )
    assert key.status_code == 201, key.text
    raw = key.json()["api_key"]
    key_id = UUID(key.json()["id"])
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row
    ) as conn:
        row = conn.execute(
            "SELECT tenant_id FROM braincore.api_keys WHERE id=%s", (key_id,)
        ).fetchone()
    return raw, Principal(
        key_id, row["tenant_id"], frozenset({"read", "write"}), {"user_id": "alice"}
    )


@pytest.fixture
def db(writer: tuple[str, Principal]) -> Iterator[Any]:
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as conn:
        assert (
            conn.execute("SELECT current_database() AS name").fetchone()["name"] == "braincore_test"
        )
        yield conn


def seed(conn: Any, actor: Principal, kind: str, failure: str | None = None) -> UUID:
    value = uuid4()
    if failure == "missing":
        return value
    tenant = actor.tenant_id
    if failure == "foreign":
        tenant = conn.execute(
            "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id",
            ("synthetic-" + uuid4().hex,),
        ).fetchone()["id"]
    scope = {"user_id": "bob" if failure == "scope" else "alice"}
    if failure == "type":
        kind = "episode" if kind != "episode" else "fact"
    if kind in CORE_TYPES:
        conn.execute(
            "INSERT INTO "
            "braincore.memories(id,tenant_id,content,dedupe_key,user_id,scope,object_type) "
            "VALUES(%s,%s,'Synthetic endpoint',%s,%s,%s,%s)",
            (
                value,
                tenant,
                hashlib.sha256(value.bytes).digest(),
                scope["user_id"],
                Jsonb(scope),
                kind,
            ),
        )
    elif kind == "artifact":
        conn.execute(
            "INSERT INTO "
            "braincore.artifacts(id,tenant_id,source_type,content,content_sha256,scope) "
            "VALUES(%s,%s,'statement','Synthetic endpoint',%s,%s)",
            (value, tenant, hashlib.sha256(value.bytes).digest(), Jsonb(scope)),
        )
    else:
        conn.execute(
            "INSERT INTO "
            "braincore.legacy_client_controls(id,tenant_id,object_type,external_id,scope,fields) "
            "VALUES(%s,%s,%s,%s,%s,'{}')",
            (value, tenant, kind, value, Jsonb(scope)),
        )
    return value


def valid_fields(conn: Any, actor: Principal, kind: str) -> dict[str, Any]:
    fields = fields_for(kind)
    for name, target, _ in auxiliary_relationships(kind, fields):
        fields[name] = str(seed(conn, actor, target))
    return fields


async def mutate(
    client: httpx.AsyncClient,
    key: str,
    kind: str,
    fields: dict[str, Any],
    *,
    object_id: str | None = None,
) -> httpx.Response:
    return await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(key, str(uuid4())),
        json={"object_type": kind, "object_id": object_id, "fields": fields},
    )


@pytest.mark.parametrize(("kind", "field", "target"), LIVE_RELATIONSHIPS)
@pytest.mark.parametrize("failure", ["missing", "foreign", "scope", "type"])
async def test_all_live_helper_endpoints_reject_invalid_targets_atomically(
    client: httpx.AsyncClient,
    writer: tuple[str, Principal],
    db: Any,
    kind: str,
    field: str,
    target: str,
    failure: str,
) -> None:
    key, actor = writer
    fields = valid_fields(db, actor, kind)
    fields[field] = str(seed(db, actor, target, failure))
    idempotency = str(uuid4())
    before = db.execute(
        "SELECT count(*) AS n FROM braincore.legacy_client_controls WHERE tenant_id=%s",
        (actor.tenant_id,),
    ).fetchone()["n"]
    response = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(key, idempotency),
        json={"object_type": kind, "fields": fields},
    )
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "control_reference_not_found"
    assert (
        db.execute(
            "SELECT count(*) AS n FROM braincore.legacy_client_controls WHERE tenant_id=%s",
            (actor.tenant_id,),
        ).fetchone()["n"]
        == before
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.mutation_receipts WHERE tenant_id=%s AND key=%s",
            (actor.tenant_id, idempotency),
        ).fetchone()
        is None
    )


@pytest.mark.parametrize("value", [-0.01, 1.01, 100, True, "0.5"])
async def test_invalid_confidence_rolls_back_without_receipt(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, value: Any
) -> None:
    key, actor = writer
    fields = {**fields_for("procedure"), "confidence": value, "procedure_fingerprint": uuid4().hex}
    idempotency = str(uuid4())
    response = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(key, idempotency),
        json={"object_type": "procedure", "fields": fields},
    )
    assert response.status_code == 409, response.text
    assert (
        db.execute(
            "SELECT 1 FROM braincore.legacy_client_controls WHERE tenant_id=%s AND "
            "fields->>'procedure_fingerprint'=%s",
            (actor.tenant_id, fields["procedure_fingerprint"]),
        ).fetchone()
        is None
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.mutation_receipts WHERE tenant_id=%s AND key=%s",
            (actor.tenant_id, idempotency),
        ).fetchone()
        is None
    )


async def test_valid_procedure_and_step_boundaries_hydrate_and_preserve_refs(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any
) -> None:
    key, actor = writer
    fact = seed(db, actor, "fact")
    created = await mutate(
        client,
        key,
        "procedure",
        {**fields_for("procedure"), "confidence": 0, "source_fact_id": str(fact)},
    )
    assert created.status_code == 200, created.text
    procedure = created.json()
    native = procedure["native_id"]
    ref = db.execute(
        "SELECT memory_id FROM braincore.legacy_client_control_refs WHERE "
        "control_id=%s AND field='source_fact_id'",
        (native,),
    ).fetchone()
    assert ref["memory_id"] == fact
    updated = await mutate(
        client,
        key,
        "procedure",
        {"confidence": 1, "summary": "Metadata update"},
        object_id=procedure["object_id"],
    )
    assert updated.status_code == 200, updated.text
    assert (
        db.execute(
            "SELECT memory_id FROM braincore.legacy_client_control_refs WHERE "
            "control_id=%s AND field='source_fact_id'",
            (native,),
        ).fetchone()
        == ref
    )
    step = await mutate(
        client,
        key,
        "procedure_step",
        {
            **fields_for("procedure_step"),
            "procedure_id": procedure["object_id"],
            "step_index": 2147483647,
        },
    )
    assert step.status_code == 200, step.text
    assert db.execute(
        "SELECT child_control_id FROM braincore.legacy_client_control_refs WHERE "
        "control_id=%s AND field='procedure_id'",
        (step.json()["native_id"],),
    ).fetchone()["child_control_id"] == UUID(native)
    with db.transaction():
        configure_legacy_read(db, actor)
        assert db.execute(
            "SELECT confidence,source_fact_id FROM preserve.procedure WHERE procedure_id=%s",
            (procedure["object_id"],),
        ).fetchone() == {"confidence": 1, "source_fact_id": fact}
        assert (
            db.execute(
                "SELECT step_index FROM preserve.procedure_step WHERE procedure_id=%s",
                (procedure["object_id"],),
            ).fetchone()["step_index"]
            == 2147483647
        )
    bad = await mutate(
        client,
        key,
        "procedure_step",
        {
            **fields_for("procedure_step"),
            "procedure_id": procedure["object_id"],
            "step_index": 2147483648,
        },
    )
    assert bad.status_code == 409


@pytest.mark.parametrize("values", [None, [], "duplicates"])
async def test_uuid_arrays_hydrate_in_order_and_keep_positional_refs(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, values: Any
) -> None:
    key, actor = writer
    memory = seed(db, actor, "memory")
    values = [str(memory), str(memory)] if values == "duplicates" else values
    audit_id = str(uuid4())
    response = await mutate(
        client,
        key,
        "memory_context_audit",
        {"audit_id": audit_id, "retrieved_memory_ids": values, "injected_memory_ids": []},
    )
    assert response.status_code == 200, response.text
    with db.transaction():
        configure_legacy_read(db, actor)
        row = db.execute(
            "SELECT retrieved_memory_ids,injected_memory_ids FROM "
            "preserve.memory_context_audit WHERE audit_id=%s",
            (response.json()["object_id"],),
        ).fetchone()
    assert row == {
        "retrieved_memory_ids": [memory, memory] if values else values,
        "injected_memory_ids": [],
    }
    refs = db.execute(
        "SELECT field,memory_id FROM braincore.legacy_client_control_refs WHERE "
        "control_id=%s ORDER BY field",
        (response.json()["native_id"],),
    ).fetchall()
    assert len(refs) == (2 if values else 0)


async def test_revision_reference_convergence_retries_and_true_conflicts(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any
) -> None:
    key, actor = writer
    replacement = "Shared corrected content " + uuid4().hex
    ids = []
    for reference in ("ref-a", "ref-b", None, None):
        body: dict[str, Any] = {
            "content": "Prior " + uuid4().hex,
            "scope": actor.scope,
            "metadata": {"marker": "preserve"},
        }
        if reference:
            body["reference"] = {"namespace": ["audit", uuid4().hex], "key": reference}
        response = await client.post(
            "/v1/memories", headers=auth_headers(key, str(uuid4())), json=body
        )
        assert response.status_code == 202, response.text
        memory = db.execute(
            "SELECT memory_id FROM braincore.operation_items WHERE operation_id=%s",
            (response.json()["operation_id"],),
        ).fetchone()["memory_id"]
        ids.append(memory)
        changed = await client.post(
            f"/v1/memories/{memory}/revisions",
            headers=auth_headers(key, str(uuid4())),
            json={"content": replacement, "reason": "Correction"},
        )
        assert changed.status_code == (409 if len(ids) == 4 else 200), changed.text
        if reference:
            body["content"] = replacement
            retried = await client.post(
                "/v1/memories", headers=auth_headers(key, str(uuid4())), json=body
            )
            assert retried.status_code == 202, retried.text
            retry_id = db.execute(
                "SELECT memory_id FROM braincore.operation_items WHERE operation_id=%s",
                (retried.json()["operation_id"],),
            ).fetchone()["memory_id"]
            assert retry_id == memory
            stored = db.execute(
                "SELECT reference_namespace,reference_key,metadata FROM "
                "braincore.memories WHERE id=%s",
                (memory,),
            ).fetchone()
            assert stored == {
                "reference_namespace": body["reference"]["namespace"],
                "reference_key": reference,
                "metadata": {"marker": "preserve"},
            }


async def test_langgraph_root_search_returns_visible_langgraph_records_only(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any
) -> None:
    key, actor = writer
    for user, langgraph in (("alice", True), ("bob", True), ("alice", False)):
        value = uuid4().hex
        body: dict[str, Any] = {"content": value, "scope": {"user_id": user}}
        if langgraph:
            body.update(
                reference={"namespace": [user, "audit"], "key": value},
                metadata={"langgraph": {"value": {"content": value}}},
            )
        response = await client.post(
            "/v1/memories", headers=auth_headers(idempotency=str(uuid4())), json=body
        )
        assert response.status_code == 202, response.text
    sync = BrainCore(api_key=key)
    store = BrainCoreStore(client=sync, async_client=AsyncBrainCore(api_key=key, client=client))
    try:
        results = await store.asearch((), limit=100)
        assert results
        assert all(
            item.namespace[0] == "alice" and isinstance(item.value, dict) for item in results
        )
    finally:
        sync.close()


@pytest.mark.parametrize(
    ("kind", "field", "type_field"),
    [
        ("memory_edge", "source_id", "source_type"),
        ("memory_edge", "target_id", "target_type"),
        ("review_queue", "target_id", "target_type"),
        ("working_memory", "promotion_target_id", "promotion_target_kind"),
    ],
)
@pytest.mark.parametrize("failure", ["missing", "foreign", "scope", "type", "unknown"])
async def test_dynamic_helper_endpoints_reject_invalid_targets(
    client: httpx.AsyncClient,
    writer: tuple[str, Principal],
    db: Any,
    kind: str,
    field: str,
    type_field: str,
    failure: str,
) -> None:
    key, actor = writer
    fields = valid_fields(db, actor, kind)
    fields.update(
        {
            field: str(seed(db, actor, "memory", failure)),
            type_field: "unknown" if failure == "unknown" else "memory",
        }
    )
    response = await mutate(client, key, kind, fields)
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == (
        "unsupported polymorphic auxiliary endpoint"
        if failure == "unknown"
        else "control_reference_not_found"
    )


@pytest.mark.parametrize("failure", ["missing", "foreign", "scope", "type"])
async def test_uuid_array_endpoint_failures_are_atomic(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, failure: str
) -> None:
    key, actor = writer
    good, bad = seed(db, actor, "memory"), seed(db, actor, "memory", failure)
    audit_id = str(uuid4())
    response = await mutate(
        client,
        key,
        "memory_context_audit",
        {"audit_id": audit_id, "retrieved_memory_ids": [str(good), str(bad)]},
    )
    assert response.status_code == 409, response.text
    assert (
        db.execute(
            "SELECT 1 FROM braincore.legacy_client_controls WHERE fields->>'audit_id'=%s",
            (audit_id,),
        ).fetchone()
        is None
    )


@pytest.mark.parametrize("use_native", [False, True])
async def test_imported_helper_parent_keeps_display_key_and_constrained_reference(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, use_native: bool
) -> None:
    key, actor = writer
    source, native, external = uuid4(), uuid4(), uuid4()
    db.execute(
        "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) "
        "VALUES(%s,'synthetic',%s)",
        (source, Jsonb({"procedure": ["procedure_id"]})),
    )
    db.execute(
        "INSERT INTO "
        "braincore.legacy_auxiliary_objects(source_id,source_table,record_key,"
        "native_id,tenant_id,scope,payload) VALUES(%s,'procedure',%s,%s,%s,%s,'{}')",
        (
            source,
            Jsonb({"procedure_id": str(external)}),
            native,
            actor.tenant_id,
            Jsonb(actor.scope),
        ),
    )
    response = await mutate(
        client,
        key,
        "procedure_step",
        {**fields_for("procedure_step"), "procedure_id": str(native if use_native else external)},
    )
    assert response.status_code == 200, response.text
    row = db.execute(
        "SELECT n.fields,r.auxiliary_id FROM braincore.legacy_client_controls n JOIN "
        "braincore.legacy_client_control_refs r ON r.control_id=n.id WHERE n.id=%s AND "
        "r.field='procedure_id'",
        (response.json()["native_id"],),
    ).fetchone()
    assert row["fields"]["procedure_id"] == str(external)
    assert row["auxiliary_id"] == native


@pytest.mark.parametrize(
    ("kind", "original", "changes", "metadata"),
    [
        (
            "procedure",
            {"source_fact_id": None},
            {"source_fact_id": "foreign"},
            {"summary": "Updated summary"},
        ),
        (
            "memory_context_audit",
            {"retrieved_memory_ids": []},
            {"retrieved_memory_ids": ["foreign"]},
            {"actor": "Updated actor"},
        ),
        (
            "memory_edge",
            {"source_type": "memory"},
            {"source_type": "fact"},
            {"edge_type": "Updated edge"},
        ),
    ],
)
async def test_imported_relationship_changes_reject_and_metadata_remains_writable(
    client: httpx.AsyncClient,
    writer: tuple[str, Principal],
    db: Any,
    kind: str,
    original: dict[str, Any],
    changes: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    key, actor = writer
    source, native, external = uuid4(), uuid4(), uuid4()
    column = {
        "procedure": "procedure_id",
        "memory_context_audit": "audit_id",
        "memory_edge": "edge_id",
    }[kind]
    foreign = str(seed(db, actor, "fact", "foreign"))
    changes = {
        name: [foreign] if isinstance(value, list) else foreign if value == "foreign" else value
        for name, value in changes.items()
    }
    payload = {column: str(external), **original}
    db.execute(
        "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) "
        "VALUES(%s,'synthetic',%s)",
        (source, Jsonb({kind: [column]})),
    )
    db.execute(
        "INSERT INTO braincore.legacy_auxiliary_objects(source_id,source_table,record_key,"
        "native_id,tenant_id,scope,payload) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        (
            source,
            kind,
            Jsonb({column: str(external)}),
            native,
            actor.tenant_id,
            Jsonb(actor.scope),
            Jsonb(payload),
        ),
    )
    response = await mutate(client, key, kind, changes, object_id=str(external))
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "legacy_endpoint_change_requires_translation"
    assert (
        db.execute(
            "SELECT payload FROM braincore.legacy_auxiliary_objects WHERE native_id=%s", (native,)
        ).fetchone()["payload"]
        == payload
    )
    response = await mutate(client, key, kind, metadata, object_id=str(external))
    assert response.status_code == 200, response.text
    stored = db.execute(
        "SELECT payload FROM braincore.legacy_auxiliary_objects WHERE native_id=%s", (native,)
    ).fetchone()["payload"]
    assert all(stored[name] == value for name, value in payload.items())
    assert all(stored[name] == value for name, value in metadata.items())


async def create_quoted_evidence(
    client: httpx.AsyncClient, key: str, actor: Principal, db: Any, *, shared: bool = False
) -> tuple[UUID, UUID, UUID, dict[str, Any]]:
    fact, segment = (seed(db, actor, kind) for kind in ("fact", "segment"))
    artifact = seed(db, actor, "artifact", "foreign" if shared else None)
    owner = db.execute(
        "SELECT tenant_id FROM braincore.artifacts WHERE id=%s", (artifact,)
    ).fetchone()["tenant_id"]
    grant = None
    if shared:
        grant = db.execute(
            "INSERT INTO braincore.artifact_access_grants"
            "(artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason) "
            "VALUES(%s,%s,%s,%s,'Synthetic reviewed sharing') RETURNING id",
            (artifact, owner, actor.tenant_id, Jsonb(actor.scope)),
        ).fetchone()["id"]
    db.execute(
        "INSERT INTO braincore.memory_evidence(tenant_id,memory_id,artifact_id,"
        "artifact_tenant_id,access_grant_id,quote) VALUES(%s,%s,%s,%s,%s,'Original excerpt')",
        (actor.tenant_id, segment, artifact, owner, grant),
    )
    response = await mutate(
        client,
        key,
        "fact_evidence",
        {
            **fields_for("fact_evidence"),
            "fact_id": str(fact),
            "segment_id": str(segment),
            "excerpt": "First excerpt",
        },
    )
    assert response.status_code == 200, response.text
    return fact, segment, artifact, response.json()


@pytest.mark.parametrize("shared", [False, True])
async def test_evidence_quote_update_reuses_constrained_relationships(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, shared: bool
) -> None:
    key, actor = writer
    fact, segment, artifact, control = await create_quoted_evidence(
        client, key, actor, db, shared=shared
    )
    before = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
    ).fetchone()
    refs = db.execute(
        "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
        (control["native_id"],),
    ).fetchall()
    idempotency = str(uuid4())
    body = {
        "object_type": "fact_evidence",
        "object_id": control["object_id"],
        "fields": {"excerpt": "Corrected excerpt"},
        "reason": "Correct source quotation",
    }
    changed = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=body
    )
    assert changed.status_code == 200, changed.text
    replayed = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=body
    )
    assert replayed.status_code == 200 and replayed.json() == changed.json()
    after = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
    ).fetchone()
    assert after == {**before, "quote": "Corrected excerpt"}
    assert (
        db.execute(
            "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
            (control["native_id"],),
        ).fetchall()
        == refs
    )
    assert (
        db.execute(
            "SELECT quote FROM braincore.memory_evidence WHERE memory_id=%s", (segment,)
        ).fetchone()["quote"]
        == "Original excerpt"
    )
    with db.transaction():
        configure_legacy_read(db, actor)
        visible = db.execute(
            "SELECT excerpt FROM preserve.fact_evidence WHERE fact_evidence_id=%s",
            (control["object_id"],),
        ).fetchone()
        assert visible["excerpt"] == "Corrected excerpt"
    audits = db.execute(
        "SELECT details FROM braincore.audit_events WHERE target_id=%s "
        "AND action='legacy_client.fact_evidence.quote_corrected'",
        (control["native_id"],),
    ).fetchall()
    assert len(audits) == 1
    assert audits[0]["details"] == {
        "fact_id": str(fact),
        "segment_id": str(segment),
        "prior_fact_id": str(fact),
        "prior_segment_id": str(segment),
        "artifact_id": str(artifact),
        "artifact_tenant_id": str(before["artifact_tenant_id"]),
        "access_grant_id": str(before["access_grant_id"]) if before["access_grant_id"] else None,
        "prior_quote": "First excerpt",
        "source_quote": "Original excerpt",
        "next_quote": "Corrected excerpt",
        "prior_control_excerpt": "First excerpt",
        "reason": "Correct source quotation",
    }


@pytest.mark.parametrize(
    "failure", ["fact_scope", "segment_scope", "artifact_scope", "revoked_grant", "missing_ref"]
)
async def test_rejected_quote_corrections_preserve_fields_evidence_history_and_receipts(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, failure: str
) -> None:
    key, actor = writer
    fact, segment, artifact, control = await create_quoted_evidence(
        client, key, actor, db, shared=failure == "revoked_grant"
    )
    before = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
    ).fetchone()
    if failure in {"fact_scope", "segment_scope"}:
        db.execute(
            "UPDATE braincore.memories SET scope=%s,user_id='bob' WHERE id=%s",
            (Jsonb({"user_id": "bob"}), fact if failure == "fact_scope" else segment),
        )
    elif failure == "artifact_scope":
        db.execute(
            "UPDATE braincore.artifacts SET scope=%s WHERE id=%s",
            (Jsonb({"user_id": "bob"}), artifact),
        )
    elif failure == "revoked_grant":
        db.execute(
            "UPDATE braincore.artifact_access_grants SET revoked_at=now() WHERE id=%s",
            (before["access_grant_id"],),
        )
    else:
        db.execute(
            "DELETE FROM braincore.legacy_client_control_refs WHERE control_id=%s "
            "AND field='segment_id'",
            (control["native_id"],),
        )
    idempotency = str(uuid4())
    rejected = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(key, idempotency),
        json={
            "object_type": "fact_evidence",
            "object_id": control["object_id"],
            "fields": {"excerpt": "Unauthorized correction"},
        },
    )
    assert rejected.status_code == 409, rejected.text
    assert (
        db.execute("SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)).fetchone()
        == before
    )
    assert (
        db.execute(
            "SELECT fields->>'excerpt' AS excerpt FROM braincore.legacy_client_controls "
            "WHERE id=%s",
            (control["native_id"],),
        ).fetchone()["excerpt"]
        == "First excerpt"
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.mutation_receipts WHERE key=%s AND tenant_id=%s",
            (idempotency, actor.tenant_id),
        ).fetchone()
        is None
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.audit_events WHERE target_id=%s "
            "AND action='legacy_client.fact_evidence.quote_corrected'",
            (control["native_id"],),
        ).fetchone()
        is None
    )
    metadata = await mutate(
        client,
        key,
        "fact_evidence",
        {"source_relpath": "Updated metadata"},
        object_id=control["object_id"],
    )
    assert metadata.status_code == 200, metadata.text
    assert (
        db.execute(
            "SELECT quote FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
        ).fetchone()["quote"]
        == "First excerpt"
    )


@pytest.mark.parametrize("imported", [False, True])
async def test_context_audit_multi_reader_visibility_keeps_scope_and_revocation_checks(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, imported: bool
) -> None:
    key, actor = writer
    other, unrequested = [
        db.execute(
            "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id",
            ("synthetic-readers-" + uuid4().hex,),
        ).fetchone()["id"]
        for _ in range(2)
    ]
    second_key = _credential(db, other, roles=["read"], scope=actor.scope)
    mismatched_key = _credential(db, other, roles=["read"], scope={"user_id": "bob"})
    source = uuid4()
    if imported:
        db.execute(
            "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) "
            "VALUES(%s,'synthetic',%s)",
            (source, Jsonb({"memory_context_audit": ["audit_id"]})),
        )
    identities = []
    for tenant, user in (
        (actor.tenant_id, "alice"),
        (other, "alice"),
        (actor.tenant_id, "bob"),
        (other, "bob"),
        (unrequested, "alice"),
    ):
        audit_id = uuid4()
        identities.append(audit_id)
        fields = {"audit_id": str(audit_id), "retrieved_memory_ids": [], "injected_memory_ids": []}
        if imported:
            db.execute(
                "INSERT INTO braincore.legacy_auxiliary_objects"
                "(source_id,source_table,record_key,tenant_id,scope,payload) "
                "VALUES(%s,'memory_context_audit',%s,%s,%s,%s)",
                (
                    source,
                    Jsonb({"audit_id": str(audit_id)}),
                    tenant,
                    Jsonb({"user_id": user}),
                    Jsonb(fields),
                ),
            )
        else:
            db.execute(
                "INSERT INTO braincore.legacy_client_controls"
                "(tenant_id,object_type,external_id,scope,fields) "
                "VALUES(%s,'memory_context_audit',%s,%s,%s)",
                (tenant, audit_id, Jsonb({"user_id": user}), Jsonb(fields)),
            )

    def visible() -> set[UUID]:
        return {
            row["audit_id"]
            for row in db.execute(
                "SELECT audit_id FROM preserve.memory_context_audit WHERE audit_id=ANY(%s)",
                (identities,),
            )
        }

    with db.transaction():
        configure_authenticated_readers(db, [key, second_key])
        assert visible() == set(identities[:2])
        configure_legacy_read(db, actor)
        assert visible() == {identities[0]}
    with pytest.raises(ValueError, match="credential unavailable"), db.transaction():
        configure_authenticated_readers(db, [key, "bck_invalid_synthetic_credential"])
    with pytest.raises(ValueError, match="different reader scopes"), db.transaction():
        configure_authenticated_readers(db, [key, mismatched_key])
    db.execute(
        "UPDATE braincore.api_keys SET revoked_at=now() WHERE digest=%s", (key_digest(second_key),)
    )
    with pytest.raises(ValueError, match="credential unavailable"), db.transaction():
        configure_authenticated_readers(db, [key, second_key])
    with db.transaction():
        configure_authenticated_readers(db, [key])
        assert visible() == {identities[0]}


def attach_synthetic_source(
    db: Any, actor: Principal, segment: UUID, *, shared: bool = False, artifact: UUID | None = None
) -> tuple[UUID, UUID | None]:
    artifact = artifact or seed(db, actor, "artifact", "foreign" if shared else None)
    owner = db.execute(
        "SELECT tenant_id FROM braincore.artifacts WHERE id=%s", (artifact,)
    ).fetchone()["tenant_id"]
    grant = None
    if owner != actor.tenant_id:
        grant = db.execute(
            "INSERT INTO braincore.artifact_access_grants"
            "(artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason) "
            "VALUES(%s,%s,%s,%s,'Synthetic endpoint correction grant') RETURNING id",
            (artifact, owner, actor.tenant_id, Jsonb(actor.scope)),
        ).fetchone()["id"]
    db.execute(
        "INSERT INTO braincore.memory_evidence"
        "(tenant_id,memory_id,artifact_id,artifact_tenant_id,access_grant_id,quote) "
        "VALUES(%s,%s,%s,%s,%s,'Second original excerpt')",
        (actor.tenant_id, segment, artifact, owner, grant),
    )
    return artifact, grant


@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("change", ["segment", "fact", "both", "same_artifact"])
async def test_endpoint_and_quote_corrections_attach_evidence_and_preserve_history(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, shared: bool, change: str
) -> None:
    key, actor = writer
    old_fact, old_segment, old_artifact, control = await create_quoted_evidence(
        client, key, actor, db, shared=shared
    )
    before = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (old_fact,)
    ).fetchone()
    fact = seed(db, actor, "fact") if change in {"fact", "both"} else old_fact
    segment, artifact = old_segment, old_artifact
    source_quote = "Original excerpt"
    if change in {"segment", "both", "same_artifact"}:
        segment = seed(db, actor, "segment")
        artifact, _ = attach_synthetic_source(
            db,
            actor,
            segment,
            shared=shared,
            artifact=old_artifact if change == "same_artifact" else None,
        )
        source_quote = "Second original excerpt"
    body = {
        "object_type": "fact_evidence",
        "object_id": control["object_id"],
        "fields": {
            "fact_id": str(fact),
            "segment_id": str(segment),
            "excerpt": "New authorized excerpt",
        },
        "reason": "Correct evidence endpoints and quote",
    }
    idempotency = str(uuid4())
    response = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=body
    )
    assert response.status_code == 200, response.text
    replay = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=body
    )
    assert replay.status_code == 200 and replay.json() == response.json()
    current = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s AND artifact_id=%s",
        (fact, artifact),
    ).fetchone()
    assert current["quote"] == "New authorized excerpt"
    historical = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s AND artifact_id=%s",
        (old_fact, old_artifact),
    ).fetchone()
    assert (
        historical == {**before, "quote": "New authorized excerpt"}
        if change == "same_artifact"
        else historical == before
    )
    refs = db.execute(
        "SELECT field,memory_id FROM braincore.legacy_client_control_refs WHERE control_id=%s",
        (control["native_id"],),
    ).fetchall()
    assert {row["field"]: row["memory_id"] for row in refs} == {
        "fact_id": fact,
        "segment_id": segment,
    }
    assert (
        db.execute(
            "SELECT quote FROM braincore.memory_evidence WHERE memory_id=%s AND artifact_id=%s",
            (old_segment, old_artifact),
        ).fetchone()["quote"]
        == "Original excerpt"
    )
    assert (
        db.execute(
            "SELECT quote FROM braincore.memory_evidence WHERE memory_id=%s AND artifact_id=%s",
            (segment, artifact),
        ).fetchone()["quote"]
        == source_quote
    )
    audits = db.execute(
        "SELECT details FROM braincore.audit_events WHERE target_id=%s "
        "AND action='legacy_client.fact_evidence.quote_corrected'",
        (control["native_id"],),
    ).fetchall()
    assert len(audits) == 1
    detail = audits[0]["details"]
    assert detail["prior_fact_id"] == str(old_fact) and detail["fact_id"] == str(fact)
    assert detail["prior_segment_id"] == str(old_segment) and detail["segment_id"] == str(segment)
    assert detail["artifact_id"] == str(artifact)
    assert detail["prior_quote"] == ("First excerpt" if change == "same_artifact" else None)
    assert (
        detail["source_quote"] == source_quote and detail["next_quote"] == "New authorized excerpt"
    )
    assert detail["prior_control_excerpt"] == "First excerpt"
    assert detail["reason"] == body["reason"]
    with db.transaction():
        configure_legacy_read(db, actor)
        visible = db.execute(
            "SELECT fact_id,segment_id,excerpt FROM preserve.fact_evidence "
            "WHERE fact_evidence_id=%s",
            (control["object_id"],),
        ).fetchone()
    assert visible == {"fact_id": fact, "segment_id": segment, "excerpt": "New authorized excerpt"}


@pytest.mark.parametrize(
    "failure",
    ["missing", "foreign", "scope", "type", "revoked_grant", "artifact_scope", "fact_scope"],
)
async def test_invalid_endpoint_quote_combinations_roll_back_all_changes(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any, failure: str
) -> None:
    key, actor = writer
    fact, segment, artifact, control = await create_quoted_evidence(client, key, actor, db)
    before = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
    ).fetchall()
    prior_fields = db.execute(
        "SELECT fields FROM braincore.legacy_client_controls WHERE id=%s", (control["native_id"],)
    ).fetchone()["fields"]
    prior_refs = db.execute(
        "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
        (control["native_id"],),
    ).fetchall()
    new_fact = seed(db, actor, "fact", "scope") if failure == "fact_scope" else fact
    new_segment = seed(
        db,
        actor,
        "segment",
        failure if failure in {"missing", "foreign", "scope", "type"} else None,
    )
    if failure in {"revoked_grant", "artifact_scope", "fact_scope"}:
        new_artifact, grant = attach_synthetic_source(
            db, actor, new_segment, shared=failure == "revoked_grant"
        )
        if grant:
            db.execute(
                "UPDATE braincore.artifact_access_grants SET revoked_at=now() WHERE id=%s", (grant,)
            )
        elif failure == "artifact_scope":
            db.execute(
                "UPDATE braincore.artifacts SET scope=%s WHERE id=%s",
                (Jsonb({"user_id": "bob"}), new_artifact),
            )
    idempotency = str(uuid4())
    rejected = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(key, idempotency),
        json={
            "object_type": "fact_evidence",
            "object_id": control["object_id"],
            "fields": {
                "fact_id": str(new_fact),
                "segment_id": str(new_segment),
                "excerpt": "Rejected combined correction",
            },
        },
    )
    assert rejected.status_code == 409, rejected.text
    assert (
        db.execute("SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)).fetchall()
        == before
    )
    assert (
        db.execute(
            "SELECT fields FROM braincore.legacy_client_controls WHERE id=%s",
            (control["native_id"],),
        ).fetchone()["fields"]
        == prior_fields
    )
    assert (
        db.execute(
            "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
            (control["native_id"],),
        ).fetchall()
        == prior_refs
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.mutation_receipts WHERE key=%s AND tenant_id=%s",
            (idempotency, actor.tenant_id),
        ).fetchone()
        is None
    )
    assert (
        db.execute(
            "SELECT 1 FROM braincore.audit_events WHERE target_id=%s "
            "AND action='legacy_client.fact_evidence.quote_corrected'",
            (control["native_id"],),
        ).fetchone()
        is None
    )


async def test_control_excerpt_history_is_audited_when_canonical_quote_already_matches(
    client: httpx.AsyncClient, writer: tuple[str, Principal], db: Any
) -> None:
    key, actor = writer
    fact, first_segment, artifact, first_control = await create_quoted_evidence(
        client, key, actor, db
    )
    second_segment = seed(db, actor, "segment")
    attach_synthetic_source(db, actor, second_segment, artifact=artifact)
    created = await mutate(
        client,
        key,
        "fact_evidence",
        {
            **fields_for("fact_evidence"),
            "fact_id": str(fact),
            "segment_id": str(second_segment),
            "excerpt": "Distinct second control excerpt",
        },
    )
    assert created.status_code == 200, created.text
    second_control = created.json()
    before = db.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)
    ).fetchall()
    assert len(before) == 1 and before[0]["quote"] == "First excerpt"
    refs = db.execute(
        "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
        (second_control["native_id"],),
    ).fetchall()
    request = {
        "object_type": "fact_evidence",
        "object_id": second_control["object_id"],
        "fields": {"excerpt": "First excerpt"},
        "reason": "Correct distinct control excerpt",
    }
    idempotency = str(uuid4())
    corrected = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=request
    )
    assert corrected.status_code == 200, corrected.text
    replayed = await client.post(
        "/v1/legacy/objects:mutate", headers=auth_headers(key, idempotency), json=request
    )
    assert replayed.status_code == 200 and replayed.json() == corrected.json()
    assert (
        db.execute("SELECT * FROM braincore.memory_evidence WHERE memory_id=%s", (fact,)).fetchall()
        == before
    )
    assert (
        db.execute(
            "SELECT * FROM braincore.legacy_client_control_refs WHERE control_id=%s ORDER BY field",
            (second_control["native_id"],),
        ).fetchall()
        == refs
    )
    audits = db.execute(
        "SELECT details FROM braincore.audit_events WHERE target_id=%s "
        "AND action='legacy_client.fact_evidence.quote_corrected'",
        (second_control["native_id"],),
    ).fetchall()
    assert len(audits) == 1
    assert audits[0]["details"]["prior_control_excerpt"] == "Distinct second control excerpt"
    assert (
        audits[0]["details"]["prior_quote"] == audits[0]["details"]["next_quote"] == "First excerpt"
    )
    assert audits[0]["details"]["reason"] == request["reason"]
    with db.transaction():
        configure_legacy_read(db, actor)
        records = db.execute(
            "SELECT fact_evidence_id,excerpt FROM preserve.fact_evidence "
            "WHERE fact_evidence_id=ANY(%s)",
            ([UUID(first_control["object_id"]), UUID(second_control["object_id"])],),
        ).fetchall()
    assert len(records) == 2 and all(row["excerpt"] == "First excerpt" for row in records)
    originals = db.execute(
        "SELECT memory_id,quote FROM braincore.memory_evidence WHERE memory_id=ANY(%s)",
        ([first_segment, second_segment],),
    ).fetchall()
    assert {row["memory_id"]: row["quote"] for row in originals} == {
        first_segment: "Original excerpt",
        second_segment: "Second original excerpt",
    }

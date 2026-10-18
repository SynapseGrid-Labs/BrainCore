"""Opt-in disposable PostgreSQL and worker proof for legacy activation repair."""

import hashlib
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from psycopg.types.json import Jsonb

from braincore_memory.auth import Principal, key_digest
from braincore_memory.config import get_settings
from braincore_memory.db import connection
from braincore_memory.embedding import EmbeddingResult
from braincore_memory.legacy_clients import mutate_legacy_object
from braincore_memory.legacy_evidence import import_artifacts
from braincore_memory.models import SearchRequest
from braincore_memory.service import search_memories

from .conftest import auth_headers
from .test_e2e import run_worker_once
from .test_legacy_authority import principal
from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source


async def mutate(
    client: httpx.AsyncClient,
    body: dict[str, Any],
    *,
    key: str | None = None,
    credential: str | None = None,
) -> dict[str, Any]:
    headers = auth_headers(idempotency=key or f"restore-{uuid4()}")
    if credential is not None:
        headers["Authorization"] = f"Bearer {credential}"
    result = await client.post("/v1/legacy/objects:mutate", headers=headers, json=body)
    assert result.status_code == 200, result.text
    return result.json()


async def seed_episode(client: httpx.AsyncClient) -> tuple[dict[str, Any], str]:
    scope = f"example/restore/{uuid4()}"
    result = await mutate(
        client,
        {
            "object_type": "episode",
            "fields": {
                "title": "Synthetic activation",
                "summary": f"Original durable statement {uuid4()}",
                "scope_path": scope,
                "severity": "notice",
            },
        },
    )
    return result, scope


async def snapshot(memory_id: str) -> dict[str, Any]:
    queries = {
        "memory": "SELECT to_jsonb(m) AS row FROM braincore.memories m WHERE id=%s",
        "revisions": "SELECT to_jsonb(r) AS row FROM braincore.memory_revisions r "
        "WHERE memory_id=%s ORDER BY id",
        "lifecycle": "SELECT to_jsonb(e) AS row FROM braincore.lifecycle_events e "
        "WHERE memory_id=%s ORDER BY id",
        "audit": "SELECT to_jsonb(e) AS row FROM braincore.audit_events e "
        "WHERE target_id=%s ORDER BY id",
        "refs": "SELECT to_jsonb(r) AS row FROM braincore.legacy_subtype_refs r "
        "WHERE memory_id=%s ORDER BY field",
        "evidence": "SELECT to_jsonb(e) AS row FROM braincore.memory_evidence e "
        "WHERE memory_id=%s ORDER BY artifact_id",
        "jobs": "SELECT to_jsonb(j) AS row FROM braincore.jobs j "
        "WHERE payload->>'memory_id'=%s ORDER BY id",
        "items": "SELECT to_jsonb(i) AS row FROM braincore.operation_items i "
        "WHERE memory_id=%s ORDER BY id",
        "operations": "SELECT to_jsonb(o) AS row FROM braincore.operations o "
        "WHERE id IN (SELECT operation_id FROM braincore.operation_items WHERE memory_id=%s) "
        "ORDER BY id",
    }
    async with connection() as conn:
        return {
            name: await (await conn.execute(query, (memory_id,))).fetchall()
            for name, query in queries.items()
        }


@pytest.mark.parametrize("inactive_action", ["suppress", "retire"])
async def test_inactive_edit_restore_worker_vector_retrieval_and_replay(
    client: httpx.AsyncClient, inactive_action: str
) -> None:
    seeded, scope = await seed_episode(client)
    native_id = seeded["native_id"]
    body = {"object_type": "episode", "object_id": seeded["object_id"], "fields": {}}
    run_worker_once()
    ready = (await snapshot(native_id))["memory"][0]["row"]
    assert ready["embedding_state"] == "ready" and ready["embedding"] is not None
    await mutate(client, {**body, "action": inactive_action})
    revised_summary = f"Restored vector statement {uuid4()}"
    await mutate(client, {**body, "fields": {"summary": revised_summary}})
    inactive = await snapshot(native_id)
    memory = inactive["memory"][0]["row"]
    assert memory["state"] == {"suppress": "suppressed", "retire": "retired"}[inactive_action]
    assert memory["content"] == "Synthetic activation " + revised_summary
    assert memory["embedding_state"] == "pending"
    assert all(
        memory[field] is None
        for field in ("embedding", "embedding_model", "embedding_preprocessing_revision")
    )
    assert len(inactive["jobs"]) == 1  # Only the completed original embedding.
    key = f"restore-activation-{uuid4()}"
    restored = await mutate(client, {**body, "action": "restore"}, key=key)
    queued = await snapshot(native_id)
    assert queued["memory"][0]["row"]["state"] == "active"
    assert queued["revisions"] == inactive["revisions"]
    assert queued["refs"] == inactive["refs"] and queued["evidence"] == inactive["evidence"]
    repairs = [
        job["row"]
        for job in queued["jobs"]
        if job["row"]["operation_id"] == restored["operation_id"]
    ]
    assert len(repairs) == 1
    assert repairs[0]["state"] == "queued" and repairs[0]["attempts"] == 0
    assert repairs[0]["tenant_id"] == memory["tenant_id"]
    assert repairs[0]["payload"] == {"memory_id": native_id}
    operation = await client.get(
        f"/v1/operations/{restored['operation_id']}", headers=auth_headers()
    )
    assert operation.json()["state"] == "queued"
    assert operation.json()["items"][0]["id"] == repairs[0]["operation_item_id"]
    replay = await mutate(client, {**body, "action": "restore"}, key=key)
    assert replay == restored
    assert await snapshot(native_id) == queued
    for request in ({**body, "action": "restore"}, {**body, "fields": {"severity": "info"}}):
        await mutate(client, request)
    repeated = await snapshot(native_id)
    assert repeated["jobs"] == queued["jobs"]
    assert repeated["memory"] == queued["memory"]
    run_worker_once()
    completed = await snapshot(native_id)
    current = completed["memory"][0]["row"]
    settings = get_settings()
    assert current["embedding_state"] == "ready" and current["embedding"] is not None
    assert current["embedding_model"] == settings.embedding_model
    assert current["embedding_preprocessing_revision"] == settings.embedding_preprocessing_revision
    assert current["content"] == memory["content"]
    completed_repair = next(
        job["row"]
        for job in completed["jobs"]
        if job["row"]["operation_id"] == restored["operation_id"]
    )
    assert (
        completed_repair["payload"]["embedding_content_hash"]
        == hashlib.sha256(current["content"].encode()).hexdigest()
    )
    assert completed_repair["payload"]["embedding_dedupe_key"]
    assert completed_repair["payload"]["embedding_memory_version"]
    operation = await client.get(
        f"/v1/operations/{restored['operation_id']}", headers=auth_headers()
    )
    assert operation.json()["state"] == "succeeded"
    assert operation.json()["items"][0]["state"] == "succeeded"
    # A lexical miss plus the stored vector proves actual vector retrieval.
    vector = [float(value) for value in current["embedding"].strip("[]").split(",")]
    actor = Principal(
        UUID(operation.json()["actor_key_id"]),
        UUID(memory["tenant_id"]),
        frozenset({"admin"}),
    )
    outcome = await search_memories(
        connection,
        actor,
        SearchRequest(
            query="lexicalmiss" + uuid4().hex,
            scope={"project_id": scope},
            object_type="episode",
        ),
        EmbeddingResult(vector, "ok"),
        settings,
    )
    assert [str(item["id"]) for item in outcome.payload["results"]] == [native_id]
    assert outcome.payload["streams"]["vector"] == {"state": "ok", "warning": None}
    assert await mutate(client, {**body, "action": "restore"}, key=key) == restored
    assert await snapshot(native_id) == completed


@pytest.mark.parametrize(
    ("state", "vector", "model", "preprocessing"),
    [
        ("pending", None, None, None),
        ("failed", None, None, None),
        ("ready", None, "feature-hash-v1", "token-v2"),
        ("ready", "unit", "previous-model", "token-v2"),
        ("ready", "unit", "feature-hash-v1", "token-v1"),
        ("ready", "unit", "feature-hash-v1", "token-v2"),
    ],
    ids=["pending", "failed", "ready-missing", "old-model", "old-preprocessing", "ready"],
)
async def test_activation_repairs_only_unusable_vector_identity(
    client: httpx.AsyncClient,
    state: str,
    vector: str | None,
    model: str | None,
    preprocessing: str | None,
) -> None:
    seeded, _ = await seed_episode(client)
    body = {"object_type": "episode", "object_id": seeded["object_id"], "fields": {}}
    await mutate(client, {**body, "action": "suppress"})
    async with connection() as conn:
        # The original job is terminal: the activation owns its repair outcome.
        await conn.execute(
            "UPDATE braincore.jobs SET state='succeeded' WHERE payload->>'memory_id'=%s",
            (seeded["native_id"],),
        )
        await conn.execute(
            "UPDATE braincore.memories SET embedding_state=%s,embedding=%s::vector,"
            "embedding_model=%s,embedding_preprocessing_revision=%s WHERE id=%s",
            (
                state,
                "[1," + ",".join(["0"] * 383) + "]" if vector else None,
                model,
                preprocessing,
                seeded["native_id"],
            ),
        )
    before = await snapshot(seeded["native_id"])
    restored = await mutate(client, {**body, "action": "restore"})
    after = await snapshot(seeded["native_id"])
    native = after["memory"][0]["row"]
    settings = get_settings()
    compatible = (
        vector is not None
        and state == "ready"
        and model == settings.embedding_model
        and preprocessing == settings.embedding_preprocessing_revision
    )
    assert len(after["jobs"]) == len(before["jobs"]) + (0 if compatible else 1)
    assert after["revisions"] == before["revisions"]
    if compatible:
        for field in (
            "embedding",
            "embedding_state",
            "embedding_model",
            "embedding_preprocessing_revision",
        ):
            assert native[field] == before["memory"][0]["row"][field]
    else:
        assert native["embedding_state"] == "pending"
        assert native["embedding"] is None
        assert native["embedding_model"] is None
        assert native["embedding_preprocessing_revision"] is None
        repair = next(
            job["row"]
            for job in after["jobs"]
            if job["row"]["operation_id"] == restored["operation_id"]
        )
        assert repair["tenant_id"] == native["tenant_id"] and repair["state"] == "queued"


@pytest.mark.parametrize("old_state", ["queued", "running"])
async def test_activation_with_prior_job_owns_new_repair_outcome(
    client: httpx.AsyncClient, old_state: str
) -> None:
    seeded, _ = await seed_episode(client)
    body = {"object_type": "episode", "object_id": seeded["object_id"], "fields": {}}
    await mutate(client, {**body, "action": "retire"})
    async with connection() as conn:
        await conn.execute(
            "UPDATE braincore.jobs SET state=%s WHERE operation_id=%s",
            (old_state, seeded["operation_id"]),
        )
    before = await snapshot(seeded["native_id"])
    restored = await mutate(client, {**body, "action": "restore"})
    after = await snapshot(seeded["native_id"])
    assert len(after["jobs"]) == len(before["jobs"]) + 1
    assert [job for job in after["jobs"] if job in before["jobs"]] == before["jobs"]
    repair = next(
        job["row"]
        for job in after["jobs"]
        if job["row"]["operation_id"] == restored["operation_id"]
    )
    assert repair["state"] == "queued" and repair["attempts"] == 0
    assert repair["payload"] == {"memory_id": seeded["native_id"]}


async def test_restore_respects_tenant_scope_and_request_hash(client: httpx.AsyncClient) -> None:
    seeded, scope = await seed_episode(client)
    body = {
        "object_type": "episode",
        "object_id": seeded["object_id"],
        "fields": {},
        "action": "restore",
    }
    await mutate(client, {**body, "action": "suppress"})
    before = await snapshot(seeded["native_id"])
    tenant_id = before["memory"][0]["row"]["tenant_id"]
    for other_tenant, key_scope, expected_status in (
        (False, {"project_id": "example/forbidden"}, 403),
        (True, {"project_id": scope}, 409),
    ):
        credential = "bck_" + uuid4().hex
        async with connection() as conn:
            owner = tenant_id
            if other_tenant:
                row = await (
                    await conn.execute(
                        "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id",
                        ("synthetic-restore-" + uuid4().hex,),
                    )
                ).fetchone()
                assert row is not None
                owner = row["id"]
            await conn.execute(
                "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope) "
                "VALUES (%s,'synthetic-restore',%s,%s,ARRAY['read','write'],%s)",
                (owner, credential[:12], key_digest(credential), Jsonb(key_scope)),
            )
        rejected = await client.post(
            "/v1/legacy/objects:mutate",
            headers=auth_headers(credential, idempotency=f"forbidden-{uuid4()}"),
            json=body,
        )
        assert rejected.status_code == expected_status, rejected.text
        assert await snapshot(seeded["native_id"]) == before
    key = f"restore-hash-{uuid4()}"
    restored = await mutate(client, body, key=key)
    accepted = await snapshot(seeded["native_id"])
    conflict = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(idempotency=key),
        json={**body, "reason": "Different request under the same key"},
    )
    assert conflict.status_code == 409 and "idempotency_conflict" in conflict.text
    assert await snapshot(seeded["native_id"]) == accepted
    assert await mutate(client, body, key=key) == restored
    assert await snapshot(seeded["native_id"]) == accepted


def test_metadata_and_restore_preserve_ready_vector_and_nonempty_relationships(
    typed_source: dict[str, Any],
) -> None:
    conn = typed_source["target"]
    actor = principal(typed_source)
    settings = get_settings()
    import_artifacts(conn, typed_source["source_id"])
    args = {
        "object_type": "segment",
        "object_id": typed_source["same_external"],
    }
    linked = mutate_legacy_object(
        conn,
        actor,
        **args,
        fields={
            "project_entity_id": str(typed_source["same_external"]),
            "artifact_id": str(typed_source["same_external"]),
            "line_start": 5,
        },
        idempotency_key="linked-segment",
    )
    native_id = linked["native_id"]
    conn.execute(
        "UPDATE braincore.memories SET embedding=%s::vector,embedding_state='ready',"
        "embedding_model=%s,embedding_preprocessing_revision=%s,trust='corroborated',"
        "metadata=%s WHERE id=%s",
        (
            "[1," + ",".join(["0"] * 383) + "]",
            settings.embedding_model,
            settings.embedding_preprocessing_revision,
            Jsonb({"synthetic": True}),
            native_id,
        ),
    )

    def retained() -> dict[str, Any]:
        memory = conn.execute("SELECT * FROM braincore.memories WHERE id=%s", (native_id,))
        return {
            "memory": memory.fetchone(),
            "refs": conn.execute(
                "SELECT * FROM braincore.legacy_subtype_refs WHERE memory_id=%s ORDER BY field",
                (native_id,),
            ).fetchall(),
            "evidence": conn.execute(
                "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s ORDER BY artifact_id",
                (native_id,),
            ).fetchall(),
            "revisions": conn.execute(
                "SELECT * FROM braincore.memory_revisions WHERE memory_id=%s ORDER BY id",
                (native_id,),
            ).fetchall(),
            "jobs": conn.execute(
                "SELECT * FROM braincore.jobs WHERE payload->>'memory_id'=%s ORDER BY id",
                (native_id,),
            ).fetchall(),
        }

    before = retained()
    assert before["refs"] and before["evidence"]
    mutate_legacy_object(
        conn, actor, **args, fields={"line_start": 7}, idempotency_key="ready-metadata"
    )
    assert retained() == before
    mutate_legacy_object(
        conn, actor, **args, fields={}, action="suppress", idempotency_key="linked-suppress"
    )
    suppressed = retained()
    mutate_legacy_object(
        conn, actor, **args, fields={}, action="restore", idempotency_key="linked-restore"
    )
    restored = retained()
    assert restored["refs"] == before["refs"] and restored["evidence"] == before["evidence"]
    assert restored["jobs"] == before["jobs"] and restored["revisions"] == before["revisions"]
    for name in suppressed["memory"]:
        if name not in {"updated_at", "state"}:
            assert restored["memory"][name] == suppressed["memory"][name]
    assert suppressed["memory"]["state"] == "suppressed"
    assert restored["memory"]["state"] == "active"
    mutate_legacy_object(
        conn, actor, **args, fields={}, action="retire", idempotency_key="linked-retire"
    )
    mutate_legacy_object(
        conn,
        actor,
        **args,
        fields={"content": "Changed inactive segment with retained evidence"},
        idempotency_key="linked-inactive-edit",
    )
    inactive = retained()
    repaired = mutate_legacy_object(
        conn, actor, **args, fields={}, action="restore", idempotency_key="linked-repair"
    )
    queued = retained()
    assert queued["refs"] == before["refs"] and queued["evidence"] == before["evidence"]
    assert queued["revisions"] == inactive["revisions"]
    assert len(queued["jobs"]) == len(inactive["jobs"]) + 1
    assert str(queued["jobs"][-1]["operation_id"]) == repaired["operation_id"]
    for name in ("trust", "scope", "valid_from", "valid_until", "metadata", "content"):
        assert queued["memory"][name] == inactive["memory"][name]

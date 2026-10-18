"""Authenticated reference embedding contracts on disposable PostgreSQL and worker."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from braincore_memory import service
from braincore_memory.auth import Principal
from braincore_memory.config import get_settings
from braincore_memory.db import connection
from braincore_memory.embedding import EmbeddingResult
from braincore_memory.models import SearchRequest

from .conftest import auth_headers
from .test_e2e import run_worker_once
from .test_memory_input_contracts_e2e import write_counts


async def operation(client: httpx.AsyncClient, operation_id: str) -> dict[str, Any]:
    response = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert response.status_code == 200, response.text
    return response.json()


async def write(
    client: httpx.AsyncClient,
    body: dict[str, Any],
    *,
    batched: bool = False,
    key: str | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return await client.post(
        "/v1/memories:batch" if batched else "/v1/memories",
        json={"items": [body]} if batched else body,
        headers=headers or auth_headers(idempotency=key or f"reference-{uuid4()}"),
    )


async def seed(client: httpx.AsyncClient) -> tuple[dict[str, Any], str]:
    marker = uuid4().hex
    body = {
        "content": f"Useful synthetic reference {marker}.",
        "scope": {"user_id": "alice", "run_id": marker},
        "reference": {"namespace": ["example", "embedding"], "key": marker},
        "metadata": {"version": "original"},
    }
    result = await write(client, body)
    assert result.status_code == 202, result.text
    run_worker_once()
    completed = await operation(client, result.json()["operation_id"])
    assert completed["state"] == "succeeded", completed
    return body, completed["items"][0]["memory_id"]


async def snapshot(memory_id: str) -> dict[str, Any]:
    async with connection() as conn:
        memory = await (
            await conn.execute(
                "SELECT to_jsonb(m) AS row FROM braincore.memories m WHERE id=%s", (memory_id,)
            )
        ).fetchone()
        assert memory is not None
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM braincore.memory_revisions WHERE memory_id=%s) "
                "AS revisions,(SELECT count(*) FROM braincore.memory_evidence WHERE memory_id=%s) "
                "AS evidence,(SELECT count(*) FROM braincore.jobs WHERE payload->>'memory_id'=%s) "
                "AS jobs",
                (memory_id, memory_id, memory_id),
            )
        ).fetchone()
    assert counts is not None
    return {"memory": memory["row"], **counts}


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("structured", [False, True])
async def test_healthy_reference_metadata_update_is_terminal_without_provider_work(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    batched: bool,
    structured: bool,
) -> None:
    body, memory_id = await seed(client)
    if structured:
        async with connection() as conn:
            await conn.execute(
                "UPDATE braincore.memories SET object_type='fact',legacy_attributes_ready=true,"
                "legacy_attributes=%s::jsonb WHERE id=%s",
                ('{"synthetic":"preserved"}', memory_id),
            )
    before = await snapshot(memory_id)
    # The update has no dependency on the provider: the operation finishes
    # synchronously and there is no new job that could call an unavailable URL.
    settings = get_settings().model_copy(update={"embedder_url": "http://127.0.0.1:1"})
    monkeypatch.setattr(service, "get_settings", lambda: settings)
    target = datetime.now(UTC) - timedelta(days=1)
    update = {
        **body,
        "metadata": {"version": "metadata only"},
        "valid_from": target.isoformat(),
        "evidence": {"source_type": "manual", "source_uri": "https://example.com/source"},
    }
    key = f"healthy-update-{uuid4()}"
    response = await write(client, update, batched=batched, key=key)
    assert response.status_code == 202, response.text
    accepted = await operation(client, response.json()["operation_id"])
    assert response.json()["state"] == accepted["state"] == "succeeded"
    assert accepted["succeeded_items"] == 1 and accepted["failed_items"] == 0
    assert accepted["completed_at"] is not None
    assert accepted["items"][0]["state"] == "succeeded"
    assert accepted["items"][0]["completed_at"] is not None
    after = await snapshot(memory_id)
    for field in (
        "content",
        "embedding",
        "embedding_state",
        "embedding_model",
        "embedding_preprocessing_revision",
        "dedupe_key",
        "scope",
        "valid_until",
        "legacy_attributes",
        "object_type",
        "state",
    ):
        assert after["memory"][field] == before["memory"][field]
    assert after["jobs"] == before["jobs"]
    assert after["revisions"] == before["revisions"]
    assert after["evidence"] == before["evidence"] + 1
    assert after["memory"]["metadata"] == update["metadata"]
    assert datetime.fromisoformat(after["memory"]["valid_from"]) == target
    counts = await write_counts()
    replay = await write(client, update, batched=batched, key=key)
    assert replay.status_code == 202 and replay.json()["reused"] is True
    assert replay.json()["operation_id"] == response.json()["operation_id"]
    assert await write_counts() == counts and await snapshot(memory_id) == after
    # A second key also reuses the ready vector without a repair job.
    again = await write(client, update, batched=batched)
    assert again.status_code == 202 and again.json()["state"] == "succeeded"
    assert (await snapshot(memory_id))["jobs"] == before["jobs"]
    vector = [float(v) for v in after["memory"]["embedding"].strip("[]").split(",")]
    actor = Principal(
        UUID(accepted["actor_key_id"]), UUID(after["memory"]["tenant_id"]), frozenset({"admin"})
    )
    outcome = await service.search_memories(
        connection,
        actor,
        SearchRequest(query="lexicalmiss" + uuid4().hex, scope=body["scope"]),
        EmbeddingResult(vector, "ok"),
        settings,
    )
    assert [str(item["id"]) for item in outcome.payload["results"]] == [memory_id]
    assert outcome.payload["streams"]["vector"] == {"state": "ok", "warning": None}


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("defect", ["missing", "pending", "failed", "model", "preprocessing"])
async def test_unusable_reference_vector_repairs_through_actual_worker(
    client: httpx.AsyncClient,
    batched: bool,
    defect: str,
) -> None:
    body, memory_id = await seed(client)
    async with connection() as conn:
        await conn.execute(
            "UPDATE braincore.memories SET embedding=CASE WHEN %s THEN NULL ELSE embedding END,"
            "embedding_state=%s,embedding_model=%s,embedding_preprocessing_revision=%s WHERE id=%s",
            (
                defect == "missing",
                defect if defect in {"pending", "failed"} else "ready",
                "previous-model" if defect == "model" else get_settings().embedding_model,
                "previous-preprocessing"
                if defect == "preprocessing"
                else get_settings().embedding_preprocessing_revision,
                memory_id,
            ),
        )
    before = await snapshot(memory_id)
    key = f"repair-reference-{uuid4()}"
    response = await write(client, body, batched=batched, key=key)
    assert response.status_code == 202 and response.json()["state"] == "queued", response.text
    queued = await snapshot(memory_id)
    assert queued["memory"]["embedding_state"] == "pending"
    assert queued["jobs"] == before["jobs"] + 1
    replay = await write(client, body, batched=batched, key=key)
    assert replay.json()["reused"] is True
    assert await snapshot(memory_id) == queued
    run_worker_once()
    completed = await operation(client, response.json()["operation_id"])
    assert completed["state"] == completed["items"][0]["state"] == "succeeded"
    after = await snapshot(memory_id)
    assert after["memory"]["embedding"] is not None
    assert after["memory"]["embedding_state"] == "ready"
    assert after["memory"]["embedding_model"] == get_settings().embedding_model
    assert (
        after["memory"]["embedding_preprocessing_revision"]
        == get_settings().embedding_preprocessing_revision
    )
    assert after["revisions"] == before["revisions"]


@pytest.mark.parametrize("batched", [False, True])
async def test_changed_reference_content_invalidates_and_recomputes_vector(
    client: httpx.AsyncClient,
    batched: bool,
) -> None:
    body, memory_id = await seed(client)
    before = await snapshot(memory_id)
    changed = {**body, "content": f"Completely changed reference statement {uuid4()}."}
    response = await write(client, changed, batched=batched)
    assert response.status_code == 202 and response.json()["state"] == "queued", response.text
    queued = await snapshot(memory_id)
    assert all(
        queued["memory"][field] is None
        for field in ("embedding", "embedding_model", "embedding_preprocessing_revision")
    )
    assert queued["memory"]["embedding_state"] == "pending"
    assert queued["jobs"] == before["jobs"] + 1
    assert queued["revisions"] == before["revisions"] + 1
    run_worker_once()
    assert (await operation(client, response.json()["operation_id"]))["state"] == "succeeded"
    after = await snapshot(memory_id)
    assert after["memory"]["embedding"] != before["memory"]["embedding"]
    assert after["memory"]["content"] == changed["content"]
    assert after["memory"]["embedding_state"] == "ready"


@pytest.mark.parametrize("failed_first", [False, True])
@pytest.mark.parametrize("failure", ["validation", "conflict"])
async def test_ready_reference_with_failed_batch_sibling_finishes_partial(
    client: httpx.AsyncClient,
    failed_first: bool,
    failure: str,
) -> None:
    body, memory_id = await seed(client)
    bad = {"content": "short"}
    if failure == "conflict":
        async with connection() as conn:
            await conn.execute(
                "UPDATE braincore.memories SET object_type='fact' WHERE id=%s", (memory_id,)
            )
        bad = {**body, "content": "A changed structured fact must use revision semantics."}
    before = await snapshot(memory_id)
    items = [bad, body] if failed_first else [body, bad]
    response = await client.post(
        "/v1/memories:batch",
        headers=auth_headers(idempotency=f"mixed-ready-{uuid4()}"),
        json={"items": items},
    )
    assert response.status_code == 202 and response.json()["state"] == "partial", response.text
    result = await operation(client, response.json()["operation_id"])
    assert result["state"] == "partial" and result["completed_at"] is not None
    assert result["succeeded_items"] == result["failed_items"] == 1
    assert [item["state"] for item in result["items"]] == (
        ["failed", "succeeded"] if failed_first else ["succeeded", "failed"]
    )
    after = await snapshot(memory_id)
    assert after["jobs"] == before["jobs"] and after["revisions"] == before["revisions"]
    assert after["memory"]["embedding_state"] == "ready"


async def test_ready_and_queued_batch_siblings_finish_after_worker(
    client: httpx.AsyncClient,
) -> None:
    body, memory_id = await seed(client)
    fresh = {"content": f"A genuinely new queued memory {uuid4()}.", "scope": body["scope"]}
    response = await client.post(
        "/v1/memories:batch",
        headers=auth_headers(idempotency=f"mixed-queue-{uuid4()}"),
        json={"items": [body, fresh]},
    )
    assert response.status_code == 202 and response.json()["state"] == "queued", response.text
    result = await operation(client, response.json()["operation_id"])
    assert [item["state"] for item in result["items"]] == ["succeeded", "queued"]
    assert result["succeeded_items"] == 1 and result["completed_at"] is None
    assert result["items"][0]["memory_id"] == memory_id
    run_worker_once()
    result = await operation(client, response.json()["operation_id"])
    assert result["state"] == "succeeded" and result["succeeded_items"] == 2


async def test_scope_rejection_cannot_invalidate_another_reference(
    client: httpx.AsyncClient,
) -> None:
    body, memory_id = await seed(client)
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"reference-key-{uuid4()}"),
        json={
            "name": "synthetic reference writer",
            "roles": ["write"],
            "scope": {"user_id": "bob"},
        },
    )
    assert created.status_code == 201, created.text
    before = await snapshot(memory_id)
    counts = await write_counts()
    rejected = await write(
        client,
        {**body, "metadata": {"unauthorized": True}},
        headers=auth_headers(created.json()["api_key"], idempotency=f"forbidden-{uuid4()}"),
    )
    assert rejected.status_code == 403, rejected.text
    assert await snapshot(memory_id) == before and await write_counts() == counts

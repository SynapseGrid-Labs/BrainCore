from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import HTTPException

from braincore_memory import api
from braincore_memory.auth import Principal
from braincore_memory.db import connection

from .conftest import auth_headers
from .test_e2e import run_worker_once


@pytest.mark.parametrize("session", [False, True])
async def test_rotation_rejects_authorizing_key_before_any_mutation(
    session: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = Principal(
        uuid4(), uuid4(), frozenset({"admin"}), session_id=uuid4() if session else None
    )

    def forbidden_connection() -> Any:
        raise AssertionError("self-rotation must not access the mutation transaction")

    monkeypatch.setattr(api, "connection", forbidden_connection)
    with pytest.raises(HTTPException) as error:
        await api.rotate_api_key(actor.key_id, "self-rotation", actor)
    assert error.value.status_code == 409
    assert error.value.detail == "cannot_rotate_current_key"


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("failure", ["conflict", "database"])
async def test_batch_runtime_failure_preserves_surrounding_items_and_replay(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    failure: str,
) -> None:
    original = api.remember_one if endpoint == "memories" else api.enqueue_ingestion_one
    marker = uuid4().hex
    contents = [f"Synthetic batch {marker} item {index}." for index in range(3)]

    async def fail_middle(*args: Any) -> Any:
        result = await original(*args)
        if args[-1].content == contents[1]:
            if failure == "conflict":
                raise HTTPException(status_code=409, detail="structured_fact_revision_required")
            # A real PostgreSQL error aborts its subtransaction. The failure
            # outcome and later item must still be writable after rollback.
            await args[0].execute("SELECT 1 / 0 /* synthetic private database detail */")
        return result

    monkeypatch.setattr(
        api, "remember_one" if endpoint == "memories" else "enqueue_ingestion_one", fail_middle
    )
    body = {"items": [{"content": text, "scope": {"user_id": "alice"}} for text in contents]}
    headers = auth_headers(idempotency=f"runtime-batch-{marker}")
    accepted = await client.post(f"/v1/{endpoint}:batch", headers=headers, json=body)
    assert accepted.status_code == 202, accepted.text
    replay = await client.post(f"/v1/{endpoint}:batch", headers=headers, json=body)
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    operation_id = accepted.json()["operation_id"]
    operation = (await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())).json()
    assert [item["state"] for item in operation["items"]] == ["queued", "failed", "queued"]
    error = operation["items"][1]["error"]
    assert error["code"] == (
        "structured_fact_revision_required" if failure == "conflict" else "batch_item_failed"
    )
    assert "private" not in str(error)
    assert "division" not in str(error)
    assert operation["items"][1]["completed_at"] is not None
    async with connection() as conn:
        artifacts = await (
            await conn.execute(
                "SELECT content FROM braincore.artifacts WHERE content=ANY(%s) ORDER BY content",
                (contents,),
            )
        ).fetchall()
        assert [row["content"] for row in artifacts] == [contents[0], contents[2]]
        jobs = await (
            await conn.execute(
                "SELECT count(*) AS total FROM braincore.jobs WHERE operation_id=%s",
                (operation_id,),
            )
        ).fetchone()
        assert jobs is not None and jobs["total"] == 2
    run_worker_once()
    final = (await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())).json()
    assert final["state"] == "partial"
    assert final["succeeded_items"] == 2
    assert final["failed_items"] == 1
    assert [item["state"] for item in final["items"]] == ["succeeded", "failed", "succeeded"]


async def test_batch_with_only_runtime_failures_has_a_terminal_operation(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def reject(*args: Any) -> None:
        raise HTTPException(status_code=409, detail="memory_conflict")

    monkeypatch.setattr(api, "remember_one", reject)
    headers = auth_headers(idempotency=f"all-conflicts-{uuid4()}")
    body = {
        "items": [
            {"content": "A conflicting memory.", "scope": {"user_id": "alice"}},
            {"content": "An item without a scope."},
        ]
    }
    accepted = await client.post("/v1/memories:batch", headers=headers, json=body)
    assert accepted.status_code == 202, accepted.text
    assert accepted.json()["state"] == "failed"
    operation = (
        await client.get(
            f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
        )
    ).json()
    assert operation["failed_items"] == 2
    assert operation["completed_at"] is not None
    assert operation["items"][0]["error"]["code"] == "memory_conflict"
    replay = await client.post("/v1/memories:batch", headers=headers, json=body)
    assert replay.json()["reused"] is True
    assert replay.json()["state"] == "failed"


async def test_batch_preserves_real_structured_fact_conflict(client: httpx.AsyncClient) -> None:
    marker = uuid4().hex
    reference = {"namespace": ["batch", "facts"], "key": marker}
    scope = {"user_id": "alice"}
    original = {"content": f"Original fact {marker}.", "scope": scope, "reference": reference}
    created = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"seed-fact-{marker}"),
        json=original,
    )
    assert created.status_code == 202
    run_worker_once()
    operation = (
        await client.get(f"/v1/operations/{created.json()['operation_id']}", headers=auth_headers())
    ).json()
    memory_id = operation["items"][0]["memory_id"]
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE braincore.memories SET object_type='fact' WHERE id=%s", (memory_id,)
        )
    batch = await client.post(
        "/v1/memories:batch",
        headers=auth_headers(idempotency=f"fact-conflict-{marker}"),
        json={
            "items": [
                {"content": f"Surviving first item {marker}.", "scope": scope},
                {**original, "content": f"Changed structured fact {marker}."},
                {"content": f"Surviving last item {marker}.", "scope": scope},
            ]
        },
    )
    assert batch.status_code == 202, batch.text
    run_worker_once()
    result = (
        await client.get(f"/v1/operations/{batch.json()['operation_id']}", headers=auth_headers())
    ).json()
    assert result["state"] == "partial"
    assert [item["state"] for item in result["items"]] == ["succeeded", "failed", "succeeded"]
    assert result["items"][1]["error"]["code"] == "structured_fact_revision_required"
    unchanged = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
    assert unchanged.json()["content"] == original["content"]


async def test_self_rotation_preserves_bearer_and_dashboard_but_other_key_rotation_revokes_sessions(
    client: httpx.AsyncClient,
) -> None:
    key = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"rotation-admin-{uuid4()}"),
        json={"name": "rotation-contract-admin", "roles": ["admin"], "scope": None},
    )
    assert key.status_code == 201, key.text
    key_id = UUID(key.json()["id"])
    secret = key.json()["api_key"]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as dashboard:
        session = await dashboard.post(
            "/v1/dashboard/session",
            headers=auth_headers(secret, idempotency=f"rotation-session-{uuid4()}"),
        )
        assert session.status_code == 201
        for transport in (client, dashboard):
            headers = {"Idempotency-Key": f"self-rotation-{uuid4()}"}
            if transport is client:
                headers.update(auth_headers(secret))
            else:
                headers["X-CSRF-Token"] = session.json()["csrf_token"]
            rejected = await transport.post(f"/v1/admin/api-keys/{key_id}/rotate", headers=headers)
            assert rejected.status_code == 409, rejected.text
            assert rejected.json()["error"]["code"] == "cannot_rotate_current_key"
            assert "api_key" not in rejected.json()
        assert (await client.get("/v1/health", headers=auth_headers(secret))).status_code == 200
        assert (await dashboard.get("/v1/admin/status")).status_code == 200
        rotation_headers = auth_headers(idempotency=f"other-admin-rotation-{uuid4()}")
        rotated = await client.post(f"/v1/admin/api-keys/{key_id}/rotate", headers=rotation_headers)
        assert rotated.status_code == 200, rotated.text
        assert (await client.get("/v1/health", headers=auth_headers(secret))).status_code == 401
        assert (await dashboard.get("/v1/admin/status")).status_code == 401
        assert (
            await client.get("/v1/health", headers=auth_headers(rotated.json()["api_key"]))
        ).status_code == 200
        replay = await client.post(f"/v1/admin/api-keys/{key_id}/rotate", headers=rotation_headers)
        assert replay.status_code == 409
        assert replay.json()["error"]["code"] == "api_key_secret_not_replayable"

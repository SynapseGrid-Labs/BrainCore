"""Rejected blank native writes must create no PostgreSQL work or mutation."""

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest

from braincore_memory.db import connection

from .conftest import auth_headers
from .test_memory_input_contracts import BLANK_CONTENT


async def write_counts() -> dict[str, Any]:
    async with connection() as conn:
        row = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM braincore.operations) AS operations,"
                "(SELECT count(*) FROM braincore.operation_items) AS items,"
                "(SELECT count(*) FROM braincore.jobs) AS jobs,"
                "(SELECT count(*) FROM braincore.memories) AS memories,"
                "(SELECT count(*) FROM braincore.artifacts) AS artifacts,"
                "(SELECT count(*) FROM braincore.memory_revisions) AS revisions,"
                "(SELECT count(*) FROM braincore.idempotency_records) AS receipts"
            )
        ).fetchone()
    assert row is not None
    return dict(row)


@pytest.mark.parametrize("content", BLANK_CONTENT)
@pytest.mark.parametrize("mode", ["remember", "revision", "reference-update", "all-blank-batch"])
async def test_blank_native_writes_leave_no_postgresql_work(
    client: httpx.AsyncClient, mode: str, content: str
) -> None:
    marker = uuid4().hex
    reference = {"namespace": ["alice", "blank-contract"], "key": marker}
    seed = {
        "content": f"Original native content {marker}.",
        "scope": {"user_id": "alice"},
        "reference": reference,
    }
    memory_id = None
    if mode in {"revision", "reference-update"}:
        accepted = await client.post(
            "/v1/memories", headers=auth_headers(idempotency=f"seed-{marker}"), json=seed
        )
        assert accepted.status_code == 202, accepted.text
        operation = (
            await client.get(
                f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
            )
        ).json()
        memory_id = operation["items"][0]["memory_id"]
    path = f"/v1/memories/{memory_id}/revisions" if mode == "revision" else "/v1/memories"
    body: dict[str, Any] = (
        {"content": content, "reason": "Synthetic correction"}
        if mode == "revision"
        else {"content": content, "scope": {"user_id": "alice"}}
    )
    if mode == "reference-update":
        body["reference"] = reference
    if mode == "all-blank-batch":
        path, body = "/v1/memories:batch", {"items": [body, body]}
    before = await write_counts()
    response = await client.post(
        path, headers=auth_headers(idempotency=f"blank-{marker}"), json=body
    )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_failed"
    assert await write_counts() == before
    if memory_id is not None:
        memory = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
        assert memory.json()["content"] == seed["content"]


@pytest.mark.parametrize("content", BLANK_CONTENT)
async def test_mixed_blank_batch_preserves_valid_whitespace_and_metadata_and_replay(
    client: httpx.AsyncClient, content: str
) -> None:
    marker = uuid4().hex
    first = f" \tValid Unicode 🧠 界 {marker}.\n "
    value = {"style": "concise", "marker": marker}
    body = {
        "items": [
            {"content": first, "scope": {"user_id": "alice"}},
            {"content": content, "scope": {"user_id": "alice"}},
            {
                "content": json.dumps(value),
                "scope": {"user_id": "alice"},
                "metadata": {"langgraph": {"value": value}},
            },
        ]
    }
    headers = auth_headers(idempotency=f"mixed-blank-{marker}")
    accepted = await client.post("/v1/memories:batch", headers=headers, json=body)
    assert accepted.status_code == 202, accepted.text
    after = await write_counts()
    replay = await client.post("/v1/memories:batch", headers=headers, json=body)
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    assert await write_counts() == after
    operation = (
        await client.get(
            f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
        )
    ).json()
    assert [item["state"] for item in operation["items"]] == ["queued", "failed", "queued"]
    assert operation["items"][1]["error"]["code"] == "validation_failed"
    first_memory = (
        await client.get(
            f"/v1/memories/{operation['items'][0]['memory_id']}", headers=auth_headers()
        )
    ).json()
    last_memory = (
        await client.get(
            f"/v1/memories/{operation['items'][2]['memory_id']}", headers=auth_headers()
        )
    ).json()
    assert first_memory["content"] == first
    assert last_memory["metadata"]["langgraph"]["value"] == value

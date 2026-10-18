"""Blank audit reasons cannot mutate canonical state or consume a write key."""

from typing import Any
from uuid import uuid4

import httpx
import pytest

from braincore_memory.db import connection

from .conftest import auth_headers
from .test_audit_reasons import AUDIT_MODES, BLANK_REASONS, audit_body
from .test_memory_input_contracts_e2e import write_counts


async def audit_snapshot(memory_id: str) -> dict[str, Any]:
    async with connection() as conn:
        memory = await (
            await conn.execute("SELECT * FROM braincore.memories WHERE id=%s", (memory_id,))
        ).fetchone()
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM braincore.lifecycle_events) AS lifecycle,"
                "(SELECT count(*) FROM braincore.audit_events) AS audit"
            )
        ).fetchone()
    assert memory is not None and counts is not None
    return {"memory": dict(memory), "write_counts": await write_counts(), **counts}


@pytest.mark.parametrize("mode", AUDIT_MODES)
@pytest.mark.parametrize("reason", BLANK_REASONS)
async def test_blank_audit_reason_no_writes_then_same_key_valid_reason_persists_and_replays(
    client: httpx.AsyncClient, mode: str, reason: str
) -> None:
    marker = uuid4().hex
    seed = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"audit-seed-{marker}"),
        json={"content": f"Original audited content {marker}.", "scope": {"user_id": "alice"}},
    )
    assert seed.status_code == 202, seed.text
    operation = await client.get(
        f"/v1/operations/{seed.json()['operation_id']}", headers=auth_headers()
    )
    assert operation.status_code == 200, operation.text
    memory_id = operation.json()["items"][0]["memory_id"]
    path = f"/v1/memories/{memory_id}/" + ("revisions" if mode == "revision" else "lifecycle")
    method = "POST" if mode == "revision" else "PATCH"
    headers = auth_headers(idempotency=f"audit-action-{marker}")
    before = await audit_snapshot(memory_id)
    rejected = await client.request(method, path, headers=headers, json=audit_body(mode, reason))
    assert rejected.status_code == 422, rejected.text
    assert rejected.json()["error"]["code"] == "validation_failed"
    assert await audit_snapshot(memory_id) == before

    accepted_reason = f" \tUser corrected 🧠 界 café {marker}.\u2003\n "
    body = audit_body(mode, accepted_reason)
    if mode == "revision":
        body["content"] = f" \tCorrected Unicode 🧠 界 content {marker}.\n "
    accepted = await client.request(method, path, headers=headers, json=body)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["reused"] is False
    async with connection() as conn:
        if mode == "revision":
            event = await (
                await conn.execute(
                    "SELECT reason,content FROM braincore.memory_revisions WHERE id=%s",
                    (accepted.json()["revision_id"],),
                )
            ).fetchone()
            assert event is not None and event["content"] == body["content"]
        else:
            event = await (
                await conn.execute(
                    "SELECT reason,action FROM braincore.lifecycle_events WHERE memory_id=%s",
                    (memory_id,),
                )
            ).fetchone()
            assert event is not None and event["action"] == mode
            audit = await (
                await conn.execute(
                    "SELECT details FROM braincore.audit_events "
                    "WHERE target_id=%s AND action='memory.lifecycle'",
                    (memory_id,),
                )
            ).fetchone()
            assert audit is not None and audit["details"]["reason"] == accepted_reason
    assert event["reason"] == accepted_reason
    after = await audit_snapshot(memory_id)
    if mode == "revision":
        assert after["memory"]["content"] == body["content"]
    else:
        expected_state = {"suppress": "suppressed", "retire": "retired"}.get(mode, "active")
        assert after["memory"]["state"] == expected_state
        assert after["memory"]["content"] == before["memory"]["content"]
        if mode == "promote":
            assert after["memory"]["trust"] == "corroborated"
    replay = await client.request(method, path, headers=headers, json=body)
    assert replay.status_code == 200, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    if mode == "revision":
        assert replay.json()["revision_id"] == accepted.json()["revision_id"]
    assert await audit_snapshot(memory_id) == after

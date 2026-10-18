"""Reference validity through authenticated HTTP, search predicates and real PostgreSQL."""

import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import httpx
import pytest

from braincore_memory import api
from braincore_memory.db import connection
from braincore_memory.embedder import feature_hash

from .conftest import auth_headers
from .test_memory_input_contracts_e2e import write_counts


def parsed_date(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


async def write(
    client: httpx.AsyncClient,
    body: dict[str, Any],
    key: str,
    batched: bool = False,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return await client.post(
        "/v1/memories:batch" if batched else "/v1/memories",
        headers=headers or auth_headers(idempotency=key),
        json={"items": [body]} if batched else body,
    )


async def seed(client: httpx.AsyncClient, valid_from: datetime) -> tuple[dict[str, Any], str]:
    marker = uuid4().hex
    body = {
        "content": f"Original 🧠 reference {marker}.",
        "scope": {"user_id": "alice", "run_id": marker},
        "reference": {"namespace": ["alice", "validity"], "key": marker},
        "valid_from": valid_from.isoformat(),
        "metadata": {"version": "original"},
    }
    accepted = await write(client, body, f"seed-{marker}")
    assert accepted.status_code == 202, accepted.text
    operation = (
        await client.get(
            f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
        )
    ).json()
    return body, operation["items"][0]["memory_id"]


async def memory(client: httpx.AsyncClient, memory_id: str) -> dict[str, Any]:
    response = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
    assert response.status_code == 200, response.text
    return response.json()


async def visible(
    client: httpx.AsyncClient, scope: dict[str, str], memory_id: str, as_of: datetime | None = None
) -> bool:
    body: dict[str, Any] = {"scope": scope, "query": None}
    if as_of is not None:
        body["as_of"] = as_of.isoformat()
    response = await client.post("/v1/search", headers=auth_headers(), json=body)
    assert response.status_code == 200, response.text
    return memory_id in {item["id"] for item in response.json()["results"]}


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("changed_content", [False, True])
@pytest.mark.parametrize("future_to_past", [False, True])
async def test_reference_update_moves_current_and_as_of_visibility(
    client: httpx.AsyncClient, batched: bool, changed_content: bool, future_to_past: bool
) -> None:
    now = datetime.now(UTC)
    past, future = now - timedelta(days=30), now + timedelta(days=30)
    original, target = (future, past) if future_to_past else (past, future)
    body, memory_id = await seed(client, original)
    assert await visible(client, body["scope"], memory_id) is (original <= now)
    assert await visible(client, body["scope"], memory_id, original)
    update = {
        **body,
        "valid_from": target.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat(),
        "metadata": {"version": "updated"},
    }
    if changed_content:
        update["content"] = "Changed " + body["content"]
    key = f"move-{uuid4()}"
    accepted = await write(client, update, key, batched)
    assert accepted.status_code == 202, accepted.text
    current = await memory(client, memory_id)
    assert parsed_date(current["valid_from"]) == target
    assert current["content"] == update["content"]
    assert current["metadata"] == update["metadata"]
    assert await visible(client, body["scope"], memory_id) is (target <= now)
    assert not await visible(client, body["scope"], memory_id, target - timedelta(microseconds=1))
    assert await visible(client, body["scope"], memory_id, target)
    async with connection() as conn:
        projection = await (
            await conn.execute(
                "SELECT valid_from FROM braincore.memories_lexical WHERE id=%s", (memory_id,)
            )
        ).fetchone()
    assert projection is not None and projection["valid_from"] == target
    counts = await write_counts()
    replay = await write(client, update, key, batched)
    assert replay.status_code == 202, replay.text
    assert replay.json()["reused"] is True
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert await write_counts() == counts
    assert (await memory(client, memory_id))["updated_at"] == current["updated_at"]


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("initial", ["past", "future", "unknown"])
async def test_omission_and_null_preserve_timestamp_and_existing_replay_contract(
    client: httpx.AsyncClient, batched: bool, initial: str
) -> None:
    original = datetime.now(UTC) + timedelta(days=30 if initial == "future" else -30)
    body, memory_id = await seed(client, original)
    if initial == "unknown":
        async with connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE braincore.memories SET valid_from=NULL WHERE id=%s", (memory_id,)
            )
        original = None
    update = {key: value for key, value in body.items() if key != "valid_from"}
    update["metadata"] = {"version": "metadata only"}
    key = f"omitted-{uuid4()}"
    accepted = await write(client, update, key, batched)
    assert accepted.status_code == 202, accepted.text
    current = await memory(client, memory_id)
    assert parsed_date(current["valid_from"]) == original
    counts = await write_counts()
    replay = await write(client, {**update, "valid_from": None}, key, batched)
    assert replay.status_code == (409 if batched else 202), replay.text
    if not batched:
        assert replay.json()["operation_id"] == accepted.json()["operation_id"]
        assert replay.json()["reused"] is True
    assert await write_counts() == counts
    assert (await memory(client, memory_id))["updated_at"] == current["updated_at"]
    accepted_null = await write(client, {**update, "valid_from": None}, f"null-{uuid4()}", batched)
    assert accepted_null.status_code == 202, accepted_null.text
    assert parsed_date((await memory(client, memory_id))["valid_from"]) == original
    if initial == "unknown":
        known_start = datetime.now(UTC) + timedelta(days=30)
        explicit = await write(
            client,
            {**update, "valid_from": known_start.isoformat()},
            f"known-{uuid4()}",
            batched,
        )
        assert explicit.status_code == 202, explicit.text
        assert parsed_date((await memory(client, memory_id))["valid_from"]) == known_start
        assert not await visible(client, body["scope"], memory_id)
        assert await visible(client, body["scope"], memory_id, known_start)


@pytest.mark.parametrize("structured", [False, True])
async def test_same_content_date_update_preserves_vector_legacy_fields_and_valid_until(
    client: httpx.AsyncClient, structured: bool
) -> None:
    now = datetime.now(UTC)
    body, memory_id = await seed(client, now - timedelta(days=30))
    valid_until = now + timedelta(days=90)
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE braincore.memories SET valid_until=%s,embedding=%s::vector,"
            "embedding_model='feature-hash-v1',embedding_preprocessing_revision='token-v2',"
            "embedding_state='ready',object_type=%s,legacy_attributes_ready=%s,"
            "legacy_attributes=%s::jsonb WHERE id=%s",
            (
                valid_until,
                str(feature_hash(body["content"])),
                "fact" if structured else "memory",
                structured,
                json.dumps({"statement": body["content"], "synthetic": "kept"}),
                memory_id,
            ),
        )
        before = await (
            await conn.execute(
                "SELECT embedding::text,embedding_model,embedding_preprocessing_revision,"
                "legacy_attributes,object_type,dedupe_key,(SELECT count(*) "
                "FROM braincore.memory_revisions WHERE memory_id=%s) AS revisions "
                "FROM braincore.memories WHERE id=%s",
                (memory_id, memory_id),
            )
        ).fetchone()
    target = now + timedelta(days=30)
    accepted = await write(
        client,
        {**body, "valid_from": target.isoformat(), "metadata": {"version": "date only"}},
        f"date-only-{uuid4()}",
    )
    assert accepted.status_code == 202, accepted.text
    async with connection() as conn:
        after = await (
            await conn.execute(
                "SELECT embedding::text,embedding_model,embedding_preprocessing_revision,"
                "legacy_attributes,object_type,dedupe_key,(SELECT count(*) "
                "FROM braincore.memory_revisions WHERE memory_id=%s) AS revisions "
                "FROM braincore.memories WHERE id=%s",
                (memory_id, memory_id),
            )
        ).fetchone()
    assert after == before
    current = await memory(client, memory_id)
    assert current["content"] == body["content"]
    assert parsed_date(current["valid_from"]) == target
    assert parsed_date(current["valid_until"]) == valid_until
    assert not await visible(client, body["scope"], memory_id, valid_until)
    assert await visible(client, body["scope"], memory_id, valid_until - timedelta(microseconds=1))


async def test_failed_batch_item_rolls_back_its_date_but_replays_successful_siblings(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = datetime.now(UTC) - timedelta(days=30)
    target = original + timedelta(days=60)
    rows = [await seed(client, original) for _ in range(3)]
    real_remember = api.remember_one

    async def fail_after_write(*args: Any) -> Any:
        result = await real_remember(*args)
        if args[-1].metadata.get("synthetic_failure"):
            await args[0].execute("SELECT 1 / 0")
        return result

    monkeypatch.setattr(api, "remember_one", fail_after_write)
    items = [
        {**body, "valid_from": target.isoformat(), "metadata": {"synthetic_failure": index == 1}}
        for index, (body, _) in enumerate(rows)
    ]
    items[1]["content"] = "Must roll back " + items[1]["content"]
    key = f"rollback-date-{uuid4()}"
    response = await client.post(
        "/v1/memories:batch", headers=auth_headers(idempotency=key), json={"items": items}
    )
    assert response.status_code == 202, response.text
    operation = (
        await client.get(
            f"/v1/operations/{response.json()['operation_id']}", headers=auth_headers()
        )
    ).json()
    assert [item["state"] for item in operation["items"]] == ["queued", "failed", "queued"]
    assert operation["items"][1]["error"]["code"] == "batch_item_failed"
    for index, (body, memory_id) in enumerate(rows):
        current = await memory(client, memory_id)
        assert parsed_date(current["valid_from"]) == (original if index == 1 else target)
        assert current["content"] == body["content"]
    counts = await write_counts()
    replay = await client.post(
        "/v1/memories:batch", headers=auth_headers(idempotency=key), json={"items": items}
    )
    assert replay.status_code == 202 and replay.json()["reused"] is True
    assert replay.json()["operation_id"] == response.json()["operation_id"]
    assert await write_counts() == counts


async def test_scoped_key_cannot_move_another_scope_reference_start(
    client: httpx.AsyncClient,
) -> None:
    original = datetime.now(UTC) - timedelta(days=30)
    body, memory_id = await seed(client, original)
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"date-key-{uuid4()}"),
        json={
            "name": "synthetic validity writer",
            "roles": ["read", "write"],
            "scope": {"user_id": "bob"},
        },
    )
    assert created.status_code == 201, created.text
    counts = await write_counts()
    rejected = await write(
        client,
        {**body, "valid_from": (original + timedelta(days=60)).isoformat()},
        "forbidden",
        headers=auth_headers(created.json()["api_key"], idempotency=f"forbidden-{uuid4()}"),
    )
    assert rejected.status_code == 403, rejected.text
    assert await write_counts() == counts
    assert parsed_date((await memory(client, memory_id))["valid_from"]) == original


async def test_nonreference_content_dedupe_keeps_initial_validity_and_metadata(
    client: httpx.AsyncClient,
) -> None:
    original = datetime.now(UTC) - timedelta(days=30)
    marker = uuid4().hex
    body = {
        "content": f"Content dedupe {marker}.",
        "scope": {"user_id": "alice", "run_id": marker},
        "valid_from": original.isoformat(),
        "metadata": {"version": "initial"},
    }
    first = await write(client, body, f"dedupe-first-{marker}")
    assert first.status_code == 202, first.text
    operation = (
        await client.get(f"/v1/operations/{first.json()['operation_id']}", headers=auth_headers())
    ).json()
    memory_id = operation["items"][0]["memory_id"]
    second = await write(
        client,
        {
            **body,
            "valid_from": (original + timedelta(days=60)).isoformat(),
            "metadata": {"version": "ignored by dedupe"},
        },
        f"dedupe-second-{marker}",
    )
    assert second.status_code == 202, second.text
    current = await memory(client, memory_id)
    assert parsed_date(current["valid_from"]) == original
    assert current["metadata"] == body["metadata"]

"""Unicode validation and mixed-item receipts against the disposable PostgreSQL fixture."""

import json
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from braincore_memory.db import connection

from .conftest import auth_headers
from .test_unicode_request_validation import (
    INVALID_LOCATIONS,
    NONFINITE_NUMBERS,
    assert_safe_error,
    invalid_item,
    nonfinite_item_json,
)


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("location", INVALID_LOCATIONS)
@pytest.mark.parametrize("invalid", ["synthetic\x00value", "synthetic\ud800value"])
async def test_malformed_unicode_batch_retains_siblings_and_replay_in_postgresql(
    client: httpx.AsyncClient, endpoint: str, location: str, invalid: str
) -> None:
    marker = uuid4().hex
    valid = {"content": f"Synthetic sibling 🧠 界 {marker}.", "scope": {"user_id": "alice"}}
    last = {**valid, "content": f"Another valid sibling 🧠 界 {marker}."}
    payload = {"items": [valid, invalid_item(location, invalid), last]}
    headers = {
        **auth_headers(idempotency=f"unicode-validation-{marker}"),
        "Content-Type": "application/json",
    }
    accepted = await client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=json.dumps(payload).encode("ascii")
    )
    assert accepted.status_code == 202, accepted.text
    replay = await client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=json.dumps(payload).encode("ascii")
    )
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    response = await client.get(
        f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
    )
    assert response.status_code == 200, response.text
    operation = response.json()
    assert [item["state"] for item in operation["items"]] == ["queued", "failed", "queued"]
    assert operation["failed_items"] == 1
    error = operation["items"][1]["error"]
    assert error["code"] == "validation_failed"
    assert_safe_error(error)

    payload["items"][1] = invalid_item(location, invalid + "changed")
    conflict = await client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=json.dumps(payload).encode("ascii")
    )
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["code"] == "idempotency_conflict"


async def test_supported_unicode_null_and_control_characters_round_trip(
    client: httpx.AsyncClient,
) -> None:
    content = f"Synthetic 🧠 界 café \x01\t\n{uuid4()}."
    payload = {
        "content": content,
        "scope": {"user_id": "alice"},
        "metadata": {"nested": [None, {"\x01": "🧠\t\n"}, 0.0, -1.25, 1e308]},
        "source_uri": None,
    }
    headers = auth_headers(idempotency=f"unicode-valid-controls-{uuid4()}")
    accepted = await client.post("/v1/ingestions", headers=headers, json=payload)
    assert accepted.status_code == 202, accepted.text
    replay = await client.post("/v1/ingestions", headers=headers, json=payload)
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    async with connection() as conn:
        row = await (
            await conn.execute(
                "SELECT content,metadata,source_uri FROM braincore.artifacts WHERE content=%s",
                (content,),
            )
        ).fetchone()
    assert row is not None
    assert row["content"] == content
    # JSONB expands decimal exponents, so psycopg can return an integer for 1e308.
    # Compare JSON numeric values rather than Python's binary-float approximation.
    expected_metadata = json.loads(
        json.dumps(payload["metadata"]), parse_float=Decimal, parse_int=Decimal
    )
    actual_metadata = json.loads(
        json.dumps(row["metadata"]), parse_float=Decimal, parse_int=Decimal
    )
    assert actual_metadata == expected_metadata
    assert row["source_uri"] is None


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("number", NONFINITE_NUMBERS)
async def test_nonfinite_batch_persists_item_failure_and_replay_in_postgresql(
    client: httpx.AsyncClient, endpoint: str, number: str
) -> None:
    valid = json.dumps(
        {"content": f"Synthetic finite sibling {uuid4()}.", "scope": {"user_id": "alice"}}
    )
    payload = '{"items":[' + valid + "," + nonfinite_item_json(number) + "]}"
    headers = {
        **auth_headers(idempotency=f"nonfinite-validation-{uuid4()}"),
        "Content-Type": "application/json",
    }
    accepted = await client.post(f"/v1/{endpoint}:batch", headers=headers, content=payload.encode())
    assert accepted.status_code == 202, accepted.text
    replay = await client.post(f"/v1/{endpoint}:batch", headers=headers, content=payload.encode())
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    response = await client.get(
        f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
    )
    assert response.status_code == 200, response.text
    operation = response.json()
    assert [item["state"] for item in operation["items"]] == ["queued", "failed"]
    error = operation["items"][1]["error"]
    assert error["code"] == "validation_failed"
    assert_safe_error(error)
    changed = '{"items":[' + valid + "," + nonfinite_item_json("0") + "]}"
    conflict = await client.post(f"/v1/{endpoint}:batch", headers=headers, content=changed.encode())
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["code"] == "idempotency_conflict"

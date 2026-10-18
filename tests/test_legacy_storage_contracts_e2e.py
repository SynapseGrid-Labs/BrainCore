"""Legacy validation is atomic and leaves rejected keys reusable in PostgreSQL."""

import hashlib
import json
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx
import pytest

from braincore_memory.db import connection

from .conftest import auth_headers
from .test_audit_reasons import BLANK_REASONS
from .test_legacy_storage_contracts import (
    INVALID_TEXT,
    LEGACY_ACTIONS,
    TEXT_LOCATIONS,
    invalid_legacy_body,
    legacy_body,
)
from .test_memory_input_contracts_e2e import write_counts
from .test_unicode_request_validation import NONFINITE_NUMBERS, assert_safe_error

INVALID_CASES = (
    [f"blank:{action}:{index}" for action in LEGACY_ACTIONS for index in range(len(BLANK_REASONS))]
    + [
        f"text:{location}:{index}"
        for location in TEXT_LOCATIONS
        for index in range(len(INVALID_TEXT))
    ]
    + [f"number:{number}" for number in NONFINITE_NUMBERS]
)


async def legacy_snapshot(native_id: str) -> dict[str, Any]:
    async with connection() as conn:
        row = await (
            await conn.execute(
                "SELECT row_to_json(m)::jsonb AS memory,row_to_json(a)::jsonb AS attributes,"
                "row_to_json(i)::jsonb AS aliases "
                "FROM braincore.memories m LEFT JOIN braincore.legacy_client_attributes a "
                "ON a.memory_id=m.id LEFT JOIN braincore.legacy_client_ids i ON i.memory_id=m.id "
                "WHERE m.id=%s",
                (native_id,),
            )
        ).fetchone()
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM braincore.mutation_receipts) AS mutations,"
                "(SELECT count(*) FROM braincore.audit_events) AS audits,"
                "(SELECT count(*) FROM braincore.lifecycle_events) AS lifecycle,"
                "(SELECT count(*) FROM braincore.legacy_client_attributes) AS extensions,"
                "(SELECT count(*) FROM braincore.legacy_client_ids) AS aliases,"
                "(SELECT count(*) FROM braincore.legacy_client_controls) AS controls,"
                "(SELECT count(*) FROM braincore.legacy_native_authority) AS authority"
            )
        ).fetchone()
    assert row is not None and counts is not None
    return {**row, **counts, "work": await write_counts()}


def normalized_json(value: Any) -> Any:
    # JSONB can expand 1e308 to an integer; compare JSON numeric values exactly.
    return json.loads(json.dumps(value), parse_int=Decimal, parse_float=Decimal)


async def assert_exact_entity(result: dict[str, Any], body: dict[str, Any]) -> None:
    assert result["object_id"] == body["object_id"]
    async with connection() as conn:
        row = await (
            await conn.execute(
                "SELECT m.content,m.legacy_attributes AS attributes,"
                "m.legacy_attributes_ready,i.external_id FROM braincore.memories m "
                "JOIN braincore.legacy_client_ids i ON i.memory_id=m.id WHERE m.id=%s",
                (result["native_id"],),
            )
        ).fetchone()
    assert row is not None
    assert row["legacy_attributes_ready"] is True
    assert isinstance(row["attributes"], dict)
    assert row["content"] == body["fields"]["canonical_name"]
    assert str(row["external_id"]) == body["object_id"]
    for name, value in body["fields"].items():
        assert normalized_json(row["attributes"][name]) == normalized_json(value)


@pytest.mark.parametrize("mode", ["mutate", "batch"])
@pytest.mark.parametrize("case", INVALID_CASES)
async def test_invalid_legacy_request_has_no_postgresql_effects_and_key_is_reusable(
    client: httpx.AsyncClient, mode: str, case: str
) -> None:
    marker = uuid4().hex
    seed = legacy_body()
    seed["fields"]["canonical_name"] = f"Original synthetic entity {marker}."
    accepted_seed = await client.post(
        "/v1/legacy/objects:mutate",
        headers=auth_headers(idempotency=f"legacy-seed-{marker}"),
        json=seed,
    )
    assert accepted_seed.status_code == 200, accepted_seed.text
    native_id = accepted_seed.json()["native_id"]
    action = case.split(":")[1] if case.startswith("blank:") else "upsert"
    if action == "restore":
        suppressed = await client.post(
            "/v1/legacy/objects:mutate",
            headers=auth_headers(idempotency=f"legacy-suppress-{marker}"),
            json={**seed, "fields": {}, "action": "suppress"},
        )
        assert suppressed.status_code == 200, suppressed.text

    reason = f" \tSynthetic legacy reason 🧠 界 café {marker}.\u2003\n "
    valid = {**seed, "reason": reason, "action": action}
    valid["fields"] = {
        "entity_type": "project",
        "canonical_name": f" \tCorrected synthetic 🧠 界 café \x01 {marker}.\n ",
        "aliases": [" \t🧠界 café.\n ", "\x01"],
        "attrs": {"nested": [None, {"\x01": "🧠\t\n"}, 0.0, -1.25, 1e308]},
    }
    if action != "upsert":
        # Lifecycle actions retain the accepted canonical text and subtype fields.
        valid["fields"] = {}
    invalid = {**valid, "fields": dict(valid["fields"])}
    if case.startswith("blank:"):
        invalid["reason"] = BLANK_REASONS[int(case.split(":")[2])]
    elif case.startswith("text:"):
        _, location, index = case.split(":")
        malformed = invalid_legacy_body(location, INVALID_TEXT[int(index)])
        if location == "reason":
            invalid["reason"] = malformed["reason"]
        else:
            invalid["fields"].update(malformed["fields"])
    else:
        invalid["fields"]["attrs"] = {"nested": [{"value": "__number__"}]}

    sibling = legacy_body(reason)
    sibling["fields"]["canonical_name"] = f"Valid sibling 🧠 界 {marker}."
    body = {"items": [sibling, invalid]} if mode == "batch" else invalid
    encoded = json.dumps(body)
    if case.startswith("number:"):
        encoded = encoded.replace('"__number__"', case.split(":", 1)[1])
    headers = {
        **auth_headers(idempotency=f"legacy-contract-{marker}"),
        "Content-Type": "application/json",
    }
    before = await legacy_snapshot(native_id)
    rejected = await client.post(
        f"/v1/legacy/objects:{mode}", headers=headers, content=encoded.encode("ascii")
    )
    assert rejected.status_code == 422, rejected.text
    assert rejected.json()["error"]["code"] == "validation_failed"
    assert_safe_error(rejected.json()["error"])
    assert await legacy_snapshot(native_id) == before

    # The valid sibling was never dispatched; correcting the same request key
    # commits it together with the repaired item under the existing atomic contract.
    corrected = {"items": [sibling, valid]} if mode == "batch" else valid
    accepted = await client.post(f"/v1/legacy/objects:{mode}", headers=headers, json=corrected)
    assert accepted.status_code == 200, accepted.text
    results = accepted.json()["results"] if mode == "batch" else [accepted.json()]
    result = results[-1]
    assert result["native_id"] == native_id
    assert result["object_id"] == seed["object_id"]
    assert result["state"] == {"suppress": "suppressed", "retire": "retired"}.get(action, "active")
    if mode == "batch":
        await assert_exact_entity(results[0], sibling)
    if action == "upsert":
        await assert_exact_entity(result, valid)
    else:
        assert (await legacy_snapshot(native_id))["memory"]["content"] == seed["fields"][
            "canonical_name"
        ]

    async with connection() as conn:
        audit = await (
            await conn.execute(
                "SELECT reason FROM braincore.memory_revisions WHERE memory_id=%s "
                "ORDER BY created_at DESC LIMIT 1"
                if action == "upsert"
                else "SELECT reason FROM braincore.lifecycle_events WHERE memory_id=%s "
                "ORDER BY created_at DESC LIMIT 1",
                (native_id,),
            )
        ).fetchone()
        receipt = await (
            await conn.execute(
                "SELECT request_hash,response FROM braincore.mutation_receipts WHERE key=%s",
                (f"legacy-contract-{marker}",),
            )
        ).fetchone()
    assert audit is not None and audit["reason"] == reason
    assert receipt is not None and receipt["response"] == accepted.json()
    hash_body: dict[str, Any] = (
        {"batch": corrected}
        if mode == "batch"
        else {
            "type": valid["object_type"],
            "fields": valid["fields"],
            "id": valid["object_id"],
            "action": valid["action"],
            "reason": reason,
        }
    )
    assert isinstance(receipt["request_hash"], bytes | bytearray | memoryview)
    assert (
        bytes(receipt["request_hash"])
        == hashlib.sha256(
            json.dumps(hash_body, sort_keys=True, separators=(",", ":")).encode()
        ).digest()
    )
    after = await legacy_snapshot(native_id)
    replay = await client.post(f"/v1/legacy/objects:{mode}", headers=headers, json=corrected)
    assert replay.status_code == 200, replay.text
    assert replay.json() == accepted.json()
    assert await legacy_snapshot(native_id) == after

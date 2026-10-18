"""Unsupported request text must fail safely before hashing or PostgreSQL writes."""

import json
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request

from braincore_memory import api
from braincore_memory.auth import Principal
from braincore_memory.models import IngestRequest
from braincore_memory.service import begin_idempotent_operation, canonical_hash

INVALID_LOCATIONS = (
    "content",
    "scope",
    "nested-metadata-value",
    "nested-metadata-key",
    "extra-value",
    "extra-key",
    "nested-extra-key",
)
NONFINITE_NUMBERS = ("NaN", "Infinity", "-Infinity", "1e400")


def nonfinite_item_json(number: str) -> str:
    return (
        '{"content":"Synthetic finite validation.","scope":{"user_id":"alice"},'
        '"metadata":{"nested":[{"number":' + number + "}]}}"
    )


def invalid_item(location: str, invalid: str) -> dict[str, Any]:
    item: dict[str, Any] = {"content": "Synthetic valid statement.", "scope": {"user_id": "alice"}}
    if location == "content":
        item["content"] = invalid
    elif location == "scope":
        item["scope"]["user_id"] = invalid
    elif location == "nested-metadata-value":
        item["metadata"] = {"nested": [{"value": invalid}]}
    elif location == "nested-metadata-key":
        item["metadata"] = {"nested": [{invalid: None}]}
    elif location == "extra-value":
        item["unsupported"] = invalid
    elif location == "extra-key":
        item[invalid] = "synthetic"
    elif location == "nested-extra-key":
        item["scope"][invalid] = "synthetic"
    else:
        raise AssertionError(location)
    return item


def assert_safe_error(error: dict[str, Any]) -> None:
    serialized = json.dumps(error, ensure_ascii=False, allow_nan=False).encode("utf-8")
    assert b"\x00" not in serialized
    for detail in error["details"]["errors"]:
        assert set(detail) == {"type", "loc", "msg"}
        assert "input" not in detail and "ctx" not in detail
        for value in (detail["type"], detail["msg"], *detail["loc"]):
            if isinstance(value, str):
                assert "\x00" not in value
                value.encode("utf-8")


@pytest_asyncio.fixture
async def validation_client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    monkeypatch.setitem(api.app.dependency_overrides, api.require_write, lambda: principal)

    def forbidden_connection() -> Any:
        raise AssertionError("invalid request text must not reach storage")

    monkeypatch.setattr(api, "connection", forbidden_connection)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        yield client


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("location", INVALID_LOCATIONS)
@pytest.mark.parametrize("invalid", ["synthetic\x00value", "synthetic\ud800value", "\udfff"])
async def test_unsupported_text_returns_safe_422_without_storage(
    validation_client: httpx.AsyncClient, endpoint: str, location: str, invalid: str
) -> None:
    response = await validation_client.post(
        f"/v1/{endpoint}",
        headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-text"},
        content=json.dumps(invalid_item(location, invalid)).encode("ascii"),
    )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_failed"
    assert_safe_error(response.json()["error"])


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -float("inf")])
async def test_nonfinite_validation_input_does_not_break_error_serialization(
    validation_client: httpx.AsyncClient, invalid: float
) -> None:
    response = await validation_client.post(
        "/v1/ingestions",
        headers={"Content-Type": "application/json", "Idempotency-Key": "nonfinite"},
        content=json.dumps({"content": invalid, "scope": {"user_id": "alice"}}).encode(),
    )
    assert response.status_code == 422, response.text
    assert_safe_error(response.json()["error"])


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("number", NONFINITE_NUMBERS)
async def test_nonfinite_nested_metadata_is_rejected_before_storage(
    validation_client: httpx.AsyncClient, endpoint: str, number: str
) -> None:
    response = await validation_client.post(
        f"/v1/{endpoint}",
        headers={"Content-Type": "application/json", "Idempotency-Key": "nonfinite-metadata"},
        content=nonfinite_item_json(number).encode(),
    )
    assert response.status_code == 422, response.text
    assert_safe_error(response.json()["error"])


async def test_validation_errors_project_safe_locations_messages_and_types() -> None:
    error = RequestValidationError(
        [
            {
                "type": "synthetic\ud800",
                "loc": ("body", "unknown\x00\ud800", 3),
                "msg": "Synthetic 界 \udfff\x00 diagnostic.",
                "input": {"secret\ud800": float("nan")},
                "ctx": {"private\x00": "\ud800"},
                "url": "not-public-input",
            }
        ]
    )
    response = await api.validation_error(Request({"type": "http"}), error)
    assert response.status_code == 422
    detail = json.loads(response.body)["error"]
    assert_safe_error(detail)
    assert detail["details"]["errors"] == [
        {
            "type": "synthetic\\ud800",
            "loc": ["body", "unknown\\u0000\\ud800", 3],
            "msg": "Synthetic 界 \\udfff\\u0000 diagnostic.",
        }
    ]


def test_supported_text_controls_and_json_null_are_not_changed() -> None:
    payload = {
        "content": "Synthetic 🧠 界 café \x01\t\n statement.",
        "scope": {"user_id": "alice"},
        "metadata": {"nested": [None, {"\x01": "🧠\t\n"}, 0.0, -1.25, 1e308]},
        "source_uri": None,
    }
    model = IngestRequest.model_validate(payload)
    assert model.content == payload["content"]
    assert model.metadata == payload["metadata"]
    assert model.source_uri is None


class Cursor:
    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self.row = row

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row


class OperationStore:
    """Keep actual operation hashing/replay while replacing the database transport."""

    def __init__(self) -> None:
        self.receipts: dict[str, dict[str, Any]] = {}
        self.errors: list[dict[str, Any]] = []
        self.operation_id = uuid4()

    def transaction(self) -> Any:
        return nullcontext()

    async def execute(self, query: str, values: tuple[Any, ...] = ()) -> Cursor:
        if "SELECT request_hash, operation_id" in query:
            return Cursor(self.receipts.get(values[2]))
        if "INSERT INTO braincore.operations" in query:
            self.operation_id = uuid4()
            return Cursor({"id": self.operation_id})
        if "INSERT INTO braincore.idempotency_records" in query:
            self.receipts[values[2]] = {"request_hash": values[3], "operation_id": values[4]}
        if "INSERT INTO braincore.operation_items" in query:
            error = json.loads(values[3])
            assert_safe_error(error)
            self.errors.append(error)
        return Cursor({"state": "queued"})


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("location", INVALID_LOCATIONS)
@pytest.mark.parametrize("invalid", ["synthetic\x00value", "synthetic\ud800value"])
async def test_mixed_batch_preserves_valid_item_safe_failure_and_replay(
    validation_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    location: str,
    invalid: str,
) -> None:
    store = OperationStore()
    monkeypatch.setattr(api, "connection", lambda: nullcontext(store))
    handler = AsyncMock()
    monkeypatch.setattr(
        api, "remember_one" if endpoint == "memories" else "enqueue_ingestion_one", handler
    )
    payload = {
        "items": [
            {"content": "A valid sibling 🧠 界.", "scope": {"user_id": "alice"}},
            invalid_item(location, invalid),
        ]
    }
    headers = {"Content-Type": "application/json", "Idempotency-Key": "mixed-unicode"}
    accepted = await validation_client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=json.dumps(payload).encode("ascii")
    )
    assert accepted.status_code == 202, accepted.text
    assert len(store.errors) == 1
    assert store.errors[0]["code"] == "validation_failed"
    handler.assert_awaited_once()
    assert handler.await_args.args[3] == 0
    assert handler.await_args.args[-1].content == payload["items"][0]["content"]
    replay = await validation_client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=json.dumps(payload).encode("ascii")
    )
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    handler.assert_awaited_once()
    assert len(store.errors) == 1


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("number", NONFINITE_NUMBERS)
async def test_nonfinite_mixed_batch_preserves_valid_item_and_replay(
    validation_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    number: str,
) -> None:
    store = OperationStore()
    monkeypatch.setattr(api, "connection", lambda: nullcontext(store))
    handler = AsyncMock()
    monkeypatch.setattr(
        api, "remember_one" if endpoint == "memories" else "enqueue_ingestion_one", handler
    )
    valid = json.dumps({"content": "Synthetic valid sibling.", "scope": {"user_id": "alice"}})
    payload = '{"items":[' + valid + "," + nonfinite_item_json(number) + "]}"
    headers = {"Content-Type": "application/json", "Idempotency-Key": "nonfinite-batch"}
    accepted = await validation_client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=payload.encode()
    )
    assert accepted.status_code == 202, accepted.text
    assert len(store.errors) == 1
    assert store.errors[0]["code"] == "validation_failed"
    handler.assert_awaited_once()
    replay = await validation_client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=payload.encode()
    )
    assert replay.status_code == 202, replay.text
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    assert replay.json()["reused"] is True
    handler.assert_awaited_once()
    assert len(store.errors) == 1
    changed = '{"items":[' + valid + "," + nonfinite_item_json("0") + "]}"
    conflict = await validation_client.post(
        f"/v1/{endpoint}:batch", headers=headers, content=changed.encode()
    )
    assert conflict.status_code == 409, conflict.text


async def test_valid_unicode_operation_hash_and_existing_replay_are_unchanged() -> None:
    body = {
        "items": [
            {
                "content": "🧠 café 界",
                "scope": {"user_id": "alice"},
                "metadata": {"nested": [None, "\x01"]},
            }
        ]
    }
    expected = bytes.fromhex("5e1c54f4beb61e18d76a01dad34b8bbdb0754707226fd15504355ff051011b8d")
    assert canonical_hash({"kind": "ingest_batch", "body": body}) == expected
    store = OperationStore()
    operation_id = uuid4()
    store.receipts["existing"] = {"request_hash": expected, "operation_id": operation_id}
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    result = await begin_idempotent_operation(store, principal, "existing", body, "ingest_batch", 1)  # type: ignore[arg-type]
    assert result == (operation_id, True)


async def test_invalid_raw_unicode_hash_distinguishes_replay_and_changed_requests() -> None:
    store = OperationStore()
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    body = {"items": [invalid_item("extra-value", "\ud800")]}
    operation_id, reused = await begin_idempotent_operation(
        store, principal, "bad", body, "ingest_batch", 1
    )  # type: ignore[arg-type]
    assert not reused
    replay = await begin_idempotent_operation(store, principal, "bad", body, "ingest_batch", 1)  # type: ignore[arg-type]
    assert replay == (operation_id, True)
    for replacement in ("\ud801", "\\ud800", "�", "🧠", "\x00"):
        changed = {"items": [invalid_item("extra-value", replacement)]}
        with pytest.raises(HTTPException) as error:
            await begin_idempotent_operation(store, principal, "bad", changed, "ingest_batch", 1)  # type: ignore[arg-type]
        assert error.value.status_code == 409

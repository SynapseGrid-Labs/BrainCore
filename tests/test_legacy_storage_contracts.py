"""Legacy requests reject unstorable values before reserving mutation receipts."""

import hashlib
import json
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
from pydantic import ValidationError

from braincore_memory import api, legacy_api
from braincore_memory.auth import Principal, authenticate
from braincore_memory.legacy_api import LegacyBatchRequest, LegacyMutationRequest
from braincore_memory.legacy_types import FIELD_TYPES

from .test_audit_reasons import BLANK_REASONS, VALID_REASONS
from .test_unicode_request_validation import NONFINITE_NUMBERS, assert_safe_error

LEGACY_ACTIONS = ("upsert", "suppress", "retire", "restore")
TEXT_LOCATIONS = ("reason", "text-field", "field-key", "nested-key", "nested-value")
INVALID_TEXT = ("synthetic\x00value", "synthetic\ud800value", "synthetic\udfffvalue")


def legacy_body(reason: Any = "Synthetic legacy mutation.") -> dict[str, Any]:
    return {
        "object_type": "entity",
        "object_id": str(uuid4()),
        "action": "upsert",
        "reason": reason,
        "fields": {"entity_type": "project", "canonical_name": "Synthetic legacy entity."},
    }


def invalid_legacy_body(location: str, invalid: Any) -> dict[str, Any]:
    body = legacy_body()
    if location == "reason":
        body["reason"] = invalid
    elif location == "text-field":
        body["fields"]["canonical_name"] = invalid
    elif location == "field-key":
        body["fields"][invalid] = "synthetic"
    elif location == "nested-key":
        body["fields"]["attrs"] = {"nested": [{invalid: None}]}
    elif location == "nested-value":
        body["fields"]["attrs"] = {"nested": [{"value": invalid}]}
    else:
        raise AssertionError(location)
    return body


def request_body(mode: str, body: dict[str, Any]) -> dict[str, Any]:
    # A valid sibling must not start work when another typed legacy item is invalid.
    return {"items": [legacy_body(), body]} if mode == "batch" else body


def validate_body(mode: str, body: dict[str, Any]) -> Any:
    model = LegacyBatchRequest if mode == "batch" else LegacyMutationRequest
    return model.model_validate(request_body(mode, body))


@pytest.mark.parametrize("action", LEGACY_ACTIONS)
@pytest.mark.parametrize("reason", BLANK_REASONS)
@pytest.mark.parametrize("mode", ["mutate", "batch"])
def test_legacy_models_reject_blank_audit_reasons(mode: str, action: str, reason: str) -> None:
    body = {**legacy_body(reason), "action": action}
    with pytest.raises(ValidationError) as error:
        validate_body(mode, body)
    assert error.value.errors()[0]["loc"] == (
        ("items", 1, "reason") if mode == "batch" else ("reason",)
    )


@pytest.mark.parametrize("location", TEXT_LOCATIONS)
@pytest.mark.parametrize("invalid", INVALID_TEXT)
@pytest.mark.parametrize("mode", ["mutate", "batch"])
def test_legacy_models_reject_unstorable_strings(mode: str, location: str, invalid: str) -> None:
    with pytest.raises(ValidationError, match="NUL or surrogate"):
        validate_body(mode, invalid_legacy_body(location, invalid))


@pytest.mark.parametrize("number", NONFINITE_NUMBERS)
@pytest.mark.parametrize("mode", ["mutate", "batch"])
def test_legacy_models_reject_nonfinite_json_numbers(mode: str, number: str) -> None:
    body = invalid_legacy_body("nested-value", "__number__")
    encoded = json.dumps(request_body(mode, body)).replace('"__number__"', number)
    model = LegacyBatchRequest if mode == "batch" else LegacyMutationRequest
    with pytest.raises(ValidationError, match="finite"):
        model.model_validate(json.loads(encoded))


@pytest.mark.parametrize("action", LEGACY_ACTIONS)
@pytest.mark.parametrize("reason", VALID_REASONS)
def test_valid_legacy_values_keep_exact_unicode_reason_and_hash(action: str, reason: str) -> None:
    body = legacy_body(reason)
    body["action"] = action
    body["fields"]["canonical_name"] = " \tSynthetic 🧠 界 café \x01.\n "
    body["fields"]["aliases"] = ["🧠", "café", "\x01\t\n"]
    body["fields"]["attrs"] = {"nested": [None, {"\x01": "🧠\t\n"}, 0.0, -1.25, 1e308]}
    model = LegacyMutationRequest.model_validate(body)
    assert model.reason == reason
    assert model.model_dump(mode="json") == body

    def encoded(value: Any) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    assert (
        hashlib.sha256(encoded(model.model_dump(mode="json"))).digest()
        == hashlib.sha256(encoded(body)).digest()
    )
    batch = LegacyBatchRequest(items=[model])
    assert batch.items[0] is model
    assert batch.model_dump(mode="json") == {"items": [body]}


@pytest.mark.parametrize("object_type", sorted(FIELD_TYPES))
def test_every_supported_legacy_kind_uses_same_validation_boundary(object_type: str) -> None:
    assert LegacyMutationRequest(object_type=object_type).reason == "legacy client mutation"
    with pytest.raises(ValidationError):
        LegacyMutationRequest(object_type=object_type, fields={"nested": [{"bad\x00": None}]})
    with pytest.raises(ValidationError):
        LegacyMutationRequest(object_type=object_type, reason="\u2003\t")


@pytest.mark.parametrize("reason", ["", None, 42, [], "x" * 1001])
def test_legacy_reason_existing_type_and_length_limits_remain(reason: Any) -> None:
    with pytest.raises(ValidationError) as error:
        LegacyMutationRequest.model_validate(legacy_body(reason))
    assert error.value.errors()[0]["loc"] == ("reason",)


@pytest.mark.parametrize(
    ("changes", "location", "error_type"),
    [
        ({"unknown": "synthetic"}, "unknown", "extra_forbidden"),
        ({"object_type": "unsupported"}, "object_type", "value_error"),
        ({"object_type": 42}, "object_type", "string_type"),
        ({"action": "correct"}, "action", "literal_error"),
        ({"object_id": "invalid"}, "object_id", "uuid_parsing"),
        ({"fields": []}, "fields", "dict_type"),
        ({"fields": {str(i): None for i in range(111)}}, "fields", "too_long"),
    ],
)
def test_legacy_existing_request_shape_rejections_remain(
    changes: dict[str, Any], location: str, error_type: str
) -> None:
    with pytest.raises(ValidationError) as error:
        LegacyMutationRequest.model_validate({**legacy_body(), **changes})
    assert error.value.errors()[0]["loc"] == (location,)
    assert error.value.errors()[0]["type"] == error_type


@pytest.mark.parametrize("mode", ["mutate", "batch"])
@pytest.mark.parametrize(
    "invalid_body",
    [
        *[
            {**legacy_body(reason), "action": action}
            for action in LEGACY_ACTIONS
            for reason in BLANK_REASONS
        ],
        *[
            invalid_legacy_body(location, invalid)
            for location in TEXT_LOCATIONS
            for invalid in INVALID_TEXT
        ],
        *[invalid_legacy_body("nested-value", float(number)) for number in NONFINITE_NUMBERS],
    ],
    ids=[
        f"blank-{action}-{index}"
        for action in LEGACY_ACTIONS
        for index in range(len(BLANK_REASONS))
    ]
    + [f"{location}-{index}" for location in TEXT_LOCATIONS for index in range(len(INVALID_TEXT))]
    + [f"nonfinite-{number}" for number in NONFINITE_NUMBERS],
)
async def test_legacy_http_rejects_before_storage_or_receipt(
    monkeypatch: pytest.MonkeyPatch, mode: str, invalid_body: dict[str, Any]
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset({"write", "admin"}))
    authentications: list[Principal] = []
    storage_attempts: list[str] = []

    def authenticated_actor() -> Principal:
        authentications.append(actor)
        return actor

    def forbidden_storage(*args: Any, **kwargs: Any) -> Any:
        storage_attempts.append("storage or receipt")
        raise AssertionError("invalid legacy requests must fail before storage or receipts")

    overrides: dict[Callable[..., Any], Callable[..., Any]] = dict(api.app.dependency_overrides)
    overrides[authenticate] = authenticated_actor
    monkeypatch.setattr(api.app, "dependency_overrides", overrides)
    monkeypatch.setattr(psycopg, "connect", forbidden_storage)
    monkeypatch.setattr(legacy_api, "_prior_receipt", forbidden_storage)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/v1/legacy/objects:{mode}",
            headers={
                "Authorization": "Bearer synthetic-key",
                "Idempotency-Key": "legacy-invalid",
                "Content-Type": "application/json",
            },
            content=json.dumps(request_body(mode, invalid_body)).encode("ascii"),
        )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_failed"
    assert_safe_error(response.json()["error"])
    assert authentications == [actor]
    assert storage_attempts == []


@pytest.mark.parametrize("mode", ["mutate", "batch"])
async def test_legacy_http_overflowing_finite_json_literal_rejected(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    # Unlike Infinity, 1e400 is valid JSON syntax; parsing overflows a Python float.
    actor = Principal(uuid4(), uuid4(), frozenset({"write", "admin"}))
    overrides: dict[Callable[..., Any], Callable[..., Any]] = dict(api.app.dependency_overrides)
    overrides[authenticate] = lambda: actor
    monkeypatch.setattr(api.app, "dependency_overrides", overrides)
    attempts: list[str] = []

    def forbidden_storage(*args: Any, **kwargs: Any) -> Any:
        attempts.append("storage")
        raise AssertionError("overflowing JSON numbers must not reach storage")

    monkeypatch.setattr(psycopg, "connect", forbidden_storage)
    body = request_body(mode, invalid_legacy_body("nested-value", "__number__"))
    encoded = json.dumps(body).replace('"__number__"', "1e400")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/v1/legacy/objects:{mode}",
            headers={"Content-Type": "application/json", "Idempotency-Key": "overflow"},
            content=encoded.encode("ascii"),
        )
    assert response.status_code == 422, response.text
    assert_safe_error(response.json()["error"])
    assert attempts == []

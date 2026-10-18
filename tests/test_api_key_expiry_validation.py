"""API-key expiry uses aware instants and rejects unusable keys before writes."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from braincore_memory import api, models
from braincore_memory.auth import Principal, authenticate
from braincore_memory.models import ApiKeyCreateRequest

NOW = datetime(2030, 6, 15, 12, tzinfo=UTC)
INVALID_EXPIRIES = (
    "2000-01-01T00:00:00Z",
    "2030-06-15T12:00:00Z",
    "2030-06-15T17:30:00+05:30",
    "2030-06-15T11:59:59.999999Z",
    "2099-01-01T00:00:00",
    "2099-01-01",
    "not-a-timestamp",
    "10000-01-01T00:00:00Z",
    "2099-01-01T00:00:00+25:00",
    "9999-12-31T23:59:59-01:00",
    1e100,
    True,
    [],
    {},
)


@pytest.fixture
def fixed_expiry_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(models, "datetime", FrozenDatetime)


@pytest.mark.parametrize("expires_at", INVALID_EXPIRIES)
def test_invalid_expiry_model_rejects_at_expiry_field(
    fixed_expiry_clock: None, expires_at: Any
) -> None:
    with pytest.raises(ValidationError) as error:
        ApiKeyCreateRequest.model_validate(
            {"name": "synthetic-expiry", "roles": ["admin"], "expires_at": expires_at}
        )
    assert error.value.errors()[0]["loc"] == ("expires_at",)


@pytest.mark.parametrize(
    "expires_at",
    [
        "2030-06-15T12:00:00.000001Z",
        "2030-06-15T17:30:00.000001+05:30",
        "2030-06-15T05:00:00.000001-07:00",
        datetime(2030, 6, 15, 12, 0, 0, 1, tzinfo=UTC),
    ],
)
def test_future_offsets_preserve_instant_and_model_serialization(
    fixed_expiry_clock: None, expires_at: Any
) -> None:
    result = ApiKeyCreateRequest.model_validate(
        {"name": "synthetic-expiry", "roles": ["admin"], "expires_at": expires_at}
    )
    assert result.expires_at == NOW + timedelta(microseconds=1)
    if isinstance(expires_at, str):
        assert result.model_dump(mode="json")["expires_at"] == expires_at


@pytest.mark.parametrize("include_null", [False, True])
def test_omitted_and_null_expiry_remain_nonexpiring(include_null: bool) -> None:
    body: dict[str, Any] = {"name": "synthetic-expiry", "roles": ["admin"]}
    if include_null:
        body["expires_at"] = None
    assert ApiKeyCreateRequest.model_validate(body).expires_at is None


@pytest.mark.parametrize("expires_at", INVALID_EXPIRIES)
async def test_invalid_expiry_http_rejects_without_write_or_receipt_access(
    monkeypatch: pytest.MonkeyPatch, fixed_expiry_clock: None, expires_at: Any
) -> None:
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    monkeypatch.setitem(api.app.dependency_overrides, authenticate, lambda: principal)
    attempts: list[str] = []

    def forbidden_connection() -> Any:
        attempts.append("write storage")
        raise AssertionError("invalid expiry must be rejected before write storage")

    monkeypatch.setattr(api, "connection", forbidden_connection)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/v1/admin/api-keys",
            headers={"Authorization": "Bearer synthetic-key", "Idempotency-Key": "expiry-check"},
            json={"name": "synthetic-expiry", "roles": ["admin"], "expires_at": expires_at},
        )
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "validation_failed"
    assert error["details"]["errors"][0]["loc"] == ["body", "expires_at"]
    assert attempts == []
    assert "input" not in error["details"]["errors"][0]


def test_expiry_openapi_shape_stays_optional_datetime() -> None:
    field = api.app.openapi()["components"]["schemas"]["ApiKeyCreateRequest"]["properties"][
        "expires_at"
    ]
    assert field == {
        "anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}],
        "title": "Expires At",
    }

"""Audit reasons require non-whitespace content without normalizing accepted text."""

from typing import Any
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from braincore_memory import api
from braincore_memory.auth import Principal, authenticate
from braincore_memory.models import LifecycleRequest, RevisionRequest
from braincore_memory.service import canonical_hash

AUDIT_MODES = ("revision", "suppress", "retire", "restore", "promote")
BLANK_REASONS = (" ", "\t\r\n", "\u2003\u00a0")
VALID_REASONS = (
    "User corrected the record.",
    " \tUser corrected 🧠 界 café.\u2003\n ",
    " \x01 ",
    " " * 498 + "🧠界" + " " * 500,
)


def audit_body(mode: str, reason: Any) -> dict[str, Any]:
    if mode == "revision":
        return {"content": " \tCorrected Unicode 🧠 界 content.\n ", "reason": reason}
    return {"action": mode, "reason": reason}


def audit_model(mode: str) -> type[RevisionRequest] | type[LifecycleRequest]:
    return RevisionRequest if mode == "revision" else LifecycleRequest


@pytest.mark.parametrize("mode", AUDIT_MODES)
@pytest.mark.parametrize("reason", BLANK_REASONS)
def test_audit_reason_models_reject_whitespace_only(mode: str, reason: str) -> None:
    with pytest.raises(ValidationError) as error:
        audit_model(mode).model_validate(audit_body(mode, reason))
    assert error.value.errors()[0]["loc"] == ("reason",)


@pytest.mark.parametrize("mode", AUDIT_MODES)
@pytest.mark.parametrize("reason", VALID_REASONS)
def test_nonblank_audit_reasons_keep_exact_text_and_request_hash(mode: str, reason: str) -> None:
    body = audit_body(mode, reason)
    model = audit_model(mode).model_validate(body)
    assert model.reason == reason
    assert model.model_dump() == body
    memory_id = str(uuid4())
    assert canonical_hash({"memory_id": memory_id, "body": model.model_dump()}) == canonical_hash(
        {"memory_id": memory_id, "body": body}
    )


@pytest.mark.parametrize("mode", AUDIT_MODES)
@pytest.mark.parametrize("reason", ["", None, 42, [], "x" * 1001])
def test_audit_reason_existing_type_and_length_limits_remain(mode: str, reason: Any) -> None:
    with pytest.raises(ValidationError) as error:
        audit_model(mode).model_validate(audit_body(mode, reason))
    assert error.value.errors()[0]["loc"] == ("reason",)


@pytest.mark.parametrize("mode", AUDIT_MODES)
@pytest.mark.parametrize("reason", BLANK_REASONS)
async def test_blank_audit_reason_http_rejects_before_storage_or_idempotency(
    monkeypatch: pytest.MonkeyPatch, mode: str, reason: str
) -> None:
    principal = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    monkeypatch.setitem(api.app.dependency_overrides, authenticate, lambda: principal)
    storage_attempts: list[str] = []

    def forbidden_connection() -> Any:
        storage_attempts.append("connection")
        raise AssertionError("blank audit reasons must not access write storage")

    async def forbidden_idempotency(*args: Any, **kwargs: Any) -> Any:
        storage_attempts.append("idempotency")
        raise AssertionError("blank audit reasons must not reserve an idempotency key")

    monkeypatch.setattr(api, "connection", forbidden_connection)
    monkeypatch.setattr(api, "begin_idempotent_operation", forbidden_idempotency)
    path = f"/v1/memories/{uuid4()}/" + ("revisions" if mode == "revision" else "lifecycle")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.request(
            "POST" if mode == "revision" else "PATCH",
            path,
            headers={"Authorization": "Bearer synthetic-key", "Idempotency-Key": "audit-reason"},
            json=audit_body(mode, reason),
        )
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "validation_failed"
    assert error["details"]["errors"][0]["loc"] == ["body", "reason"]
    assert storage_attempts == []

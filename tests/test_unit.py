import pytest
from pydantic import ValidationError

from braincore_memory import BrainCore
from braincore_memory.api import _request_id
from braincore_memory.config import Settings
from braincore_memory.embedder import DIMENSIONS, feature_hash
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.models import ApiKeyCreateRequest, EvidenceInput, RememberRequest, Scope
from braincore_memory.service import canonical_hash


def test_feature_hash_is_stable_and_normalized() -> None:
    first = feature_hash("Alice prefers Python")
    second = feature_hash("Alice prefers Python")
    assert first == second
    assert len(first) == DIMENSIONS
    assert abs(sum(value * value for value in first) - 1.0) < 1e-9


def test_scope_requires_at_least_one_dimension() -> None:
    assert Scope(user_id="alice").user_id == "alice"


def test_request_models_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        RememberRequest.model_validate(
            {
                "content": "Alice prefers dark mode.",
                "scope": {"user_id": "alice"},
                "unsupported": True,
            }
        )


def test_canonical_hash_ignores_object_key_order() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})


def test_non_admin_keys_require_scope_and_admin_keys_are_explicitly_tenant_wide() -> None:
    scoped = ApiKeyCreateRequest(name="worker", roles={"read"}, scope=Scope(user_id="alice"))
    assert scoped.scope == Scope(user_id="alice")
    assert ApiKeyCreateRequest(name="admin", roles={"admin"}).scope is None
    with pytest.raises(ValidationError):
        ApiKeyCreateRequest(name="unscoped", roles={"read"})
    with pytest.raises(ValidationError):
        ApiKeyCreateRequest(name="scoped-admin", roles={"admin"}, scope=Scope(user_id="alice"))


def test_cors_requires_explicit_http_origins() -> None:
    assert Settings(allowed_origins="http://127.0.0.1:8787").origins == ["http://127.0.0.1:8787"]
    with pytest.raises(ValueError, match="explicit origins"):
        _ = Settings(allowed_origins="*").origins
    with pytest.raises(ValueError, match="http or https"):
        _ = Settings(allowed_origins="file:///tmp/dashboard").origins


def test_request_id_accepts_only_bounded_log_safe_values() -> None:
    assert _request_id("client-request_123") == "client-request_123"
    generated = _request_id("forged\nlog-entry")
    assert generated.startswith("req_")
    assert generated != "forged\nlog-entry"
    assert _request_id("x" * 129).startswith("req_")


def test_remember_evidence_quote_must_be_verbatim() -> None:
    RememberRequest(
        content="Alice prefers dark mode.",
        scope=Scope(user_id="alice"),
        evidence=EvidenceInput(quote="prefers dark mode"),
    )
    with pytest.raises(ValidationError):
        RememberRequest(
            content="Alice prefers dark mode.",
            scope=Scope(user_id="alice"),
            evidence=EvidenceInput(quote="prefers light mode"),
        )


async def test_documented_langgraph_constructor() -> None:
    client = BrainCore(api_key="your-write-key")
    store = BrainCoreStore(client=client, scope_field="user_id")
    try:
        assert store.scope_field == "user_id"
        assert store._scope(("user-42", "preferences")) == {"user_id": "user-42"}
    finally:
        client.close()
        await store.aclose()

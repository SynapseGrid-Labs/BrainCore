"""Stable-reference validity updates preserve request/hash and native identity contracts."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from braincore_memory.auth import Principal
from braincore_memory.models import RememberRequest
from braincore_memory.service import canonical_hash, memory_dedupe_hash, remember_one


class RecordingReference:
    def __init__(self) -> None:
        self.memory_id = uuid4()
        self.calls: list[tuple[str, Any]] = []
        self.row: dict[str, Any] | None = None

    async def execute(self, query: str, values: Any = None) -> "RecordingReference":
        self.calls.append((query, values))
        self.row = None
        if "SELECT id,content,project_id" in query:
            self.row = {
                "id": self.memory_id,
                "content": "Original 🧠 content.",
                "project_id": None,
                "user_id": "alice",
                "agent_id": None,
                "run_id": None,
                "object_type": "memory",
                "legacy_source_id": None,
                "legacy_attributes_ready": False,
            }
        elif "RETURNING id" in query or "SELECT id FROM braincore.memory_revisions" in query:
            self.row = {"id": uuid4()}
        return self

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row


@pytest.mark.parametrize("changed_content", [False, True])
@pytest.mark.parametrize("validity", ["omitted", "null", "explicit"])
async def test_reference_update_binds_validity_without_replacing_identity(
    changed_content: bool, validity: str
) -> None:
    request: dict[str, Any] = {
        "content": "Changed 🧠 content." if changed_content else "Original 🧠 content.",
        "scope": {"user_id": "alice"},
        "reference": {"namespace": ["alice", "preferences"], "key": "value"},
        "metadata": {"synthetic": "preserved"},
    }
    if validity != "omitted":
        request["valid_from"] = datetime(2040, 3, 4, tzinfo=UTC) if validity == "explicit" else None
    model = RememberRequest.model_validate(request)
    conn = RecordingReference()
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    memory_id = await remember_one(conn, principal, uuid4(), 0, model)  # type: ignore[arg-type]
    assert memory_id == conn.memory_id
    update, values = next(
        (query, values) for query, values in conn.calls if "UPDATE braincore.memories" in query
    )
    normalized = "".join(update.split())
    assert "valid_until=" not in normalized
    if validity == "explicit":
        assert "valid_from=" in normalized
        assert model.valid_from in values
    assert values[0] == model.content
    assert values[1] == memory_dedupe_hash(
        model.content, model.scope.model_dump(), model.reference.model_dump()
    )
    assert values[-2:] == (conn.memory_id, principal.tenant_id)


def test_omitted_and_null_validity_keep_identical_canonical_requests() -> None:
    body = {
        "content": "Valid 🧠 content.",
        "scope": {"user_id": "alice"},
        "reference": {"namespace": ["alice"], "key": "value"},
    }
    omitted = RememberRequest.model_validate(body).model_dump(mode="json")
    explicit_null = RememberRequest.model_validate({**body, "valid_from": None}).model_dump(
        mode="json"
    )
    assert omitted == explicit_null
    assert canonical_hash({"kind": "remember", "body": omitted}) == canonical_hash(
        {"kind": "remember", "body": explicit_null}
    )

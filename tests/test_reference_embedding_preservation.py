"""Reference updates reuse compatible vectors without making another provider call."""

from typing import Any
from uuid import uuid4

import pytest

from braincore_memory import service
from braincore_memory.auth import Principal
from braincore_memory.config import Settings
from braincore_memory.models import RememberRequest


class ReferenceConnection:
    def __init__(self, **overrides: Any) -> None:
        self.existing = {
            "id": uuid4(),
            "content": "A useful synthetic reference.",
            "project_id": None,
            "user_id": "alice",
            "agent_id": None,
            "run_id": None,
            "object_type": "memory",
            "legacy_source_id": None,
            "legacy_attributes_ready": False,
            "embedding": "[1,0]",
            "embedding_state": "ready",
            "embedding_model": "test-model",
            "embedding_preprocessing_revision": "test-revision",
            **overrides,
        }
        self.calls: list[tuple[str, Any]] = []
        self.row: dict[str, Any] | None = None

    async def execute(self, query: str, values: Any = None) -> "ReferenceConnection":
        self.calls.append((query, values))
        self.row = None
        if "SELECT id,content,project_id" in query:
            self.row = self.existing
        elif "RETURNING id" in query:
            self.row = {"id": uuid4()}
        return self

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row


@pytest.mark.parametrize("changed", [False, True])
@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"embedding": None},
        {"embedding_state": "pending"},
        {"embedding_state": "failed"},
        {"embedding_model": "old-model"},
        {"embedding_preprocessing_revision": "old-revision"},
        {"embedding_model": None},
        {"embedding_preprocessing_revision": None},
    ],
    ids=[
        "ready",
        "missing",
        "pending",
        "failed",
        "old-model",
        "old-preprocessing",
        "missing-model",
        "missing-preprocessing",
    ],
)
async def test_reference_embedding_reuse_requires_same_content_and_current_identity(
    monkeypatch: pytest.MonkeyPatch, changed: bool, overrides: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        service,
        "get_settings",
        lambda: Settings(
            embedding_model="test-model", embedding_preprocessing_revision="test-revision"
        ),
        raising=False,
    )
    conn = ReferenceConnection(**overrides)
    request = RememberRequest(
        content="An actually changed synthetic reference." if changed else conn.existing["content"],
        scope={"user_id": "alice"},
        reference={"namespace": ["example"], "key": "stable"},
        metadata={"updated": True},
    )
    actor = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    memory_id = await service.remember_one(conn, actor, uuid4(), 0, request)  # type: ignore[arg-type]
    assert memory_id == conn.existing["id"]
    jobs = [
        (query, values) for query, values in conn.calls if "INSERT INTO braincore.jobs" in query
    ]
    needs_embedding = changed or bool(overrides)
    assert len(jobs) == int(needs_embedding)
    item_query, item_values = next(
        (query, values)
        for query, values in conn.calls
        if "INSERT INTO braincore.operation_items" in query
    )
    assert ("succeeded" in item_values or "'succeeded'" in item_query) is (not needs_embedding)
    assert any("INSERT INTO braincore.memory_evidence" in query for query, _ in conn.calls)
    revisions = [query for query, _ in conn.calls if "'external reference update'" in query]
    assert len(revisions) == int(changed)
    update_query, update_values = next(
        (query, values) for query, values in conn.calls if "UPDATE braincore.memories" in query
    )
    assert update_values[0] == request.content
    assert update_values[-2:] == (memory_id, actor.tenant_id)
    assert "valid_from=COALESCE(%s,valid_from)" in update_query
    assert "valid_until=" not in update_query

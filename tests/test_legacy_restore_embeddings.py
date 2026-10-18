"""Legacy activation decisions without database or embedder access."""

import hashlib
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from braincore_memory import legacy_clients
from braincore_memory.auth import Principal

from .test_legacy_client_preservation import (
    RecordingConnection,
    existing_object,
    install_existing,
)


@pytest.fixture(autouse=True)
def current_embedding_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        legacy_clients,
        "get_settings",
        lambda: SimpleNamespace(
            embedding_model="feature-hash-v1", embedding_preprocessing_revision="token-v2"
        ),
    )


def inactive_object(state: str = "suppressed", **embedding: Any) -> dict[str, Any]:
    existing = existing_object({"content": "Exact accepted canonical content"})
    existing.update(
        state=state,
        extension_content_hash=None,
        legacy_content_hash=hashlib.sha256(existing["content"].encode()).digest(),
        embedding=None,
        embedding_state="pending",
        embedding_model=None,
        embedding_preprocessing_revision=None,
    )
    existing.update(embedding)
    return existing


@pytest.mark.parametrize("state", ["suppressed", "retired"])
@pytest.mark.parametrize(
    "embedding",
    [
        {},
        {"embedding_state": "failed"},
        {"embedding_state": "ready"},
        {
            "embedding": "[1,0]",
            "embedding_state": "ready",
            "embedding_model": "previous-model",
            "embedding_preprocessing_revision": "token-v2",
        },
        {
            "embedding": "[1,0]",
            "embedding_state": "ready",
            "embedding_model": "feature-hash-v1",
            "embedding_preprocessing_revision": "token-v1",
        },
    ],
    ids=["pending", "failed", "ready-missing-vector", "old-model", "old-preprocessing"],
)
def test_restore_queues_unusable_embeddings_for_unchanged_content(
    monkeypatch: pytest.MonkeyPatch, state: str, embedding: dict[str, Any]
) -> None:
    existing = inactive_object(state, **embedding)
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    response = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type="segment",
        object_id=existing["legacy_external_id"],
        fields={},
        action="restore",
        idempotency_key="restore-unusable-vector",
    )
    assert response["state"] == "active"
    jobs = conn.matching("INSERT INTO braincore.jobs")
    assert len(jobs) == 1
    assert jobs[0][1][0] == actor.tenant_id
    assert str(jobs[0][1][1]) == response["operation_id"]
    assert jobs[0][1][3].obj == {"memory_id": str(existing["id"])}
    assert not conn.matching("INSERT INTO braincore.memory_revisions")
    assert not conn.matching("state='succeeded'")


@pytest.mark.parametrize(
    ("object_type", "attributes", "fields"),
    [
        (
            "fact",
            {"predicate": "assertion", "current_status": "uncertain"},
            {"current_status": "active"},
        ),
        (
            "memory",
            {
                "title": "Exact accepted",
                "narrative": "canonical content",
                "lifecycle_state": "draft",
                "governance_status": "validated",
            },
            {"lifecycle_state": "published"},
        ),
        (
            "memory",
            {
                "title": "Exact accepted",
                "narrative": "canonical content",
                "lifecycle_state": "published",
                "governance_status": "candidate",
            },
            {"governance_status": "validated"},
        ),
    ],
)
def test_subtype_status_activation_also_queues_missing_embedding(
    monkeypatch: pytest.MonkeyPatch,
    object_type: str,
    attributes: dict[str, Any],
    fields: dict[str, Any],
) -> None:
    existing = inactive_object()
    existing["legacy_attributes"] = attributes
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type=object_type,
        object_id=existing["legacy_external_id"],
        fields=fields,
        idempotency_key="subtype-reactivation",
    )
    assert result["state"] == "active"
    assert len(conn.matching("INSERT INTO braincore.jobs")) == 1
    assert not conn.matching("INSERT INTO braincore.memory_revisions")


@pytest.mark.parametrize("state", ["active", "suppressed", "retired"])
def test_unchanged_upsert_does_not_schedule_pending_embedding(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    existing = inactive_object(state)
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type="segment",
        object_id=existing["legacy_external_id"],
        fields={"line_start": 7},
        idempotency_key="pending-metadata",
    )
    assert result["state"] == state
    assert not conn.matching("INSERT INTO braincore.jobs")
    assert not conn.matching("INSERT INTO braincore.memories")
    assert not conn.matching("INSERT INTO braincore.memory_revisions")


def test_unchanged_active_restore_does_not_duplicate_pending_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = inactive_object("active")
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type="segment",
        object_id=existing["legacy_external_id"],
        fields={},
        action="restore",
        idempotency_key="already-active-pending",
    )
    assert not conn.matching("INSERT INTO braincore.jobs")
    assert not conn.matching("INSERT INTO braincore.memories")


@pytest.mark.parametrize("state", ["suppressed", "retired"])
def test_restore_retains_compatible_ready_vector(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    existing = inactive_object(
        state,
        embedding="[1,0]",
        embedding_state="ready",
        embedding_model="custom-model",
        embedding_preprocessing_revision="custom-preprocessing",
    )
    monkeypatch.setattr(
        legacy_clients,
        "get_settings",
        lambda: SimpleNamespace(
            embedding_model="custom-model",
            embedding_preprocessing_revision="custom-preprocessing",
        ),
    )
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type="segment",
        object_id=existing["legacy_external_id"],
        fields={},
        action="restore",
        idempotency_key="ready-restore",
    )
    assert not conn.matching("INSERT INTO braincore.jobs")
    assert not conn.matching("INSERT INTO braincore.memory_revisions")
    # The canonical write retains all four stored embedding fields.
    assert conn.matching("INSERT INTO braincore.memories")[0][1][-4:] == (False,) * 4


@pytest.mark.parametrize("state", ["suppressed", "retired"])
def test_inactive_content_edit_invalidates_without_scheduling_work(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    existing = inactive_object(state)
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type="segment",
        object_id=existing["legacy_external_id"],
        fields={"content": "Revised while inactive"},
        idempotency_key="inactive-edit",
    )
    assert result["state"] == state
    assert not conn.matching("INSERT INTO braincore.jobs")
    assert len(conn.matching("INSERT INTO braincore.memory_revisions")) == 1
    assert conn.matching("INSERT INTO braincore.memories")[0][1][-4:] == (True,) * 4


def test_restore_requires_existing_scope_before_scheduling_embedding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = inactive_object()
    actor = install_existing(monkeypatch, existing)
    restricted = Principal(
        actor.key_id, actor.tenant_id, frozenset({"read", "write"}), {"project_id": "other"}
    )
    conn = RecordingConnection()
    with pytest.raises(HTTPException) as rejected:
        legacy_clients.mutate_legacy_object(
            conn,
            restricted,
            object_type="segment",
            object_id=existing["legacy_external_id"],
            fields={},
            action="restore",
            idempotency_key="forbidden-restore",
        )
    assert rejected.value.status_code == 403
    assert not any(query.startswith(("INSERT", "UPDATE", "DELETE")) for query, _ in conn.calls)

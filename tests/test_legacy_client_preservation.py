"""Synthetic regressions for metadata updates and explicit legacy corrections."""

import hashlib
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from braincore_memory import legacy_clients
from braincore_memory.auth import Principal

from .test_legacy_authority import principal
from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source


class RecordingConnection:
    """Observe mutation decisions without connecting to a database."""

    def __init__(self, rendered: Any = "updated assertion null") -> None:
        self.calls: list[tuple[str, Any]] = []
        self.rendered = rendered
        self.row: dict[str, Any] | None = None

    def transaction(self) -> Any:
        return nullcontext()

    def execute(self, query: str, params: Any = None) -> Any:
        self.calls.append((query, params))
        self.row = None
        if query.startswith("WITH merged"):
            self.row = {"content": self.rendered}
        elif "RETURNING id" in query:
            self.row = {"id": uuid4()}
        return self

    def fetchone(self) -> dict[str, Any] | None:
        return self.row

    def matching(self, fragment: str) -> list[tuple[str, Any]]:
        return [call for call in self.calls if fragment in call[0]]


def existing_object(attributes: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "legacy_external_id": uuid4(),
        "extension_override": False,
        "extension_attributes": None,
        "legacy_attributes_ready": True,
        "legacy_attributes": attributes,
        "project_id": "example/project",
        "scope": {"project_id": "example/project"},
        "content": "Exact accepted canonical content",
        "state": "active",
        "valid_from": datetime(2025, 1, 1, tzinfo=UTC),
        "valid_until": None,
        "trust": "extracted",
    }


def install_existing(monkeypatch: pytest.MonkeyPatch, existing: dict[str, Any]) -> Principal:
    def resolve(conn: Any, actor: Any, kind: Any, object_id: Any, identity: Any = None) -> Any:
        if object_id == existing["legacy_external_id"]:
            return existing
        raise AssertionError("unchanged historical relationship was resolved again")

    monkeypatch.setattr(legacy_clients, "resolve_memory", resolve)
    return Principal(uuid4(), existing["tenant_id"], frozenset({"read", "write", "admin"}))


@pytest.mark.parametrize(
    ("object_type", "attributes", "fields"),
    [
        ("fact", {"predicate": "stale", "object_value": {"b": 2, "a": 1}}, {"priority": 6}),
        ("segment", {"content": "stale subtype text"}, {"line_start": 7}),
    ],
)
def test_metadata_preserves_exact_content_and_unchanged_relationships(
    monkeypatch: pytest.MonkeyPatch,
    object_type: str,
    attributes: dict[str, Any],
    fields: dict[str, Any],
) -> None:
    attributes = {**attributes, "project_entity_id": str(uuid4()), "artifact_id": str(uuid4())}
    existing = existing_object(attributes)
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type=object_type,
        object_id=existing["legacy_external_id"],
        fields=fields,
        idempotency_key="synthetic-metadata",
    )
    assert result["native_id"] == str(existing["id"])
    assert not conn.matching("WITH merged")
    assert not conn.matching("INSERT INTO braincore.memories")
    assert not conn.matching("DELETE FROM braincore.legacy_subtype_refs")
    assert not conn.matching("INSERT INTO braincore.memory_revisions")
    assert not conn.matching("INSERT INTO braincore.jobs")
    assert not conn.matching("INSERT INTO braincore.memory_evidence")
    writes = conn.matching("INSERT INTO braincore.legacy_client_attributes")
    assert len(writes) == 1
    assert writes[0][1][1] == hashlib.sha256(existing["content"].encode()).digest()
    identity_writes = conn.matching("UPDATE braincore.legacy_client_ids SET identity")
    assert identity_writes[0][1][0] is None  # SQL NULL, never Jsonb(None).


@pytest.mark.parametrize(
    ("object_type", "fields", "expected"),
    [
        ("fact", {"predicate": "updated assertion"}, "updated assertion null"),
        ("segment", {"content": "New exact text"}, "New exact text"),
    ],
)
def test_explicit_content_change_creates_revision_and_embedding_job(
    monkeypatch: pytest.MonkeyPatch, object_type: str, fields: dict[str, Any], expected: str
) -> None:
    existing = existing_object({"predicate": "stale", "object_value": None})
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type=object_type,
        object_id=existing["legacy_external_id"],
        fields=fields,
        idempotency_key="synthetic-correction",
    )
    assert conn.matching("INSERT INTO braincore.memories")[0][1][2] == expected
    assert conn.matching("INSERT INTO braincore.memory_revisions")[0][1][3] == expected
    assert len(conn.matching("INSERT INTO braincore.jobs")) == 1


@pytest.mark.parametrize("rendered", [None, "", "   "])
def test_invalid_fact_render_rejects_before_any_persistent_write(
    monkeypatch: pytest.MonkeyPatch, rendered: Any
) -> None:
    existing = existing_object({"predicate": "old"})
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection(rendered)
    with pytest.raises(ValueError, match="nonempty canonical content"):
        legacy_clients.mutate_legacy_object(
            conn,
            actor,
            object_type="fact",
            object_id=existing["legacy_external_id"],
            fields={"predicate": ""},
            idempotency_key="synthetic-empty",
        )
    assert not any(query.startswith(("INSERT", "UPDATE", "DELETE")) for query, _ in conn.calls)


def test_explicit_relationship_changes_are_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = existing_object({"predicate": "old"})
    actor = install_existing(monkeypatch, existing)
    conn = RecordingConnection()
    with pytest.raises(AssertionError, match="resolved again"):
        legacy_clients.mutate_legacy_object(
            conn,
            actor,
            object_type="fact",
            object_id=existing["legacy_external_id"],
            fields={"project_entity_id": str(uuid4())},
            idempotency_key="synthetic-relationship",
        )
    assert not conn.matching("INSERT INTO braincore.legacy_client_attributes")


@pytest.mark.parametrize("object_type", ["fact", "segment"])
def test_database_metadata_preserves_native_content_embedding_and_subtype_links(
    typed_source: dict[str, Any], object_type: str
) -> None:
    conn = typed_source["target"]
    actor = principal(typed_source)
    row = conn.execute(
        "SELECT id FROM braincore.memories WHERE object_type=%s AND legacy_external_id=%s",
        (object_type, typed_source["same_external"]),
    ).fetchone()
    entity = conn.execute(
        "SELECT id FROM braincore.memories WHERE object_type='entity' AND tenant_id=%s",
        (actor.tenant_id,),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO braincore.legacy_subtype_refs "
        "(memory_id,tenant_id,field,target_tenant_id,target_memory_id) "
        "VALUES(%s,%s,'project_entity_id',%s,%s)",
        (row["id"], actor.tenant_id, actor.tenant_id, entity),
    )
    vector = "[1," + ",".join(["0"] * 383) + "]"
    # A native correction can intentionally differ from historical subtype rendering.
    conn.execute(
        "UPDATE braincore.memories SET content='Accepted native correction',"
        "embedding=%s::vector,embedding_state='ready',embedding_model='synthetic',"
        "embedding_preprocessing_revision='synthetic' WHERE id=%s",
        (vector, row["id"]),
    )
    before = conn.execute(
        "SELECT row_to_json(m)::jsonb AS memory FROM braincore.memories m WHERE id=%s",
        (row["id"],),
    ).fetchone()
    refs = conn.execute(
        "SELECT * FROM braincore.legacy_subtype_refs WHERE memory_id=%s", (row["id"],)
    ).fetchall()
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type=object_type,
        object_id=typed_source["same_external"],
        fields={"priority": 7} if object_type == "fact" else {"line_start": 7},
        idempotency_key="synthetic-database-metadata",
    )
    assert result["native_id"] == str(row["id"])
    assert (
        conn.execute(
            "SELECT row_to_json(m)::jsonb AS memory FROM braincore.memories m WHERE id=%s",
            (row["id"],),
        ).fetchone()
        == before
    )
    assert (
        conn.execute(
            "SELECT * FROM braincore.legacy_subtype_refs WHERE memory_id=%s", (row["id"],)
        ).fetchall()
        == refs
    )
    assert (
        conn.execute(
            "SELECT attributes,content_hash FROM braincore.legacy_client_attributes "
            "WHERE memory_id=%s",
            (row["id"],),
        ).fetchone()["content_hash"]
        == hashlib.sha256(b"Accepted native correction").digest()
    )


def test_database_two_identity_free_updates_keep_sql_null(typed_source: dict[str, Any]) -> None:
    conn = typed_source["target"]
    actor = principal(typed_source)
    ids = []
    for index in range(2):
        result = legacy_clients.mutate_legacy_object(
            conn,
            actor,
            object_type="episode",
            fields={
                "title": f"Synthetic episode {index}",
                "summary": "Synthetic identity-free object",
                "project_entity_id": str(typed_source["same_external"]),
            },
            idempotency_key=f"synthetic-create-{index}",
        )
        ids.append(result["native_id"])
        legacy_clients.mutate_legacy_object(
            conn,
            actor,
            object_type="episode",
            object_id=result["object_id"],
            fields={"outcome": f"Synthetic outcome {index}"},
            idempotency_key=f"synthetic-update-{index}",
        )
    rows = conn.execute(
        "SELECT identity IS NULL AS absent FROM braincore.legacy_client_ids "
        "WHERE memory_id=ANY(%s::uuid[])",
        (ids,),
    ).fetchall()
    assert len(rows) == 2 and all(row["absent"] for row in rows)


@pytest.mark.parametrize(
    ("object_type", "fields", "expected"),
    [
        ("segment", {"content": "Deliberate replacement"}, "Deliberate replacement"),
        (
            "fact",
            {"predicate": "updated", "object_value": {"nested": {"z": 2, "a": 1}}},
            'updated {"nested": {"a": 1, "z": 2}}',
        ),
    ],
)
def test_database_explicit_content_correction_records_revision_and_job(
    typed_source: dict[str, Any], object_type: str, fields: dict[str, Any], expected: str
) -> None:
    conn = typed_source["target"]
    actor = principal(typed_source)
    # Existing source artifact ownership must be established for an intentional
    # content change, which revalidates all relationships.
    from braincore_memory.legacy_evidence import import_artifacts

    import_artifacts(conn, typed_source["source_id"])
    result = legacy_clients.mutate_legacy_object(
        conn,
        actor,
        object_type=object_type,
        object_id=typed_source["same_external"],
        fields=fields,
        idempotency_key="synthetic-explicit-content",
    )
    memory = conn.execute(
        "SELECT content,embedding_state FROM braincore.memories WHERE id=%s",
        (result["native_id"],),
    ).fetchone()
    assert memory == {"content": expected, "embedding_state": "pending"}
    assert (
        conn.execute(
            "SELECT content FROM braincore.memory_revisions WHERE memory_id=%s",
            (result["native_id"],),
        ).fetchone()["content"]
        == expected
    )
    assert (
        conn.execute(
            "SELECT count(*) AS n FROM braincore.jobs WHERE operation_id=%s",
            (result["operation_id"],),
        ).fetchone()["n"]
        == 1
    )


def test_database_empty_fact_render_rolls_back(typed_source: dict[str, Any]) -> None:
    conn = typed_source["target"]
    actor = principal(typed_source)
    fact = conn.execute(
        "SELECT id,legacy_external_id,content FROM braincore.memories "
        "WHERE object_type='fact' AND content='expired assertion '",
    ).fetchone()
    assert fact is not None
    with pytest.raises(ValueError, match="nonempty canonical content"):
        legacy_clients.mutate_legacy_object(
            conn,
            actor,
            object_type="fact",
            object_id=fact["legacy_external_id"],
            fields={"predicate": ""},
            idempotency_key="synthetic-empty-fact",
        )
    assert (
        conn.execute(
            "SELECT content FROM braincore.memories WHERE id=%s",
            (fact["id"],),
        ).fetchone()["content"]
        == fact["content"]
    )
    assert (
        conn.execute(
            "SELECT count(*) AS n FROM braincore.mutation_receipts "
            "WHERE key='synthetic-empty-fact'",
        ).fetchone()["n"]
        == 0
    )

"""Synthetic regressions for reference identity and typed helper writes."""

import json
from contextlib import asynccontextmanager, nullcontext
from typing import Any
from uuid import uuid4

import httpx
import pytest
from langgraph.store.base import SearchOp

from braincore_memory import AsyncBrainCore, BrainCore, api, legacy_writes
from braincore_memory.auth import Principal
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.legacy_relationships import RELATIONSHIPS, auxiliary_relationships
from braincore_memory.legacy_types import FIELD_TYPES, REQUIRED_FIELDS, validate_subtype_fields
from braincore_memory.models import RevisionRequest, SearchRequest
from braincore_memory.service import canonical_hash, memory_dedupe_hash


@pytest.mark.parametrize("prefix", [(), ("alice", "preferences")])
@pytest.mark.parametrize("filter_value", [None, {"favorite": "tea"}])
async def test_langgraph_search_validates_root_and_nested_prefixes(
    prefix: tuple[str, ...], filter_value: dict[str, str] | None
) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        validated = SearchRequest.model_validate(body)
        assert validated.reference_namespace_prefix == (list(prefix) or None)
        assert validated.metadata_filter == {"langgraph": {"value": filter_value or {}}}
        assert validated.scope is None if not prefix else validated.scope.user_id == "alice"
        calls.append(body)
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "reference_namespace": ["alice", "preferences"],
                        "reference_key": "tea",
                        "metadata": {"langgraph": {"value": {"favorite": "tea"}}},
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test") as sync_raw:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://test"
        ) as async_raw:
            store = BrainCoreStore(
                client=BrainCore(client=sync_raw), async_client=AsyncBrainCore(client=async_raw)
            )
            op = SearchOp(prefix, query="tea", filter=filter_value)
            assert store.batch([op])[0][0].key == "tea"
            assert (await store.abatch([op]))[0][0].key == "tea"
    assert len(calls) == 2


class AsyncRecordingConnection:
    def __init__(self, current: dict[str, Any]) -> None:
        self.current = current
        self.calls: list[tuple[str, Any]] = []
        self.row: Any = None

    def transaction(self) -> Any:
        return self.context()

    @asynccontextmanager
    async def context(self) -> Any:
        yield

    async def execute(self, query: str, params: Any = None) -> Any:
        self.calls.append((query, params))
        self.row = None
        if query.startswith("SELECT content,project_id"):
            self.row = self.current
        elif "RETURNING id" in query:
            self.row = {"id": uuid4()}
        return self

    async def fetchone(self) -> Any:
        return self.row


@pytest.mark.parametrize("reference_key", [None, "ref-a", "ref-b"])
async def test_revision_preserves_creation_dedupe_and_external_identity(
    monkeypatch: pytest.MonkeyPatch, reference_key: str | None
) -> None:
    scope = {"project_id": None, "user_id": "alice", "agent_id": None, "run_id": None}
    current = {
        **scope,
        "content": "Old text",
        "object_type": "memory",
        "legacy_source_id": None,
        "legacy_attributes_ready": False,
        "reference_namespace": ["preferences"] if reference_key else None,
        "reference_key": reference_key,
    }
    conn = AsyncRecordingConnection(current)

    @asynccontextmanager
    async def connection() -> Any:
        yield conn

    async def begin(*args: Any) -> tuple[Any, bool]:
        return uuid4(), False

    monkeypatch.setattr(api, "connection", connection)
    monkeypatch.setattr(api, "begin_idempotent_operation", begin)
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    await api.revise(
        uuid4(), RevisionRequest(content="Replacement", reason="Correction"), "revision-test", actor
    )
    expected: dict[str, Any] = {"content": "Replacement", "scope": scope}
    if reference_key:
        expected["reference"] = {"namespace": ["preferences"], "key": reference_key}
    query, params = next(
        call for call in conn.calls if call[0].startswith("UPDATE braincore.memories ")
    )
    assert params[1] == canonical_hash(expected)
    assert "reference_namespace=" not in query and "reference_key=" not in query
    assert "legacy_attributes=" not in query and "metadata=" not in query
    assert memory_dedupe_hash("Replacement", scope, expected.get("reference")) == params[1]


INTEGER_FIELDS = [
    (kind, name, sql_type)
    for kind, fields in FIELD_TYPES.items()
    for name, sql_type in fields.items()
    if sql_type in {"integer", "bigint"}
]
NUMERIC_FIELDS = [
    (kind, name, sql_type)
    for kind, fields in FIELD_TYPES.items()
    for name, sql_type in fields.items()
    if sql_type.startswith("numeric")
]


@pytest.mark.parametrize(("kind", "name", "sql_type"), INTEGER_FIELDS)
def test_sql_integer_limits(kind: str, name: str, sql_type: str) -> None:
    limit = 2 ** (31 if sql_type == "integer" else 63)
    for value in (-limit, limit - 1):
        validate_subtype_fields(kind, {name: value}, new=False)
    for value in (-limit - 1, limit, True, 1.0, "1"):
        with pytest.raises(ValueError, match="invalid legacy subtype"):
            validate_subtype_fields(kind, {name: value}, new=False)


@pytest.mark.parametrize(("kind", "name", "sql_type"), NUMERIC_FIELDS)
def test_fixed_numeric_rounding_finiteness_and_confidence(
    kind: str, name: str, sql_type: str
) -> None:
    precision, scale = map(int, sql_type[8:-1].split(","))
    ceiling = 10 ** (precision - scale)
    for value in (0, 1):
        validate_subtype_fields(kind, {name: value}, new=False)
    bad = [True, "0.5", float("inf"), -float("inf"), float("nan"), ceiling]
    if name == "confidence":
        bad += [-0.001, 1.001, 100]
    else:
        validate_subtype_fields(kind, {name: ceiling - 0.6 * 10**-scale}, new=False)
        bad += [ceiling - 0.4 * 10**-scale]
    for value in bad:
        with pytest.raises(ValueError, match="invalid legacy subtype"):
            validate_subtype_fields(kind, {name: value}, new=False)


@pytest.mark.parametrize("value", ["{}", "[]", [None], [True], ["not-a-uuid"], [1], {}])
def test_uuid_arrays_reject_non_arrays_and_invalid_elements(value: Any) -> None:
    with pytest.raises(ValueError):
        validate_subtype_fields("memory_context_audit", {"retrieved_memory_ids": value}, new=False)


def test_uuid_array_null_empty_duplicate_and_positional_contract() -> None:
    value = str(uuid4())
    for values in (None, [], [value, value]):
        validate_subtype_fields("memory_context_audit", {"retrieved_memory_ids": values}, new=False)
    assert auxiliary_relationships(
        "memory_context_audit", {"retrieved_memory_ids": [value, value]}
    ) == [("retrieved_memory_ids:0", "memory", value), ("retrieved_memory_ids:1", "memory", value)]


class RecordingConnection:
    def __init__(self, endpoint: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.endpoint = endpoint
        self.row: Any = None

    def transaction(self) -> Any:
        return nullcontext()

    def execute(self, query: str, params: Any = None) -> Any:
        self.calls.append((query, params))
        self.row = (
            self.endpoint if query.startswith("SELECT id,tenant_id,scope,external_id") else None
        )
        return self

    def fetchone(self) -> Any:
        return self.row


def fields_for(kind: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in REQUIRED_FIELDS.get(kind, []):
        sql_type = FIELD_TYPES[kind][name]
        result[name] = (
            str(uuid4())
            if sql_type == "uuid"
            else 1
            if sql_type in {"integer", "bigint"} or sql_type.startswith("numeric")
            else "deterministic"
            if name == "assertion_class"
            else "memory"
            if name in {"source_type", "target_type"}
            else "example"
        )
    if kind == "working_memory":
        result["expires_at"] = "2027-01-01T00:00:00Z"
    for name in {
        "fact_evidence": ("fact_id", "segment_id"),
        "memory_support": ("memory_id", "fact_id"),
        "extraction_run": ("artifact_id",),
        "publish_note": ("memory_id",),
    }.get(kind, ()):
        result[name] = str(uuid4())
    return result


LIVE_RELATIONSHIPS = [
    (kind, name, target)
    for kind, fields in RELATIONSHIPS.items()
    if kind in legacy_writes.CONTROL_TYPES
    for name, target in fields.items()
    if name in FIELD_TYPES[kind]
]


@pytest.mark.parametrize(("kind", "name", "target"), LIVE_RELATIONSHIPS)
def test_every_declared_control_relationship_is_resolved_and_constrained(
    monkeypatch: pytest.MonkeyPatch, kind: str, name: str, target: str
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    calls = []
    endpoint = {"id": uuid4(), "scope": actor.scope, "reference_kind": "memory"}

    def reference(*args: Any) -> tuple[Any, str]:
        calls.append((args[2], args[4]))
        return endpoint, str(endpoint["id"])

    monkeypatch.setattr(legacy_writes, "_reference", reference)
    fields = {**fields_for(kind), name: str(uuid4())}
    conn = RecordingConnection()
    legacy_writes.mutate_control(
        conn, actor, object_type=kind, fields=fields, idempotency_key="control"
    )
    assert (name, target) in calls
    refs = [p for q, p in conn.calls if "INSERT INTO braincore.legacy_client_control_refs" in q]
    assert any(params[1] == name and params[3] == endpoint["id"] for params in refs)


@pytest.mark.parametrize("kind", ["memory_edge", "review_queue", "working_memory"])
def test_unknown_polymorphic_kind_rejected_before_control_write(kind: str) -> None:
    fields = fields_for(kind)
    type_field, id_field = (
        ("promotion_target_kind", "promotion_target_id")
        if kind == "working_memory"
        else ("target_type", "target_id")
    )
    fields.update({type_field: "unknown-type", id_field: str(uuid4())})
    conn = RecordingConnection()
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    with pytest.raises(ValueError, match="unsupported polymorphic"):
        legacy_writes.mutate_control(
            conn, actor, object_type=kind, fields=fields, idempotency_key="invalid"
        )
    assert not any("INSERT INTO" in query for query, _ in conn.calls)


@pytest.mark.parametrize("failure", ["missing", "foreign", "scope", "type"])
def test_procedure_endpoint_failures_do_not_write_or_acknowledge(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})

    def resolve(*args: Any) -> Any:
        if failure == "type":
            raise ValueError("legacy_object_type_conflict")
        if failure == "missing":
            return None
        return {
            "tenant_id": uuid4() if failure == "foreign" else actor.tenant_id,
            "scope": {"user_id": "bob"} if failure == "scope" else actor.scope,
        }

    monkeypatch.setattr(legacy_writes, "resolve_memory", resolve)
    conn = RecordingConnection()
    with pytest.raises(ValueError, match="control_reference_not_found"):
        legacy_writes.mutate_control(
            conn,
            actor,
            object_type="procedure",
            fields={**fields_for("procedure"), "source_fact_id": str(uuid4())},
            idempotency_key="bad",
        )
    assert not any("INSERT INTO" in query for query, _ in conn.calls)


@pytest.mark.parametrize("scope", [{"user_id": "alice"}, {"user_id": "bob"}])
def test_control_parent_resolves_with_exact_type_and_scope(scope: dict[str, str]) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    row = {
        "id": uuid4(),
        "tenant_id": actor.tenant_id,
        "scope": scope,
        "external_id": uuid4(),
        "reference_kind": "control",
    }
    conn = RecordingConnection(row)
    if scope == actor.scope:
        result, external = legacy_writes._reference(
            conn, actor, "procedure_id", row["external_id"], "procedure"
        )
        assert result["id"] == row["id"] and external == str(row["external_id"])
    else:
        with pytest.raises(ValueError, match="control_reference_not_found"):
            legacy_writes._reference(conn, actor, "procedure_id", row["external_id"], "procedure")
    assert conn.calls[0][1][:2] == (actor.tenant_id, "procedure")


def test_worker_failure_diagnostics_are_bounded_and_redact_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from .test_worker_performance_e2e import safe_worker_stderr

    monkeypatch.setenv("BRAINCORE_LLM_API_KEY", "synthetic-key-never-print")
    text = safe_worker_stderr(
        (
            "x" * 6000 + " synthetic-key-never-print postgresql://user:pass@example.invalid/test"
        ).encode()
    )
    assert len(text) <= 4096
    assert "synthetic-key-never-print" not in text and "user:pass" not in text
    assert "[redacted]" in text

"""Portable structural legacy subtype contracts; no runtime source access."""

import json as _json
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Any
from uuid import UUID

FIELD_TYPES = {
    "archive_object": {
        "archive_id": "uuid",
        "source_key": "text",
        "source_type": "text",
        "original_path": "text",
        "archive_path": "text",
        "manifest_path": "text",
        "format": "text",
        "original_bytes": "bigint",
        "compressed_bytes": "bigint",
        "checksum_sha256": "text",
        "redundancy_count": "integer",
        "restore_tested_at": "timestamp with time zone",
        "verified_at": "timestamp with time zone",
        "host": "text",
        "created_at": "timestamp with time zone",
    },
    "artifact": {
        "source_type": "preserve.source_type",
        "source_key": "text",
        "original_path": "text",
        "host": "text",
        "ttl_at": "timestamp with time zone",
        "sha256": "text",
        "size_bytes": "bigint",
        "value_score": "numeric(5,2)",
        "preservation_state": "preserve.preservation_state",
        "can_evict_hot": "boolean",
        "can_query_raw": "boolean",
        "can_promote_memory": "boolean",
        "scope_path": "text",
        "meta": "jsonb",
        "opsvault_file_id": "uuid",
        "opsvault_incident_id": "uuid",
        "discovered_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
        "project_entity_id": "uuid",
        "priority": "integer",
    },
    "entity": {
        "entity_type": "preserve.entity_type",
        "canonical_name": "text",
        "aliases": "jsonb",
        "attrs": "jsonb",
        "first_seen_at": "timestamp with time zone",
        "last_seen_at": "timestamp with time zone",
    },
    "episode": {
        "episode_type": "text",
        "title": "text",
        "start_at": "timestamp with time zone",
        "end_at": "timestamp with time zone",
        "severity": "text",
        "outcome": "text",
        "summary": "text",
        "primary_artifact_id": "uuid",
        "scope_path": "text",
        "created_at": "timestamp with time zone",
        "project_entity_id": "uuid",
    },
    "event": {
        "episode_id": "uuid",
        "event_type": "text",
        "occurred_at": "timestamp with time zone",
        "description": "text",
        "entity_id": "uuid",
        "segment_id": "uuid",
        "meta": "jsonb",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
    },
    "event_frame": {
        "frame_fingerprint": "text",
        "episode_id": "uuid",
        "event_id": "uuid",
        "source_fact_id": "uuid",
        "event_type": "text",
        "actor_entity_id": "uuid",
        "action": "text",
        "target_entity_id": "uuid",
        "object_value": "jsonb",
        "time_start": "timestamp with time zone",
        "time_end": "timestamp with time zone",
        "location_entity_id": "uuid",
        "cause_fact_id": "uuid",
        "effect_fact_id": "uuid",
        "outcome": "text",
        "confidence": "numeric(3,2)",
        "assertion_class": "preserve.assertion_class",
        "evidence_segment_id": "uuid",
        "scope_path": "text",
        "frame_json": "jsonb",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    },
    "extraction_run": {
        "run_id": "uuid",
        "artifact_id": "uuid",
        "pipeline_version": "text",
        "model_name": "text",
        "prompt_version": "text",
        "schema_version": "integer",
        "idempotency_key": "text",
        "status": "preserve.extraction_status",
        "raw_output": "jsonb",
        "validation_errors": "jsonb",
        "duration_ms": "integer",
        "started_at": "timestamp with time zone",
        "finished_at": "timestamp with time zone",
    },
    "fact": {
        "subject_entity_id": "uuid",
        "predicate": "text",
        "object_entity_id": "uuid",
        "object_value": "jsonb",
        "fact_kind": "preserve.fact_kind",
        "assertion_class": "preserve.assertion_class",
        "confidence": "numeric(3,2)",
        "valid_from": "timestamp with time zone",
        "valid_to": "timestamp with time zone",
        "first_seen_at": "timestamp with time zone",
        "last_seen_at": "timestamp with time zone",
        "current_status": "preserve.fact_status",
        "canonical_fingerprint": "text",
        "episode_id": "uuid",
        "created_run_id": "uuid",
        "scope_path": "text",
        "created_at": "timestamp with time zone",
        "project_entity_id": "uuid",
        "is_milestone": "boolean",
        "importance_score": "numeric(5,2)",
        "priority": "integer",
    },
    "fact_evidence": {
        "fact_id": "uuid",
        "segment_id": "uuid",
        "excerpt": "text",
        "extraction_method": "preserve.extraction_method",
        "weight": "numeric(3,2)",
        "source_sha256": "text",
        "source_relpath": "text",
        "line_start": "integer",
        "line_end": "integer",
        "excerpt_hash": "text",
        "created_at": "timestamp with time zone",
    },
    "memory": {
        "memory_type": "preserve.memory_type",
        "scope_entity_id": "uuid",
        "fingerprint": "text",
        "title": "text",
        "narrative": "text",
        "support_count": "integer",
        "contradiction_count": "integer",
        "confidence": "numeric(3,2)",
        "valid_from": "timestamp with time zone",
        "valid_to": "timestamp with time zone",
        "lifecycle_state": "preserve.lifecycle_state",
        "pipeline_version": "text",
        "model_name": "text",
        "prompt_version": "text",
        "scope_path": "text",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
        "project_entity_id": "uuid",
        "last_supported_at": "timestamp with time zone",
        "priority": "integer",
        "namespace": "preserve.memory_namespace",
        "governance_status": "preserve.memory_governance_status",
        "source_class": "preserve.memory_source_class",
        "trust_class": "preserve.memory_trust_class",
        "salience": "numeric(4,3)",
        "strength": "numeric(4,3)",
        "stability": "numeric(4,3)",
        "quality_score": "numeric(4,3)",
        "summary_fidelity_score": "numeric(4,3)",
        "token_count": "integer",
        "schema_version": "integer",
        "config_version": "text",
        "redaction_metadata": "jsonb",
        "governance_meta": "jsonb",
        "last_reinforced_at": "timestamp with time zone",
        "last_decayed_at": "timestamp with time zone",
    },
    "memory_context_audit": {
        "audit_id": "uuid",
        "query": "text",
        "trigger": "text",
        "retrieved_memory_ids": "uuid[]",
        "injected_memory_ids": "uuid[]",
        "omitted": "jsonb",
        "prompt_package": "jsonb",
        "total_tokens": "integer",
        "max_tokens": "integer",
        "relevance_reason": "text",
        "actor": "text",
        "route": "text",
        "request_id": "text",
        "created_at": "timestamp with time zone",
    },
    "memory_edge": {
        "edge_id": "uuid",
        "source_type": "text",
        "source_id": "uuid",
        "target_type": "text",
        "target_id": "uuid",
        "edge_type": "text",
        "edge_fingerprint": "text",
        "confidence": "numeric(3,2)",
        "assertion_class": "preserve.assertion_class",
        "evidence_segment_id": "uuid",
        "created_run_id": "uuid",
        "valid_from": "timestamp with time zone",
        "valid_to": "timestamp with time zone",
        "scope_path": "text",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    },
    "memory_edge_evidence": {
        "edge_evidence_id": "uuid",
        "edge_id": "uuid",
        "fact_id": "uuid",
        "episode_id": "uuid",
        "segment_id": "uuid",
        "notes": "text",
        "weight": "numeric(3,2)",
        "created_at": "timestamp with time zone",
    },
    "memory_support": {
        "support_id": "uuid",
        "memory_id": "uuid",
        "fact_id": "uuid",
        "episode_id": "uuid",
        "support_type": "text",
        "notes": "text",
        "created_at": "timestamp with time zone",
    },
    "procedure": {
        "procedure_fingerprint": "text",
        "title": "text",
        "summary": "text",
        "source_fact_id": "uuid",
        "source_memory_id": "uuid",
        "source_episode_id": "uuid",
        "scope_entity_id": "uuid",
        "project_entity_id": "uuid",
        "evidence_segment_id": "uuid",
        "assertion_class": "preserve.assertion_class",
        "confidence": "numeric(3,2)",
        "lifecycle_state": "preserve.lifecycle_state",
        "scope_path": "text",
        "procedure_json": "jsonb",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    },
    "procedure_step": {
        "procedure_id": "uuid",
        "step_index": "integer",
        "action": "text",
        "expected_result": "text",
        "source_fact_id": "uuid",
        "evidence_segment_id": "uuid",
        "assertion_class": "preserve.assertion_class",
        "confidence": "numeric(3,2)",
        "scope_path": "text",
        "step_json": "jsonb",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
    },
    "publish_note": {
        "publish_id": "uuid",
        "memory_id": "uuid",
        "file_path": "text",
        "content_hash": "text",
        "published_at": "timestamp with time zone",
    },
    "review_queue": {
        "review_id": "uuid",
        "target_type": "text",
        "target_id": "uuid",
        "reason": "text",
        "status": "preserve.review_status",
        "reviewer_notes": "text",
        "created_at": "timestamp with time zone",
        "resolved_at": "timestamp with time zone",
    },
    "segment": {
        "artifact_id": "uuid",
        "ordinal": "integer",
        "line_start": "integer",
        "line_end": "integer",
        "ts_start": "timestamp with time zone",
        "ts_end": "timestamp with time zone",
        "section_label": "text",
        "content": "text",
        "scope_path": "text",
        "source_sha256": "text",
        "source_relpath": "text",
        "archive_member_path": "text",
        "excerpt_hash": "text",
        "byte_start": "bigint",
        "byte_end": "bigint",
        "created_at": "timestamp with time zone",
        "project_entity_id": "uuid",
    },
    "task_session": {
        "session_id": "uuid",
        "session_key": "text",
        "agent_name": "text",
        "task_title": "text",
        "status": "text",
        "source_artifact_id": "uuid",
        "parent_session_id": "uuid",
        "scope_entity_id": "uuid",
        "project_entity_id": "uuid",
        "scope_path": "text",
        "started_at": "timestamp with time zone",
        "last_seen_at": "timestamp with time zone",
        "ended_at": "timestamp with time zone",
        "expires_at": "timestamp with time zone",
        "session_json": "jsonb",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    },
    "working_memory": {
        "session_id": "uuid",
        "working_memory_fingerprint": "text",
        "memory_kind": "text",
        "content": "text",
        "content_json": "jsonb",
        "source_segment_id": "uuid",
        "source_fact_id": "uuid",
        "evidence_segment_id": "uuid",
        "confidence": "numeric(3,2)",
        "promotion_status": "text",
        "promotion_reason": "text",
        "promotion_block_reason": "text",
        "promoted_memory_id": "uuid",
        "expires_at": "timestamp with time zone",
        "created_run_id": "uuid",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
        "promotion_target_kind": "text",
        "promotion_target_id": "uuid",
        "promotion_marked_at": "timestamp with time zone",
    },
}

ENUM_VALUES = {
    "assertion_class": [
        "deterministic",
        "human_curated",
        "corroborated_llm",
        "single_source_llm",
        "retired",
    ],
    "entity_type": [
        "device",
        "service",
        "project",
        "file",
        "config_item",
        "pattern_scope",
        "incident",
        "session",
    ],
    "extraction_method": ["rule", "llm", "human_curated"],
    "extraction_status": ["pending", "running", "success", "partial", "failed"],
    "fact_kind": [
        "state",
        "cause",
        "impact",
        "decision",
        "remediation",
        "lesson",
        "constraint",
        "config_change",
        "event",
    ],
    "fact_status": ["active", "superseded", "contradicted", "uncertain"],
    "lifecycle_state": ["draft", "published", "retired"],
    "memory_governance_status": [
        "candidate",
        "archived",
        "active",
        "review_required",
        "validated",
        "disputed",
        "quarantined",
        "suppressed",
        "retired",
    ],
    "memory_namespace": ["working", "episodic", "semantic", "procedural", "policy"],
    "memory_outbox_status": ["pending", "processing", "completed", "failed", "dead_letter"],
    "memory_source_class": [
        "observed",
        "user_stated",
        "system_inferred",
        "summary_derived",
        "replay_derived",
        "imported_knowledge",
        "corrected_by_user",
    ],
    "memory_trust_class": [
        "deterministic",
        "human_curated",
        "corroborated_llm",
        "single_source_llm",
        "retired_superseded",
    ],
    "memory_type": ["entity_summary", "pattern", "playbook", "heuristic"],
    "preservation_state": [
        "discovered",
        "archived",
        "extracted",
        "published",
        "blocked",
        "failed",
        "pending_escalation",
    ],
    "review_status": ["pending", "approved", "rejected", "deferred"],
    "source_type": [
        "opsvault_incident",
        "claude_session",
        "claude_plan",
        "device_log",
        "config_diff",
        "project_doc",
        "pai_memory",
        "codex_session",
        "codex_shared",
        "discord_conversation",
        "telegram_chat",
        "monitoring_alert",
        "asana_task",
        "git_commit",
        "research_paper",
        "vestige_memory",
        "pai_auto_memory",
    ],
}

DEFAULT_FIELDS: dict[str, dict[str, Any]] = {
    "archive_object": {
        "source_type": "opsvault_incident",
        "format": "tar.zst",
        "redundancy_count": 1,
    },
    "artifact": {
        "value_score": 0,
        "preservation_state": "discovered",
        "can_evict_hot": False,
        "can_query_raw": False,
        "can_promote_memory": False,
        "meta": "{}",
        "priority": 5,
    },
    "entity": {"aliases": "[]", "attrs": "{}"},
    "event": {"meta": "{}"},
    "event_frame": {"frame_json": "{}"},
    "extraction_run": {"schema_version": 1, "status": "pending"},
    "fact": {
        "assertion_class": "single_source_llm",
        "current_status": "active",
        "is_milestone": False,
        "importance_score": 0,
        "priority": 5,
    },
    "memory": {
        "support_count": 0,
        "contradiction_count": 0,
        "lifecycle_state": "draft",
        "priority": 5,
        "namespace": "semantic",
        "governance_status": "active",
        "source_class": "system_inferred",
        "trust_class": "single_source_llm",
        "token_count": 0,
        "schema_version": 1,
        "config_version": "braincore-memory-governance-v1",
        "redaction_metadata": "{}",
        "governance_meta": "{}",
    },
    "memory_context_audit": {"omitted": "[]", "prompt_package": "[]", "total_tokens": 0},
    "procedure": {"lifecycle_state": "draft", "procedure_json": "{}"},
    "procedure_step": {"step_json": "{}"},
    "review_queue": {"status": "pending"},
    "task_session": {"status": "active", "session_json": "{}"},
    "working_memory": {"content_json": "{}", "promotion_status": "not_promoted"},
}

REQUIRED_FIELDS = {
    "archive_object": [
        "source_key",
        "original_path",
        "archive_path",
        "compressed_bytes",
        "checksum_sha256",
    ],
    "artifact": ["source_type", "source_key", "original_path", "sha256", "size_bytes"],
    "entity": ["entity_type", "canonical_name"],
    "event_frame": [
        "frame_fingerprint",
        "episode_id",
        "event_type",
        "action",
        "confidence",
        "assertion_class",
    ],
    "fact": [
        "subject_entity_id",
        "predicate",
        "fact_kind",
        "confidence",
        "canonical_fingerprint",
        "created_run_id",
    ],
    "fact_evidence": ["excerpt", "source_sha256", "excerpt_hash"],
    "memory_edge": [
        "source_type",
        "source_id",
        "target_type",
        "target_id",
        "edge_type",
        "edge_fingerprint",
        "confidence",
        "assertion_class",
    ],
    "memory_edge_evidence": ["edge_id"],
    "procedure": ["procedure_fingerprint", "title", "assertion_class", "confidence"],
    "procedure_step": ["procedure_id", "step_index", "action", "assertion_class", "confidence"],
    "publish_note": ["file_path", "content_hash"],
    "review_queue": ["target_type", "target_id", "reason"],
    "segment": ["artifact_id", "ordinal", "content", "source_sha256"],
    "task_session": ["session_key", "agent_name"],
    "working_memory": [
        "session_id",
        "working_memory_fingerprint",
        "memory_kind",
        "content",
        "expires_at",
    ],
}


def validate_subtype_fields(object_type: str, fields: dict[str, Any], *, new: bool) -> None:
    types = FIELD_TYPES[object_type]
    for name, value in fields.items():
        if name.startswith("__braincore"):
            continue
        if name not in types:
            raise ValueError("unsupported legacy subtype field")
        if value is None:
            if name in REQUIRED_FIELDS.get(object_type, []):
                raise ValueError("required legacy subtype field cannot be null")
            continue
        kind = types[name]
        valid = True
        if kind.startswith("preserve."):
            valid = isinstance(value, str) and value in ENUM_VALUES[kind.split(".", 1)[1]]
        elif kind == "text":
            valid = isinstance(value, str)
        elif kind in {"integer", "bigint"}:
            bits = 32 if kind == "integer" else 64
            valid = (
                isinstance(value, int)
                and not isinstance(value, bool)
                and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1)
            )
        elif kind.startswith("numeric"):
            valid = isinstance(value, int | float) and not isinstance(value, bool)
            if valid:
                precision, scale = (int(part) for part in kind[8:-1].split(","))
                try:
                    number = Decimal(str(value))
                    with localcontext() as context:
                        context.prec = max(precision + 2, len(number.as_tuple().digits) + 2)
                        rounded = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
                    valid = number.is_finite() and abs(rounded) < 10 ** (precision - scale)
                    if name == "confidence":
                        valid = valid and 0 <= number <= 1
                except (InvalidOperation, ValueError):
                    valid = False
        elif kind == "uuid[]":
            valid = isinstance(value, list)
            if valid:
                try:
                    for element in value:
                        UUID(str(element))
                except (ValueError, TypeError, AttributeError):
                    valid = False
        elif kind == "boolean":
            valid = isinstance(value, bool)
        elif kind == "uuid":
            try:
                UUID(str(value))
            except (ValueError, TypeError, AttributeError):
                valid = False
        elif kind == "timestamp with time zone":
            try:
                datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                valid = False
        if not valid:
            raise ValueError("invalid legacy subtype field type")
    if new and any(fields.get(name) is None for name in REQUIRED_FIELDS.get(object_type, [])):
        raise ValueError("missing required legacy subtype field")


# JSON defaults are JSON values, rather than their PostgreSQL literal text.

for _kind, _defaults in DEFAULT_FIELDS.items():
    for _name, _value in list(_defaults.items()):
        if FIELD_TYPES[_kind].get(_name) == "jsonb" and isinstance(_value, str):
            _defaults[_name] = _json.loads(_value)

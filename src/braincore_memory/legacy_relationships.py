"""Shared typed relationship contracts for imports and live helper mutations."""

from typing import Any

CORE_TYPES = frozenset({"entity", "fact", "memory", "segment", "episode"})

# Fixed source contracts, verified against the legacy relationship catalog.
AUXILIARY_FKS: dict[str, dict[str, str]] = {
    "archive_object": {},
    "eval_run": {},
    "schema_migration": {},
    "reflection_class": {"parent_class_key": "reflection_class"},
    "eval_case": {"artifact_id": "artifact"},
    "extraction_run": {"artifact_id": "artifact"},
    "project_service_map": {"project_entity_id": "entity"},
    "publish_note": {"memory_id": "memory"},
    "review_queue": {},
    "memory_edge": {"evidence_segment_id": "segment", "created_run_id": "extraction_run"},
    "memory_edge_evidence": {
        "edge_id": "memory_edge",
        "fact_id": "fact",
        "episode_id": "episode",
        "segment_id": "segment",
    },
    "event": {
        "episode_id": "episode",
        "entity_id": "entity",
        "segment_id": "segment",
        "created_run_id": "extraction_run",
    },
    "event_frame": {
        "actor_entity_id": "entity",
        "cause_fact_id": "fact",
        "created_run_id": "extraction_run",
        "effect_fact_id": "fact",
        "episode_id": "episode",
        "event_id": "event",
        "evidence_segment_id": "segment",
        "location_entity_id": "entity",
        "source_fact_id": "fact",
        "target_entity_id": "entity",
    },
    "media_artifact": {
        "artifact_id": "artifact",
        "created_run_id": "extraction_run",
        "project_entity_id": "entity",
        "source_segment_id": "segment",
    },
    "memory_context_audit": {},
    "procedure": {
        "created_run_id": "extraction_run",
        "evidence_segment_id": "segment",
        "project_entity_id": "entity",
        "scope_entity_id": "entity",
        "source_episode_id": "episode",
        "source_fact_id": "fact",
        "source_memory_id": "memory",
    },
    "procedure_step": {
        "created_run_id": "extraction_run",
        "evidence_segment_id": "segment",
        "procedure_id": "procedure",
        "source_fact_id": "fact",
    },
    "task_session": {
        "created_run_id": "extraction_run",
        "parent_session_id": "task_session",
        "project_entity_id": "entity",
        "scope_entity_id": "entity",
        "source_artifact_id": "artifact",
    },
    "visual_region": {
        "created_run_id": "extraction_run",
        "linked_entity_id": "entity",
        "linked_event_frame_id": "event_frame",
        "linked_fact_id": "fact",
        "linked_memory_id": "memory",
        "linked_procedure_id": "procedure",
        "media_artifact_id": "media_artifact",
        "source_segment_id": "segment",
    },
    "working_memory": {
        "created_run_id": "extraction_run",
        "evidence_segment_id": "segment",
        "promoted_memory_id": "memory",
        "session_id": "task_session",
        "source_fact_id": "fact",
        "source_segment_id": "segment",
    },
}


RELATIONSHIPS = {
    **AUXILIARY_FKS,
    "fact_evidence": {"fact_id": "fact", "segment_id": "segment"},
    "memory_support": {"memory_id": "memory", "fact_id": "fact", "episode_id": "episode"},
}


def auxiliary_relationships(table: str, payload: dict[str, Any]) -> list[tuple[str, str, str]]:
    refs = [
        (column, other, str(payload[column]))
        for column, other in RELATIONSHIPS[table].items()
        if payload.get(column) is not None
    ]
    dynamic = []
    if table == "memory_edge":
        dynamic = [("source_type", "source_id"), ("target_type", "target_id")]
    elif table == "review_queue":
        dynamic = [("target_type", "target_id")]
    elif table == "working_memory":
        dynamic = [("promotion_target_kind", "promotion_target_id")]
    for type_column, id_column in dynamic:
        if payload.get(id_column) is not None:
            kind = payload.get(type_column)
            if kind not in {*CORE_TYPES, "artifact", *AUXILIARY_FKS}:
                raise ValueError("unsupported polymorphic auxiliary endpoint")
            refs.append((id_column, kind, str(payload[id_column])))
    if table == "memory_context_audit":
        for column in ("retrieved_memory_ids", "injected_memory_ids"):
            for position, external in enumerate(payload.get(column) or []):
                refs.append((column + ":" + str(position), "memory", str(external)))
    return refs

"""Tenant-bound current aliases and logical identity for legacy producers."""

import json
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from .auth import Principal


def logical_identity(object_type: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    names = {
        "entity": ("entity_type", "canonical_name"),
        "fact": ("canonical_fingerprint",),
        "memory": ("fingerprint",),
        "segment": ("artifact_id", "ordinal"),
        "artifact": ("source_key",),
        "fact_evidence": ("fact_id", "segment_id"),
        "review_queue": ("target_type", "target_id", "reason", "status"),
        "publish_note": ("memory_id",),
    }.get(object_type)
    if names is None or any(fields.get(name) is None for name in names):
        return None
    return {name: fields[name] for name in names}


def lock_identity(
    conn: psycopg.Connection[Any],
    principal: Principal,
    object_type: str,
    identity: dict[str, Any] | None,
) -> None:
    if identity is not None:
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (
                str(principal.tenant_id)
                + ":"
                + object_type
                + ":"
                + json.dumps(identity, sort_keys=True),
            ),
        )


def resolve_memory(
    conn: psycopg.Connection[Any],
    principal: Principal,
    object_type: str,
    object_id: UUID | None,
    identity: dict[str, Any] | None = None,
) -> Any:
    base = """SELECT m.*,e.attributes AS extension_attributes,
              e.content_hash AS extension_content_hash,e.current_override AS extension_override,
              a.external_id AS client_external_id
              FROM braincore.memories m
              LEFT JOIN braincore.legacy_client_attributes e ON e.memory_id=m.id
              LEFT JOIN braincore.legacy_client_ids a ON a.memory_id=m.id
              WHERE m.tenant_id=%s AND m.object_type=%s"""
    if object_id is not None:
        row = conn.execute(
            base + " AND (m.id=%s OR m.legacy_external_id=%s OR a.external_id=%s) FOR UPDATE OF m",
            (principal.tenant_id, object_type, object_id, object_id, object_id),
        ).fetchone()
        if row is not None:
            return row
        other = conn.execute(
            "SELECT 1 FROM braincore.memories m LEFT JOIN braincore.legacy_client_ids a "
            "ON a.memory_id=m.id WHERE m.tenant_id=%s "
            "AND (m.id=%s OR m.legacy_external_id=%s OR a.external_id=%s)",
            (principal.tenant_id, object_id, object_id, object_id),
        ).fetchone()
        if other is not None:
            raise ValueError("legacy_object_type_conflict")
    if identity is None:
        return None
    row = conn.execute(
        base + " AND a.identity=%s::jsonb AND (%s<>'entity' OR m.content=%s) FOR UPDATE OF m",
        (
            principal.tenant_id,
            object_type,
            Jsonb(identity),
            object_type,
            identity.get("canonical_name"),
        ),
    ).fetchone()
    if row is not None:
        return row
    # Imported rows already have exact external IDs. Match canonical current
    # subtype fields without manufacturing a new source or ledger receipt.
    return conn.execute(
        base + " AND (CASE WHEN e.current_override THEN e.attributes "
        "WHEN m.legacy_attributes_ready THEN m.legacy_attributes "
        "ELSE e.attributes END) @> %s::jsonb AND (%s<>'entity' OR m.content=%s) "
        "ORDER BY m.updated_at DESC,m.id "
        "LIMIT 1 FOR UPDATE OF m",
        (
            principal.tenant_id,
            object_type,
            Jsonb(
                {
                    k: v
                    for k, v in identity.items()
                    if object_type != "entity" or k != "canonical_name"
                }
            ),
            object_type,
            identity.get("canonical_name"),
        ),
    ).fetchone()


def resolve_artifact(conn: psycopg.Connection[Any], principal: Principal, object_id: UUID) -> Any:
    return conn.execute(
        "SELECT a.*,i.external_id AS "
        "client_external_id,o.record_key->>'artifact_id' AS imported_id "
        "FROM braincore.artifacts a LEFT JOIN braincore.legacy_client_artifact_ids i "
        "ON i.artifact_id=a.id LEFT JOIN braincore.legacy_artifact_map o ON "
        "o.native_artifact_id=a.id "
        "WHERE a.id=%s OR i.external_id=%s OR (o.record_key->>'artifact_id')::uuid=%s "
        "ORDER BY (a.tenant_id=%s) DESC,a.id LIMIT 1",
        (object_id, object_id, object_id, principal.tenant_id),
    ).fetchone()


def external_memory_id(row: Any) -> str:
    return str(row["legacy_external_id"] or row["client_external_id"] or row["id"])

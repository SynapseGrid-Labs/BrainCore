"""Canonical artifact, provenance and control writes for legacy producers."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from .auth import Principal
from .legacy_identity import (
    external_memory_id,
    lock_identity,
    logical_identity,
    resolve_artifact,
    resolve_memory,
)
from .legacy_relationships import AUXILIARY_FKS, CORE_TYPES, auxiliary_relationships
from .legacy_types import DEFAULT_FIELDS, FIELD_TYPES, validate_subtype_fields

CONTROL_TYPES = frozenset(FIELD_TYPES) - {
    "entity",
    "fact",
    "memory",
    "segment",
    "episode",
    "artifact",
}


def _scope(principal: Principal, fields: dict[str, Any], prior: Any = None) -> dict[str, str]:
    if fields.get("scope_path"):
        result = {"project_id": str(fields["scope_path"])}
    elif prior is not None:
        result = dict(prior["scope"])
    else:
        result = principal.scope or {"run_id": "legacy-client:" + str(principal.key_id)}
    principal.require_scope(result)
    return result


def _prior_receipt(
    conn: psycopg.Connection[Any], principal: Principal, key: str, body: dict[str, Any]
) -> tuple[bytes, dict[str, Any] | None]:
    if not key.strip() or len(key) > 200:
        raise ValueError("invalid idempotency key")
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).digest()
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (str(principal.tenant_id) + str(principal.key_id) + key,),
    )
    prior = conn.execute(
        "SELECT request_hash,response FROM braincore.mutation_receipts "
        "WHERE tenant_id=%s AND actor_key_id=%s AND key=%s",
        (principal.tenant_id, principal.key_id, key),
    ).fetchone()
    if prior:
        if bytes(prior["request_hash"]) != digest:
            raise ValueError("idempotency_conflict")
        return digest, dict(prior["response"])
    return digest, None


def _finish(
    conn: psycopg.Connection[Any],
    principal: Principal,
    key: str,
    digest: bytes,
    kind: str,
    native_id: UUID,
    response: dict[str, Any],
) -> dict[str, Any]:
    conn.execute(
        "INSERT INTO braincore.audit_events(tenant_id,actor_key_id,action,target_type,target_id) "
        "VALUES (%s,%s,%s,%s,%s)",
        (principal.tenant_id, principal.key_id, "legacy_client." + kind, kind, native_id),
    )
    conn.execute(
        "INSERT INTO "
        "braincore.mutation_receipts(tenant_id,actor_key_id,key,kind,request_hash,response) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (principal.tenant_id, principal.key_id, key, kind, digest, Jsonb(response)),
    )
    return response


def mutate_artifact(
    conn: psycopg.Connection[Any],
    principal: Principal,
    *,
    fields: dict[str, Any],
    idempotency_key: str,
    object_id: UUID | None = None,
    action: str = "upsert",
    reason: str = "legacy artifact mutation",
) -> dict[str, Any]:
    principal.require("write")
    if any(name.startswith("__braincore") for name in fields):
        raise ValueError("internal subtype fields cannot be supplied")
    if action not in {"upsert", "retire", "restore"}:
        raise ValueError("unsupported artifact mutation")
    validate_subtype_fields("artifact", fields, new=False)
    with conn.transaction():
        digest, prior = _prior_receipt(
            conn,
            principal,
            idempotency_key,
            {"fields": fields, "id": str(object_id), "action": action, "reason": reason},
        )
        if prior:
            return prior
        identity = logical_identity("artifact", fields)
        lock_identity(conn, principal, "artifact", identity)
        existing = resolve_artifact(conn, principal, object_id) if object_id else None
        if existing is None and fields.get("source_key") is not None:
            existing = conn.execute(
                "SELECT a.*,i.external_id AS client_external_id,"
                "o.record_key->>'artifact_id' AS imported_id FROM braincore.artifacts a "
                "LEFT JOIN braincore.legacy_client_artifact_ids i ON i.artifact_id=a.id "
                "LEFT JOIN braincore.legacy_artifact_map o ON o.native_artifact_id=a.id "
                "WHERE a.tenant_id=%s AND (i.source_key=%s OR "
                "a.legacy_attributes->>'source_key'=%s) "
                "FOR UPDATE OF a",
                (principal.tenant_id, fields["source_key"], fields["source_key"]),
            ).fetchone()
        if existing is not None:
            if existing["tenant_id"] != principal.tenant_id:
                raise ValueError("artifact_not_owned")
            principal.require_scope(existing["scope"])
        elif action != "upsert":
            raise ValueError("artifact_not_found")
        patch = {**(DEFAULT_FIELDS.get("artifact", {}) if existing is None else {}), **fields}
        validate_subtype_fields("artifact", patch, new=existing is None)
        if existing is not None:
            patch["__braincore_sql_null_columns"] = [
                name
                for name in (existing["legacy_attributes"] or {}).get(
                    "__braincore_sql_null_columns", []
                )
                if name not in fields
            ]
        scope = _scope(principal, patch, existing)
        native_id = existing["id"] if existing else uuid4()
        display_id = (
            existing["imported_id"] or existing["client_external_id"] or existing["id"]
            if existing
            else object_id or uuid4()
        )
        metadata = (
            dict(existing["metadata"])
            if existing
            else {"legacy": {"content_kind": "source_record_manifest"}}
        )
        metadata["legacy"] = {**metadata.get("legacy", {}), "source_deleted": action == "retire"}
        # The artifact record is a source manifest; extracted segment content is
        # canonical on memories. Do not mislabel manifest bytes as a raw file.
        manifest = json.dumps(
            {"legacy_artifact": str(display_id), "producer": "legacy_client"}, sort_keys=True
        )
        conn.execute(
            """INSERT INTO braincore.artifacts
             (id,tenant_id,source_type,source_uri,content,content_sha256,scope,metadata,
              legacy_attributes,legacy_attributes_ready)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,true)
             ON CONFLICT(id) DO UPDATE SET source_type=EXCLUDED.source_type,
              source_uri=COALESCE(EXCLUDED.source_uri,braincore.artifacts.source_uri),
              scope=EXCLUDED.scope,metadata=EXCLUDED.metadata,
              legacy_attributes=COALESCE(braincore.artifacts.legacy_attributes,'{}'::jsonb)
                || EXCLUDED.legacy_attributes,legacy_attributes_ready=true""",
            (
                native_id,
                principal.tenant_id,
                patch.get("source_type", existing["source_type"] if existing else None),
                patch.get("original_path"),
                manifest,
                hashlib.sha256(manifest.encode()).digest(),
                Jsonb(scope),
                Jsonb(metadata),
                Jsonb(patch),
            ),
        )
        if existing is not None and "source_key" in fields:
            conn.execute(
                "UPDATE braincore.legacy_client_artifact_ids SET source_key=%s "
                "WHERE artifact_id=%s",
                (fields["source_key"], native_id),
            )
        if existing is None:
            conn.execute(
                "INSERT INTO braincore.legacy_client_artifact_ids"
                "(artifact_id,tenant_id,external_id,source_key) VALUES (%s,%s,%s,%s)",
                (native_id, principal.tenant_id, display_id, patch["source_key"]),
            )
        return _finish(
            conn,
            principal,
            idempotency_key,
            digest,
            "artifact",
            native_id,
            {
                "object_id": str(display_id),
                "native_id": str(native_id),
                "state": action,
                "created": existing is None,
            },
        )


def _reference(
    conn: psycopg.Connection[Any],
    principal: Principal,
    name: str,
    value: Any,
    expected: str,
) -> tuple[Any, str]:
    if expected == "artifact":
        row = resolve_artifact(conn, principal, UUID(str(value)))
        if (
            row is None
            or row["tenant_id"] != principal.tenant_id
            or not principal.permits_scope(row["scope"])
        ):
            raise ValueError("control_reference_not_found")
        row = {**row, "reference_kind": "artifact"}
        return row, str(row["imported_id"] or row["client_external_id"] or row["id"])
    if expected in CORE_TYPES:
        try:
            row = resolve_memory(conn, principal, expected, UUID(str(value)))
        except ValueError:
            raise ValueError("control_reference_not_found") from None
        if (
            row is None
            or row["tenant_id"] != principal.tenant_id
            or not principal.permits_scope(row["scope"])
        ):
            raise ValueError("control_reference_not_found")
        row = {**row, "reference_kind": "memory"}
        return row, external_memory_id(row)
    if expected in AUXILIARY_FKS:
        row = conn.execute(
            "SELECT id,tenant_id,scope,external_id,'control' AS reference_kind "
            "FROM braincore.legacy_client_controls WHERE tenant_id=%s AND object_type=%s "
            "AND (id::text=%s OR external_id::text=%s) AND NOT deleted FOR UPDATE",
            (principal.tenant_id, expected, str(value), str(value)),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT a.native_id AS id,a.tenant_id,a.scope,a.record_key,"
                "s.key_mapping->%s AS key_columns,'auxiliary' AS reference_kind "
                "FROM braincore.legacy_auxiliary_objects a JOIN braincore.legacy_sources s "
                "ON s.id=a.source_id WHERE a.tenant_id=%s AND a.source_table=%s "
                "AND (a.native_id::text=%s OR (jsonb_array_length(s.key_mapping->%s)=1 AND "
                "a.record_key=jsonb_build_object(s.key_mapping->%s->>0,%s::text))) "
                "AND NOT a.deleted FOR UPDATE OF a",
                (
                    expected,
                    principal.tenant_id,
                    expected,
                    str(value),
                    expected,
                    expected,
                    str(value),
                ),
            ).fetchone()
        if row is None or not principal.permits_scope(row["scope"]):
            raise ValueError("control_reference_not_found")
        external = row.get("external_id")
        if external is None:
            keys = row["key_columns"]
            if not isinstance(keys, list) or len(keys) != 1 or set(row["record_key"]) != set(keys):
                raise ValueError("control_reference_not_found")
            external = row["record_key"][keys[0]]
        return row, str(external)
    raise ValueError("unsupported control reference")


def _correct_evidence_quote(
    conn: psycopg.Connection[Any],
    principal: Principal,
    control_id: UUID,
    endpoints: dict[str, Any],
    excerpt: str,
    prior_excerpt: Any,
    prior_endpoints: dict[str, Any],
    reason: str,
) -> None:
    if not {"fact_id", "segment_id"} <= endpoints.keys():
        raise ValueError("control_reference_not_found")
    fact_id, segment_id = (endpoints[name]["id"] for name in ("fact_id", "segment_id"))
    memories = {
        row["id"]: row
        for row in conn.execute(
            "SELECT id,object_type,scope FROM braincore.memories "
            "WHERE tenant_id=%s AND id=ANY(%s) ORDER BY id FOR UPDATE",
            (principal.tenant_id, [fact_id, segment_id]),
        )
    }
    for memory_id, expected in ((fact_id, "fact"), (segment_id, "segment")):
        row = memories.get(memory_id)
        if (
            row is None
            or row["object_type"] != expected
            or not principal.permits_scope(row["scope"])
        ):
            raise ValueError("control_reference_not_found")
    evidence = conn.execute(
        "SELECT s.artifact_id,s.quote AS source_quote,s.artifact_tenant_id,"
        "s.access_grant_id AS source_grant_id,a.scope AS artifact_scope "
        "FROM braincore.memory_evidence s JOIN braincore.artifacts a "
        "ON a.id=s.artifact_id AND a.tenant_id=s.artifact_tenant_id "
        "WHERE s.memory_id=%s AND s.tenant_id=%s ORDER BY s.artifact_id "
        "FOR UPDATE OF s FOR SHARE OF a",
        (segment_id, principal.tenant_id),
    ).fetchall()
    if not evidence:
        raise ValueError("control_evidence_not_found")
    endpoints_changed = any(
        prior_endpoints.get(name, {}).get("id") != endpoints[name]["id"]
        for name in ("fact_id", "segment_id")
    )
    for row in evidence:
        current = conn.execute(
            "SELECT quote,artifact_tenant_id,access_grant_id FROM braincore.memory_evidence "
            "WHERE memory_id=%s AND tenant_id=%s AND artifact_id=%s FOR UPDATE",
            (fact_id, principal.tenant_id, row["artifact_id"]),
        ).fetchone()
        if current and current["artifact_tenant_id"] != row["artifact_tenant_id"]:
            raise ValueError("control_reference_not_found")
        grant_id = current["access_grant_id"] if current else row["source_grant_id"]
        if row["artifact_tenant_id"] == principal.tenant_id:
            if not principal.permits_scope(row["artifact_scope"]):
                raise ValueError("control_reference_not_found")
        else:
            for endpoint_grant, memory_id in (
                (grant_id, fact_id),
                (row["source_grant_id"], segment_id),
            ):
                grant = conn.execute(
                    "SELECT id FROM braincore.artifact_access_grants WHERE id=%s "
                    "AND artifact_id=%s AND owner_tenant_id=%s AND grantee_tenant_id=%s "
                    "AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now()) "
                    "AND %s::jsonb @> scope FOR SHARE",
                    (
                        endpoint_grant,
                        row["artifact_id"],
                        row["artifact_tenant_id"],
                        principal.tenant_id,
                        Jsonb(memories[memory_id]["scope"]),
                    ),
                ).fetchone()
                if grant is None:
                    raise ValueError("control_reference_not_found")
        if current:
            # Existing owner, artifact and grant stay fixed. The trigger checks
            # the current root grant again when replacing the quote.
            conn.execute(
                "UPDATE braincore.memory_evidence SET quote=%s "
                "WHERE memory_id=%s AND tenant_id=%s AND artifact_id=%s",
                (excerpt, fact_id, principal.tenant_id, row["artifact_id"]),
            )
        else:
            # Endpoint corrections can attach new evidence; prior canonical
            # links and original segment quotations remain intact.
            conn.execute(
                "INSERT INTO braincore.memory_evidence"
                "(tenant_id,memory_id,artifact_id,artifact_tenant_id,access_grant_id,quote) "
                "VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    principal.tenant_id,
                    fact_id,
                    row["artifact_id"],
                    row["artifact_tenant_id"],
                    grant_id,
                    excerpt,
                ),
            )
        if (
            current is None
            or current["quote"] != excerpt
            or endpoints_changed
            or prior_excerpt != excerpt
        ):
            conn.execute(
                "INSERT INTO braincore.audit_events"
                "(tenant_id,actor_key_id,action,target_type,target_id,details) "
                "VALUES(%s,%s,'legacy_client.fact_evidence.quote_corrected',"
                "'fact_evidence',%s,%s)",
                (
                    principal.tenant_id,
                    principal.key_id,
                    control_id,
                    Jsonb(
                        {
                            "fact_id": str(fact_id),
                            "segment_id": str(segment_id),
                            "prior_fact_id": str(prior_endpoints["fact_id"]["id"])
                            if "fact_id" in prior_endpoints
                            else None,
                            "prior_segment_id": str(prior_endpoints["segment_id"]["id"])
                            if "segment_id" in prior_endpoints
                            else None,
                            "artifact_id": str(row["artifact_id"]),
                            "artifact_tenant_id": str(row["artifact_tenant_id"]),
                            "access_grant_id": str(grant_id) if grant_id else None,
                            "source_quote": row["source_quote"],
                            "prior_quote": current["quote"] if current else None,
                            "next_quote": excerpt,
                            "prior_control_excerpt": prior_excerpt,
                            "reason": reason,
                        }
                    ),
                ),
            )


def mutate_control(
    conn: psycopg.Connection[Any],
    principal: Principal,
    *,
    object_type: str,
    fields: dict[str, Any],
    idempotency_key: str,
    object_id: UUID | None = None,
    action: str = "upsert",
    reason: str = "legacy control mutation",
) -> dict[str, Any]:
    principal.require("write")
    if any(name.startswith("__braincore") for name in fields):
        raise ValueError("internal subtype fields cannot be supplied")
    if object_type not in CONTROL_TYPES or action not in {"upsert", "retire", "restore"}:
        raise ValueError("unsupported control mutation")
    validate_subtype_fields(object_type, fields, new=False)
    with conn.transaction():
        digest, prior = _prior_receipt(
            conn,
            principal,
            idempotency_key,
            {
                "kind": object_type,
                "fields": fields,
                "id": str(object_id),
                "action": action,
                "reason": reason,
            },
        )
        if prior:
            return prior
        candidate = {**DEFAULT_FIELDS.get(object_type, {}), **fields}
        identity = logical_identity(object_type, candidate)
        lock_identity(conn, principal, object_type, identity)
        existing = None
        if object_id is not None or identity is not None:
            existing = conn.execute(
                "SELECT * FROM braincore.legacy_client_controls WHERE tenant_id=%s "
                "AND object_type=%s "
                "AND (external_id=%s OR identity=%s::jsonb) FOR UPDATE",
                (
                    principal.tenant_id,
                    object_type,
                    object_id,
                    Jsonb(identity) if identity else None,
                ),
            ).fetchone()
        if existing is None:
            auxiliary = conn.execute(
                "SELECT * FROM braincore.legacy_auxiliary_objects WHERE source_table=%s "
                "AND (tenant_id=%s OR (tenant_id IS NULL AND %s)) "
                "AND (native_id=%s OR EXISTS(SELECT 1 FROM "
                "jsonb_each_text(record_key) k WHERE k.value=%s) OR payload @> %s::jsonb) "
                "ORDER BY native_id LIMIT 1 FOR UPDATE",
                (
                    object_type,
                    principal.tenant_id,
                    object_type == "extraction_run" and "admin" in principal.roles,
                    object_id,
                    str(object_id),
                    Jsonb(identity) if identity else None,
                ),
            ).fetchone()
            if auxiliary is not None:
                if auxiliary["scope"] is None:
                    principal.require("admin")
                else:
                    principal.require_scope(auxiliary["scope"])
                immutable = set(auxiliary["record_key"])
                immutable.update(AUXILIARY_FKS.get(object_type, {}))
                immutable.update(
                    {
                        "source_type",
                        "source_id",
                        "target_type",
                        "target_id",
                        "promotion_target_kind",
                        "promotion_target_id",
                        "retrieved_memory_ids",
                        "injected_memory_ids",
                    }
                )
                immutable.update(
                    r["source_column"]
                    for r in conn.execute(
                        "SELECT source_column FROM braincore.legacy_auxiliary_refs "
                        "WHERE source_id=%s AND source_table=%s AND record_key=%s",
                        (auxiliary["source_id"], object_type, Jsonb(auxiliary["record_key"])),
                    )
                )
                for name in immutable | {"tenant", "scope_path"}:
                    if name in fields and fields[name] != auxiliary["payload"].get(name):
                        raise ValueError("legacy_endpoint_change_requires_translation")
                changes = {
                    **fields,
                    "__braincore_sql_null_columns": [
                        name
                        for name in auxiliary["payload"].get("__braincore_sql_null_columns", [])
                        if name not in fields
                    ],
                }
                conn.execute(
                    "UPDATE braincore.legacy_auxiliary_objects SET payload=payload||%s::jsonb,"
                    "deleted=%s,updated_at=now() WHERE native_id=%s",
                    (Jsonb(changes), action == "retire", auxiliary["native_id"]),
                )
                display = object_id or next(iter(auxiliary["record_key"].values()))
                return _finish(
                    conn,
                    principal,
                    idempotency_key,
                    digest,
                    object_type,
                    auxiliary["native_id"],
                    {
                        "object_id": str(display),
                        "native_id": str(auxiliary["native_id"]),
                        "state": action,
                    },
                )
        if existing is None and action != "upsert":
            # Original migrated evidence/support remains canonical in typed
            # graph tables. Retirement preserves its attributes and source IDs.
            graph = {
                "fact_evidence": "legacy_fact_evidence",
                "memory_support": "legacy_memory_support",
            }.get(object_type)
            if graph and object_id:
                key = "fact_evidence_id" if object_type == "fact_evidence" else "support_id"
                from psycopg import sql

                row = conn.execute(
                    sql.SQL(
                        "SELECT * FROM braincore.{} WHERE tenant_id=%s "
                        "AND attributes->>%s=%s FOR UPDATE"
                    ).format(sql.Identifier(graph)),
                    (principal.tenant_id, key, str(object_id)),
                ).fetchone()
                if row is None:
                    raise ValueError("control_not_found")
                parent = row[
                    "native_fact_id" if object_type == "fact_evidence" else "native_memory_id"
                ]
                root = conn.execute(
                    "SELECT scope FROM braincore.memories WHERE id=%s AND tenant_id=%s",
                    (parent, principal.tenant_id),
                ).fetchone()
                if root is None:
                    raise ValueError("control_parent_missing")
                principal.require_scope(root["scope"])
                conn.execute(
                    sql.SQL(
                        "UPDATE braincore.{} SET deleted=%s WHERE source_id=%s AND record_key=%s"
                    ).format(sql.Identifier(graph)),
                    (action == "retire", row["source_id"], Jsonb(row["record_key"])),
                )
                return _finish(
                    conn,
                    principal,
                    idempotency_key,
                    digest,
                    object_type,
                    row["native_id"],
                    {
                        "object_id": str(object_id),
                        "native_id": str(row["native_id"]),
                        "state": action,
                    },
                )
            raise ValueError("control_not_found")
        if existing:
            principal.require_scope(existing["scope"])
        patch = {**(DEFAULT_FIELDS.get(object_type, {}) if existing is None else {}), **fields}
        validate_subtype_fields(object_type, patch, new=existing is None)
        if existing is not None:
            patch["__braincore_sql_null_columns"] = [
                name
                for name in existing["fields"].get("__braincore_sql_null_columns", [])
                if name not in fields
            ]
        merged = {**(existing["fields"] if existing else {}), **patch}
        required_endpoints = {
            "fact_evidence": ("fact_id", "segment_id"),
            "memory_support": ("memory_id",),
            "extraction_run": ("artifact_id",),
            "publish_note": ("memory_id",),
        }.get(object_type, ())
        if any(merged.get(name) is None for name in required_endpoints):
            raise ValueError("required control endpoint missing")
        if object_type == "memory_support" and not any(
            merged.get(name) is not None for name in ("fact_id", "episode_id")
        ):
            raise ValueError("memory_support requires fact or episode")
        refs = []
        scope = _scope(principal, merged, existing)
        relationships = auxiliary_relationships(object_type, merged)
        old_relationships = (
            auxiliary_relationships(object_type, existing["fields"]) if existing else []
        )
        changed_refs = {
            name
            for name, kind, value in relationships + old_relationships
            if existing is None
            or (name, kind, value) not in relationships
            or (name, kind, value) not in old_relationships
        }
        if existing and (
            scope != existing["scope"]
            or (changed_refs and object_type in {"fact_evidence", "memory_support"})
        ):
            changed_refs.update(name for name, _, _ in relationships + old_relationships)
        for name, kind, value in relationships:
            if existing is not None and name not in changed_refs:
                continue
            row, external_id = _reference(conn, principal, name, value, kind)
            if ":" in name:
                column, position = name.split(":", 1)
                merged[column] = list(merged[column])
                merged[column][int(position)] = external_id
                patch[column] = merged[column]
            else:
                merged[name] = external_id
                patch[name] = external_id
            refs.append((name, kind, row))
            parent_field = {
                "fact_evidence": "fact_id",
                "memory_support": "memory_id",
                "publish_note": "memory_id",
                "memory_context_audit": "memory_id",
                "extraction_run": "artifact_id",
                "review_queue": "target_id",
            }.get(object_type)
            if existing is None and name == parent_field:
                scope = dict(row["scope"])
        principal.require_scope(scope)
        identity = logical_identity(object_type, merged)
        native_id = existing["id"] if existing else uuid4()
        display_id = (
            existing["external_id"]
            if existing
            else object_id
            or (UUID(merged["memory_id"]) if object_type == "publish_note" else uuid4())
        )
        now = datetime.now(UTC).isoformat()
        if existing is None:
            for name in ("created_at", "started_at"):
                if name in FIELD_TYPES[object_type] and name not in patch:
                    patch[name] = now
        conn.execute(
            """INSERT INTO braincore.legacy_client_controls
             (id,tenant_id,object_type,external_id,scope,fields,identity,deleted)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
             ON CONFLICT(id) DO UPDATE SET fields=braincore.legacy_client_controls.fields
              || EXCLUDED.fields,scope=EXCLUDED.scope,identity=EXCLUDED.identity,
              deleted=EXCLUDED.deleted,updated_at=now()""",
            (
                native_id,
                principal.tenant_id,
                object_type,
                display_id,
                Jsonb(scope),
                Jsonb(patch),
                Jsonb(identity) if identity else None,
                action == "retire",
            ),
        )
        correct_quote = (
            existing is not None and object_type == "fact_evidence" and "excerpt" in fields
        )
        prior_evidence_endpoints = (
            {
                row["field"]: {"id": row["memory_id"]}
                for row in conn.execute(
                    "SELECT field,memory_id FROM braincore.legacy_client_control_refs "
                    "WHERE control_id=%s AND memory_id IS NOT NULL",
                    (native_id,),
                )
            }
            if correct_quote
            else {}
        )
        for name in changed_refs:
            conn.execute(
                "DELETE FROM braincore.legacy_client_control_refs WHERE control_id=%s AND field=%s",
                (native_id, name),
            )
        for name, _kind, row in refs:
            conn.execute(
                "INSERT INTO braincore.legacy_client_control_refs"
                "(control_id,field,tenant_id,memory_id,artifact_id,child_control_id,auxiliary_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    native_id,
                    name,
                    principal.tenant_id,
                    row["id"] if row["reference_kind"] == "memory" else None,
                    row["id"] if row["reference_kind"] == "artifact" else None,
                    row["id"] if row["reference_kind"] == "control" else None,
                    row["id"] if row["reference_kind"] == "auxiliary" else None,
                ),
            )
        endpoints = {name: row for name, _kind, row in refs}
        if correct_quote and not refs:
            endpoints = prior_evidence_endpoints
        if (
            object_type in {"fact_evidence", "memory_support"}
            and action == "upsert"
            and endpoints
            and not correct_quote
        ):
            root_name = "fact_id" if object_type == "fact_evidence" else "memory_id"
            root_id = endpoints[root_name]["id"]
            children = [
                row["id"]
                for name, row in endpoints.items()
                if name in {"segment_id", "fact_id", "episode_id"} and name != root_name
            ]
            for child_id in children:
                conn.execute(
                    "INSERT INTO braincore.memory_evidence(tenant_id,memory_id,artifact_id,quote,"
                    "artifact_tenant_id,access_grant_id) SELECT %s,%s,e.artifact_id,"
                    "COALESCE(%s,e.quote),e.artifact_tenant_id,e.access_grant_id "
                    "FROM braincore.memory_evidence e WHERE e.memory_id=%s AND e.tenant_id=%s "
                    "AND NOT EXISTS(SELECT 1 FROM braincore.memory_evidence old "
                    "WHERE old.tenant_id=%s AND old.memory_id=%s AND old.artifact_id=e.artifact_id "
                    "AND old.quote=COALESCE(%s,e.quote)) ON CONFLICT DO NOTHING",
                    (
                        principal.tenant_id,
                        root_id,
                        merged.get("excerpt"),
                        child_id,
                        principal.tenant_id,
                        principal.tenant_id,
                        root_id,
                        merged.get("excerpt"),
                    ),
                )
        if existing is not None and correct_quote and action == "upsert":
            _correct_evidence_quote(
                conn,
                principal,
                native_id,
                endpoints,
                merged["excerpt"],
                existing["fields"].get("excerpt"),
                prior_evidence_endpoints,
                reason,
            )
        return _finish(
            conn,
            principal,
            idempotency_key,
            digest,
            object_type,
            native_id,
            {"object_id": str(display_id), "native_id": str(native_id), "state": action},
        )

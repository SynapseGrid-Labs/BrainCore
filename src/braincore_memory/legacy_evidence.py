"""Original artifact manifests and direct typed evidence links.

Cross-owner grants require an explicit protected policy for each original
artifact, recipient and exact native scope. No default owner is globally shared.
Fact evidence/support graphs and auxiliary contracts have independent gates.
"""

import hashlib
import json
from typing import Any
from uuid import UUID, uuid5

import psycopg
from psycopg import sql

from .legacy_authority import guard_import_rows
from .legacy_objects import prepare_tenants


def scope_hash(scope: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(scope, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def import_artifacts(
    target: psycopg.Connection[Any],
    source_id: UUID,
    batch_size: int = 500,
    *,
    changed_only: bool = False,
) -> int:
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid artifact batch size")
    prepare_tenants(target, source_id, ("artifact",))
    after = None
    imported = 0
    while True:
        with target.transaction():
            rows = target.execute(
                """SELECT h.record_key::text AS key,h.payload_hash,p.tenant_id,
                braincore.legacy_object_id(h.source_id,h.source_table,h.record_key) AS id,
                v.payload::text AS attributes,h.deleted,
                mapped.imported_payload_hash AS prior_hash,native.legacy_attributes_ready,
                COALESCE((native.metadata->'legacy'->>'source_deleted')::boolean,false)
                  AS prior_deleted,
                (v.payload - '__braincore_sql_null_columns')::text AS manifest,
                v.payload->>'source_type' AS source_type,v.payload->>'original_path' AS source_uri,
                v.payload->>'scope_path' AS scope_path,v.payload->>'sha256' AS original_sha256,
                (v.payload->>'discovered_at')::timestamptz AS original_created
                FROM braincore.legacy_record_heads h
                JOIN braincore.legacy_record_versions v USING
                  (source_id,source_table,record_key,payload_hash)
                JOIN braincore.legacy_tenant_map p ON p.source_id=h.source_id
                                                AND p.original_tenant=v.payload->>'tenant'
                LEFT JOIN braincore.legacy_artifact_map mapped
                  ON mapped.source_id=h.source_id AND mapped.record_key=h.record_key
                LEFT JOIN braincore.artifacts native ON native.id=mapped.native_artifact_id
                WHERE h.source_id=%s AND h.source_table='artifact'
                  AND (%s OR native.legacy_attributes_ready IS DISTINCT FROM true
                    OR mapped.imported_payload_hash IS DISTINCT FROM h.payload_hash
                    OR COALESCE((native.metadata->'legacy'->>'source_deleted')::boolean,false)
                       IS DISTINCT FROM h.deleted)
                  AND (%s::jsonb IS NULL OR h.record_key > %s::jsonb)
                ORDER BY h.record_key LIMIT %s""",
                (source_id, not changed_only, after, after, batch_size),
            ).fetchall()
            if not rows:
                break
            preserved = guard_import_rows(
                target, "artifacts", [(r["id"], bytes(r["payload_hash"])) for r in rows]
            )
            for row in rows:
                if row["id"] in preserved:
                    continue
                if (
                    not row["legacy_attributes_ready"]
                    and row["prior_hash"] == row["payload_hash"]
                    and row["prior_deleted"] == row["deleted"]
                ):
                    # First compatibility hydration must not rewrite unchanged
                    # manifest content or current metadata after column installation.
                    hydrated = target.execute(
                        "UPDATE braincore.artifacts SET legacy_attributes=%s::jsonb, "
                        "legacy_attributes_ready=true WHERE id=%s AND tenant_id=%s RETURNING id",
                        (row["attributes"], row["id"], row["tenant_id"]),
                    ).fetchone()
                    if hydrated is None:
                        raise ValueError("artifact hydration ownership conflicts")
                    continue
                if not row["source_type"]:
                    raise ValueError("source artifact type is missing")
                scope = (
                    {"project_id": row["scope_path"]}
                    if row["scope_path"]
                    else {"run_id": "legacy-source:" + str(source_id)}
                )
                metadata = {
                    "legacy": {
                        "content_kind": "source_record_manifest",
                        "original_sha256": row["original_sha256"],
                    }
                }
                if row["deleted"]:
                    metadata["legacy"]["source_deleted"] = True
                record = target.execute(
                    """INSERT INTO braincore.artifacts AS a
                    (id,tenant_id,source_type,source_uri,content,content_sha256,scope,metadata,created_at,
                     legacy_attributes,legacy_attributes_ready)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(%s,now()),%s::jsonb,true)
                    ON CONFLICT (id) DO UPDATE SET
                      content=EXCLUDED.content,content_sha256=EXCLUDED.content_sha256,
                      source_type=EXCLUDED.source_type,source_uri=EXCLUDED.source_uri,
                      scope=EXCLUDED.scope,metadata=(a.metadata-'legacy')||EXCLUDED.metadata,
                      legacy_attributes=EXCLUDED.legacy_attributes,legacy_attributes_ready=true
                    WHERE a.tenant_id=EXCLUDED.tenant_id RETURNING id""",
                    (
                        row["id"],
                        row["tenant_id"],
                        row["source_type"],
                        row["source_uri"],
                        row["manifest"],
                        hashlib.sha256(row["manifest"].encode()).digest(),
                        json.dumps(scope),
                        json.dumps(metadata),
                        row["original_created"],
                        row["attributes"],
                    ),
                ).fetchone()
                if record is None:
                    raise ValueError("artifact ownership changed; audited reassignment is required")
                target.execute(
                    """INSERT INTO braincore.legacy_artifact_map AS m
                    (source_id,record_key,tenant_id,native_artifact_id,imported_payload_hash)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT (source_id,record_key)
                    DO UPDATE SET imported_payload_hash=EXCLUDED.imported_payload_hash
                    WHERE m.tenant_id=EXCLUDED.tenant_id
                      AND m.native_artifact_id=EXCLUDED.native_artifact_id""",
                    (source_id, row["key"], row["tenant_id"], row["id"], row["payload_hash"]),
                )
                # A changed map hash invalidates compatibility attributes. The
                # exact new attributes above are current in this same transaction.
                target.execute(
                    "UPDATE braincore.artifacts SET legacy_attributes_ready=true "
                    "WHERE id=%s AND tenant_id=%s AND NOT legacy_attributes_ready",
                    (row["id"], row["tenant_id"]),
                )
            after = rows[-1]["key"]
            imported += len(rows)
    return imported


def validate_sharing_policy(
    source_id: UUID, policy: dict[str, Any]
) -> dict[tuple[str, str], dict[str, Any]]:
    """Validate resource/recipient pairs and exact canonical scope hashes."""
    if policy.get("format_version") != 1 or policy.get("source_id") != str(source_id):
        raise ValueError("sharing policy does not match source")
    approved: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in policy.get("approved_shares", []):
        artifact_id = str(UUID(entry["artifact_external_id"]))
        recipient = entry["grantee_original_tenant"]
        if not isinstance(recipient, str) or not recipient.strip():
            raise ValueError("sharing policy requires an original recipient tenant")
        if not entry.get("reason") or not entry.get("scope_hashes"):
            raise ValueError("sharing policy requires an audit reason and exact scopes")
        hashes = entry["scope_hashes"]
        if not isinstance(hashes, list) or any(
            not isinstance(value, str)
            or len(value) != 64
            or any(c not in "0123456789abcdef" for c in value)
            for value in hashes
        ):
            raise ValueError("sharing policy requires canonical scope hashes")
        if (artifact_id, recipient) in approved:
            raise ValueError("duplicate sharing policy resource/recipient")
        approved[(artifact_id, recipient)] = entry
    return approved


def ensure_evidence_grant(
    target: psycopg.Connection[Any],
    source_id: UUID,
    row: dict[str, Any],
    approval: dict[str, Any],
) -> UUID:
    """Create an audited exact-scope grant without renewing revoked/expired grants."""
    grant_id = uuid5(
        source_id,
        "grant:"
        + row["artifact_external_id"]
        + ":"
        + row["original_tenant"]
        + ":"
        + scope_hash(row["scope"]),
    )
    exists = target.execute(
        "SELECT *, (expires_at IS NOT NULL AND expires_at <= statement_timestamp()) "
        "AS expired FROM "
        "braincore.artifact_access_grants WHERE id=%s",
        (grant_id,),
    ).fetchone()
    if exists and exists["revoked_at"] is not None:
        raise ValueError("migration cannot re-enable a revoked evidence grant")
    if exists and exists["expired"]:
        raise ValueError("migration cannot renew an expired evidence grant")
    if exists and (
        exists["artifact_id"] != row["native_artifact_id"]
        or exists["owner_tenant_id"] != row["artifact_tenant_id"]
        or exists["grantee_tenant_id"] != row["tenant_id"]
        or exists["scope"] != row["scope"]
    ):
        raise ValueError("existing evidence grant has conflicting ownership or scope")
    if not exists:
        target.execute(
            """INSERT INTO braincore.artifact_access_grants
            (id,artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (
                grant_id,
                row["native_artifact_id"],
                row["artifact_tenant_id"],
                row["tenant_id"],
                json.dumps(row["scope"]),
                approval["reason"],
            ),
        )
        target.execute(
            """INSERT INTO braincore.audit_events
            (tenant_id,action,target_type,target_id,details)
            VALUES (%s,'migration.evidence.grant','artifact',%s,%s)""",
            (
                row["artifact_tenant_id"],
                row["native_artifact_id"],
                json.dumps(
                    {
                        "source_id": str(source_id),
                        "grant_id": str(grant_id),
                        "grantee_tenant_id": str(row["tenant_id"]),
                        "scope": row["scope"],
                        "reason": approval["reason"],
                        "event_origin": "migration",
                    }
                ),
            ),
        )
    return grant_id


def import_direct_evidence(
    target: psycopg.Connection[Any], source_id: UUID, policy: dict[str, Any], batch_size: int = 1000
) -> dict[str, Any]:
    if not 1 <= batch_size <= 1000:
        raise ValueError("invalid evidence batch size")
    approved = validate_sharing_policy(source_id, policy)
    after = None
    linked = 0
    cross_owner = 0
    native_preserved = 0
    while True:
        with target.transaction():
            cursor_predicate = "AND id>%s::uuid" if after is not None else ""
            values: list[object] = [source_id]
            if after is not None:
                values.append(after)
            values.append(batch_size)
            rows = target.execute(
                sql.SQL("""WITH selected_memories AS MATERIALIZED (
                SELECT id,tenant_id,scope,content,object_type
                FROM braincore.memories
                WHERE legacy_source_id=%s AND object_type IN ('segment','episode')
                  {cursor_predicate}
                ORDER BY id LIMIT %s
                )
                SELECT m.id,m.tenant_id,m.scope,m.content,m.object_type,h.payload_hash,
                CASE WHEN m.object_type='segment' THEN v.payload->>'artifact_id'
                     ELSE v.payload->>'primary_artifact_id' END AS artifact_external_id,
                a.native_artifact_id,a.tenant_id AS artifact_tenant_id,t.original_tenant
                FROM selected_memories m
                JOIN braincore.legacy_object_map o ON o.native_memory_id=m.id
                JOIN braincore.legacy_record_heads h ON h.source_id=o.source_id
                  AND h.source_table=o.source_table AND h.record_key=o.record_key
                JOIN braincore.legacy_record_versions v ON v.source_id=h.source_id
                  AND v.source_table=h.source_table AND v.record_key=h.record_key
                  AND v.payload_hash=h.payload_hash
                JOIN braincore.legacy_tenant_map t ON t.source_id=o.source_id
                  AND t.tenant_id=m.tenant_id
                LEFT JOIN braincore.legacy_artifact_map a ON a.source_id=o.source_id
                  AND a.record_key=
                  jsonb_build_object('artifact_id',CASE WHEN m.object_type='segment'
                    THEN v.payload->>'artifact_id' ELSE v.payload->>'primary_artifact_id' END)
                ORDER BY m.id FOR SHARE OF h""").format(cursor_predicate=sql.SQL(cursor_predicate)),
                values,
            ).fetchall()
            if not rows:
                break
            preserved = guard_import_rows(
                target, "memories", [(r["id"], bytes(r["payload_hash"])) for r in rows]
            )
            for row in rows:
                if row["id"] in preserved:
                    native_preserved += 1
                    continue
                if row["artifact_external_id"] is None:
                    if row["object_type"] == "segment":
                        raise ValueError("source segment is missing its artifact")
                    continue
                if row["native_artifact_id"] is None:
                    raise ValueError("source artifact relationship has not been mapped")
                grant_id = None
                if row["tenant_id"] != row["artifact_tenant_id"]:
                    approval = approved.get((row["artifact_external_id"], row["original_tenant"]))
                    if approval is None or scope_hash(row["scope"]) not in approval["scope_hashes"]:
                        raise ValueError(
                            "cross-owner evidence lacks reviewed record/scope approval"
                        )
                    grant_id = ensure_evidence_grant(target, source_id, row, approval)
                    cross_owner += 1
                target.execute(
                    """INSERT INTO braincore.memory_evidence
                    (tenant_id,memory_id,artifact_id,artifact_tenant_id,access_grant_id,quote)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (memory_id,artifact_id) DO UPDATE
                      SET quote=EXCLUDED.quote,access_grant_id=EXCLUDED.access_grant_id
                    WHERE braincore.memory_evidence.quote IS DISTINCT FROM EXCLUDED.quote
                       OR braincore.memory_evidence.access_grant_id
                          IS DISTINCT FROM EXCLUDED.access_grant_id""",
                    (
                        row["tenant_id"],
                        row["id"],
                        row["native_artifact_id"],
                        row["artifact_tenant_id"],
                        grant_id,
                        row["content"] if row["object_type"] == "segment" else "",
                    ),
                )
                linked += 1
            after = rows[-1]["id"]
    return {
        "direct_evidence_gate": "PASS",
        "linked_objects": linked,
        "cross_owner_links": cross_owner,
        "native_preserved_objects": native_preserved,
        "fact_support_graph_gate": "NOT RUN",
        "auxiliary_semantic_gate": "NOT RUN",
    }

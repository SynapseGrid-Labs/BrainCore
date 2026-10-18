"""Canonical legacy-client compatibility; never query a second core store.

Read views retain legacy field names and IDs but use native content, validity,
lifecycle, embeddings and tenant ownership. Source-only subtype attributes are
validated native extensions. SQL writers using ON CONFLICT on old tables must
call the explicit mutation contract instead: PostgreSQL views do not support it.
"""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from .auth import Principal
from .config import get_settings
from .legacy_identity import (
    external_memory_id,
    lock_identity,
    logical_identity,
    resolve_artifact,
    resolve_memory,
)
from .legacy_types import DEFAULT_FIELDS, validate_subtype_fields

OBJECT_TYPES = frozenset({"entity", "fact", "memory", "segment", "episode"})


def hydrate_client_attributes(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 2000
) -> dict[str, Any]:
    """Attach reconciled original subtype fields, excluding stale source vectors.

    Run after every native import/delta. A map hash change invalidates the
    extension in the same transaction, so a partially imported row is never
    exposed by the compatibility views. Canonical content is not replaced here.
    """
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid compatibility batch size")
    rows = 0
    for object_type in sorted(OBJECT_TYPES):
        after = None
        while True:
            with target.transaction():
                batch = target.execute(
                    """WITH selected AS MATERIALIZED (
                       SELECT o.* FROM braincore.legacy_object_map o
                       LEFT JOIN braincore.legacy_client_attributes e
                         ON e.memory_id=o.native_memory_id
                       WHERE o.source_id=%s AND o.source_table=%s AND e.memory_id IS NULL
                         AND (%s::jsonb IS NULL OR o.record_key>%s::jsonb)
                       ORDER BY o.record_key LIMIT %s
                     ), page AS (
                       SELECT o.native_memory_id AS id,o.tenant_id,o.imported_payload_hash,
                         v.payload,m.content,o.record_key
                       FROM selected o JOIN braincore.legacy_record_versions v
                         ON v.source_id=o.source_id AND v.source_table=o.source_table
                          AND v.record_key=o.record_key AND v.payload_hash=o.imported_payload_hash
                       JOIN braincore.memories m ON m.id=o.native_memory_id
                         AND m.tenant_id=o.tenant_id
                     ), changed AS (
                       INSERT INTO braincore.legacy_client_attributes
                         (memory_id,tenant_id,attributes,content_hash,search_vector,imported_payload_hash)
                       SELECT id,tenant_id,payload-'embedding'-'fts'-'content',
                         public.digest(content,'sha256'),(payload->>'fts')::tsvector,
                         imported_payload_hash FROM page ON CONFLICT DO NOTHING RETURNING memory_id
                     ) SELECT count(*) AS n,
                       (SELECT record_key::text FROM selected
                         ORDER BY record_key DESC LIMIT 1) AS last
                       FROM changed""",
                    (source_id, object_type, after, after, batch_size),
                ).fetchone()
                assert batch is not None
                rows += batch["n"]
                if batch["n"] == 0:
                    break
                after = batch["last"]
    missing = target.execute(
        "SELECT count(*) AS n FROM braincore.memories m LEFT JOIN "
        "braincore.legacy_client_attributes e ON e.memory_id=m.id WHERE m.legacy_source_id=%s "
        "AND e.memory_id IS NULL",
        (source_id,),
    ).fetchone()
    assert missing is not None
    return {
        "hydrated_rows": rows,
        "missing_rows": missing["n"],
        "attribute_gate": "PASS" if missing["n"] == 0 else "FAIL",
    }


def configure_legacy_read(
    conn: psycopg.Connection[Any], principal: Principal, *, include_inactive: bool = False
) -> None:
    """Set transaction-local identity from an authenticated principal only."""
    principal.require("read")
    conn.execute("SELECT set_config('braincore.compat_read_tenants','',true)")
    conn.execute(
        "SELECT set_config('braincore.compat_is_admin',%s,true)",
        ("true" if "admin" in principal.roles else "false",),
    )
    conn.execute(
        "SELECT set_config('braincore.compat_include_inactive',%s,true)",
        ("true" if include_inactive else "false",),
    )
    conn.execute(
        "SELECT set_config('braincore.compat_tenant_id',%s,true)", (str(principal.tenant_id),)
    )
    conn.execute(
        "SELECT set_config('braincore.compat_scope',%s,true)",
        (json.dumps(principal.scope) if principal.scope is not None else "null",),
    )


def _render(object_type: str, fields: dict[str, Any]) -> str:
    if object_type == "entity":
        content = fields.get("canonical_name")
    elif object_type == "fact":
        value = fields.get("object_value")
        content = (
            str(fields.get("predicate") or "")
            + " "
            + (json.dumps(value, ensure_ascii=False) if value is not None else "")
        )
    elif object_type == "segment":
        content = fields.get("content")
    else:
        content = (
            str(fields.get("title") or "")
            + " "
            + str(fields.get("narrative" if object_type == "memory" else "summary") or "")
        )
    if not isinstance(content, str) or not content.strip():
        raise ValueError("legacy object requires nonempty canonical content")
    return content


def mutate_legacy_object(
    conn: psycopg.Connection[Any],
    principal: Principal,
    *,
    object_type: str,
    fields: dict[str, Any],
    idempotency_key: str,
    object_id: UUID | None = None,
    action: str = "upsert",
    reason: str = "legacy client mutation",
) -> dict[str, Any]:
    """Explicit idempotent canonical write contract for legacy subtype callers.

    It is deliberately not an arbitrary SQL translator. Embeddings are queued
    through the ordinary native worker and stale source vectors are rejected.
    Lifecycle changes retain records, revisions and authenticated audit history.
    """
    principal.require("write")
    if principal.session_id is not None and not principal.csrf_valid:
        raise ValueError("csrf_failed")
    if object_type not in OBJECT_TYPES or action not in {"upsert", "suppress", "retire", "restore"}:
        raise ValueError("unsupported legacy mutation")
    if not idempotency_key.strip() or len(idempotency_key) > 200:
        raise ValueError("invalid idempotency key")
    if set(fields) & {"embedding", "fts", "tenant", "tenant_id"}:
        raise ValueError("identity and embedding values cannot be supplied by legacy callers")
    if any(name.startswith("__braincore") for name in fields):
        raise ValueError("internal subtype fields cannot be supplied")
    if fields.get("priority") is not None and not 1 <= int(fields["priority"]) <= 10:
        raise ValueError("invalid legacy priority")
    validate_subtype_fields(object_type, fields, new=False)
    encoded = json.dumps(
        {
            "type": object_type,
            "fields": fields,
            "id": str(object_id),
            "action": action,
            "reason": reason,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    request_hash = hashlib.sha256(encoded.encode()).digest()
    with conn.transaction():
        # Serializes concurrent retries before checking the durable receipt.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (str(principal.tenant_id) + str(principal.key_id) + idempotency_key,),
        )
        prior = conn.execute(
            "SELECT request_hash,response FROM braincore.mutation_receipts "
            "WHERE tenant_id=%s AND actor_key_id=%s AND key=%s",
            (principal.tenant_id, principal.key_id, idempotency_key),
        ).fetchone()
        if prior is not None:
            if bytes(prior["request_hash"]) != request_hash:
                raise ValueError("idempotency_conflict")
            return dict(prior["response"])
        identity = logical_identity(object_type, fields)
        lock_identity(conn, principal, object_type, identity)
        existing = resolve_memory(conn, principal, object_type, object_id, identity)
        if action != "upsert" and existing is None:
            raise ValueError("legacy_object_not_found")
        patch_fields = {
            **(DEFAULT_FIELDS.get(object_type, {}) if existing is None else {}),
            **fields,
        }
        validate_subtype_fields(object_type, patch_fields, new=existing is None)
        if existing is not None:
            prior_attributes = (
                existing["extension_attributes"]
                if existing["extension_override"]
                else existing["legacy_attributes"]
                if existing["legacy_attributes_ready"]
                else existing["extension_attributes"] or {}
            )
            original_nulls = prior_attributes.get("__braincore_sql_null_columns", [])
            patch_fields["__braincore_sql_null_columns"] = [
                name for name in original_nulls if name not in fields
            ]
        attributes = {
            **(
                (
                    existing["extension_attributes"]
                    if existing["extension_override"]
                    else existing["legacy_attributes"]
                    if existing["legacy_attributes_ready"]
                    else existing["extension_attributes"] or {}
                )
                if existing
                else {}
            ),
            **patch_fields,
        }
        scope_path = (
            fields.get("scope_path")
            if "scope_path" in fields
            else existing["project_id"]
            if existing
            else attributes.get("scope_path")
        )
        scope = (
            existing["scope"]
            if existing
            else (
                {"project_id": scope_path}
                if scope_path
                else principal.scope or {"run_id": "legacy-client:" + str(principal.key_id)}
            )
        )
        if "scope_path" in fields or existing is None:
            if scope_path:
                scope = {"project_id": scope_path}
            elif "scope_path" in fields:
                scope = principal.scope or {"run_id": "legacy-client:" + str(principal.key_id)}
        if not isinstance(scope, dict) or not scope:
            raise ValueError("legacy mutation requires explicit authorized scope")
        principal.require_scope(scope)
        if existing is not None:
            principal.require_scope(existing["scope"])
        if action == "upsert":
            content_field = {"entity": "canonical_name", "segment": "content"}.get(object_type)
            if existing is not None and content_field is not None and content_field not in fields:
                attributes[content_field] = existing["content"]
            if (
                object_type == "segment"
                and existing is not None
                and "content" not in fields
                or (
                    object_type == "fact"
                    and existing is not None
                    and not set(fields) & {"predicate", "object_value"}
                )
            ):
                content = existing["content"]
            elif object_type == "fact":
                exact = conn.execute(
                    """WITH merged AS (
                      SELECT COALESCE(CASE WHEN m.legacy_attributes_ready
                        THEN m.legacy_attributes ELSE e.attributes END,'{}'::jsonb)
                        || %s::jsonb AS a
                      FROM (SELECT 1) x LEFT JOIN braincore.memories m
                        ON m.id=%s AND m.tenant_id=%s
                      LEFT JOIN braincore.legacy_client_attributes e ON e.memory_id=m.id
                    ) SELECT COALESCE(a->>'predicate','') || ' ' ||
                      CASE WHEN (a->'__braincore_sql_null_columns') ? 'object_value'
                        AND NOT (%s::jsonb ? 'object_value') THEN ''
                        ELSE COALESCE((a->'object_value')::text,'') END AS content FROM merged""",
                    (
                        Jsonb(patch_fields),
                        existing["id"] if existing else None,
                        principal.tenant_id,
                        Jsonb(patch_fields),
                    ),
                ).fetchone()
                assert exact is not None
                content = exact["content"]
                if not isinstance(content, str) or not content.strip():
                    raise ValueError("legacy object requires nonempty canonical content")
            else:
                content = _render(object_type, attributes)
        else:
            assert existing is not None
            content = existing["content"]
        if action == "upsert":
            state = "active"
            if object_type == "fact" and attributes.get("current_status", "active") != "active":
                state = "retired"
            if object_type == "memory" and (
                attributes.get("lifecycle_state", "published") != "published"
                or attributes.get("governance_status", "active") not in {"active", "validated"}
            ):
                state = "suppressed"
        else:
            state = {"suppress": "suppressed", "retire": "retired", "restore": "active"}[action]
        if (
            existing is not None
            and action == "upsert"
            and not set(fields) & {"current_status", "lifecycle_state", "governance_status"}
        ):
            state = existing["state"]
        memory_id = existing["id"] if existing else uuid4()
        changed = existing is None or content != existing["content"]
        refresh_embedding = changed
        if existing is not None and existing["state"] != "active" and state == "active":
            # Inactive edits invalidate vectors without scheduling work. Repair
            # on activation, including subtype lifecycle/governance changes,
            # while retaining ready vectors for the current embedding identity.
            settings = get_settings()
            refresh_embedding = refresh_embedding or (
                existing["embedding"] is None
                or existing["embedding_state"] != "ready"
                or existing["embedding_model"] != settings.embedding_model
                or existing["embedding_preprocessing_revision"]
                != settings.embedding_preprocessing_revision
            )
        if action == "upsert":
            content_snapshot = hashlib.sha256(content.encode()).digest()
        else:
            assert existing is not None
            content_snapshot = (
                existing["extension_content_hash"]
                if existing["extension_override"]
                else existing["legacy_content_hash"]
                if existing["legacy_attributes_ready"]
                else existing["extension_content_hash"]
            )
        native_valid_from = (
            (attributes.get("valid_from") or datetime.now(UTC))
            if existing is None
            else fields.get("valid_from", existing["valid_from"])
        )
        native_valid_until = fields.get("valid_to", existing["valid_until"] if existing else None)
        # Native trust is categorical; preserve it independently of confidence.
        native_trust = (
            existing["trust"]
            if existing
            else ("extracted" if object_type in {"fact", "memory"} else "stated")
        )
        metadata_only = (
            existing is not None
            and not changed
            and state == existing["state"]
            and (
                scope == existing["scope"]
                and native_valid_from == existing["valid_from"]
                and native_valid_until == existing["valid_until"]
                and native_trust == existing["trust"]
            )
        )
        relationship_fields = {
            "subject_entity_id",
            "object_entity_id",
            "scope_entity_id",
            "project_entity_id",
            "episode_id",
            "created_run_id",
            "artifact_id",
            "primary_artifact_id",
        }
        preserve_existing_relationships = metadata_only and not set(fields) & relationship_fields
        # Subtype relationships must resolve inside the authenticated tenant.
        subtype_refs = []
        for field, kind in (
            ("subject_entity_id", "entity"),
            ("object_entity_id", "entity"),
            ("scope_entity_id", "entity"),
            ("project_entity_id", "entity"),
            ("episode_id", "episode"),
        ):
            if preserve_existing_relationships:
                break
            if attributes.get(field) is not None:
                found = resolve_memory(conn, principal, kind, UUID(str(attributes[field])))
                if found is None:
                    raise ValueError("legacy_relationship_not_owned")
                principal.require_scope(found["scope"])
                subtype_refs.append(
                    (field, found["tenant_id"], found["id"], None, None, None, None)
                )
                attributes[field] = external_memory_id(found)
                if field in patch_fields:
                    patch_fields[field] = external_memory_id(found)
        if not preserve_existing_relationships and attributes.get("created_run_id") is not None:
            run_id = UUID(str(attributes["created_run_id"]))
            run = conn.execute(
                "SELECT id,scope,external_id FROM braincore.legacy_client_controls "
                "WHERE tenant_id=%s AND object_type='extraction_run' "
                "AND (id=%s OR external_id=%s)",
                (principal.tenant_id, run_id, run_id),
            ).fetchone()
            auxiliary = None
            if run is None:
                auxiliary = conn.execute(
                    "SELECT native_id AS id,scope,tenant_id "
                    "FROM braincore.legacy_auxiliary_objects "
                    "WHERE source_table='extraction_run' AND (tenant_id=%s OR "
                    "(tenant_id IS NULL AND source_id=%s AND record_key->>'run_id'=%s)) "
                    "AND (native_id=%s OR record_key->>'run_id'=%s)",
                    (
                        principal.tenant_id,
                        existing["legacy_source_id"] if existing else None,
                        str(run_id)
                        if existing and str(prior_attributes.get("created_run_id")) == str(run_id)
                        else None,
                        run_id,
                        str(run_id),
                    ),
                ).fetchone()
            endpoint = run or auxiliary
            if endpoint is None:
                raise ValueError("legacy_extraction_run_not_owned")
            if endpoint["scope"] is None:
                principal.require("admin")
            else:
                principal.require_scope(endpoint["scope"])
            subtype_refs.append(
                (
                    "created_run_id",
                    auxiliary["tenant_id"] if auxiliary else principal.tenant_id,
                    None,
                    None,
                    run["id"] if run else None,
                    auxiliary["id"] if auxiliary else None,
                    None,
                )
            )
        artifact_links = []
        for field in ("artifact_id", "primary_artifact_id"):
            if preserve_existing_relationships:
                break
            if attributes.get(field) is None:
                continue
            artifact = resolve_artifact(conn, principal, UUID(str(attributes[field])))
            if artifact is None:
                raise ValueError("legacy_artifact_not_found")
            grant_id = None
            if artifact["tenant_id"] == principal.tenant_id:
                principal.require_scope(artifact["scope"])
            if artifact["tenant_id"] != principal.tenant_id:
                grant = conn.execute(
                    "SELECT id FROM braincore.artifact_access_grants WHERE artifact_id=%s "
                    "AND owner_tenant_id=%s AND grantee_tenant_id=%s AND revoked_at IS NULL "
                    "AND (expires_at IS NULL OR expires_at>statement_timestamp()) "
                    "AND %s::jsonb @> scope FOR SHARE",
                    (artifact["id"], artifact["tenant_id"], principal.tenant_id, Jsonb(scope)),
                ).fetchone()
                if grant is None:
                    raise ValueError("legacy_artifact_scope_forbidden")
                grant_id = grant["id"]
            subtype_refs.append(
                (field, artifact["tenant_id"], None, artifact["id"], None, None, grant_id)
            )
            artifact_links.append((artifact["id"], artifact["tenant_id"], grant_id))
        if metadata_only:
            conn.execute(
                """INSERT INTO braincore.legacy_client_attributes
                (memory_id,tenant_id,attributes,content_hash,search_vector,imported_payload_hash,current_override)
                SELECT m.id,m.tenant_id,
                 (CASE WHEN e.current_override THEN e.attributes WHEN m.legacy_attributes_ready
                   THEN m.legacy_attributes ELSE COALESCE(e.attributes,'{}'::jsonb) END)||%s::jsonb,
                 %s,COALESCE(m.legacy_search_vector,e.search_vector),e.imported_payload_hash,true
                FROM braincore.memories m LEFT JOIN braincore.legacy_client_attributes e
                 ON e.memory_id=m.id
                WHERE m.id=%s AND m.tenant_id=%s
                ON CONFLICT(memory_id) DO UPDATE SET attributes=EXCLUDED.attributes,
                 content_hash=EXCLUDED.content_hash,current_override=true""",
                (Jsonb(patch_fields), content_snapshot, memory_id, principal.tenant_id),
            )
        else:
            conn.execute(
                """INSERT INTO braincore.memories
                   (id,tenant_id,content,dedupe_key,project_id,user_id,agent_id,run_id,scope,state,
                    valid_from,valid_until,trust,object_type,legacy_scope_path,legacy_attributes,
                    legacy_content_hash,legacy_attributes_ready)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                     COALESCE((SELECT CASE WHEN e.current_override THEN e.attributes
                       WHEN p.legacy_attributes_ready THEN p.legacy_attributes
                       ELSE e.attributes END FROM braincore.memories p
                       LEFT JOIN braincore.legacy_client_attributes e ON e.memory_id=p.id
                       WHERE p.id=%s AND p.tenant_id=%s),'{}'::jsonb) || %s::jsonb,%s,true)
                   ON CONFLICT (id) DO UPDATE SET content=EXCLUDED.content,scope=EXCLUDED.scope,
                     project_id=EXCLUDED.project_id,user_id=EXCLUDED.user_id,
                     agent_id=EXCLUDED.agent_id,run_id=EXCLUDED.run_id,state=EXCLUDED.state,
                     valid_from=EXCLUDED.valid_from,valid_until=EXCLUDED.valid_until,trust=EXCLUDED.trust,
                     legacy_scope_path=EXCLUDED.legacy_scope_path,
                     legacy_attributes=EXCLUDED.legacy_attributes,legacy_attributes_ready=true,
                     legacy_content_hash=EXCLUDED.legacy_content_hash,
                     legacy_fts_eligible=CASE WHEN %s THEN true ELSE
                       braincore.memories.legacy_fts_eligible END,
                     legacy_search_vector=CASE WHEN %s THEN NULL ELSE COALESCE(
                       braincore.memories.legacy_search_vector,(SELECT search_vector
                         FROM braincore.legacy_client_attributes
                         WHERE memory_id=braincore.memories.id)) END,
                     embedding=CASE WHEN %s THEN NULL ELSE braincore.memories.embedding END,
                     embedding_state=CASE WHEN %s THEN 'pending' ELSE
                       braincore.memories.embedding_state END,
                     embedding_model=CASE WHEN %s THEN NULL ELSE
                       braincore.memories.embedding_model END,
                     embedding_preprocessing_revision=CASE WHEN %s THEN NULL ELSE
                       braincore.memories.embedding_preprocessing_revision END,updated_at=now()""",
                (
                    memory_id,
                    principal.tenant_id,
                    content,
                    hashlib.sha256(str(memory_id).encode()).digest(),
                    scope.get("project_id"),
                    scope.get("user_id"),
                    scope.get("agent_id"),
                    scope.get("run_id"),
                    Jsonb(scope),
                    state,
                    native_valid_from,
                    native_valid_until,
                    native_trust,
                    object_type,
                    scope_path,
                    memory_id,
                    principal.tenant_id,
                    Jsonb(patch_fields),
                    content_snapshot,
                    changed,
                    changed,
                    refresh_embedding,
                    refresh_embedding,
                    refresh_embedding,
                    refresh_embedding,
                ),
            )
            conn.execute(
                "UPDATE braincore.legacy_client_attributes SET current_override=false "
                "WHERE memory_id=%s AND current_override",
                (memory_id,),
            )
        if existing is None:
            display_id = object_id or uuid4()
            conn.execute(
                "INSERT INTO "
                "braincore.legacy_client_ids(memory_id,tenant_id,object_type,external_id,identity) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    memory_id,
                    principal.tenant_id,
                    object_type,
                    display_id,
                    Jsonb(identity) if identity else None,
                ),
            )
        else:
            display_id = UUID(external_memory_id(existing))
            updated_identity = logical_identity(object_type, attributes)
            conn.execute(
                "UPDATE braincore.legacy_client_ids SET identity=%s WHERE memory_id=%s",
                (
                    Jsonb(updated_identity) if updated_identity is not None else None,
                    memory_id,
                ),
            )
        if not preserve_existing_relationships:
            conn.execute(
                "DELETE FROM braincore.legacy_subtype_refs WHERE memory_id=%s", (memory_id,)
            )
        for (
            field,
            owner,
            memory_ref,
            artifact_ref,
            control_ref,
            auxiliary_ref,
            grant_ref,
        ) in subtype_refs:
            conn.execute(
                "INSERT INTO braincore.legacy_subtype_refs(memory_id,tenant_id,field,"
                "target_tenant_id,target_memory_id,target_artifact_id,target_control_id,"
                "target_auxiliary_id,access_grant_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    memory_id,
                    principal.tenant_id,
                    field,
                    owner,
                    memory_ref,
                    artifact_ref,
                    control_ref,
                    auxiliary_ref,
                    grant_ref,
                ),
            )
        for artifact_id, artifact_tenant_id, grant_id in artifact_links:
            conn.execute(
                "INSERT INTO braincore.memory_evidence "
                "(tenant_id,memory_id,artifact_id,quote,artifact_tenant_id,access_grant_id) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    principal.tenant_id,
                    memory_id,
                    artifact_id,
                    content,
                    artifact_tenant_id,
                    grant_id,
                ),
            )
        if changed:
            prior_revision = conn.execute(
                "SELECT id FROM braincore.memory_revisions WHERE memory_id=%s AND tenant_id=%s "
                "ORDER BY created_at DESC LIMIT 1",
                (memory_id, principal.tenant_id),
            ).fetchone()
            conn.execute(
                "INSERT INTO braincore.memory_revisions "
                "(tenant_id,memory_id,prior_revision_id,content,reason) VALUES (%s,%s,%s,%s,%s)",
                (
                    principal.tenant_id,
                    memory_id,
                    prior_revision["id"] if prior_revision else None,
                    content,
                    reason,
                ),
            )
        if existing and existing["state"] != state:
            lifecycle_action = (
                action
                if action != "upsert"
                else {"active": "restore", "suppressed": "suppress", "retired": "retire"}[state]
            )
            conn.execute(
                "INSERT INTO braincore.lifecycle_events "
                "(tenant_id,memory_id,action,prior_state,next_state,reason,actor_key_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    principal.tenant_id,
                    memory_id,
                    lifecycle_action,
                    existing["state"],
                    state,
                    reason,
                    principal.key_id,
                ),
            )
        operation_id = uuid4()
        conn.execute(
            "INSERT INTO braincore.operations(id,tenant_id,actor_key_id,kind,state,accepted_items) "
            "VALUES (%s,%s,%s,'legacy_client','queued',1)",
            (operation_id, principal.tenant_id, principal.key_id),
        )
        item = conn.execute(
            "INSERT INTO "
            "braincore.operation_items(operation_id,tenant_id,item_index,state,memory_id) "
            "VALUES (%s,%s,0,'queued',%s) RETURNING id",
            (operation_id, principal.tenant_id, memory_id),
        ).fetchone()
        assert item is not None
        if refresh_embedding and state == "active":
            conn.execute(
                "INSERT INTO braincore.jobs(tenant_id,operation_id,operation_item_id,kind,payload) "
                "VALUES (%s,%s,%s,'embed_memory',%s)",
                (
                    principal.tenant_id,
                    operation_id,
                    item["id"],
                    Jsonb({"memory_id": str(memory_id)}),
                ),
            )
        else:
            conn.execute(
                "UPDATE braincore.operation_items SET "
                "state='succeeded',completed_at=now() WHERE id=%s",
                (item["id"],),
            )
            conn.execute(
                "UPDATE braincore.operations SET "
                "state='succeeded',succeeded_items=1,completed_at=now() WHERE id=%s",
                (operation_id,),
            )
        conn.execute(
            "INSERT INTO "
            "braincore.audit_events(tenant_id,actor_key_id,action,target_type,target_id) "
            "VALUES (%s,%s,%s,'memory',%s)",
            (principal.tenant_id, principal.key_id, "legacy_client." + action, memory_id),
        )
        response = {
            "object_id": str(display_id),
            "native_id": str(memory_id),
            "operation_id": str(operation_id),
            "state": state,
        }
        conn.execute(
            "INSERT INTO "
            "braincore.mutation_receipts(tenant_id,actor_key_id,key,kind,request_hash,response) "
            "VALUES (%s,%s,%s,'legacy_client',%s,%s)",
            (principal.tenant_id, principal.key_id, idempotency_key, request_hash, Jsonb(response)),
        )
        return response

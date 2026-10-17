"""Bounded, authenticated native equivalents of legacy nightly maintenance.

No caller SQL or physical maintenance is accepted. PostgreSQL and typed legacy
mutations remain authoritative; the compatibility views are read surfaces.
"""

import hashlib
from typing import Any, cast
from uuid import UUID

import psycopg
from fastapi import HTTPException
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from .auth import Principal
from .config import get_settings
from .legacy_clients import configure_legacy_read, mutate_legacy_object
from .legacy_writes import _finish, _prior_receipt
from .migrate import migration_root


class StaleDemotionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: UUID
    destination_database: str = Field(min_length=1, max_length=63)
    after_id: UUID | None = None
    limit: int = Field(default=1000, ge=1, le=1000)


_REQUIRED_UNIQUES: dict[str, set[tuple[str, ...]]] = {
    "memories": {("id", "tenant_id"), ("tenant_id", "dedupe_key")},
    "artifacts": {("id", "tenant_id")},
    "legacy_client_ids": {
        ("tenant_id", "external_id"),
        ("tenant_id", "object_type", "identity"),
    },
    "legacy_client_artifact_ids": {
        ("tenant_id", "external_id"),
        ("tenant_id", "source_key"),
    },
    "mutation_receipts": {("tenant_id", "actor_key_id", "key")},
}
_REQUIRED_FKS = {
    (
        "legacy_client_ids",
        "FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id)",
    ),
    (
        "legacy_client_artifact_ids",
        "FOREIGN KEY (artifact_id, tenant_id) REFERENCES braincore.artifacts(id, tenant_id)",
    ),
    (
        "mutation_receipts",
        "FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)",
    ),
}
_CORE_VIEWS = ("entity", "fact", "memory", "segment", "episode", "artifact")

# Selecting narrow UUID keys precedes every subtype/payload read, even when the
# page contains no stale memory. A full page advances past unchanged memories.
_STALE_KEYS = """
SELECT id,object_type FROM braincore.memories WHERE tenant_id=%s AND object_type='memory'
AND (%s::jsonb IS NULL OR scope @> %s::jsonb)
AND (%s::uuid IS NULL OR id>%s::uuid) ORDER BY id LIMIT %s
"""
_LINKED_FACTS = """
SELECT g.native_fact_id AS id FROM braincore.legacy_memory_support g
WHERE g.tenant_id=%s AND g.native_memory_id=%s AND NOT g.deleted
AND g.attributes IS NOT NULL AND g.native_fact_id IS NOT NULL
UNION
SELECT fact.memory_id AS id FROM braincore.legacy_client_control_refs parent
JOIN braincore.legacy_client_controls c ON c.id=parent.control_id
JOIN braincore.legacy_client_control_refs fact ON fact.control_id=c.id
WHERE c.tenant_id=%s AND c.object_type='memory_support' AND NOT c.deleted
AND parent.field='memory_id' AND parent.memory_id=%s AND fact.field='fact_id'
"""
_CURRENT_ATTRIBUTES = """
CASE WHEN x.current_override THEN x.attributes
WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END
"""


def _authorize(principal: Principal, *, mutating: bool = False) -> None:
    principal.require("admin")
    if mutating and principal.session_id is not None and not principal.csrf_valid:
        raise HTTPException(status_code=403, detail="csrf_failed")


def maintenance_status(
    conn: psycopg.Connection[Any],
    principal: Principal,
    *,
    include_stats: bool = False,
    embedding_checks: bool = False,
) -> dict[str, Any]:
    """Inspect the connected destination and primary tenant; never reader union."""
    _authorize(principal)
    if embedding_checks and principal.scope is not None:
        raise HTTPException(status_code=403, detail="maintenance_unscoped_admin_required")
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout='5s'")
        conn.execute("SET LOCAL search_path TO pg_catalog")
        configure_legacy_read(conn, principal, include_inactive=True)
        identity = conn.execute(
            "SELECT current_database() AS database,t.name AS tenant "
            "FROM braincore.tenants t WHERE t.id=%s",
            (principal.tenant_id,),
        ).fetchone()
        if identity is None:
            raise HTTPException(status_code=403, detail="tenant_unavailable")
        expected = {
            path.name: hashlib.sha256(path.read_text(encoding="utf-8").encode()).hexdigest()
            for path in sorted(migration_root().glob("*.sql"))
        }
        actual = {
            row["version"]: row["checksum"]
            for row in conn.execute("SELECT version,checksum FROM braincore.schema_migrations")
        }
        issues = [
            "migration:" + name
            for name, checksum in expected.items()
            if actual.get(name) != checksum
        ]
        issues += ["unexpected_migration:" + name for name in actual if name not in expected]
        unique_rows = conn.execute(
            """SELECT c.relname,array_agg(a.attname ORDER BY k.ord) AS columns
            FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
            JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum
            WHERE n.nspname='braincore' AND c.relname=ANY(%s)
            AND i.indisunique AND i.indisvalid AND i.indisready AND i.indpred IS NULL
            AND i.indexprs IS NULL AND k.ord<=i.indnkeyatts
            GROUP BY c.relname,i.indexrelid""",
            (list(_REQUIRED_UNIQUES),),
        ).fetchall()
        present = {(r["relname"], tuple(cast(list[str], r["columns"]))) for r in unique_rows}
        for relation, specifications in _REQUIRED_UNIQUES.items():
            for columns in specifications:
                if (relation, columns) not in present:
                    issues.append("unique:" + relation + ":" + ",".join(columns))
        parent_index = conn.execute(
            """SELECT i.indisvalid,i.indisready,pg_get_expr(i.indpred,i.indrelid) AS predicate,
            ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
              JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
              WHERE k.ord<=i.indnkeyatts ORDER BY k.ord) AS columns
            FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid
            WHERE i.indrelid='braincore.legacy_client_control_refs'::regclass
              AND x.relnamespace='braincore'::regnamespace
              AND x.relname='legacy_client_control_refs_memory_parent_idx'
              AND i.indexprs IS NULL"""
        ).fetchone()
        if (
            not parent_index
            or not parent_index["indisvalid"]
            or not parent_index["indisready"]
            or parent_index["columns"] != ["memory_id", "control_id"]
            or parent_index["predicate"]
            != "((field = 'memory_id'::text) AND (memory_id IS NOT NULL))"
        ):
            issues.append("partial_index:legacy_client_control_refs_memory_parent_idx")
        fks = {
            (r["relname"], r["definition"])
            for r in conn.execute(
                """SELECT c.relname,pg_get_constraintdef(x.oid) AS definition
                FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='braincore' AND x.contype='f' AND x.convalidated
                AND c.relname=ANY(%s)""",
                (sorted({name for name, _ in _REQUIRED_FKS}),),
            )
        }
        for relation, definition in _REQUIRED_FKS:
            if (relation, definition) not in fks:
                issues.append("foreign_key:" + relation)
        views = {
            r["relname"]: r
            for r in conn.execute(
                """SELECT c.relname,c.relkind,c.reloptions FROM pg_class c
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='preserve' AND c.relname=ANY(%s)""",
                (list(_CORE_VIEWS),),
            )
        }
        for name in _CORE_VIEWS:
            view = views.get(name)
            if (
                not view
                or view["relkind"] != "v"
                or "security_barrier=true" not in (view["reloptions"] or [])
            ):
                issues.append("compatibility_view:" + name)
        for table in ("lexical_projection_state", "legacy_lexical_projection_state"):
            row = conn.execute(
                sql.SQL("SELECT ready FROM braincore.{} WHERE singleton").format(
                    sql.Identifier(table)
                )
            ).fetchone()
            if not row or not row["ready"]:
                issues.append("projection:" + table)
        counts: dict[str, int] | None = None
        if include_stats:
            scope = Jsonb(principal.scope) if principal.scope is not None else None
            counts = {
                row["object_type"]: row["n"]
                for row in conn.execute(
                    """SELECT object_type,count(*)::bigint AS n FROM braincore.memories
                    WHERE tenant_id=%s AND (%s::jsonb IS NULL OR scope @> %s::jsonb)
                    GROUP BY object_type""",
                    (principal.tenant_id, scope, scope),
                )
            }
        settings = get_settings()
        checks = None
        if embedding_checks:
            # This explicit operator validation can examine the whole eligible
            # tenant when proving absence. Default status performs no such walk.
            checks = conn.execute(
                """SELECT EXISTS(SELECT 1 FROM braincore.memories m
                    WHERE m.tenant_id=%s AND m.content ~ '[^[:space:]]'
                    AND (m.embedding IS NULL OR m.embedding_state<>'ready'
                      OR m.embedding_model IS DISTINCT FROM %s
                      OR m.embedding_preprocessing_revision IS DISTINCT FROM %s))
                    AS eligible_refresh_exists,
                    EXISTS(SELECT 1 FROM braincore.jobs
                    WHERE tenant_id=%s AND kind='embed_memory'
                      AND state IN ('queued','running'))
                    AS active_embedding_jobs_exists""",
                (
                    principal.tenant_id,
                    settings.embedding_model,
                    settings.embedding_preprocessing_revision,
                    principal.tenant_id,
                ),
            ).fetchone()
        return {
            "database": identity["database"],
            "tenant": identity["tenant"],
            "tenant_id": str(principal.tenant_id),
            "schema_ready": not issues,
            "issues": sorted(set(issues)),
            **({"object_counts": counts} if counts is not None else {}),
            **({"embedding_checks": dict(checks)} if checks is not None else {}),
            "embedding_identity": {
                "model": settings.embedding_model,
                "preprocessing_revision": settings.embedding_preprocessing_revision,
            },
        }


def _attributes(row: Any) -> dict[str, Any]:
    if row["extension_override"]:
        return dict(row["extension_attributes"])
    if row["legacy_attributes_ready"]:
        return dict(row["legacy_attributes"])
    return dict(row["extension_attributes"] or {})


def _has_milestone(conn: psycopg.Connection[Any], principal: Principal, memory_id: UUID) -> bool:
    values = (principal.tenant_id, memory_id, principal.tenant_id, memory_id)
    incomplete = conn.execute(
        "SELECT 1 FROM braincore.legacy_memory_support WHERE tenant_id=%s "
        "AND native_memory_id=%s AND NOT deleted "
        "AND (attributes IS NULL OR (native_fact_id IS NULL AND native_episode_id IS NULL)) "
        "LIMIT 1",
        (principal.tenant_id, memory_id),
    ).fetchone()
    if incomplete is not None:
        raise HTTPException(status_code=503, detail="maintenance_support_not_ready")
    episode_invalid = conn.execute(
        """SELECT 1 FROM braincore.legacy_memory_support g
        LEFT JOIN braincore.memories e ON e.id=g.native_episode_id
        WHERE g.tenant_id=%s AND g.native_memory_id=%s AND NOT g.deleted
        AND g.native_fact_id IS NULL AND g.native_episode_id IS NOT NULL
        AND (e.id IS NULL OR e.tenant_id<>%s OR e.object_type<>'episode') LIMIT 1""",
        (principal.tenant_id, memory_id, principal.tenant_id),
    ).fetchone()
    if episode_invalid is not None:
        raise HTTPException(status_code=503, detail="maintenance_support_endpoint_invalid")
    invisible = conn.execute(
        sql.SQL(
            "SELECT 1 FROM ({links}) linked LEFT JOIN braincore.memories m ON m.id=linked.id "
            "WHERE m.id IS NULL OR m.tenant_id<>%s LIMIT 1"
        ).format(links=sql.SQL(_LINKED_FACTS)),
        (*values, principal.tenant_id),
    ).fetchone()
    if invisible is not None:
        raise HTTPException(status_code=403, detail="maintenance_support_owner_mismatch")
    # Parent FOR UPDATE prevents concurrent support insertion (including FK
    # KEY SHARE). Existing linked facts are locked before priority rechecking.
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT m.id FROM braincore.memories m WHERE m.tenant_id=%s "
                "AND m.id IN ({links}) ORDER BY m.id FOR SHARE OF m"
            ).format(links=sql.SQL(_LINKED_FACTS)),
            (principal.tenant_id, *values),
        )
        while cursor.fetchmany(1000):
            pass
    unknown = conn.execute(
        sql.SQL(
            "SELECT 1 FROM ({links}) linked "
            "JOIN braincore.memories m ON m.id=linked.id AND m.tenant_id=%s "
            "LEFT JOIN braincore.legacy_client_attributes x "
            "ON x.memory_id=m.id AND x.tenant_id=m.tenant_id "
            "WHERE m.object_type<>'fact' OR (m.state='active' "
            "AND m.legacy_source_id IS NOT NULL AND NOT m.legacy_attributes_ready "
            "AND x.memory_id IS NULL) LIMIT 1"
        ).format(links=sql.SQL(_LINKED_FACTS)),
        (*values, principal.tenant_id),
    ).fetchone()
    if unknown is not None:
        raise HTTPException(status_code=503, detail="maintenance_fact_subtype_not_ready")
    scope = Jsonb(principal.scope) if principal.scope is not None else None
    row = conn.execute(
        sql.SQL(
            "SELECT 1 FROM ({links}) linked "
            "JOIN braincore.memories m ON m.id=linked.id AND m.tenant_id=%s "
            "LEFT JOIN braincore.legacy_client_attributes x "
            "ON x.memory_id=m.id AND x.tenant_id=m.tenant_id "
            "WHERE m.object_type='fact' AND m.state='active' "
            "AND (%s::jsonb IS NULL OR m.scope @> %s::jsonb) "
            "AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready "
            "OR x.memory_id IS NOT NULL) "
            "AND COALESCE((({attributes})->>'priority')::integer,5)=1 LIMIT 1"
        ).format(links=sql.SQL(_LINKED_FACTS), attributes=sql.SQL(_CURRENT_ATTRIBUTES)),
        (*values, principal.tenant_id, scope, scope),
    ).fetchone()
    return row is not None


def demote_stale_page(
    conn: psycopg.Connection[Any],
    principal: Principal,
    body: StaleDemotionRequest,
    idempotency_key: str,
) -> dict[str, Any]:
    """Commit one finite page and its replay receipt, retaining native writes."""
    _authorize(principal, mutating=True)
    if idempotency_key.startswith(("maintenance-stale-run:", "stale-item:")):
        raise HTTPException(status_code=422, detail="reserved_maintenance_key")
    if principal.scope is not None:
        raise HTTPException(status_code=403, detail="maintenance_unscoped_admin_required")
    packet = {"kind": "maintenance.stale_page", **body.model_dump(mode="json")}
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout='35s'")
        conn.execute("SET LOCAL lock_timeout='3s'")
        destination = conn.execute("SELECT current_database() AS name").fetchone()
        if destination is None or destination["name"] != body.destination_database:
            raise HTTPException(status_code=409, detail="maintenance_destination_mismatch")
        digest, prior = _prior_receipt(conn, principal, idempotency_key, packet)
        if prior is not None:
            return prior
        configure_legacy_read(conn, principal, include_inactive=False)
        run_key = "maintenance-stale-run:" + str(body.run_id)
        run_digest, run = _prior_receipt(
            conn,
            principal,
            run_key,
            {
                "kind": "maintenance.stale_run",
                "run_id": str(body.run_id),
                "database": body.destination_database,
            },
        )
        if run is None:
            if body.after_id is not None:
                raise HTTPException(status_code=409, detail="maintenance_run_not_started")
            cutoff_row = conn.execute("SELECT now()-interval '6 months' AS cutoff").fetchone()
            assert cutoff_row is not None
            cutoff = cutoff_row["cutoff"].isoformat()
            run = _finish(
                conn,
                principal,
                run_key,
                run_digest,
                "stale_run",
                body.run_id,
                {
                    "cutoff": cutoff,
                    "run_id": str(body.run_id),
                    "next_cursor": None,
                    "completed": False,
                    "pages": 0,
                },
            )
            conn.execute(
                "UPDATE braincore.mutation_receipts SET expires_at='infinity' "
                "WHERE tenant_id=%s AND actor_key_id=%s AND key=%s",
                (principal.tenant_id, principal.key_id, run_key),
            )
        expected_after = run.get("next_cursor")
        if (
            run.get("completed")
            or (str(body.after_id) if body.after_id is not None else None) != expected_after
        ):
            raise HTTPException(status_code=409, detail="maintenance_cursor_conflict")
        scope = Jsonb(principal.scope) if principal.scope is not None else None
        keys = conn.execute(
            _STALE_KEYS,
            (principal.tenant_id, scope, scope, body.after_id, body.after_id, body.limit),
        ).fetchall()
        demoted = 0
        for selected in keys:
            if selected["object_type"] != "memory":
                continue
            # Resolve only this page's exact native identity and hold the parent
            # lock until audit, subtype override and durable page receipt commit.
            conn.execute(
                "SELECT id FROM braincore.memories WHERE tenant_id=%s AND id=%s "
                "AND object_type='memory' ORDER BY id FOR UPDATE",
                (principal.tenant_id, selected["id"]),
            ).fetchone()
            # A fresh statement after the lock sees concurrent subtype changes,
            # rather than retaining pre-lock joined extension attributes.
            row = conn.execute(
                """SELECT m.*,e.attributes AS extension_attributes,
                e.content_hash AS extension_content_hash,e.current_override AS extension_override,
                a.external_id AS client_external_id FROM braincore.memories m
                LEFT JOIN braincore.legacy_client_attributes e ON e.memory_id=m.id
                LEFT JOIN braincore.legacy_client_ids a ON a.memory_id=m.id
                WHERE m.tenant_id=%s AND m.object_type='memory' AND m.id=%s""",
                (principal.tenant_id, selected["id"]),
            ).fetchone()
            if row is None or not principal.permits_scope(row["scope"]):
                continue
            if row["state"] != "active":
                continue
            if row["legacy_source_id"] is not None and not (
                row["legacy_attributes_ready"] or row["extension_attributes"] is not None
            ):
                raise HTTPException(status_code=503, detail="maintenance_subtype_not_ready")
            attrs = _attributes(row)
            lifecycle = (
                "published"
                if row["state"] == "active"
                else (
                    "retired"
                    if row["state"] == "retired"
                    else attrs.get("lifecycle_state", "published")
                )
            )
            if lifecycle != "published":
                continue
            old = conn.execute(
                "SELECT %s::timestamptz < %s::timestamptz AS stale",
                (attrs.get("last_supported_at"), run["cutoff"]),
            ).fetchone()
            if old is None or old["stale"] is not True:
                continue
            if _has_milestone(conn, principal, row["id"]):
                continue
            child_key = (
                "stale-item:"
                + hashlib.sha256((idempotency_key + ":" + str(row["id"])).encode()).hexdigest()
            )
            mutation = mutate_legacy_object(
                conn,
                principal,
                object_type="memory",
                object_id=selected["id"],
                fields={"lifecycle_state": "draft"},
                idempotency_key=child_key,
                reason="Nightly demoted stale memory without milestone support",
            )
            if mutation.get("native_id") != str(selected["id"]):
                raise HTTPException(status_code=409, detail="maintenance_alias_collision")
            demoted += 1
        exhausted = len(keys) < body.limit
        response = {
            "run_id": str(body.run_id),
            "after_id": str(body.after_id) if body.after_id is not None else None,
            "cutoff": run["cutoff"],
            "scanned": len(keys),
            "demoted": demoted,
            "skipped": len(keys) - demoted,
            "next_cursor": None if exhausted else str(keys[-1]["id"]),
            "exhausted": exhausted,
        }
        # The internal run manifest is progress state, never a public replay
        # response. Public page receipts remain immutable and exact on retry.
        conn.execute(
            "UPDATE braincore.mutation_receipts SET response=%s "
            "WHERE tenant_id=%s AND actor_key_id=%s AND key=%s",
            (
                Jsonb(
                    {
                        **run,
                        "next_cursor": response["next_cursor"],
                        "completed": exhausted,
                        "pages": run["pages"] + 1,
                    }
                ),
                principal.tenant_id,
                principal.key_id,
                run_key,
            ),
        )
        _finish(conn, principal, idempotency_key, digest, "stale_page", body.run_id, response)
        conn.execute(
            "UPDATE braincore.mutation_receipts SET expires_at='infinity' "
            "WHERE tenant_id=%s AND actor_key_id=%s AND key=%s",
            (principal.tenant_id, principal.key_id, idempotency_key),
        )
        return response


def connected_status(
    principal: Principal, include_stats: bool = False, embedding_checks: bool = False
) -> dict[str, Any]:
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), connect_timeout=5, row_factory=dict_row
    ) as conn:
        return maintenance_status(
            conn, principal, include_stats=include_stats, embedding_checks=embedding_checks
        )


def connected_demote(
    principal: Principal, body: StaleDemotionRequest, idempotency_key: str
) -> dict[str, Any]:
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), connect_timeout=5, row_factory=dict_row
    ) as conn:
        return demote_stale_page(conn, principal, body, idempotency_key)

"""Bounded native mapping of original provenance and revision records.

These mappings preserve each source edge, including repeated support edges and
nullable endpoints. They do not collapse evidence to one quote per artifact or
invent original audit events. Read visibility and client parity are separate.
"""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg
from fastapi import HTTPException
from psycopg import sql

from .auth import Principal
from .legacy_authority import guard_import_rows
from .legacy_evidence import ensure_evidence_grant, scope_hash, validate_sharing_policy
from .legacy_objects import OBJECT_TYPES
from .legacy_relationships import AUXILIARY_FKS
from .legacy_relationships import auxiliary_relationships as _auxiliary_relationships

# Endpoint source tables are fixed, preventing same-UUID cross-type collisions.
GRAPH_TABLES = {
    "fact_evidence": (("fact_id", "fact"), ("segment_id", "segment")),
    "memory_support": (("memory_id", "memory"), ("fact_id", "fact"), ("episode_id", "episode")),
    "memory_revision": (("memory_id", "memory"),),
}
TARGET_TABLES = {
    "fact_evidence": "legacy_fact_evidence",
    "memory_support": "legacy_memory_support",
    "memory_revision": "legacy_memory_revisions",
}


def _batch_query(
    table: str, has_cursor: bool = False, *, reconcile: bool = False, guarded: bool = False
) -> sql.Composed:
    endpoints = GRAPH_TABLES[table]
    joins = []
    projected = []
    invalid: list[sql.Composable] = []
    owners = []
    native_columns = []
    for i, (column, object_type) in enumerate(endpoints):
        alias = sql.Identifier("p" + str(i))
        joins.append(
            sql.SQL("""LEFT JOIN braincore.legacy_object_map {alias}
          ON {alias}.source_id=r.source_id AND {alias}.source_table={object_type}
         AND {alias}.record_key=jsonb_build_object({column},r.payload->>{column})""").format(
                alias=alias, object_type=sql.Literal(object_type), column=sql.Literal(column)
            )
        )
        native = sql.Identifier("native_" + column)
        native_columns.append(native)
        projected.append(sql.SQL("{}.native_memory_id AS {}").format(alias, native))
        owners.append(sql.SQL("{}.tenant_id").format(alias))
        invalid.append(
            sql.SQL("(r.payload->>{} IS NOT NULL AND {}.native_memory_id IS NULL)").format(
                sql.Literal(column), alias
            )
        )
    owner = (
        sql.SQL("COALESCE({})").format(sql.SQL(",").join(owners)) if len(owners) > 1 else owners[0]
    )
    for other in owners:
        invalid.append(
            sql.SQL("({} IS NOT NULL AND {} IS DISTINCT FROM {})").format(other, other, owner)
        )
    if table == "memory_revision":
        invalid.append(sql.SQL("r.payload->>'tenant' IS DISTINCT FROM tm.original_tenant"))
    completion = (
        sql.SQL(""", preserved AS MATERIALIZED (
  SELECT r.record_key FROM rendered r JOIN {target} e
    ON e.source_id=r.source_id AND e.record_key=r.record_key
  WHERE NOT braincore.source_import_allowed({relation},e.native_id,r.payload_hash)
), inserted AS (
  INSERT INTO {target} AS existing
    (source_id,record_key,native_id,tenant_id,{columns},imported_payload_hash,deleted,attributes)
  SELECT source_id,record_key,braincore.legacy_object_id(source_id,{source_table},record_key),
         owner,{columns},payload_hash,deleted,payload
  FROM rendered r WHERE NOT invalid AND NOT EXISTS
    (SELECT 1 FROM preserved p WHERE p.record_key=r.record_key)
  ON CONFLICT (source_id,record_key) DO UPDATE SET
    {updates},imported_payload_hash=EXCLUDED.imported_payload_hash,deleted=EXCLUDED.deleted,
    attributes=EXCLUDED.attributes
  WHERE existing.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id
  RETURNING record_key
)
SELECT count(*)+(SELECT count(*) FROM preserved) AS imported,
  (SELECT count(*) FROM selected_heads) AS expected,
  (SELECT record_key::text FROM originals ORDER BY record_key DESC LIMIT 1) AS next_cursor
FROM inserted
""")
        if not reconcile
        else sql.SQL("""
SELECT count(*) AS expected,
 count(*) FILTER (WHERE r.invalid OR e.native_id IS DISTINCT FROM
   braincore.legacy_object_id(r.source_id,{source_table},r.record_key)
   OR e.tenant_id IS DISTINCT FROM r.owner
   OR e.imported_payload_hash IS DISTINCT FROM r.payload_hash
   OR ((e.deleted IS DISTINCT FROM r.deleted OR {endpoint_mismatch})
      AND NOT EXISTS (SELECT 1 FROM braincore.legacy_native_authority a
        WHERE a.relation_name={relation} AND a.native_id=e.native_id
          AND a.imported_payload_hash=r.payload_hash))) AS mismatched,
 (SELECT record_key::text FROM originals ORDER BY record_key DESC LIMIT 1) AS next_cursor
FROM rendered r LEFT JOIN {target} e
 ON e.source_id=r.source_id AND e.record_key=r.record_key
""")
    )
    completion_fields = {
        "source_table": sql.Literal(table),
        "target": sql.Identifier("braincore", TARGET_TABLES[table]),
        "relation": sql.Literal(TARGET_TABLES[table]),
        "columns": sql.SQL(",").join(native_columns),
        "updates": sql.SQL(",").join(
            sql.SQL("{}=EXCLUDED.{}").format(c, c) for c in native_columns
        ),
        "endpoint_mismatch": sql.SQL(" OR ").join(
            sql.SQL("e.{} IS DISTINCT FROM r.{}").format(c, c) for c in native_columns
        ),
    }
    query = sql.SQL("""
WITH selected_heads AS MATERIALIZED (
  SELECT h.source_id,h.source_table,h.record_key,h.payload_hash,h.deleted
  FROM braincore.legacy_record_heads h
  WHERE h.source_id=%s AND h.source_table={source_table} {cursor_predicate}
    {dirty_predicate} {guard_predicate}
  ORDER BY h.record_key LIMIT %s
), originals AS MATERIALIZED (
  SELECT h.source_id,h.record_key,h.payload_hash,h.deleted,v.payload
  FROM selected_heads h
  JOIN braincore.legacy_record_versions v
                    USING(source_id,source_table,record_key,payload_hash)
), rendered AS MATERIALIZED (
  SELECT r.*, {owner} AS owner, {projected},
    ({invalid}) AS invalid
  FROM originals r {joins}
  LEFT JOIN braincore.legacy_tenant_map tm
    ON tm.source_id=r.source_id AND tm.tenant_id={owner}
) {completion}
""").format(
        completion=completion.format(**completion_fields),
        source_table=sql.Literal(table),
        cursor_predicate=sql.SQL("AND h.record_key>%s::jsonb" if has_cursor else ""),
        dirty_predicate=sql.SQL("""AND NOT EXISTS (
          SELECT 1 FROM {target} done WHERE done.source_id=h.source_id
            AND done.record_key=h.record_key AND done.imported_payload_hash=h.payload_hash
            AND done.deleted=h.deleted)""").format(
            target=sql.Identifier("braincore", TARGET_TABLES[table])
        )
        if not reconcile
        else sql.SQL(""),
        guard_predicate=sql.SQL("AND h.record_key=ANY(%s::jsonb[])" if guarded else ""),
        owner=owner,
        projected=sql.SQL(",").join(projected),
        invalid=sql.SQL(" OR ").join(invalid),
        joins=sql.SQL(" ").join(joins),
    )
    return query


def import_provenance(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 1000
) -> dict[str, Any]:
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid provenance batch size")
    source = target.execute(
        "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
    ).fetchone()
    if source is None or not set(GRAPH_TABLES).issubset(source["key_mapping"]):
        raise ValueError("source provenance tables are not registered")
    counts = {}
    processed = 0
    for table in GRAPH_TABLES:
        after = None
        count = 0
        while True:
            with target.transaction():
                values: list[object] = [source_id]
                if after is not None:
                    values.append(after)
                values.append(batch_size)
                predicate = "AND h.record_key>%s::jsonb" if after is not None else ""
                heads = target.execute(
                    sql.SQL("""SELECT braincore.legacy_object_id(h.source_id,h.source_table,
                        h.record_key) AS id,h.record_key::text AS key,h.payload_hash
                    FROM braincore.legacy_record_heads h WHERE h.source_id=%s
                      AND h.source_table={table} {predicate} AND NOT EXISTS (
                        SELECT 1 FROM braincore.{target} done WHERE done.source_id=h.source_id
                          AND done.record_key=h.record_key
                          AND done.imported_payload_hash=h.payload_hash AND done.deleted=h.deleted)
                    ORDER BY h.record_key LIMIT %s FOR SHARE OF h""").format(
                        table=sql.Literal(table),
                        predicate=sql.SQL(predicate),
                        target=sql.Identifier(TARGET_TABLES[table]),
                    ),
                    values,
                ).fetchall()
                if table != "memory_revision":
                    guard_import_rows(
                        target,
                        TARGET_TABLES[table],
                        [(r["id"], bytes(r["payload_hash"])) for r in heads],
                    )
                guarded_values = [*values[:-1], [h["key"] for h in heads], values[-1]]
                row = target.execute(
                    _batch_query(table, after is not None, guarded=True), guarded_values
                ).fetchone()
                assert row is not None
                if row["imported"] != row["expected"]:
                    raise ValueError("unmapped endpoint or conflicting provenance ownership")
            count += row["imported"]
            after = row["next_cursor"]
            if after is None:
                break
        processed += count
        total = target.execute(
            sql.SQL("SELECT count(*) AS n FROM {} WHERE source_id=%s").format(
                sql.Identifier("braincore", TARGET_TABLES[table])
            ),
            (source_id,),
        ).fetchone()
        assert total is not None
        counts[table] = total["n"]
    return {
        "native_provenance_mapping_gate": "PASS",
        "objects": counts,
        "row_count": sum(counts.values()),
        "processed_rows": processed,
        "read_visibility_gate": "NOT RUN",
        "client_parity_gate": "NOT RUN",
        "auxiliary_semantic_gate": "NOT RUN",
    }


def reconcile_provenance(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 1000
) -> dict[str, Any]:
    """Compare every source key, hash, owner and typed endpoint without rewriting rows."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid provenance batch size")
    tables = {}
    with target.transaction():
        target.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for table in GRAPH_TABLES:
            after = None
            expected = mismatched = 0
            while True:
                values: list[object] = [source_id]
                if after is not None:
                    values.append(after)
                values.append(batch_size)
                row = target.execute(
                    _batch_query(table, after is not None, reconcile=True), values
                ).fetchone()
                assert row is not None
                expected += row["expected"]
                mismatched += row["mismatched"]
                after = row["next_cursor"]
                if after is None:
                    break
            extra_row = target.execute(
                sql.SQL("""SELECT count(*) AS n FROM {target} e
                WHERE e.source_id=%s AND NOT EXISTS (
                  SELECT 1 FROM braincore.legacy_record_heads h WHERE h.source_id=e.source_id
                    AND h.source_table={table} AND h.record_key=e.record_key)""").format(
                    target=sql.Identifier("braincore", TARGET_TABLES[table]),
                    table=sql.Literal(table),
                ),
                (source_id,),
            ).fetchone()
            assert extra_row is not None
            extra = extra_row["n"]
            tables[table] = {"expected": expected, "mismatched": mismatched, "extra": extra}
    return {
        "semantic_per_key_reconciliation_gate": "PASS"
        if all(row["mismatched"] == 0 and row["extra"] == 0 for row in tables.values())
        else "FAIL",
        "tables": tables,
        "row_count": sum(row["expected"] for row in tables.values()),
    }


def import_fact_evidence_grants(
    target: psycopg.Connection[Any],
    source_id: UUID,
    policy: dict[str, Any],
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Authorize original cross-owner artifact references at each verified fact root."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid provenance grant batch size")
    approved = validate_sharing_policy(source_id, policy)
    after = None
    linked = 0
    grants: set[UUID] = set()
    while True:
        with target.transaction():
            predicate = sql.SQL("AND e.record_key>%s::jsonb" if after is not None else "")
            values: list[object] = [source_id]
            if after is not None:
                values.append(after)
            values.append(batch_size)
            rows = target.execute(
                sql.SQL("""
              SELECT e.record_key::text AS key,f.tenant_id,f.scope,t.original_tenant,
                     a.id AS native_artifact_id,a.tenant_id AS artifact_tenant_id,
                     a_map.record_key->>'artifact_id' AS artifact_external_id
              FROM braincore.legacy_fact_evidence e
              JOIN braincore.memories f ON f.id=e.native_fact_id AND f.tenant_id=e.tenant_id
              JOIN braincore.memory_evidence se ON se.memory_id=e.native_segment_id
                AND se.tenant_id=e.tenant_id
              JOIN braincore.artifacts a ON a.id=se.artifact_id
                AND a.tenant_id=se.artifact_tenant_id
              JOIN braincore.legacy_artifact_map a_map ON a_map.native_artifact_id=a.id
                AND a_map.source_id=e.source_id
              JOIN braincore.legacy_tenant_map t ON t.source_id=e.source_id
                AND t.tenant_id=f.tenant_id
              WHERE e.source_id=%s AND NOT e.deleted AND a.tenant_id<>f.tenant_id {predicate}
              ORDER BY e.record_key LIMIT %s
            """).format(predicate=predicate),
                values,
            ).fetchall()
            if not rows:
                break
            for row in rows:
                approval = approved.get((row["artifact_external_id"], row["original_tenant"]))
                if approval is None or scope_hash(row["scope"]) not in approval["scope_hashes"]:
                    raise ValueError("cross-owner fact evidence lacks reviewed root/scope approval")
                grants.add(ensure_evidence_grant(target, source_id, row, approval))
                linked += 1
            after = rows[-1]["key"]
    return {
        "fact_root_grant_gate": "PASS",
        "cross_owner_fact_links": linked,
        "distinct_exact_scope_grants": len(grants),
    }


def _auxiliary_endpoint(
    target: psycopg.Connection[Any],
    source_id: UUID,
    table: str,
    external: str,
    key_mapping: dict[str, list[str]],
) -> dict[str, Any]:
    if table not in key_mapping or len(key_mapping[table]) != 1:
        raise ValueError("unsupported auxiliary endpoint identity")
    key = json.dumps({key_mapping[table][0]: external})
    if table in OBJECT_TYPES:
        row = target.execute(
            """SELECT o.native_memory_id AS id,o.tenant_id,m.scope,
          'memory' AS kind FROM braincore.legacy_object_map o
          JOIN braincore.memories m ON m.id=o.native_memory_id AND m.tenant_id=o.tenant_id
          WHERE o.source_id=%s AND o.source_table=%s AND o.record_key=%s::jsonb""",
            (source_id, table, key),
        ).fetchone()
    elif table == "artifact":
        row = target.execute(
            """SELECT o.native_artifact_id AS id,o.tenant_id,a.scope,
          'artifact' AS kind FROM braincore.legacy_artifact_map o
          JOIN braincore.artifacts a ON a.id=o.native_artifact_id AND a.tenant_id=o.tenant_id
          WHERE o.source_id=%s AND o.record_key=%s::jsonb""",
            (source_id, key),
        ).fetchone()
    elif table in AUXILIARY_FKS:
        row = target.execute(
            """SELECT native_id AS id,tenant_id,scope,'auxiliary' AS kind
          FROM braincore.legacy_auxiliary_objects
          WHERE source_id=%s AND source_table=%s AND record_key=%s::jsonb""",
            (source_id, table, key),
        ).fetchone()
    else:
        raise ValueError("unsupported auxiliary endpoint type")
    if row is None:
        raise ValueError("auxiliary relationship has an unmapped endpoint")
    return dict(row)


def import_auxiliary_objects(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 500
) -> dict[str, Any]:
    """Map current helper/system collections with explicit, constrained typed references."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid auxiliary batch size")
    source = target.execute(
        "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
    ).fetchone()
    if source is None:
        raise ValueError("source auxiliary mapping is missing")
    mapping = source["key_mapping"]
    with target.transaction():
        labels: set[str] = set()
        for table in AUXILIARY_FKS:
            if table not in mapping:
                continue
            owners = target.execute(
                """SELECT DISTINCT v.payload->>'tenant' AS owner
              FROM braincore.legacy_record_heads h JOIN braincore.legacy_record_versions v
                USING(source_id,source_table,record_key,payload_hash)
              WHERE h.source_id=%s AND h.source_table=%s AND v.payload ? 'tenant'""",
                (source_id, table),
            ).fetchall()
            for original in owners:
                label = original["owner"]
                if not isinstance(label, str) or not label.strip():
                    raise ValueError("auxiliary source owner is missing")
                labels.add(label)
        target.execute(
            "INSERT INTO braincore.tenants(name) SELECT unnest(%s::text[]) "
            "ON CONFLICT (name) DO NOTHING",
            (sorted(labels),),
        )
        target.execute(
            """INSERT INTO braincore.legacy_tenant_map(source_id,original_tenant,tenant_id)
          SELECT %s,name,id FROM braincore.tenants WHERE name=ANY(%s) ON CONFLICT DO NOTHING""",
            (source_id, sorted(labels)),
        )
    counts = {}
    for table in AUXILIARY_FKS:
        if table not in mapping:
            continue
        after = None
        count = 0
        while True:
            with target.transaction():
                predicate = "AND h.record_key>%s::jsonb" if after is not None else ""
                values: list[object] = [source_id, table]
                if after is not None:
                    values.append(after)
                values.append(batch_size)
                rows = target.execute(
                    sql.SQL("""SELECT h.record_key::text AS key,h.payload_hash,
                  h.deleted,v.payload,tm.tenant_id
                  FROM braincore.legacy_record_heads h
                  JOIN braincore.legacy_record_versions v
                    USING(source_id,source_table,record_key,payload_hash)
                  LEFT JOIN braincore.legacy_tenant_map tm ON tm.source_id=h.source_id
                    AND tm.original_tenant=v.payload->>'tenant'
                  WHERE h.source_id=%s AND h.source_table=%s {predicate}
                  ORDER BY h.record_key LIMIT %s""").format(predicate=sql.SQL(predicate)),
                    values,
                ).fetchall()
                if not rows:
                    break
                identities = target.execute(
                    "SELECT braincore.legacy_object_id(%s,%s,key::jsonb) AS id "
                    "FROM unnest(%s::text[]) AS keys(key)",
                    (source_id, table, [r["key"] for r in rows]),
                ).fetchall()
                preserved = guard_import_rows(
                    target,
                    "legacy_auxiliary_objects",
                    [
                        (i["id"], bytes(r["payload_hash"]))
                        for i, r in zip(identities, rows, strict=True)
                    ],
                )
                for identity, row in zip(identities, rows, strict=True):
                    if identity["id"] in preserved:
                        count += 1
                        continue
                    payload = row["payload"]
                    tenant = row["tenant_id"]
                    if payload.get("tenant") is not None and tenant is None:
                        raise ValueError("auxiliary source owner is not mapped")
                    scope = None
                    if tenant is None:
                        # Unowned collections inherit only their reviewed direct authority root.
                        authority = {
                            "eval_case": ("artifact", "artifact_id"),
                            "extraction_run": ("artifact", "artifact_id"),
                            "project_service_map": ("entity", "project_entity_id"),
                            "publish_note": ("memory", "memory_id"),
                            "memory_edge_evidence": ("memory_edge", "edge_id"),
                        }.get(table)
                        if table == "review_queue" and payload.get("target_id") is not None:
                            authority = (payload.get("target_type"), "target_id")
                        if authority is not None and payload.get(authority[1]) is not None:
                            root = _auxiliary_endpoint(
                                target, source_id, authority[0], str(payload[authority[1]]), mapping
                            )
                            tenant, scope = root["tenant_id"], root["scope"]
                    if tenant is not None and scope is None:
                        scope = (
                            {"project_id": payload["scope_path"]}
                            if payload.get("scope_path")
                            else {"run_id": "legacy-source:" + str(source_id)}
                        )
                    target.execute(
                        """INSERT INTO braincore.legacy_auxiliary_objects AS a
                      (source_id,source_table,record_key,native_id,tenant_id,scope,payload,
                       imported_payload_hash,deleted)
                      SELECT h.source_id,h.source_table,h.record_key,
                        braincore.legacy_object_id(h.source_id,h.source_table,h.record_key),
                        %s,%s::jsonb,v.payload,h.payload_hash,h.deleted
                      FROM braincore.legacy_record_heads h JOIN braincore.legacy_record_versions v
                        USING(source_id,source_table,record_key,payload_hash)
                      WHERE h.source_id=%s AND h.source_table=%s AND h.record_key=%s::jsonb
                      ON CONFLICT (source_id,source_table,record_key) DO UPDATE SET
                        payload=EXCLUDED.payload,scope=EXCLUDED.scope,
                        imported_payload_hash=EXCLUDED.imported_payload_hash,deleted=EXCLUDED.deleted,
                        updated_at=now()
                      WHERE a.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id
                        AND (a.imported_payload_hash,a.deleted) IS DISTINCT FROM
                            (EXCLUDED.imported_payload_hash,EXCLUDED.deleted)
                      RETURNING native_id""",
                        (
                            tenant,
                            json.dumps(scope) if scope is not None else None,
                            source_id,
                            table,
                            row["key"],
                        ),
                    ).fetchone()
                    existing = target.execute(
                        """SELECT tenant_id FROM braincore.legacy_auxiliary_objects
                        WHERE source_id=%s AND source_table=%s AND record_key=%s::jsonb""",
                        (source_id, table, row["key"]),
                    ).fetchone()
                    if existing is None or existing["tenant_id"] != tenant:
                        raise ValueError("auxiliary owner conflicts with canonical record")
                    count += 1
                after = rows[-1]["key"]
        counts[table] = count
    references = 0
    # Resolve self/forward references only after every auxiliary identity exists.
    for table in counts:
        after = None
        while True:
            with target.transaction():
                rows = target.execute(
                    """SELECT record_key::text AS key,payload,tenant_id,native_id,
                         imported_payload_hash
                  FROM braincore.legacy_auxiliary_objects WHERE source_id=%s AND source_table=%s
                    AND (%s::jsonb IS NULL OR record_key>%s::jsonb)
                  ORDER BY record_key LIMIT %s""",
                    (source_id, table, after, after, batch_size),
                ).fetchall()
                if not rows:
                    break
                preserved = guard_import_rows(
                    target,
                    "legacy_auxiliary_objects",
                    [(r["native_id"], bytes(r["imported_payload_hash"])) for r in rows],
                )
                for row in rows:
                    if row["native_id"] in preserved:
                        continue
                    expected = []
                    for column, other, external in _auxiliary_relationships(table, row["payload"]):
                        endpoint = _auxiliary_endpoint(target, source_id, other, external, mapping)
                        if (
                            row["tenant_id"] is not None
                            and endpoint["kind"] == "memory"
                            and (endpoint["tenant_id"] != row["tenant_id"])
                        ):
                            raise ValueError(
                                "auxiliary memory endpoint conflicts with source owner"
                            )
                        ids = {"memory": None, "artifact": None, "auxiliary": None}
                        ids[endpoint["kind"]] = endpoint["id"]
                        target.execute(
                            """INSERT INTO braincore.legacy_auxiliary_refs
                          (source_id,source_table,record_key,source_column,target_source_table,
                           target_tenant_id,native_memory_id,native_artifact_id,native_auxiliary_id)
                          VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
                          ON CONFLICT (source_id,source_table,record_key,source_column)
                          DO UPDATE SET
                            target_source_table=EXCLUDED.target_source_table,
                            target_tenant_id=EXCLUDED.target_tenant_id,
                            native_memory_id=EXCLUDED.native_memory_id,
                            native_artifact_id=EXCLUDED.native_artifact_id,
                            native_auxiliary_id=EXCLUDED.native_auxiliary_id
                          WHERE (braincore.legacy_auxiliary_refs.target_source_table,
                            braincore.legacy_auxiliary_refs.target_tenant_id,
                            braincore.legacy_auxiliary_refs.native_memory_id,
                            braincore.legacy_auxiliary_refs.native_artifact_id,
                            braincore.legacy_auxiliary_refs.native_auxiliary_id)
                            IS DISTINCT FROM (EXCLUDED.target_source_table,
                              EXCLUDED.target_tenant_id,
                              EXCLUDED.native_memory_id,EXCLUDED.native_artifact_id,EXCLUDED.native_auxiliary_id)""",
                            (
                                source_id,
                                table,
                                row["key"],
                                column,
                                other,
                                endpoint["tenant_id"],
                                ids["memory"],
                                ids["artifact"],
                                ids["auxiliary"],
                            ),
                        )
                        expected.append(column)
                        references += 1
                    target.execute(
                        """DELETE FROM braincore.legacy_auxiliary_refs WHERE source_id=%s
                      AND source_table=%s AND record_key=%s::jsonb
                      AND NOT (source_column=ANY(%s))""",
                        (source_id, table, row["key"], expected),
                    )
                after = rows[-1]["key"]
    return {
        "auxiliary_native_mapping_gate": "PASS",
        "objects": counts,
        "row_count": sum(counts.values()),
        "constrained_references": references,
        "current_client_semantics_gate": "NOT RUN",
    }


def reconcile_auxiliary_objects(target: psycopg.Connection[Any], source_id: UUID) -> dict[str, Any]:
    """Reconcile each baseline payload/identity and every current typed auxiliary reference."""
    tables = {}
    references = mismatched_refs = 0
    with target.transaction():
        target.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source = target.execute(
            "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
        ).fetchone()
        if source is None:
            raise ValueError("source auxiliary mapping is missing")
        mapping = source["key_mapping"]
        for table in AUXILIARY_FKS:
            if table not in mapping:
                continue
            row = target.execute(
                """SELECT count(*) AS expected,
              count(*) FILTER (WHERE a.native_id IS DISTINCT FROM
                braincore.legacy_object_id(h.source_id,h.source_table,h.record_key)
                OR ((a.payload IS DISTINCT FROM v.payload OR a.deleted IS DISTINCT FROM h.deleted)
                    AND NOT EXISTS (SELECT 1 FROM braincore.legacy_native_authority authority
                      WHERE authority.relation_name='legacy_auxiliary_objects'
                        AND authority.native_id=a.native_id
                        AND authority.imported_payload_hash=h.payload_hash))
                OR a.imported_payload_hash IS DISTINCT FROM h.payload_hash
                OR (v.payload->>'tenant' IS NOT NULL AND a.tenant_id IS DISTINCT FROM t.tenant_id)
              ) AS mismatched
              FROM braincore.legacy_record_heads h
              JOIN braincore.legacy_record_versions v
                USING(source_id,source_table,record_key,payload_hash)
              LEFT JOIN braincore.legacy_auxiliary_objects a
                USING(source_id,source_table,record_key)
              LEFT JOIN braincore.legacy_tenant_map t ON t.source_id=h.source_id
                AND t.original_tenant=v.payload->>'tenant'
              WHERE h.source_id=%s AND h.source_table=%s""",
                (source_id, table),
            ).fetchone()
            assert row is not None
            extra = target.execute(
                """SELECT count(*) AS n FROM braincore.legacy_auxiliary_objects a
               WHERE source_id=%s AND source_table=%s AND NOT EXISTS (
                 SELECT 1 FROM braincore.legacy_record_heads h WHERE h.source_id=a.source_id
                   AND h.source_table=a.source_table AND h.record_key=a.record_key)""",
                (source_id, table),
            ).fetchone()
            assert extra is not None
            tables[table] = {**dict(row), "extra": extra["n"]}
            after = None
            while True:
                rows = target.execute(
                    """SELECT record_key::text AS key,payload FROM
                  braincore.legacy_auxiliary_objects WHERE source_id=%s AND source_table=%s
                  AND (%s::jsonb IS NULL OR record_key>%s::jsonb) ORDER BY record_key LIMIT 1000""",
                    (source_id, table, after, after),
                ).fetchall()
                if not rows:
                    break
                for original in rows:
                    expected = {}
                    for column, other, external in _auxiliary_relationships(
                        table, original["payload"]
                    ):
                        endpoint = _auxiliary_endpoint(target, source_id, other, external, mapping)
                        ids = {"memory": None, "artifact": None, "auxiliary": None}
                        ids[endpoint["kind"]] = endpoint["id"]
                        expected[column] = (
                            other,
                            endpoint["tenant_id"],
                            ids["memory"],
                            ids["artifact"],
                            ids["auxiliary"],
                        )
                    stored = target.execute(
                        """SELECT source_column,target_source_table,
                      target_tenant_id,native_memory_id,native_artifact_id,native_auxiliary_id
                      FROM braincore.legacy_auxiliary_refs WHERE source_id=%s AND source_table=%s
                        AND record_key=%s::jsonb""",
                        (source_id, table, original["key"]),
                    ).fetchall()
                    observed = {
                        r["source_column"]: (
                            r["target_source_table"],
                            r["target_tenant_id"],
                            r["native_memory_id"],
                            r["native_artifact_id"],
                            r["native_auxiliary_id"],
                        )
                        for r in stored
                    }
                    references += len(expected)
                    mismatched_refs += sum(
                        expected.get(c) != observed.get(c)
                        for c in expected.keys() | observed.keys()
                    )
                after = rows[-1]["key"]
    return {
        "auxiliary_semantic_per_key_reconciliation_gate": "PASS"
        if all(r["mismatched"] == 0 and r["extra"] == 0 for r in tables.values())
        and mismatched_refs == 0
        else "FAIL",
        "tables": tables,
        "row_count": sum(r["expected"] for r in tables.values()),
        "constrained_references": references,
        "mismatched_references": mismatched_refs,
    }


def hydrate_provenance_attributes(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 5000
) -> dict[str, Any]:
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid graph attribute batch size")
    counts = {}
    for table, native in TARGET_TABLES.items():
        count = 0
        while True:
            with target.transaction():
                row = target.execute(
                    sql.SQL("""WITH selected AS MATERIALIZED (
                    SELECT record_key,imported_payload_hash FROM {target}
                    WHERE source_id=%s AND attributes IS NULL ORDER BY record_key LIMIT %s
                  ), updated AS (
                    UPDATE {target} e SET attributes=v.payload
                    FROM selected s JOIN braincore.legacy_record_versions v
                      ON v.source_id=%s AND v.source_table={table}
                      AND v.record_key=s.record_key AND v.payload_hash=s.imported_payload_hash
                    WHERE e.source_id=v.source_id AND e.record_key=v.record_key
                    RETURNING e.record_key)
                  SELECT count(*) AS n FROM updated""").format(
                        target=sql.Identifier("braincore", native), table=sql.Literal(table)
                    ),
                    (source_id, batch_size, source_id),
                ).fetchone()
                assert row is not None
            count += row["n"]
            if row["n"] == 0:
                break
        counts[table] = count
    return {
        "current_graph_attributes_gate": "PASS",
        "hydrated_rows": sum(counts.values()),
        "tables": counts,
    }


async def _async_auxiliary_endpoint(
    conn: psycopg.AsyncConnection[Any],
    source_id: UUID,
    table: str,
    external: str,
    key_mapping: dict[str, list[str]],
) -> dict[str, Any]:
    if table not in key_mapping or len(key_mapping[table]) != 1:
        raise HTTPException(status_code=422, detail="unsupported_auxiliary_endpoint")
    key = json.dumps({key_mapping[table][0]: external})
    if table in OBJECT_TYPES:
        query = """SELECT o.native_memory_id AS id,o.tenant_id,m.scope,'memory' AS kind
          FROM braincore.legacy_object_map o JOIN braincore.memories m
            ON m.id=o.native_memory_id AND m.tenant_id=o.tenant_id
          WHERE o.source_id=%s AND o.source_table=%s AND o.record_key=%s::jsonb
          FOR SHARE OF m"""
        values: tuple[object, ...] = (source_id, table, key)
    elif table == "artifact":
        query = """SELECT o.native_artifact_id AS id,o.tenant_id,a.scope,'artifact' AS kind
          FROM braincore.legacy_artifact_map o JOIN braincore.artifacts a
            ON a.id=o.native_artifact_id AND a.tenant_id=o.tenant_id
          WHERE o.source_id=%s AND o.record_key=%s::jsonb FOR SHARE OF a"""
        values = (source_id, key)
    elif table in AUXILIARY_FKS:
        query = """SELECT native_id AS id,tenant_id,scope,'auxiliary' AS kind
          FROM braincore.legacy_auxiliary_objects
          WHERE source_id=%s AND source_table=%s AND record_key=%s::jsonb FOR SHARE"""
        values = (source_id, table, key)
    else:
        raise HTTPException(status_code=422, detail="unsupported_auxiliary_endpoint")
    row = await (await conn.execute(query, values)).fetchone()
    if row is None:
        raise HTTPException(status_code=422, detail="unmapped_auxiliary_endpoint")
    return dict(row)


async def mutate_auxiliary_object(
    conn: psycopg.AsyncConnection[Any],
    principal: Principal,
    source_id: UUID,
    source_table: str,
    record_key: dict[str, Any],
    changes: dict[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    """Create/correct a scoped canonical helper object with durable replay and audit.

    Source keys, original owner, scope and endpoint identities cannot change on
    correction. New rows validate and constrain every typed relationship in the
    same transaction. SQL NULL markers can explicitly distinguish JSON null.
    """
    principal.require("write")
    if (
        source_table not in {"publish_note", "review_queue"}
        or not idempotency_key
        or len(idempotency_key) > 200
    ):
        raise HTTPException(status_code=422, detail="invalid_auxiliary_mutation")
    if not isinstance(changes, dict) or not isinstance(record_key, dict):
        raise HTTPException(status_code=422, detail="invalid_auxiliary_mutation")
    encoded = json.dumps(
        {
            "kind": "legacy.auxiliary.mutate",
            "source_id": str(source_id),
            "table": source_table,
            "record_key": record_key,
            "changes": changes,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    request_hash = hashlib.sha256(encoded.encode()).digest()
    with_scope = {"project_id", "run_id", "scope_path", "tenant"}
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (str(principal.tenant_id) + ":" + str(principal.key_id) + ":" + idempotency_key,),
        )
        receipt = await (
            await conn.execute(
                """SELECT request_hash,kind,response
          FROM braincore.mutation_receipts WHERE tenant_id=%s AND actor_key_id=%s AND key=%s""",
                (principal.tenant_id, principal.key_id, idempotency_key),
            )
        ).fetchone()
        if receipt is not None:
            if (
                receipt["request_hash"] != request_hash
                or receipt["kind"] != "legacy.auxiliary.mutate"
            ):
                raise HTTPException(status_code=409, detail="idempotency_conflict")
            return dict(receipt["response"])
        source = await (
            await conn.execute(
                "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
            )
        ).fetchone()
        if source is None or source_table not in source["key_mapping"]:
            raise HTTPException(status_code=422, detail="unregistered_auxiliary_contract")
        mapping = source["key_mapping"]
        if set(record_key) != set(mapping[source_table]):
            raise HTTPException(status_code=422, detail="invalid_auxiliary_identity")
        try:
            for value in record_key.values():
                UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="invalid_auxiliary_identity") from exc
        key = json.dumps(record_key, sort_keys=True, separators=(",", ":"))
        old = await (
            await conn.execute(
                """SELECT * FROM braincore.legacy_auxiliary_objects
          WHERE source_id=%s AND source_table=%s AND record_key=%s::jsonb FOR UPDATE""",
                (source_id, source_table, key),
            )
        ).fetchone()
        if old is None:
            prototype = await (
                await conn.execute(
                    """SELECT jsonb_object_keys(payload) AS field
              FROM braincore.legacy_auxiliary_objects WHERE source_id=%s AND source_table=%s
              AND record_key=(SELECT record_key FROM braincore.legacy_auxiliary_objects
                WHERE source_id=%s AND source_table=%s ORDER BY record_key LIMIT 1)""",
                    (source_id, source_table, source_id, source_table),
                )
            ).fetchall()
            allowed = {r["field"] for r in prototype}
            if not allowed:
                raise HTTPException(
                    status_code=422, detail="unsupported_empty_auxiliary_write_contract"
                )
            payload: dict[str, Any] = {
                field: None for field in allowed if field != "__braincore_sql_null_columns"
            }
            payload.update(record_key)
            payload.update(changes)
            timestamp = datetime.now(UTC).isoformat()
            if source_table == "publish_note":
                payload["published_at"] = payload.get("published_at") or timestamp
                required = ("memory_id", "file_path", "content_hash")
            else:
                payload["created_at"] = payload.get("created_at") or timestamp
                payload["status"] = payload.get("status") or "pending"
                required = ("target_type", "target_id", "reason")
            if any(payload.get(field) is None for field in required):
                raise HTTPException(status_code=422, detail="missing_auxiliary_field")
            if "__braincore_sql_null_columns" not in changes:
                payload["__braincore_sql_null_columns"] = sorted(
                    field for field, value in payload.items() if value is None
                )
            if any(str(payload.get(k)) != str(v) for k, v in record_key.items()):
                raise HTTPException(status_code=409, detail="auxiliary_identity_conflict")
            tenant = None
            scope = None
            if payload.get("tenant") is not None:
                owner = await (
                    await conn.execute(
                        """SELECT tenant_id FROM braincore.legacy_tenant_map
                  WHERE source_id=%s AND original_tenant=%s""",
                        (source_id, payload["tenant"]),
                    )
                ).fetchone()
                tenant = owner["tenant_id"] if owner else None
                scope = (
                    {"project_id": payload["scope_path"]}
                    if payload.get("scope_path")
                    else {"run_id": "legacy-source:" + str(source_id)}
                )
            else:
                authority = {
                    "publish_note": ("memory", "memory_id"),
                    "eval_case": ("artifact", "artifact_id"),
                    "extraction_run": ("artifact", "artifact_id"),
                    "project_service_map": ("entity", "project_entity_id"),
                    "memory_edge_evidence": ("memory_edge", "edge_id"),
                }.get(source_table)
                if source_table == "review_queue":
                    target_type = payload.get("target_type")
                    if not isinstance(target_type, str):
                        raise HTTPException(
                            status_code=422, detail="invalid_auxiliary_endpoint_type"
                        )
                    authority = (target_type, "target_id")
                if authority and payload.get(authority[1]) is not None:
                    endpoint = await _async_auxiliary_endpoint(
                        conn, source_id, authority[0], str(payload[authority[1]]), mapping
                    )
                    tenant, scope = endpoint["tenant_id"], endpoint["scope"]
        else:
            if old["tenant_id"] != principal.tenant_id or old["deleted"]:
                raise HTTPException(status_code=404, detail="resource_not_found")
            principal.require_scope(old["scope"])
            allowed = set(old["payload"])
            payload = {**old["payload"], **changes}
            tenant, scope = old["tenant_id"], old["scope"]
            immutable = {
                *mapping[source_table],
                *with_scope,
                *AUXILIARY_FKS[source_table],
                "source_type",
                "source_id",
                "target_type",
                "target_id",
                "promotion_target_kind",
                "promotion_target_id",
                "retrieved_memory_ids",
                "injected_memory_ids",
            }
            if any(payload.get(field) != old["payload"].get(field) for field in immutable):
                raise HTTPException(
                    status_code=409, detail="auxiliary_endpoint_or_identity_conflict"
                )
        if set(changes) - allowed:
            raise HTTPException(status_code=422, detail="unknown_auxiliary_field")
        if "__braincore_sql_null_columns" not in changes:
            payload["__braincore_sql_null_columns"] = [
                c
                for c in payload.get("__braincore_sql_null_columns", [])
                if c not in changes or changes[c] is None
            ]
        nulls = payload.get("__braincore_sql_null_columns", [])
        if not isinstance(nulls, list) or any(
            not isinstance(c, str) or c not in allowed for c in nulls
        ):
            raise HTTPException(status_code=422, detail="invalid_sql_null_columns")
        if any(payload.get(c) is not None for c in nulls):
            raise HTTPException(status_code=422, detail="sql_null_value_conflict")
        if tenant != principal.tenant_id or scope is None:
            raise HTTPException(status_code=403, detail="auxiliary_ownership_forbidden")
        principal.require_scope(scope)
        if source_table == "review_queue" and payload.get("status") not in {
            "pending",
            "approved",
            "rejected",
            "deferred",
        }:
            raise HTTPException(status_code=422, detail="invalid_review_status")
        required_strings = (
            ("file_path", "content_hash")
            if source_table == "publish_note"
            else ("target_type", "reason")
        )
        if any(not isinstance(payload.get(field), str) for field in required_strings):
            raise HTTPException(status_code=422, detail="invalid_auxiliary_field_type")
        refs = []
        for column, other, external in _auxiliary_relationships(source_table, payload):
            endpoint = await _async_auxiliary_endpoint(conn, source_id, other, external, mapping)
            if endpoint["kind"] in {"memory", "artifact"} and endpoint["tenant_id"] != tenant:
                raise HTTPException(status_code=403, detail="auxiliary_endpoint_owner_conflict")
            if endpoint["tenant_id"] is not None:
                principal.require_scope(endpoint["scope"])
            refs.append((column, other, endpoint))
        if old is None and source_table == "publish_note":
            memory_root = next(
                endpoint["id"] for _, _, endpoint in refs if endpoint["kind"] == "memory"
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("publish_note:" + str(source_id) + ":" + str(memory_root),),
            )
            duplicate = await (
                await conn.execute(
                    """SELECT 1 FROM braincore.legacy_auxiliary_refs
              WHERE source_id=%s AND source_table='publish_note' AND source_column='memory_id'
                AND native_memory_id=%s""",
                    (source_id, memory_root),
                )
            ).fetchone()
            if duplicate is not None:
                raise HTTPException(status_code=409, detail="publication_exists")
        canonical = await (
            await conn.execute(
                """INSERT INTO braincore.legacy_auxiliary_objects
          (source_id,source_table,record_key,native_id,tenant_id,scope,payload)
          VALUES (%s,%s,%s::jsonb,braincore.legacy_object_id(%s,%s,%s::jsonb),
                  %s,%s::jsonb,%s::jsonb)
          ON CONFLICT (source_id,source_table,record_key) DO UPDATE
            SET payload=EXCLUDED.payload,updated_at=now() RETURNING native_id""",
                (
                    source_id,
                    source_table,
                    key,
                    source_id,
                    source_table,
                    key,
                    tenant,
                    json.dumps(scope),
                    json.dumps(payload, allow_nan=False),
                ),
            )
        ).fetchone()
        assert canonical is not None
        columns = []
        for column, other, endpoint in refs:
            ids = {"memory": None, "artifact": None, "auxiliary": None}
            ids[endpoint["kind"]] = endpoint["id"]
            await conn.execute(
                """INSERT INTO braincore.legacy_auxiliary_refs
              (source_id,source_table,record_key,source_column,target_source_table,
               target_tenant_id,native_memory_id,native_artifact_id,native_auxiliary_id)
              VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (source_id,source_table,record_key,source_column) DO UPDATE
                SET target_source_table=EXCLUDED.target_source_table,
                    target_tenant_id=EXCLUDED.target_tenant_id,
                    native_memory_id=EXCLUDED.native_memory_id,
                    native_artifact_id=EXCLUDED.native_artifact_id,
                    native_auxiliary_id=EXCLUDED.native_auxiliary_id""",
                (
                    source_id,
                    source_table,
                    key,
                    column,
                    other,
                    endpoint["tenant_id"],
                    ids["memory"],
                    ids["artifact"],
                    ids["auxiliary"],
                ),
            )
            columns.append(column)
        await conn.execute(
            """DELETE FROM braincore.legacy_auxiliary_refs WHERE source_id=%s
          AND source_table=%s AND record_key=%s::jsonb AND NOT (source_column=ANY(%s))""",
            (source_id, source_table, key, columns),
        )
        result = {
            "id": str(canonical["native_id"]),
            "object_type": source_table,
            "status": "created" if old is None else "corrected",
        }
        await conn.execute(
            """INSERT INTO braincore.audit_events
          (tenant_id,actor_key_id,action,target_type,target_id,details)
          VALUES (%s,%s,%s,'legacy_auxiliary',%s,%s::jsonb)""",
            (
                tenant,
                principal.key_id,
                "legacy.auxiliary." + result["status"],
                canonical["native_id"],
                json.dumps(
                    {
                        "source_table": source_table,
                        "changed_fields": sorted(changes),
                        "before": old["payload"] if old else None,
                        "after": payload,
                    },
                    allow_nan=False,
                ),
            ),
        )
        await conn.execute(
            """INSERT INTO braincore.mutation_receipts
          (tenant_id,actor_key_id,key,kind,request_hash,response)
          VALUES (%s,%s,%s,%s,%s,%s::jsonb)""",
            (
                tenant,
                principal.key_id,
                idempotency_key,
                "legacy.auxiliary.mutate",
                request_hash,
                json.dumps(result),
            ),
        )
    return result


def _scope_filter(principal: Principal, alias: str) -> tuple[sql.Composable, list[object]]:
    fields = {"project_id", "user_id", "agent_id", "run_id"}
    if principal.scope is None:
        return sql.SQL("{}").format(sql.SQL("true" if "admin" in principal.roles else "false")), []
    if not set(principal.scope).issubset(fields):
        return sql.SQL("{}").format(sql.SQL("false")), []
    clauses: list[sql.Composable] = []
    values: list[object] = []
    for field, value in principal.scope.items():
        clauses.append(sql.SQL("{}=%s").format(sql.Identifier(alias, field)))
        values.append(value)
    return sql.SQL(" AND ").join(clauses or [sql.SQL("true")]), values


async def read_provenance(
    conn: psycopg.AsyncConnection[Any],
    principal: Principal,
    memory_id: UUID,
    limit: int = 100,
) -> dict[str, Any]:
    """Read bounded original provenance with current parent and endpoint visibility.

    Original revisions remain original records, separate from new native audit
    events. Cross-owner artifact quotes require a current grant for the root
    scope; a segment's grant cannot broaden its parent fact's authorization.
    """
    principal.require("read")
    if not 1 <= limit <= 1000:
        raise ValueError("invalid provenance read limit")
    root = await (
        await conn.execute(
            """SELECT scope,legacy_source_id FROM braincore.memories
           WHERE id=%s AND tenant_id=%s FOR SHARE""",
            (memory_id, principal.tenant_id),
        )
    ).fetchone()
    if root is None or not principal.permits_scope(root["scope"]):
        raise HTTPException(status_code=404, detail="resource_not_found")
    if root["legacy_source_id"] is None:
        return {"evidence": [], "supports": [], "revisions": [], "truncated": False}
    segment_scope, segment_params = _scope_filter(principal, "s")
    evidence = await (
        await conn.execute(
            sql.SQL("""
      SELECT e.native_id,e.record_key,v.payload->>'excerpt' AS excerpt,
             v.payload->>'weight' AS weight,v.payload->>'extraction_method' AS extraction_method,
             v.payload->>'created_at' AS original_created_at,s.legacy_external_id AS segment_id
      FROM braincore.legacy_fact_evidence e
      JOIN braincore.legacy_record_versions v ON v.source_id=e.source_id
        AND v.source_table=e.source_table AND v.record_key=e.record_key
        AND v.payload_hash=e.imported_payload_hash
      JOIN braincore.memories s ON s.id=e.native_segment_id AND s.tenant_id=e.tenant_id
      JOIN braincore.memories root ON root.id=e.native_fact_id AND root.tenant_id=e.tenant_id
      JOIN braincore.memory_evidence se ON se.memory_id=s.id AND se.tenant_id=s.tenant_id
      WHERE e.native_fact_id=%s AND e.tenant_id=%s AND NOT e.deleted AND {scope}
        AND (se.artifact_tenant_id=e.tenant_id OR EXISTS (
          SELECT 1 FROM braincore.artifact_access_grants g
          WHERE g.artifact_id=se.artifact_id AND g.owner_tenant_id=se.artifact_tenant_id
            AND g.grantee_tenant_id=e.tenant_id AND root.scope @> g.scope
            AND g.revoked_at IS NULL
            AND (g.expires_at IS NULL OR g.expires_at>statement_timestamp())))
      ORDER BY (v.payload->>'weight')::numeric DESC NULLS LAST,e.record_key LIMIT %s
    """).format(scope=segment_scope),
            (memory_id, principal.tenant_id, *segment_params, limit + 1),
        )
    ).fetchall()
    related_scope, related_params = _scope_filter(principal, "related")
    supports = await (
        await conn.execute(
            sql.SQL("""
      SELECT e.native_id,e.record_key,v.payload->>'support_type' AS support_type,
             v.payload->>'notes' AS notes,v.payload->>'created_at' AS original_created_at,
             related.object_type,related.legacy_external_id AS related_object_id
      FROM braincore.legacy_memory_support e
      JOIN braincore.legacy_record_versions v ON v.source_id=e.source_id
        AND v.source_table=e.source_table AND v.record_key=e.record_key
        AND v.payload_hash=e.imported_payload_hash
      CROSS JOIN LATERAL (VALUES (e.native_fact_id),(e.native_episode_id)) endpoints(id)
      JOIN braincore.memories related
        ON related.id=endpoints.id AND related.tenant_id=e.tenant_id
      WHERE e.native_memory_id=%s AND e.tenant_id=%s AND NOT e.deleted AND {scope}
      ORDER BY e.record_key,related.object_type LIMIT %s
    """).format(scope=related_scope),
            (memory_id, principal.tenant_id, *related_params, limit + 1),
        )
    ).fetchall()
    revisions = await (
        await conn.execute(
            """
      SELECT e.native_id,e.record_key,v.payload->>'revision_type' AS revision_type,
             v.payload->>'old_narrative' AS old_narrative,
             v.payload->>'new_narrative' AS new_narrative,
             v.payload->>'change_reason' AS change_reason,
             v.payload->>'created_at' AS original_created_at
      FROM braincore.legacy_memory_revisions e
      JOIN braincore.legacy_record_versions v ON v.source_id=e.source_id
        AND v.source_table=e.source_table AND v.record_key=e.record_key
        AND v.payload_hash=e.imported_payload_hash
      WHERE e.native_memory_id=%s AND e.tenant_id=%s
      ORDER BY (v.payload->>'created_at')::timestamptz NULLS LAST,e.record_key LIMIT %s
    """,
            (memory_id, principal.tenant_id, limit + 1),
        )
    ).fetchall()
    return {
        "evidence": evidence[:limit],
        "supports": supports[:limit],
        "revisions": revisions[:limit],
        "truncated": any(len(rows) > limit for rows in (evidence, supports, revisions)),
    }

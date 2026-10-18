"""Typed native mapping of reconciled legacy objects; no ownership invention.

Original payloads, relationships and timestamps remain in the immutable ledger.
This mapper preserves explicit source tenants and typed opaque-ID mappings; it
never guesses shared grants or relabels unknown embedding models. Auxiliary
ownership, relationship/client parity and recovery remain separate gates.
"""

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .legacy_authority import guard_import_rows

OBJECT_TYPES = ("entity", "fact", "memory", "segment", "episode")


# Bound exact receipt keys before any access to retained wide source payloads.
# The cursor remains PostgreSQL JSONB text; it never takes a Python numeric path.
_SELECTED_OWNER_PAGE = """
WITH selected_keys AS MATERIALIZED (
  SELECT k.record_key FROM pg_temp.braincore_import_keys k
  WHERE k.source_table=%s {cursor_predicate}
  ORDER BY k.record_key LIMIT %s
), selected_heads AS MATERIALIZED (
  SELECT h.source_id,h.source_table,h.record_key,h.payload_hash
  FROM selected_keys k JOIN braincore.legacy_record_heads h ON h.record_key=k.record_key
  WHERE h.source_id=%s AND h.source_table=%s
), owners AS MATERIALIZED (
  SELECT v.payload->>'tenant' AS original_tenant,count(*) AS n
  FROM selected_heads h JOIN braincore.legacy_record_versions v ON v.source_id=h.source_id
    AND v.source_table=h.source_table AND v.record_key=h.record_key
    AND v.payload_hash=h.payload_hash
  GROUP BY v.payload->>'tenant'
)
SELECT ARRAY(SELECT original_tenant FROM owners) AS labels,
  (SELECT count(*) FROM selected_keys) AS expected,
  (SELECT COALESCE(sum(n),0) FROM owners) AS resolved,
  (SELECT record_key::text FROM selected_keys ORDER BY record_key DESC LIMIT 1) AS next_cursor
"""


def prepare_tenants(
    target: psycopg.Connection[Any],
    source_id: UUID,
    source_tables: tuple[str, ...] = (*OBJECT_TYPES, "artifact"),
    *,
    selected_keys: bool = False,
    owner_batch_size: int = 1000,
) -> None:
    if not source_tables or not set(source_tables).issubset({*OBJECT_TYPES, "artifact"}):
        raise ValueError("invalid tenant mapping source tables")
    if not 1 <= owner_batch_size <= 1000:
        raise ValueError("invalid owner discovery batch size")
    with target.transaction():
        source = target.execute(
            "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
        ).fetchone()
        if source is None or not set(OBJECT_TYPES).issubset(source["key_mapping"]):
            raise ValueError("typed source mapping is incomplete")
        finished = target.execute(
            """SELECT 1 FROM braincore.legacy_copy_runs
                                     WHERE source_id=%s AND completed_at IS NOT NULL LIMIT 1""",
            (source_id,),
        ).fetchone()
        if finished is None:
            raise ValueError("source-accounting copy must finish before native mapping")
        source_count = target.execute(
            "SELECT count(*) AS n FROM braincore.legacy_sources"
        ).fetchone()
        assert source_count is not None
        if source_count["n"] != 1:
            raise ValueError("multi-source tenant merging requires explicit mapping")
        labels: set[str] = set()
        # Fixed per-table predicates avoid scanning unrelated historical payloads.
        # Validate originals on every retry, including newly captured tenants.
        for table in source_tables:
            if selected_keys:
                after = None
                while True:
                    values: list[Any] = [table]
                    if after is not None:
                        values.append(after)
                    values.extend((owner_batch_size, source_id, table))
                    page = target.execute(
                        _SELECTED_OWNER_PAGE.format(
                            cursor_predicate="AND k.record_key>%s::jsonb"
                            if after is not None
                            else ""
                        ),
                        values,
                    ).fetchone()
                    assert page is not None
                    if page["resolved"] != page["expected"]:
                        raise ValueError("finite native owner key has no retained version")
                    if not page["expected"]:
                        break
                    for label in page["labels"]:
                        if not isinstance(label, str) or not label.strip():
                            raise ValueError("native mapping requires explicit original tenants")
                        labels.add(label)
                    after = page["next_cursor"]
                    assert after is not None
                continue
            owners = target.execute(
                """SELECT v.payload->>'tenant' AS original_tenant,count(*) AS n
                   FROM braincore.legacy_record_heads h
                   JOIN braincore.legacy_record_versions v ON v.source_id=h.source_id
                     AND v.source_table=h.source_table AND v.record_key=h.record_key
                     AND v.payload_hash=h.payload_hash
                   WHERE h.source_id=%s AND h.source_table=%s AND v.source_table=%s
                   GROUP BY v.payload->>'tenant'""",
                (source_id, table, table),
            ).fetchall()
            for owner in owners:
                label = owner["original_tenant"]
                if not isinstance(label, str) or not label.strip():
                    raise ValueError("native mapping requires explicit original tenants")
                labels.add(label)
        target.execute(
            """INSERT INTO braincore.tenants(name) SELECT unnest(%s::text[])
               ON CONFLICT (name) DO NOTHING""",
            (sorted(labels),),
        )
        target.execute(
            """INSERT INTO braincore.legacy_tenant_map(source_id,original_tenant,tenant_id)
               SELECT %s,name,id FROM braincore.tenants WHERE name=ANY(%s)
               ON CONFLICT DO NOTHING""",
            (source_id, sorted(labels)),
        )


# SQL selects exact source text, never a Python float round-trip for JSON numbers.
# Source validity is not replaced by migration time. Native created/updated
# timestamps fall back only when absent; the original absence stays in the ledger.
_SELECT_HEADS = """
  SELECT legacy_record_heads.source_id,legacy_record_heads.source_table,
         legacy_record_heads.record_key,legacy_record_heads.payload_hash,legacy_record_heads.deleted
  FROM braincore.legacy_record_heads {key_join}
  WHERE legacy_record_heads.source_id=%s AND legacy_record_heads.source_table=%s
    {cursor_predicate} {changed_predicate}
  ORDER BY legacy_record_heads.record_key LIMIT %s
"""
_RENDER_HEADS = """
originals AS MATERIALIZED (
  SELECT h.record_key,h.payload_hash,h.deleted,v.payload,p.tenant_id,
         braincore.legacy_object_id(h.source_id,h.source_table,h.record_key) AS native_id,
         h.source_id,h.source_table
  FROM selected_heads h
  JOIN braincore.legacy_record_versions v USING (source_id,source_table,record_key,payload_hash)
  JOIN braincore.legacy_tenant_map p ON p.source_id=h.source_id
                                  AND p.original_tenant=v.payload->>'tenant'
), rendered AS (
  SELECT o.*,
    CASE source_table
      WHEN 'entity' THEN payload->>'canonical_name'
      WHEN 'fact' THEN (payload->>'predicate') || ' ' ||
        CASE WHEN (payload->'__braincore_sql_null_columns') ? 'object_value' THEN ''
             ELSE COALESCE((payload->'object_value')::text,'') END
      WHEN 'memory' THEN COALESCE(payload->>'title','') || ' ' || COALESCE(payload->>'narrative','')
      WHEN 'episode' THEN COALESCE(payload->>'title','') || ' ' || COALESCE(payload->>'summary','')
      WHEN 'segment' THEN payload->>'content'
    END AS content,
    CASE WHEN payload->>'scope_path' IS NOT NULL AND payload->>'scope_path' <> ''
         THEN payload->>'scope_path' END AS original_scope,
    CASE WHEN deleted THEN 'retired'
         WHEN source_table='fact' AND payload->>'current_status' IS DISTINCT FROM 'active'
           THEN 'retired'
         WHEN source_table='memory' AND
              (payload->>'lifecycle_state' IS DISTINCT FROM 'published'
               OR COALESCE(payload->>'governance_status','') NOT IN ('active','validated'))
           THEN 'suppressed'
         ELSE 'active' END AS native_state
  FROM originals o
)
"""
_RENDER_BATCH = "WITH selected_heads AS MATERIALIZED (" + _SELECT_HEADS + "), " + _RENDER_HEADS
_UPSERT_BATCH = """
  INSERT INTO braincore.memories AS m
    (id,tenant_id,content,dedupe_key,project_id,run_id,scope,state,trust,valid_from,valid_until,
     embedding_state,metadata,created_at,updated_at,object_type,legacy_source_id,legacy_record_key,
     legacy_external_id,legacy_scope_path,legacy_fts_eligible)
  SELECT native_id,tenant_id,COALESCE(content,''),public.digest(native_id::text,'sha256'),
    CASE WHEN original_scope IS NOT NULL THEN original_scope END,
    CASE WHEN original_scope IS NULL THEN 'legacy-source:'||source_id::text END,
    CASE WHEN original_scope IS NOT NULL THEN jsonb_build_object('project_id',original_scope)
         ELSE jsonb_build_object('run_id','legacy-source:'||source_id::text) END,
    native_state,CASE WHEN source_table IN ('fact','memory') THEN 'extracted' ELSE 'stated' END,
    (payload->>'valid_from')::timestamptz,(payload->>'valid_to')::timestamptz,
    CASE WHEN COALESCE(content,'') ~ '[^[:space:]]' THEN 'pending' ELSE 'not_applicable' END,
    jsonb_build_object('legacy',jsonb_strip_nulls(jsonb_build_object(
      'source_type',source_table,'confidence',payload->'confidence','title',payload->'title',
      'source_deleted',CASE WHEN deleted THEN true END,
      'created_at_origin',CASE WHEN payload->>'created_at' IS NOT NULL THEN 'source_created_at'
         WHEN payload->>'first_seen_at' IS NOT NULL THEN 'source_first_seen_at'
         ELSE 'migration' END,
      'priority',payload->'priority','original_lifecycle',payload->'lifecycle_state',
      'original_governance',payload->'governance_status','original_trust',payload->'trust_class',
      'assertion_class',payload->'assertion_class','original_created_at',payload->'created_at',
      'original_updated_at',payload->'updated_at','embedding_identity','unverified'))),
    COALESCE((payload->>'created_at')::timestamptz,(payload->>'first_seen_at')::timestamptz,now()),
    COALESCE((payload->>'updated_at')::timestamptz,(payload->>'last_seen_at')::timestamptz,
             (payload->>'created_at')::timestamptz,now()),
    source_table,source_id,record_key,(payload->>(source_table||'_id'))::uuid,original_scope,
    CASE WHEN source_table='entity' THEN true ELSE payload->>'fts' IS NOT NULL END
  FROM rendered
  ON CONFLICT (id) DO UPDATE SET
    content=EXCLUDED.content,project_id=EXCLUDED.project_id,run_id=EXCLUDED.run_id,
    scope=EXCLUDED.scope,state=EXCLUDED.state,valid_from=EXCLUDED.valid_from,
    valid_until=EXCLUDED.valid_until,metadata=EXCLUDED.metadata,updated_at=EXCLUDED.updated_at,
    legacy_scope_path=EXCLUDED.legacy_scope_path,legacy_fts_eligible=EXCLUDED.legacy_fts_eligible,
    embedding=CASE WHEN EXCLUDED.content ~ '[^[:space:]]'
                    AND m.content=EXCLUDED.content THEN m.embedding ELSE NULL END,
    embedding_state=CASE WHEN EXCLUDED.content !~ '[^[:space:]]' THEN 'not_applicable'
      WHEN m.content=EXCLUDED.content THEN m.embedding_state ELSE 'pending' END,
    embedding_model=CASE WHEN EXCLUDED.content ~ '[^[:space:]]'
      AND m.content=EXCLUDED.content THEN m.embedding_model ELSE NULL END,
    embedding_preprocessing_revision=CASE WHEN EXCLUDED.content ~ '[^[:space:]]'
      AND m.content=EXCLUDED.content
      THEN m.embedding_preprocessing_revision ELSE NULL END
  WHERE m.tenant_id=EXCLUDED.tenant_id AND m.legacy_source_id=EXCLUDED.legacy_source_id
    AND m.object_type=EXCLUDED.object_type AND m.legacy_record_key=EXCLUDED.legacy_record_key
    AND ((m.content,m.project_id,m.run_id,m.scope,m.state,m.valid_from,m.valid_until,
          m.metadata,m.legacy_scope_path,m.legacy_fts_eligible)
      IS DISTINCT FROM
         (EXCLUDED.content,EXCLUDED.project_id,EXCLUDED.run_id,EXCLUDED.scope,EXCLUDED.state,
          EXCLUDED.valid_from,EXCLUDED.valid_until,EXCLUDED.metadata,
          EXCLUDED.legacy_scope_path,EXCLUDED.legacy_fts_eligible)
      OR (m.updated_at IS DISTINCT FROM EXCLUDED.updated_at
          AND (EXCLUDED.metadata->'legacy'->>'original_updated_at' IS NOT NULL
            OR EXCLUDED.metadata->'legacy'->>'original_created_at' IS NOT NULL
            OR EXCLUDED.metadata->'legacy'->>'created_at_origin'='source_first_seen_at'))
      OR (EXCLUDED.content !~ '[^[:space:]]'
          AND (m.embedding_state IS DISTINCT FROM 'not_applicable' OR m.embedding IS NOT NULL
            OR m.embedding_model IS NOT NULL
            OR m.embedding_preprocessing_revision IS NOT NULL)))
"""
_MAP_BATCH = """
INSERT INTO braincore.legacy_object_map AS existing
 (source_id,source_table,record_key,tenant_id,native_memory_id,imported_payload_hash)
SELECT r.source_id,r.source_table,r.record_key,r.tenant_id,r.native_id,r.payload_hash
FROM pg_temp.braincore_import_batch r
JOIN braincore.memories m ON m.id=r.native_id AND m.tenant_id=r.tenant_id
 AND m.legacy_source_id=r.source_id AND m.object_type=r.source_table
 AND m.legacy_record_key=r.record_key
WHERE NOT r.native_preserved
ON CONFLICT (source_id,source_table,record_key) DO UPDATE
 SET imported_payload_hash=EXCLUDED.imported_payload_hash,mapped_at=now()
 WHERE existing.tenant_id=EXCLUDED.tenant_id
   AND existing.native_memory_id=EXCLUDED.native_memory_id
RETURNING record_key
"""


_REQUESTED_KEYS_PAGE = """
SELECT k.record_key::text AS key FROM pg_temp.braincore_import_keys k
WHERE k.source_table=%s {cursor_predicate}
ORDER BY k.record_key LIMIT %s
"""
_UNCHANGED_REQUESTED_HEADS = """
DELETE FROM pg_temp.braincore_import_heads h USING (
  SELECT candidate.record_key FROM pg_temp.braincore_import_heads candidate
  JOIN LATERAL (
    SELECT 1 FROM braincore.legacy_object_map already
    JOIN braincore.memories current ON current.id=already.native_memory_id
    WHERE already.source_id=candidate.source_id
      AND already.source_table=candidate.source_table
      AND already.record_key=candidate.record_key
      AND already.imported_payload_hash=candidate.payload_hash
      AND COALESCE((current.metadata->'legacy'->>'source_deleted')::boolean,false)=candidate.deleted
    LIMIT 1
  ) unchanged ON true
) unchanged WHERE h.record_key=unchanged.record_key
"""
_RECONCILE_REQUESTED_PAGE = """
SELECT count(*) AS expected,count(*) FILTER (WHERE verified.correct) AS mapped
FROM pg_temp.braincore_import_requested k
LEFT JOIN LATERAL (
  SELECT o.imported_payload_hash=h.payload_hash
    AND o.tenant_id=m.tenant_id AND m.legacy_source_id=h.source_id
    AND m.object_type=h.source_table AND m.legacy_record_key=h.record_key
    AND (COALESCE((m.metadata->'legacy'->>'source_deleted')::boolean,false)=h.deleted
      OR EXISTS (SELECT 1 FROM braincore.legacy_native_authority a
        WHERE a.relation_name='memories' AND a.native_id=m.id
          AND a.imported_payload_hash=h.payload_hash)) AS correct
  FROM braincore.legacy_record_heads h
  LEFT JOIN braincore.legacy_object_map o ON o.source_id=h.source_id
    AND o.source_table=h.source_table AND o.record_key=h.record_key
  LEFT JOIN braincore.memories m ON m.id=o.native_memory_id
  WHERE h.source_id=%s AND h.source_table=%s AND h.record_key=k.record_key
  LIMIT 1
) verified ON true
"""


def _stage_keys(target: psycopg.Connection[Any], changed_keys: Mapping[str, Sequence[str]]) -> None:
    if not set(changed_keys).issubset(OBJECT_TYPES):
        raise ValueError("invalid finite native source tables")
    target.execute(
        "CREATE TEMP TABLE braincore_import_keys "
        "(source_table text,record_key jsonb,PRIMARY KEY(source_table,record_key))"
    )
    # Receipts contain exact JSON text; never deserialize and round-trip numeric keys.
    with target.cursor().copy(
        "COPY pg_temp.braincore_import_keys(source_table,record_key) FROM STDIN"
    ) as copy:
        for table, keys in changed_keys.items():
            if isinstance(keys, str | bytes):
                raise ValueError("finite native keys must be a sequence of JSON objects")
            for key in set(keys):
                if not isinstance(key, str) or not isinstance(json.loads(key), dict):
                    raise ValueError("finite native key must be a JSON object string")
                copy.write_row((table, key))
    # Semantically equal JSON strings must not silently become two requested keys.
    target.execute("ANALYZE pg_temp.braincore_import_keys")


def import_objects(
    target: psycopg.Connection[Any],
    source_id: UUID,
    batch_size: int = 500,
    *,
    changed_only: bool = False,
    changed_keys: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Import transactionally bounded pages; optional receipt keys bound all accounting."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid native import batch size")
    reserved = (
        "braincore_import_keys",
        "braincore_import_heads",
        "braincore_import_batch",
        "braincore_import_requested",
    )
    for name in reserved:
        present = target.execute(
            "SELECT to_regclass(%s) AS relation", ("pg_temp." + name,)
        ).fetchone()
        assert present is not None
        if present["relation"] is not None:
            raise ValueError("native import temporary relation already exists")
    owns_keys = False
    try:
        if changed_keys is None:
            return _import_objects(target, source_id, batch_size, changed_only, False)
        with target.transaction():
            _stage_keys(target, changed_keys)
            missing = target.execute(
                """SELECT 1 FROM pg_temp.braincore_import_keys k
                   LEFT JOIN braincore.legacy_record_heads h
                     ON h.source_id=%s AND h.source_table=k.source_table
                        AND h.record_key=k.record_key
                   WHERE h.source_id IS NULL LIMIT 1""",
                (source_id,),
            ).fetchone()
            if missing:
                raise ValueError("finite native key has no retained head")
        owns_keys = True
        return _import_objects(target, source_id, batch_size, changed_only, True)
    finally:
        # These names were absent on entry; never remove caller-owned relations.
        target.execute(
            "DROP TABLE IF EXISTS pg_temp.braincore_import_heads, pg_temp.braincore_import_batch, "
            "pg_temp.braincore_import_requested"
        )
        if owns_keys:
            target.execute("DROP TABLE pg_temp.braincore_import_keys")


def _persist_import_page(target: psycopg.Connection[Any], expected: int) -> int:
    if not expected:
        return 0
    target.execute("ANALYZE pg_temp.braincore_import_heads")
    target.execute(
        sql.SQL(
            "CREATE TEMP TABLE braincore_import_batch ON COMMIT DROP "
            "AS WITH {} SELECT * FROM rendered"
        ).format(
            sql.SQL(
                _RENDER_HEADS.replace(
                    "FROM selected_heads h", "FROM pg_temp.braincore_import_heads h"
                )
            )
        )
    )
    target.execute(
        "ALTER TABLE pg_temp.braincore_import_batch ADD PRIMARY KEY(native_id), "
        "ADD UNIQUE(source_id,source_table,record_key)"
    )
    rendered = target.execute("SELECT count(*) AS n FROM pg_temp.braincore_import_batch").fetchone()
    assert rendered is not None
    if rendered["n"] != expected:
        raise ValueError("native identity or ownership conflict")
    target.execute("ANALYZE pg_temp.braincore_import_batch")
    rows = target.execute(
        "SELECT native_id,payload_hash FROM pg_temp.braincore_import_batch ORDER BY native_id"
    ).fetchall()
    preserved = guard_import_rows(
        target, "memories", [(r["native_id"], bytes(r["payload_hash"])) for r in rows]
    )
    target.execute(
        "ALTER TABLE pg_temp.braincore_import_batch ADD COLUMN native_preserved boolean "
        "NOT NULL DEFAULT false"
    )
    if preserved:
        target.execute(
            "UPDATE pg_temp.braincore_import_batch SET native_preserved=true "
            "WHERE native_id=ANY(%s)",
            (list(preserved),),
        )
    target.execute(
        _UPSERT_BATCH.replace(
            "FROM rendered", "FROM pg_temp.braincore_import_batch WHERE NOT native_preserved"
        )
    )
    imported = len(target.execute(_MAP_BATCH).fetchall()) + len(preserved)
    if imported != expected:
        raise ValueError("native identity or ownership conflict")
    return imported


def _import_objects(
    target: psycopg.Connection[Any],
    source_id: UUID,
    batch_size: int,
    changed_only: bool,
    selected_keys: bool,
) -> dict[str, Any]:
    prepare_tenants(
        target,
        source_id,
        OBJECT_TYPES if selected_keys else (*OBJECT_TYPES, "artifact"),
        selected_keys=selected_keys,
        owner_batch_size=min(batch_size, 1000),
    )
    counts: dict[str, int] = {}
    totals: dict[str, int] = {}
    for object_type in OBJECT_TYPES:
        after = None
        count = 0
        while True:
            with target.transaction():
                target.execute(
                    "DROP TABLE IF EXISTS pg_temp.braincore_import_heads, "
                    "pg_temp.braincore_import_batch, pg_temp.braincore_import_requested"
                )
                if selected_keys:
                    # Keyset pagination is over requested keys, including no-op
                    # pages, before any canonical hash/tombstone lookup.
                    values: list[Any] = [object_type]
                    if after is not None:
                        values.append(after)
                    values.append(min(batch_size, 1000))
                    requested = target.execute(
                        _REQUESTED_KEYS_PAGE.format(
                            cursor_predicate="AND k.record_key>%s::jsonb"
                            if after is not None
                            else ""
                        ),
                        values,
                    ).fetchall()
                    if not requested:
                        break
                    next_cursor = requested[-1]["key"]
                    target.execute(
                        "CREATE TEMP TABLE braincore_import_requested "
                        "(record_key jsonb PRIMARY KEY) ON COMMIT DROP"
                    )
                    # Exact JSON text remains inside PostgreSQL casts. No numeric
                    # key is deserialized/reformatted in Python.
                    target.execute(
                        "INSERT INTO pg_temp.braincore_import_requested SELECT unnest(%s::jsonb[])",
                        ([r["key"] for r in requested],),
                    )
                    target.execute("ANALYZE pg_temp.braincore_import_requested")
                    target.execute(
                        """CREATE TEMP TABLE braincore_import_heads ON COMMIT DROP AS
                           SELECT h.source_id,h.source_table,h.record_key,h.payload_hash,h.deleted
                           FROM pg_temp.braincore_import_requested k
                           JOIN braincore.legacy_record_heads h ON h.record_key=k.record_key
                           WHERE h.source_id=%s AND h.source_table=%s""",
                        (source_id, object_type),
                    )
                    retained = target.execute(
                        "SELECT count(*) AS n FROM pg_temp.braincore_import_heads"
                    ).fetchone()
                    assert retained is not None
                    if retained["n"] != len(requested):
                        raise ValueError("finite native key has no retained head")
                    target.execute("ANALYZE pg_temp.braincore_import_heads")
                    if changed_only:
                        target.execute(_UNCHANGED_REQUESTED_HEADS)
                    pending = target.execute(
                        "SELECT count(*) AS n FROM pg_temp.braincore_import_heads"
                    ).fetchone()
                    assert pending is not None
                    imported = _persist_import_page(target, pending["n"])
                    # Reconcile EVERY requested key, including no-op rows, using
                    # the same bounded physical page and source/owner guards.
                    accounting = target.execute(
                        _RECONCILE_REQUESTED_PAGE, (source_id, object_type)
                    ).fetchone()
                    assert accounting is not None
                    if accounting["expected"] != len(requested) or accounting["mapped"] != len(
                        requested
                    ):
                        raise ValueError("native object mapping accounting is incomplete")
                    totals[object_type] = totals.get(object_type, 0) + accounting["mapped"]
                else:
                    # Full-source import keeps its existing changed-only and
                    # cursor contract; finite receipt mode is opt-in.
                    values = [source_id, object_type]
                    if after is not None:
                        values.append(after)
                    values.append(batch_size)
                    selection = _SELECT_HEADS.format(
                        key_join="",
                        cursor_predicate="AND legacy_record_heads.record_key>%s::jsonb"
                        if after is not None
                        else "",
                        changed_predicate="""AND NOT EXISTS (
                          SELECT 1 FROM braincore.legacy_object_map already
                          JOIN braincore.memories current ON current.id=already.native_memory_id
                          WHERE already.source_id=legacy_record_heads.source_id
                            AND already.source_table=legacy_record_heads.source_table
                            AND already.record_key=legacy_record_heads.record_key
                            AND already.imported_payload_hash=legacy_record_heads.payload_hash
                            AND COALESCE((current.metadata->'legacy'->>'source_deleted')::boolean,
                                         false)=legacy_record_heads.deleted
                        )"""
                        if changed_only
                        else "",
                    )
                    target.execute(
                        sql.SQL(
                            "CREATE TEMP TABLE braincore_import_heads ON COMMIT DROP AS {}"
                        ).format(sql.SQL(selection)),
                        values,
                    )
                    expected = target.execute(
                        "SELECT count(*) AS n, (SELECT record_key::text "
                        "FROM pg_temp.braincore_import_heads ORDER BY record_key DESC LIMIT 1) "
                        "AS next_cursor FROM pg_temp.braincore_import_heads"
                    ).fetchone()
                    assert expected is not None
                    if not expected["n"]:
                        break
                    imported = _persist_import_page(target, expected["n"])
                    next_cursor = expected["next_cursor"]
            count += imported
            after = next_cursor
        counts[object_type] = count
    if not selected_keys:
        reconciled = target.execute(
            """SELECT h.source_table,count(*) AS expected,
               count(o.native_memory_id) FILTER
                 (WHERE o.imported_payload_hash=h.payload_hash) AS mapped
               FROM braincore.legacy_record_heads h LEFT JOIN braincore.legacy_object_map o
                 ON o.source_id=h.source_id AND o.source_table=h.source_table
                 AND o.record_key=h.record_key
               WHERE h.source_id=%s AND h.source_table=ANY(%s)
               GROUP BY h.source_table""",
            (source_id, list(OBJECT_TYPES)),
        ).fetchall()
        if any(r["expected"] != r["mapped"] for r in reconciled):
            raise ValueError("native object mapping accounting is incomplete")
        totals = {r["source_table"]: r["mapped"] for r in reconciled}
    return {
        "typed_object_gate": "PASS",
        "objects": totals,
        "row_count": sum(totals.values()),
        "processed_rows": sum(counts.values()),
        "changed_only": changed_only,
        "accounting_scope": "selected_keys" if selected_keys else "source",
        "evidence_relationship_gate": "NOT RUN",
        "auxiliary_semantic_gate": "NOT RUN",
        "embedding_coverage_gate": "NOT RUN",
        "client_parity_gate": "NOT RUN",
    }


def reconcile_objects(
    target: psycopg.Connection[Any], source_id: UUID, batch_size: int = 1000
) -> dict[str, Any]:
    """Verify every retained typed record and native semantic field without mutation."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("invalid native reconciliation batch size")
    # Reuse importer rendering; original timestamps remain in the immutable version
    # and strict metadata. Current updated_at may advance only through an exact
    # terminal embedding job/item for the same native identity, owner and timestamp.
    # JSON precision and SQL NULL distinctions still agree.
    source_query = _RENDER_BATCH
    insert_query = _UPSERT_BATCH.split("INSERT INTO braincore.memories AS m", 1)[1]
    columns, projection = insert_query.split("  SELECT ", 1)
    projection = "SELECT " + projection.split("  ON CONFLICT (id)", 1)[0]
    expected_query = source_query + ", expected " + columns.strip() + " AS (" + projection + ")"
    comparison = """
SELECT (SELECT count(*) FROM selected_heads) AS expected,
 (SELECT count(*) FROM selected_heads)-count(*) + count(*) FILTER (WHERE
   o.native_memory_id IS DISTINCT FROM e.id
   OR o.tenant_id IS DISTINCT FROM e.tenant_id
   OR o.imported_payload_hash IS DISTINCT FROM r.payload_hash
   OR (m.id,m.tenant_id,m.object_type,m.legacy_source_id,m.legacy_record_key)
      IS DISTINCT FROM (e.id,e.tenant_id,e.object_type,e.legacy_source_id,e.legacy_record_key)
   OR (NOT EXISTS (SELECT 1 FROM braincore.legacy_native_authority a
     WHERE a.relation_name='memories' AND a.native_id=m.id
       AND a.imported_payload_hash=r.payload_hash) AND (
   (m.id,m.tenant_id,m.content,m.dedupe_key,m.project_id,m.run_id,m.scope,m.state,
       m.trust,m.valid_from,m.valid_until,m.metadata,m.object_type,m.legacy_source_id,
       m.legacy_record_key,m.legacy_external_id,m.legacy_scope_path,m.legacy_fts_eligible)
     IS DISTINCT FROM
      (e.id,e.tenant_id,e.content,e.dedupe_key,e.project_id,e.run_id,e.scope,e.state,
       e.trust,e.valid_from,e.valid_until,e.metadata,e.object_type,e.legacy_source_id,
       e.legacy_record_key,e.legacy_external_id,e.legacy_scope_path,e.legacy_fts_eligible)
   OR (COALESCE(r.payload->>'created_at',r.payload->>'first_seen_at') IS NOT NULL
       AND m.created_at IS DISTINCT FROM e.created_at)
   OR (COALESCE(r.payload->>'updated_at',r.payload->>'last_seen_at',
               r.payload->>'created_at') IS NOT NULL
       AND m.updated_at IS DISTINCT FROM e.updated_at
       AND NOT EXISTS (
         SELECT 1 FROM braincore.jobs j JOIN braincore.operation_items item
           ON item.id=j.operation_item_id AND item.operation_id=j.operation_id
             AND item.tenant_id=j.tenant_id
         WHERE j.kind='embed_memory' AND j.state IN ('succeeded','failed','dead_letter')
           AND item.state IN ('succeeded','failed','partial')
           AND item.memory_id=m.id AND j.tenant_id=m.tenant_id
           AND j.completed_at=m.updated_at AND item.completed_at=m.updated_at
           AND j.payload->>'memory_id'=m.id::text
       ))
   ))
 ) AS mismatched,
 (SELECT record_key::text FROM selected_heads ORDER BY record_key DESC LIMIT 1) AS next_cursor
FROM rendered r JOIN expected e ON e.id=r.native_id
LEFT JOIN braincore.legacy_object_map o ON o.source_id=r.source_id
 AND o.source_table=r.source_table AND o.record_key=r.record_key
LEFT JOIN braincore.memories m ON m.id=e.id
"""
    tables = {}
    with target.transaction():
        target.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for table in OBJECT_TYPES:
            after = None
            expected = mismatched = 0
            while True:
                values: list[object] = [source_id, table]
                if after is not None:
                    values.append(after)
                values.append(batch_size)
                row = target.execute(
                    expected_query.format(
                        key_join="",
                        cursor_predicate="AND legacy_record_heads.record_key>%s::jsonb"
                        if after
                        else "",
                        changed_predicate="",
                    )
                    + comparison,
                    values,
                ).fetchone()
                assert row is not None
                expected += row["expected"]
                mismatched += row["mismatched"]
                after = row["next_cursor"]
                if after is None:
                    break
            extra_row = target.execute(
                """SELECT count(*) AS n FROM braincore.memories m
               WHERE legacy_source_id=%s AND object_type=%s AND NOT EXISTS (
                 SELECT 1 FROM braincore.legacy_record_heads h WHERE h.source_id=m.legacy_source_id
                   AND h.source_table=m.object_type AND h.record_key=m.legacy_record_key)""",
                (source_id, table),
            ).fetchone()
            assert extra_row is not None
            extra = extra_row["n"]
            tables[table] = {"expected": expected, "mismatched": mismatched, "extra": extra}
    return {
        "typed_semantic_per_key_reconciliation_gate": "PASS"
        if all(row["mismatched"] == 0 and row["extra"] == 0 for row in tables.values())
        else "FAIL",
        "tables": tables,
        "row_count": sum(row["expected"] for row in tables.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-id", type=UUID, required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--changed-only",
        action="store_true",
        help="Resume by importing only heads whose mapped payload hash changed",
    )
    args = parser.parse_args()
    target_url = os.environ.get("BRAINCORE_DATABASE_URL")
    if not target_url or args.output.exists():
        parser.exit(2, "target configuration and a fresh receipt path are required\n")
    try:
        with psycopg.connect(target_url, autocommit=True, row_factory=dict_row) as target:
            report = import_objects(
                target, args.source_id, args.batch_size, changed_only=args.changed_only
            )
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    except (psycopg.Error, ValueError, OSError):
        parser.exit(
            2, "native object mapping failed; retained ledger and batches are safe to retry\n"
        )
    print(json.dumps({k: v for k, v in report.items() if k.endswith("gate") or k == "row_count"}))


if __name__ == "__main__":
    main()

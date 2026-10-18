"""Resumable lexical projection backfill; never modifies canonical memories."""

import argparse
import json
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import get_settings

_UPSERT = """INSERT INTO braincore.memories_lexical
    (id,tenant_id,project_id,user_id,agent_id,run_id,object_type,legacy_scope_path,
     reference_namespace,metadata,state,valid_from,valid_until,legacy_fts_eligible,
     updated_at,search_vector)
    SELECT id,tenant_id,project_id,user_id,agent_id,run_id,object_type,legacy_scope_path,
           reference_namespace,metadata,state,valid_from,valid_until,legacy_fts_eligible,
           updated_at,search_vector
    FROM braincore.memories WHERE id=ANY(%s::uuid[])
    ON CONFLICT(id) DO UPDATE SET
      tenant_id=EXCLUDED.tenant_id,project_id=EXCLUDED.project_id,user_id=EXCLUDED.user_id,
      agent_id=EXCLUDED.agent_id,run_id=EXCLUDED.run_id,object_type=EXCLUDED.object_type,
      legacy_scope_path=EXCLUDED.legacy_scope_path,
      reference_namespace=EXCLUDED.reference_namespace,metadata=EXCLUDED.metadata,
      state=EXCLUDED.state,valid_from=EXCLUDED.valid_from,valid_until=EXCLUDED.valid_until,
      legacy_fts_eligible=EXCLUDED.legacy_fts_eligible,updated_at=EXCLUDED.updated_at,
      search_vector=EXCLUDED.search_vector"""


def backfill_batch(
    conn: psycopg.Connection[dict[str, Any]], *, batch_size: int = 1000
) -> dict[str, Any]:
    """Commit one bounded batch and its checkpoint, or certify completeness.

    Concurrent backfill callers serialize on the singleton state row. Canonical
    rows are locked before copying, so a trigger update cannot be overwritten by
    an older backfill snapshot. A stopped batch rolls back with its checkpoint.
    The final missing-row check is an explicit one-time corpus verification,
    not a request-path scan. Use an autocommit connection for independent commits.
    """
    if not 1 <= batch_size <= 10_000:
        raise ValueError("batch_size must be between 1 and 10000")
    with conn.transaction():
        state = conn.execute(
            """SELECT ready,backfill_after_id,rows_processed
               FROM braincore.lexical_projection_state WHERE singleton=true FOR UPDATE"""
        ).fetchone()
        if state is None:
            raise RuntimeError("Lexical projection state is missing; run migrations first")
        if state["ready"]:
            return {"ready": True, "scanned_items": 0, **state}
        cursor = state["backfill_after_id"]
        if cursor is None:
            rows = conn.execute(
                "SELECT id FROM braincore.memories ORDER BY id LIMIT %s FOR UPDATE",
                (batch_size,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id FROM braincore.memories WHERE id>%s ORDER BY id LIMIT %s FOR UPDATE",
                (cursor, batch_size),
            ).fetchall()
        if rows:
            ids = [row["id"] for row in rows]
            conn.execute(_UPSERT, (ids,))
            state = conn.execute(
                """UPDATE braincore.lexical_projection_state
                   SET backfill_after_id=%s,rows_processed=rows_processed+%s,updated_at=now()
                   WHERE singleton=true RETURNING ready,backfill_after_id,rows_processed""",
                (ids[-1], len(ids)),
            ).fetchone()
            assert state is not None
            return {"scanned_items": len(ids), **state}
        missing = conn.execute(
            """SELECT EXISTS(
                 SELECT 1 FROM braincore.memories m
                 WHERE NOT EXISTS(SELECT 1 FROM braincore.memories_lexical l
                                  WHERE l.id=m.id AND l.tenant_id=m.tenant_id)
               ) AS missing"""
        ).fetchone()
        if missing is None or missing["missing"]:
            raise RuntimeError("Lexical projection is incomplete; readiness remains false")
        state = conn.execute(
            """UPDATE braincore.lexical_projection_state SET ready=true,updated_at=now()
               WHERE singleton=true RETURNING ready,backfill_after_id,rows_processed"""
        ).fetchone()
        assert state is not None
        return {"scanned_items": 0, **state}


def backfill_projection(
    dsn: str, *, batch_size: int = 1000, max_batches: int | None = None
) -> dict[str, Any]:
    """Backfill until ready or the explicit batch limit; a later call resumes."""
    if not 1 <= batch_size <= 10_000:
        raise ValueError("batch_size must be between 1 and 10000")
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be positive")
    batches = 0
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        while True:
            result = backfill_batch(conn, batch_size=batch_size)
            batches += 1
            if result["ready"] or (max_batches is not None and batches >= max_batches):
                return {"batches": batches, **result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()
    result = backfill_projection(
        get_settings().database_url.get_secret_value(),
        batch_size=args.batch_size,
        max_batches=args.max_batches,
    )
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()

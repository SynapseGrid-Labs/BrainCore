"""Resumable compatibility lexical backfill; canonical memories remain authoritative."""

import argparse
import json
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import get_settings


def backfill_batch(
    conn: psycopg.Connection[dict[str, Any]], *, batch_size: int = 1000
) -> dict[str, Any]:
    """Copy one locked canonical-ID batch and atomically commit its checkpoint.

    The separate singleton gate prevents partially initialized compatibility
    vectors from appearing complete. Extension writers refresh after acquiring
    the same canonical lock, preserving corrections concurrent with this copy.
    Use an autocommit connection to commit each bounded batch independently.
    """
    if not 1 <= batch_size <= 10_000:
        raise ValueError("batch_size must be between 1 and 10000")
    with conn.transaction():
        state = conn.execute(
            """SELECT ready,backfill_after_id,rows_processed
               FROM braincore.legacy_lexical_projection_state
               WHERE singleton=true FOR UPDATE"""
        ).fetchone()
        if state is None:
            raise RuntimeError("Compatibility lexical state is missing; run migrations first")
        if state["ready"]:
            return {"scanned_items": 0, **state}
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
            conn.execute("SELECT braincore.refresh_legacy_lexical_projection(%s::uuid[])", (ids,))
            state = conn.execute(
                """UPDATE braincore.legacy_lexical_projection_state
                   SET backfill_after_id=%s,rows_processed=rows_processed+%s,updated_at=now()
                   WHERE singleton=true RETURNING ready,backfill_after_id,rows_processed""",
                (ids[-1], len(ids)),
            ).fetchone()
            assert state is not None
            return {"scanned_items": len(ids), **state}
        # One final explicit maintenance verification, never a request-path scan.
        missing = conn.execute(
            """SELECT EXISTS(
                 SELECT 1 FROM braincore.memories m
                 WHERE NOT EXISTS(SELECT 1 FROM braincore.memories_lexical l
                                  WHERE l.id=m.id AND l.tenant_id=m.tenant_id
                                    AND l.scope IS NOT NULL)
               ) AS missing"""
        ).fetchone()
        if missing is None or missing["missing"]:
            raise RuntimeError(
                "Compatibility lexical projection is incomplete; readiness remains false"
            )
        state = conn.execute(
            """UPDATE braincore.legacy_lexical_projection_state SET ready=true,updated_at=now()
               WHERE singleton=true RETURNING ready,backfill_after_id,rows_processed"""
        ).fetchone()
        assert state is not None
        return {"scanned_items": 0, **state}


def backfill_projection(
    dsn: str, *, batch_size: int = 1000, max_batches: int | None = None
) -> dict[str, Any]:
    """Backfill until ready or a bounded batch limit; subsequent calls resume."""
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

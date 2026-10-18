"""Prevent retained-source imports from replacing accepted native semantic writes.

Authority is captured atomically by the audit INSERT trigger. Imports require
READ COMMITTED: row locks and the subsequent lookup must have separate command
snapshots, including after waiting for an accepted writer. Source capture may
continue to use its independent repeatable-read connection.
"""

from collections.abc import Sequence
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

RELATIONS = frozenset(
    {
        "memories",
        "artifacts",
        "legacy_fact_evidence",
        "legacy_memory_support",
        "legacy_auxiliary_objects",
    }
)


def guard_import_rows(
    conn: psycopg.Connection[Any], relation: str, rows: Sequence[tuple[UUID, bytes]]
) -> set[UUID]:
    """Lock a physical batch, then return unchanged-source native-owned IDs.

    A changed source head raises before any caller upsert or map advancement.
    Advisory locks also serialize competing importers of newly mapped IDs.
    They do not substitute for the canonical row locks used by native writers.
    """
    if relation not in RELATIONS:
        raise ValueError("unsupported source authority relation")
    if not rows:
        return set()
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise ValueError("source authority guard requires an open transaction")
    if len(rows) > 5000 or len({item[0] for item in rows}) != len(rows):
        raise ValueError("invalid source authority batch")
    isolation = conn.execute("SHOW transaction_isolation").fetchone()
    if isolation is None or next(iter(isolation.values())) != "read committed":
        raise ValueError("source authority guard requires READ COMMITTED")
    ids = [item[0] for item in rows]
    # A materialized sorted input establishes the same order for every importer.
    conn.execute(
        "WITH ordered AS MATERIALIZED (SELECT unnest(%s::uuid[]) AS id ORDER BY id) "
        "SELECT pg_advisory_xact_lock(hashtextextended('braincore-source-native:'||id::text,0)) "
        "FROM ordered",
        (ids,),
    ).fetchall()
    key = "id" if relation in {"memories", "artifacts"} else "native_id"
    conn.execute(
        sql.SQL("SELECT {} FROM braincore.{} WHERE {}=ANY(%s) ORDER BY {} FOR UPDATE").format(
            sql.Identifier(key), sql.Identifier(relation), sql.Identifier(key), sql.Identifier(key)
        ),
        (ids,),
    ).fetchall()
    # Deliberately separate from the locking statement: a native writer that
    # committed while we waited is now visible to this READ COMMITTED snapshot.
    allowed = conn.execute(
        "SELECT id,braincore.source_import_allowed(%s,id,payload_hash) AS allowed "
        "FROM unnest(%s::uuid[],%s::bytea[]) AS batch(id,payload_hash)",
        (relation, ids, [item[1] for item in rows]),
    ).fetchall()
    return {item["id"] for item in allowed if not item["allowed"]}

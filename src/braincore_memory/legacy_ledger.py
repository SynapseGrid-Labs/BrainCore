"""Lossless bounded legacy copy and idempotent delta ledger.

This is the source-accounting layer of migration, not proof of native projection,
client compatibility or cutover readiness. All source payloads stay in protected
PostgreSQL storage. Receipts contain only counts and aggregate fingerprints.
"""

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .legacy_capture import (
    acknowledge_changes,
    payload_expression,
    pending_changes,
    source_columns,
    source_keys,
)

Record = tuple[str, str, str, bool, int, bool]


def register_source(
    target: psycopg.Connection[Any], source_id: UUID, schema: str, keys: dict[str, list[str]]
) -> None:
    with target.transaction():
        target.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (str(source_id),))
        existing = target.execute(
            "SELECT source_schema,key_mapping FROM braincore.legacy_sources WHERE id=%s",
            (source_id,),
        ).fetchone()
        if existing and (existing["source_schema"] != schema or existing["key_mapping"] != keys):
            raise ValueError("source identity changed")
        target.execute(
            """INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping)
                          VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
            (source_id, schema, json.dumps(keys)),
        )
    target.execute("""CREATE TEMP TABLE IF NOT EXISTS legacy_stage (
                      source_table text,record_key jsonb,payload jsonb,
                      deleted boolean,change_id bigint,update_head boolean)
                      ON COMMIT DELETE ROWS""")
    target.commit()


def stage_records(target: psycopg.Connection[Any], source_id: UUID, rows: Sequence[Record]) -> None:
    target.execute("TRUNCATE pg_temp.legacy_stage")
    with target.cursor().copy("COPY pg_temp.legacy_stage FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    target.execute(
        """INSERT INTO braincore.legacy_record_versions
                      (source_id,source_table,record_key,payload_hash,payload)
                      SELECT DISTINCT %s,source_table,record_key,
                             public.digest(payload::text,'sha256'),payload FROM pg_temp.legacy_stage
                      ON CONFLICT DO NOTHING""",
        (source_id,),
    )
    # Multiple changes to the same key in a batch collapse only the current head;
    # every distinct original version above remains immutable.
    target.execute(
        """INSERT INTO braincore.legacy_record_heads AS h
                      (source_id,source_table,record_key,payload_hash,deleted,last_change_id)
                      SELECT DISTINCT ON (source_table,record_key)
                        %s,source_table,record_key,public.digest(payload::text,'sha256'),
                        deleted,change_id FROM pg_temp.legacy_stage WHERE update_head
                      ORDER BY source_table,record_key,change_id DESC
                      ON CONFLICT (source_id,source_table,record_key) DO UPDATE
                      SET payload_hash=EXCLUDED.payload_hash,deleted=EXCLUDED.deleted,
                          last_change_id=EXCLUDED.last_change_id
                      WHERE EXCLUDED.last_change_id >= h.last_change_id""",
        (source_id,),
    )


def row_query(schema: str, table: str, keys: list[str], columns: list[str]) -> sql.Composed:
    key = sql.SQL("jsonb_build_object({})").format(
        sql.SQL(",").join(
            part for column in keys for part in (sql.Literal(column), sql.Identifier("s", column))
        )
    )
    return sql.SQL(
        "SELECT {key}::text AS record_key,({payload})::text AS payload "
        "FROM {table} s ORDER BY {key}"
    ).format(key=key, table=sql.Identifier(schema, table), payload=payload_expression("s", columns))


def copy_source(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    source_id: UUID,
    schema: str = "preserve",
    batch_size: int = 1000,
) -> dict[str, Any]:
    if not 1 <= batch_size <= 10000:
        raise ValueError("invalid copy batch size")
    counts: dict[str, int] = {}
    with source.transaction():
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        keys = source_keys(source, schema)
        all_columns = source_columns(source, schema)
        register_source(target, source_id, schema, keys)
        snapshot_row = source.execute("SELECT pg_current_snapshot()::text AS id").fetchone()
        assert snapshot_row is not None
        snapshot = snapshot_row["id"]
        run_id = uuid4()
        with target.transaction():
            target.execute(
                """INSERT INTO braincore.legacy_copy_runs(id,source_id,snapshot_id)
                              VALUES (%s,%s,%s)""",
                (run_id, source_id, snapshot),
            )
        for table, columns in keys.items():
            count = 0
            with source.cursor(name="copy_" + uuid4().hex, row_factory=dict_row) as cursor:
                cursor.execute(row_query(schema, table, columns, all_columns[table]))
                index = 0
                while batch := cursor.fetchmany(batch_size):
                    rows = [(table, r["record_key"], r["payload"], False, 0, True) for r in batch]
                    fingerprint = hashlib.sha256()
                    for r in batch:
                        fingerprint.update(r["payload"].encode())
                        fingerprint.update(b"\n")
                    with target.transaction():
                        stage_records(target, source_id, rows)
                        target.execute(
                            """INSERT INTO braincore.legacy_copy_batches
                                          (run_id,source_table,batch_index,row_count,batch_hash)
                                          VALUES (%s,%s,%s,%s,%s)""",
                            (run_id, table, index, len(batch), fingerprint.digest()),
                        )
                    index += 1
                    count += len(batch)
            counts[table] = count
        with target.transaction():
            target.execute(
                """UPDATE braincore.legacy_copy_runs
                              SET completed_at=now(),copied_rows=%s WHERE id=%s""",
                (sum(counts.values()), run_id),
            )
    return {
        "source_accounting_copy_gate": "PASS",
        "run_id": str(run_id),
        "tables": counts,
        "row_count": sum(counts.values()),
        "native_import_gate": "NOT RUN",
        "client_parity_gate": "NOT RUN",
    }


def replay_changes(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    source_id: UUID,
    batch_size: int = 1000,
) -> dict[str, Any]:
    applied = 0
    while batch := pending_changes(source, str(source_id), batch_size):
        rows: list[Record] = []
        with target.transaction():
            for change in batch:
                exists = target.execute(
                    """SELECT 1 FROM braincore.legacy_applied_changes
                                          WHERE source_id=%s AND change_id=%s""",
                    (source_id, change["id"]),
                ).fetchone()
                if exists:
                    continue
                if change["old_payload"] is not None:
                    tombstone = (
                        change["operation"] == "DELETE" or change["old_key"] != change["new_key"]
                    )
                    rows.append(
                        (
                            change["source_table"],
                            change["old_key"],
                            change["old_payload"],
                            tombstone,
                            change["id"],
                            tombstone,
                        )
                    )
                if change["new_payload"] is not None:
                    rows.append(
                        (
                            change["source_table"],
                            change["new_key"],
                            change["new_payload"],
                            False,
                            change["id"],
                            True,
                        )
                    )
                target.execute(
                    """INSERT INTO braincore.legacy_applied_changes(source_id,change_id)
                                  VALUES (%s,%s)""",
                    (source_id, change["id"]),
                )
            stage_records(target, source_id, rows)
        # A crash between target commit and this acknowledgement replays harmlessly.
        acknowledge_changes(source, str(source_id), [r["id"] for r in batch])
        applied += len(batch)
    return {
        "delta_ledger_gate": "PASS",
        "acknowledged_changes": applied,
        "final_writer_drain_gate": "NOT RUN",
    }


def reconcile(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    source_id: UUID,
    schema: str = "preserve",
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    with source.transaction(), target.transaction():
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        target.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        keys = source_keys(source, schema)
        all_columns = source_columns(source, schema)
        declared = target.execute(
            "SELECT key_mapping FROM braincore.legacy_sources WHERE id=%s", (source_id,)
        ).fetchone()
        if declared is None or declared["key_mapping"] != keys:
            raise ValueError("source table/key coverage changed")
        for table, columns in keys.items():
            hashes: list[str] = []
            counts: list[int] = []
            for is_source, conn in [(True, source), (False, target)]:
                digest = hashlib.sha256()
                count = 0
                with conn.cursor(name="verify_" + uuid4().hex, row_factory=dict_row) as cursor:
                    if is_source:
                        cursor.execute(row_query(schema, table, columns, all_columns[table]))
                    else:
                        cursor.execute(
                            """SELECT h.record_key::text AS record_key,v.payload::text AS payload
                                          FROM braincore.legacy_record_heads h
                                          JOIN braincore.legacy_record_versions v USING
                                            (source_id,source_table,record_key,payload_hash)
                                          WHERE h.source_id=%s AND h.source_table=%s
                                            AND NOT h.deleted
                                          ORDER BY h.record_key""",
                            (source_id, table),
                        )
                    while batch := cursor.fetchmany(1000):
                        for row in batch:
                            digest.update(row["record_key"].encode())
                            digest.update(b"\n")
                            digest.update(row["payload"].encode())
                            digest.update(b"\n")
                            count += 1
                hashes.append(digest.hexdigest())
                counts.append(count)
            results[table] = {
                "source_rows": counts[0],
                "target_rows": counts[1],
                "source_hash": hashes[0],
                "target_hash": hashes[1],
                "gate": "PASS" if hashes[0] == hashes[1] and counts[0] == counts[1] else "FAIL",
            }
    return {
        "source_accounting_gate": "PASS"
        if all(r["gate"] == "PASS" for r in results.values())
        else "FAIL",
        "tables": results,
        "table_count": len(results),
        "row_count": sum(r["source_rows"] for r in results.values()),
        "native_semantic_gate": "NOT RUN",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["copy", "replay", "reconcile"])
    parser.add_argument("--source-id", type=UUID, required=True)
    parser.add_argument("--schema", default="preserve")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source_url = os.environ.get("BRAINCORE_LEGACY_DATABASE_URL")
    target_url = os.environ.get("BRAINCORE_DATABASE_URL")
    if not source_url or not target_url or args.output.exists():
        parser.exit(2, "source/target configuration and a fresh output path are required\n")
    try:
        with (
            psycopg.connect(source_url, autocommit=True, row_factory=dict_row) as source,
            psycopg.connect(target_url, autocommit=True, row_factory=dict_row) as target,
        ):
            if args.action == "copy":
                report = copy_source(source, target, args.source_id, args.schema, args.batch_size)
            elif args.action == "replay":
                keys = source_keys(source, args.schema)
                register_source(target, args.source_id, args.schema, keys)
                report = replay_changes(source, target, args.source_id, args.batch_size)
            else:
                report = reconcile(source, target, args.source_id, args.schema)
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    except (psycopg.Error, ValueError, OSError):
        parser.exit(2, "ledger action failed; retained batches are safe to retry\n")
    print(json.dumps({k: v for k, v in report.items() if k.endswith("gate") or k == "row_count"}))
    if any(v == "FAIL" for k, v in report.items() if k.endswith("gate")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

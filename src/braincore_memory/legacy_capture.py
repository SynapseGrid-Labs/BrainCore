"""Privileged, opt-in durable source change capture for legacy migration.

Journal identifiers are allocation order, not commit watermarks. Consumers must
acknowledge each change independently after a durable idempotent target commit,
rescan unacknowledged changes, and drain all writers before final reconciliation.
The journal contains private source payloads; never export it into text receipts.
"""

import argparse
import hashlib
import json
import os
from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

CAPTURE_SCHEMA = "braincore_legacy_capture"


def source_keys(conn: psycopg.Connection[Any], schema: str) -> dict[str, list[str]]:
    rows = conn.execute(
        """SELECT t.tablename,array_agg(a.attname ORDER BY k.ordinality)
                  FILTER (WHERE a.attname IS NOT NULL) AS keys
           FROM pg_tables t JOIN pg_namespace n ON n.nspname=t.schemaname
           JOIN pg_class c ON c.relname=t.tablename AND c.relnamespace=n.oid
           LEFT JOIN pg_constraint p ON p.conrelid=c.oid AND p.contype='p'
           LEFT JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(attnum,ordinality) ON true
           LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum
           WHERE t.schemaname=%s GROUP BY t.tablename ORDER BY t.tablename""",
        (schema,),
    ).fetchall()
    if not rows or any(not row["keys"] for row in rows):
        raise ValueError("every source table requires a stable primary key")
    return {row["tablename"]: row["keys"] for row in rows}


NULL_COLUMNS_FIELD = "__braincore_sql_null_columns"


def source_columns(conn: psycopg.Connection[Any], schema: str) -> dict[str, list[str]]:
    rows = conn.execute(
        """SELECT table_name,column_name FROM information_schema.columns
           WHERE table_schema=%s ORDER BY table_name,ordinal_position""",
        (schema,),
    ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        if row["column_name"] == NULL_COLUMNS_FIELD:
            raise ValueError("source column conflicts with reserved migration metadata")
        result.setdefault(row["table_name"], []).append(row["column_name"])
    return result


def payload_expression(alias: str, columns: list[str]) -> sql.Composed:
    nulls = sql.SQL(",").join(
        sql.SQL("CASE WHEN {} IS NULL THEN {}::text END").format(
            sql.Identifier(alias, column), sql.Literal(column)
        )
        for column in columns
    )
    return sql.SQL(
        "to_jsonb({}) || jsonb_build_object({}, to_jsonb(array_remove(ARRAY[{}]::text[],NULL)))"
    ).format(sql.Identifier(alias), sql.Literal(NULL_COLUMNS_FIELD), nulls)


def install_capture(conn: psycopg.Connection[Any], schema: str = "preserve") -> dict[str, Any]:
    if schema in {CAPTURE_SCHEMA, "braincore", "pg_catalog", "information_schema"}:
        raise ValueError("invalid source schema")
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '5s'")
        keys = source_keys(conn, schema)
        # Lock all source writers atomically before the journal becomes authoritative.
        for table in keys:
            conn.execute(
                sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
                    sql.Identifier(schema, table)
                )
            )
        all_columns = source_columns(conn, schema)
        signature = hashlib.sha256(
            json.dumps({"keys": keys, "columns": all_columns}, sort_keys=True).encode()
        ).hexdigest()
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(CAPTURE_SCHEMA))
        )
        conn.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(CAPTURE_SCHEMA))
        )
        conn.execute("""CREATE TABLE IF NOT EXISTS braincore_legacy_capture.configuration (
                        singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                        source_schema text NOT NULL, key_signature text NOT NULL,
                        installed_at timestamptz NOT NULL DEFAULT now())""")
        conn.execute("""ALTER TABLE braincore_legacy_capture.configuration
                        ADD COLUMN IF NOT EXISTS format_version integer NOT NULL DEFAULT 1""")
        existing = conn.execute(
            "SELECT source_schema,key_signature,format_version "
            "FROM braincore_legacy_capture.configuration"
        ).fetchone()
        old_signature = hashlib.sha256(json.dumps(keys, sort_keys=True).encode()).hexdigest()
        legacy_empty = existing and existing["format_version"] == 1
        if existing and (
            existing["source_schema"] != schema
            or existing["key_signature"] != (old_signature if legacy_empty else signature)
        ):
            raise ValueError("source schema/key mapping changed; capture requires reviewed renewal")
        conn.execute("""CREATE TABLE IF NOT EXISTS braincore_legacy_capture.changes (
                        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        source_table text NOT NULL, operation text NOT NULL,
                        old_key jsonb, new_key jsonb, old_payload jsonb, new_payload jsonb,
                        transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
                        captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                        CHECK (operation IN ('INSERT','UPDATE','DELETE')))""")
        if (
            legacy_empty
            and conn.execute("SELECT 1 FROM braincore_legacy_capture.changes LIMIT 1").fetchone()
        ):
            raise ValueError(
                "old capture lacks SQL-null metadata; preserve it and establish a fresh source"
            )
        conn.execute("""CREATE TABLE IF NOT EXISTS braincore_legacy_capture.deliveries (
                        subscriber_id uuid NOT NULL, change_id bigint NOT NULL
                          REFERENCES braincore_legacy_capture.changes(id),
                        delivered_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (subscriber_id,change_id))""")
        conn.execute("""CREATE OR REPLACE FUNCTION braincore_legacy_capture.prevent_truncate()
                        RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
                        BEGIN
                          RAISE EXCEPTION 'truncate is incompatible with migration capture';
                        END;
                        $$""")
        for table, _columns in keys.items():
            relation = sql.Identifier(schema, table)
            function = sql.Identifier(
                CAPTURE_SCHEMA,
                "capture_row_" + hashlib.sha256((schema + "." + table).encode()).hexdigest()[:24],
            )
            before = payload_expression("old", all_columns[table])
            after = payload_expression("new", all_columns[table])
            before_key = sql.SQL("jsonb_build_object({})").format(
                sql.SQL(",").join(
                    part
                    for col in keys[table]
                    for part in (sql.Literal(col), sql.Identifier("old", col))
                )
            )
            after_key = sql.SQL("jsonb_build_object({})").format(
                sql.SQL(",").join(
                    part
                    for col in keys[table]
                    for part in (sql.Literal(col), sql.Identifier("new", col))
                )
            )
            body = sql.SQL("""DECLARE before_row jsonb; after_row jsonb;
                                     before_key jsonb; after_key jsonb;
                BEGIN
                  IF TG_TABLE_SCHEMA <> {schema} OR TG_TABLE_NAME <> {table} THEN
                    RAISE EXCEPTION 'capture source does not match configured table';
                  END IF;
                  IF TG_OP <> 'INSERT' THEN
                    before_row := {before}; before_key := {before_key};
                  END IF;
                  IF TG_OP <> 'DELETE' THEN
                    after_row := {after}; after_key := {after_key};
                  END IF;
                  INSERT INTO braincore_legacy_capture.changes
                    (source_table,operation,old_key,new_key,old_payload,new_payload)
                    VALUES (TG_TABLE_NAME,TG_OP,before_key,after_key,before_row,after_row);
                  RETURN NULL;
                END;""").format(
                schema=sql.Literal(schema),
                table=sql.Literal(table),
                before=before,
                after=after,
                before_key=before_key,
                after_key=after_key,
            )
            conn.execute(
                sql.SQL("""CREATE OR REPLACE FUNCTION {}() RETURNS trigger
                LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS {}""").format(
                    function, sql.Literal(body.as_string(conn))
                )
            )
            conn.execute(sql.SQL("REVOKE ALL ON FUNCTION {}() FROM PUBLIC").format(function))
            conn.execute(
                sql.SQL("DROP TRIGGER IF EXISTS braincore_capture_row ON {}").format(relation)
            )
            conn.execute(
                sql.SQL("DROP TRIGGER IF EXISTS braincore_capture_truncate ON {}").format(relation)
            )
            conn.execute(
                sql.SQL("""CREATE TRIGGER braincore_capture_row
                AFTER INSERT OR UPDATE OR DELETE ON {} FOR EACH ROW
                EXECUTE FUNCTION {}()""").format(relation, function)
            )
            conn.execute(
                sql.SQL("""CREATE TRIGGER braincore_capture_truncate
                BEFORE TRUNCATE ON {} FOR EACH STATEMENT
                EXECUTE FUNCTION braincore_legacy_capture.prevent_truncate()""").format(relation)
            )
        conn.execute(
            """INSERT INTO braincore_legacy_capture.configuration
                        (source_schema,key_signature,format_version)
                        VALUES (%s,%s,2) ON CONFLICT (singleton) DO UPDATE
                        SET key_signature=EXCLUDED.key_signature,format_version=2""",
            (schema, signature),
        )
    return {
        "capture_gate": "PASS",
        "table_count": len(keys),
        "key_signature": signature,
        "ordering": "per-change acknowledgement; identifiers are not commit watermarks",
    }


def pending_changes(
    conn: psycopg.Connection[Any], subscriber_id: str, limit: int = 1000
) -> list[dict[str, Any]]:
    if not 1 <= limit <= 10000:
        raise ValueError("invalid capture batch limit")
    return list(
        conn.execute(
            """SELECT c.id,c.source_table,c.operation,c.old_key::text,c.new_key::text,
                  c.old_payload::text,c.new_payload::text
           FROM braincore_legacy_capture.changes c
           WHERE NOT EXISTS (SELECT 1 FROM braincore_legacy_capture.deliveries d
                             WHERE d.subscriber_id=%s AND d.change_id=c.id)
           ORDER BY c.id LIMIT %s""",
            (subscriber_id, limit),
        ).fetchall()
    )


def committed_transaction_page_query(
    transaction_ids: list[str], *, after_id: int | None, page_size: int
) -> tuple[str, tuple[Any, ...]]:
    """Return a bounded keyset page of complete selected transactions.

    A page limit bounds each payload fetch without truncating the transaction:
    the caller continues until the immutable selected set is exhausted in its
    READ ONLY REPEATABLE READ snapshot. Ordering and page boundaries match the
    previous globally ordered stream, preserving durable resume receipts.
    """
    if not 1 <= page_size <= 1000:
        raise ValueError("capture page limit must be between 1 and 1000")
    if len(transaction_ids) > 1000:
        raise ValueError("capture transaction selection must not exceed 1000")
    # xid8 casts numeric aliases (for example "1" and "01") to the same
    # transaction. Normalize unsigned ASCII decimal input before deduplicating
    # streams so the former ANY predicate's set semantics are preserved.
    normalized_transactions: list[str] = []
    for transaction_id in transaction_ids:
        if (
            not isinstance(transaction_id, str)
            or not transaction_id.isascii()
            or not transaction_id.isdecimal()
            or int(transaction_id) >= 2**64
        ):
            raise ValueError("capture transaction IDs must be unsigned xid8 decimal strings")
        normalized_transactions.append(str(int(transaction_id)))
    transaction_ids = list(dict.fromkeys(normalized_transactions))
    if len(transaction_ids) > 1:
        branches: list[str] = []
        merge_parameters: list[Any] = []
        for transaction_id in transaction_ids:
            predicate = "transaction_id=%s::xid8"
            merge_parameters.append(transaction_id)
            if after_id is not None:
                predicate += " AND id>%s"
                merge_parameters.append(after_id)
            branches.append(
                sql.SQL(
                    "(SELECT id FROM braincore_legacy_capture.changes WHERE {} "
                    "ORDER BY id LIMIT %s)"
                )
                .format(sql.SQL(predicate))
                .as_string()
            )
            merge_parameters.append(page_size)
        merge_parameters.extend((page_size, page_size))
        # Each equality stream is ordered by the transaction/id index. A
        # Merge Append can take the global first page without sorting all
        # selected events or scanning unrelated IDs in the primary index.
        # Merge IDs first so private payloads are fetched only for the final
        # bounded page. OFFSET 0 retains an ordered per-ID lateral lookup.
        return (
            sql.SQL(
                "SELECT c.id,c.source_table,c.operation,c.old_key::text,c.new_key::text,"
                "c.old_payload::text,c.new_payload::text FROM (SELECT id FROM ({}) "
                "selected ORDER BY id LIMIT %s) selected_ids "
                "CROSS JOIN LATERAL (SELECT c.id,c.source_table,c.operation,c.old_key,c.new_key,"
                "c.old_payload,c.new_payload FROM braincore_legacy_capture.changes c "
                "WHERE c.id=selected_ids.id OFFSET 0) c ORDER BY selected_ids.id LIMIT %s"
            )
            .format(sql.SQL(" UNION ALL ").join(sql.SQL(branch) for branch in branches))
            .as_string(),
            tuple(merge_parameters),
        )
    if len(transaction_ids) == 1:
        # Equality lets (transaction_id, id) provide event ordering even when
        # a later page reaches a sparse tail. ANY loses that leading constant.
        predicate = "c.transaction_id=%s::xid8"
        selected: str | list[str] = transaction_ids[0]
    else:
        predicate = "c.transaction_id=ANY(%s::xid8[])"
        selected = transaction_ids
    parameters: tuple[Any, ...]
    if after_id is None:
        parameters = (selected, page_size)
    else:
        predicate += " AND c.id>%s"
        parameters = (selected, after_id, page_size)
    return (
        "SELECT c.id,c.source_table,c.operation,c.old_key::text,c.new_key::text,"
        "c.old_payload::text,c.new_payload::text "
        "FROM braincore_legacy_capture.changes c "
        "WHERE " + predicate + " ORDER BY c.id LIMIT %s",
        parameters,
    )


def stage_committed_transactions(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    source_id: UUID,
    subscriber_id: str,
    *,
    schema: str = "preserve",
    selection_limit: int = 1000,
    page_size: int = 1000,
    run_id: UUID | None = None,
    page_receipt: Callable[[dict[str, Any]], None] | None = None,
    page_query_guard: Callable[[str, tuple[Any, ...]], None] | None = None,
) -> dict[str, Any]:
    """Durably stage complete committed transactions without acknowledging them.

    The caller owns an active READ ONLY REPEATABLE READ source snapshot. At most
    selection_limit oldest eligible event IDs select transactions; a selected
    transaction may contain any number of events. Keyset pages bound payload
    memory to page_size events, and each ledger page commits with its receipt.
    No native mapping or source acknowledgement occurs here. Callers must wait
    for the PASS return before mapping, verify destination correctness, and only
    then acknowledge event IDs. An interrupted run resumes using the same run_id.

    Receipts live in existing copy-run/batch tables. The callback receives only
    event IDs and changed keys after each durable page, including verified pages
    on resume; protected spool storage and deduplication belong to the caller.
    Optional page_query_guard runs before every exact unprepared page query,
    including the terminal empty query; a raised error leaves source ACKs
    untouched and retains already durable target pages for the same run_id.
    Previously acknowledged events in a selected transaction are included to
    preserve full transaction staging for subscribers with an older partial ACK.
    """
    # Import lazily: the ledger also imports capture primitives.
    from .legacy_ledger import register_source, stage_records

    if not 1 <= selection_limit <= 1000 or not 1 <= page_size <= 1000:
        raise ValueError("capture selection and page limits must be between 1 and 1000")
    if source is target or source.info.transaction_status != TransactionStatus.INTRANS:
        raise ValueError("an active independent source snapshot is required")
    if target.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("target must have no active transaction")
    isolation = source.execute("SHOW transaction_isolation").fetchone()
    readonly = source.execute("SHOW transaction_read_only").fetchone()
    if (
        isolation is None
        or readonly is None
        or isolation["transaction_isolation"] != "repeatable read"
        or readonly["transaction_read_only"] != "on"
    ):
        raise ValueError("source snapshot must be READ ONLY REPEATABLE READ")
    subscriber = str(UUID(subscriber_id))
    keys = source_keys(source, schema)
    columns = source_columns(source, schema)
    capture = source.execute(
        "SELECT source_schema,key_signature,format_version "
        "FROM braincore_legacy_capture.configuration"
    ).fetchone()
    signature = hashlib.sha256(
        json.dumps({"keys": keys, "columns": columns}, sort_keys=True).encode()
    ).hexdigest()
    if not capture or (
        capture["source_schema"] != schema
        or capture["format_version"] != 2
        or capture["key_signature"] != signature
    ):
        raise ValueError("capture source schema, format or signature changed")
    with target.transaction():
        declared = target.execute(
            "SELECT source_schema,key_mapping FROM braincore.legacy_sources WHERE id=%s",
            (source_id,),
        ).fetchone()
    if not declared or declared["source_schema"] != schema or declared["key_mapping"] != keys:
        raise ValueError("registered target source identity or key mapping changed")
    register_source(target, source_id, schema, keys)
    run = run_id or uuid4()
    snapshot_row = source.execute("SELECT pg_current_snapshot()::text AS snapshot").fetchone()
    if snapshot_row is None:
        raise RuntimeError("source snapshot unavailable")
    snapshot = snapshot_row["snapshot"]
    marker = "__capture_events__"
    with target.transaction():
        existing = target.execute(
            "SELECT source_id,snapshot_id,completed_at FROM braincore.legacy_copy_runs "
            "WHERE id=%s FOR UPDATE",
            (run,),
        ).fetchone()
        if existing is None:
            selected = source.execute(
                """SELECT DISTINCT transaction_id::text AS xid FROM (
                     SELECT c.transaction_id FROM braincore_legacy_capture.changes c
                     WHERE NOT EXISTS(SELECT 1 FROM braincore_legacy_capture.deliveries d
                       WHERE d.subscriber_id=%s::uuid AND d.change_id=c.id)
                     ORDER BY c.id LIMIT %s
                   ) eligible""",
                (subscriber, selection_limit),
            ).fetchall()
            header = {
                "kind": "committed-capture-staging-v1",
                "subscriber_id": subscriber,
                "snapshot": snapshot,
                "transaction_ids": [row["xid"] for row in selected],
                "page_size": page_size,
            }
            target.execute(
                """INSERT INTO braincore.legacy_copy_runs(id,source_id,snapshot_id)
                   VALUES(%s,%s,%s) ON CONFLICT(id) DO NOTHING""",
                (run, source_id, json.dumps(header, sort_keys=True)),
            )
            existing = target.execute(
                "SELECT source_id,snapshot_id,completed_at FROM braincore.legacy_copy_runs "
                "WHERE id=%s FOR UPDATE",
                (run,),
            ).fetchone()
        assert existing is not None
        try:
            header = json.loads(existing["snapshot_id"])
        except (ValueError, TypeError) as error:
            raise ValueError("run is not a committed capture staging receipt") from error
        if existing["source_id"] != source_id or (
            not isinstance(header, dict)
            or header.get("kind") != "committed-capture-staging-v1"
            or header.get("subscriber_id") != subscriber
            or header.get("page_size") != page_size
            or not isinstance(header.get("transaction_ids"), list)
            or len(header["transaction_ids"]) > 1000
        ):
            raise ValueError("capture staging run identity or page size changed")
        transactions = header["transaction_ids"]
        previously_complete = existing["completed_at"] is not None
    events = pages = 0
    after_id: int | None = None
    while True:
        expansion, parameters = committed_transaction_page_query(
            transactions, after_id=after_id, page_size=page_size
        )
        if page_query_guard is not None:
            page_query_guard(expansion, parameters)
        # Match a caller's EXPLAIN with the same custom parameter plan. A
        # prepared generic plan could otherwise differ after several pages.
        batch = source.execute(expansion, parameters, prepare=False).fetchall()
        if not batch:
            break
        rows: list[tuple[str, str, str, bool, int, bool]] = []
        heads: set[tuple[str, str]] = set()
        digest = hashlib.sha256()
        for event in batch:
            digest.update(json.dumps(event, sort_keys=True).encode())
            digest.update(b"\n")
            for prefix in ("old", "new"):
                key, payload = event[prefix + "_key"], event[prefix + "_payload"]
                if key is None:
                    continue
                if payload is None:
                    raise ValueError("capture key has no source payload")
                tombstone = prefix == "old" and (
                    event["operation"] == "DELETE" or event["old_key"] != event["new_key"]
                )
                rows.append(
                    (
                        event["source_table"],
                        key,
                        payload,
                        tombstone,
                        event["id"],
                        prefix == "new" or tombstone,
                    )
                )
                heads.add((event["source_table"], key))
        with target.transaction():
            # Serialize concurrent resumes; ledger writes and receipt are atomic.
            target.execute(
                "SELECT id FROM braincore.legacy_copy_runs WHERE id=%s FOR UPDATE",
                (run,),
            )
            receipt = target.execute(
                """SELECT row_count,batch_hash FROM braincore.legacy_copy_batches
                   WHERE run_id=%s AND source_table=%s AND batch_index=%s""",
                (run, marker, pages),
            ).fetchone()
            if receipt is not None:
                if receipt["row_count"] != len(batch) or bytes(receipt["batch_hash"]) != (
                    digest.digest()
                ):
                    raise ValueError("committed capture page no longer matches its receipt")
            else:
                if previously_complete:
                    raise ValueError("completed capture run has unexpected events")
                stage_records(target, source_id, rows)
                target.execute(
                    """INSERT INTO braincore.legacy_copy_batches
                       (run_id,source_table,batch_index,row_count,batch_hash)
                       VALUES(%s,%s,%s,%s,%s)""",
                    (run, marker, pages, len(batch), digest.digest()),
                )
                target.execute(
                    "UPDATE braincore.legacy_copy_runs SET copied_rows=copied_rows+%s WHERE id=%s",
                    (len(batch), run),
                )
        if page_receipt is not None:
            page_receipt(
                {
                    "run_id": str(run),
                    "page_index": pages,
                    "event_ids": [event["id"] for event in batch],
                    "changed_heads": [
                        {"source_table": table, "key": key} for table, key in sorted(heads)
                    ],
                }
            )
        events += len(batch)
        pages += 1
        after_id = batch[-1]["id"]
        if len(batch) < page_size:
            # LIMIT exhausted the immutable source snapshot. Avoid rescanning
            # a sparse tail solely to rediscover an already proven empty page.
            break
    with target.transaction():
        durable = target.execute(
            "SELECT copied_rows FROM braincore.legacy_copy_runs WHERE id=%s FOR UPDATE",
            (run,),
        ).fetchone()
        receipts = target.execute(
            """SELECT count(*) AS pages,coalesce(sum(row_count),0) AS events
               FROM braincore.legacy_copy_batches WHERE run_id=%s AND source_table=%s""",
            (run, marker),
        ).fetchone()
        if (
            durable is None
            or durable["copied_rows"] != events
            or receipts != {"pages": pages, "events": events}
        ):
            raise ValueError("capture run is incomplete or receipt accounting changed")
        target.execute(
            "UPDATE braincore.legacy_copy_runs SET completed_at=coalesce(completed_at,now()) "
            "WHERE id=%s",
            (run,),
        )
    return {
        "delta_staging_gate": "PASS",
        "run_id": str(run),
        "events": events,
        "pages": pages,
        "transaction_count": len(transactions),
        "snapshot": snapshot,
        "initial_snapshot": header["snapshot"],
        "source_capture_acknowledged": False,
        "native_import_gate": "NOT RUN",
    }


def acknowledge_changes(conn: psycopg.Connection[Any], subscriber_id: str, ids: list[int]) -> None:
    # The caller must commit the idempotent target ledger before calling this function.
    with conn.transaction():
        # Preserve explicit per-event delivery and one atomic ACK transaction.
        # Bound parameter/query size without issuing a round trip for every event.
        for offset in range(0, len(ids), 1000):
            conn.execute(
                """INSERT INTO braincore_legacy_capture.deliveries(subscriber_id,change_id)
                   SELECT %s::uuid,change_id FROM unnest(%s::bigint[]) AS events(change_id)
                   ON CONFLICT DO NOTHING""",
                (subscriber_id, ids[offset : offset + 1000]),
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="preserve")
    parser.add_argument("--acknowledge-source-schema-change", action="store_true")
    args = parser.parse_args()
    if not args.acknowledge_source_schema_change:
        parser.exit(2, "explicit source-schema acknowledgement is required\n")
    source = os.environ.get("BRAINCORE_LEGACY_DATABASE_URL")
    if not source:
        parser.exit(2, "BRAINCORE_LEGACY_DATABASE_URL is required\n")
    try:
        with psycopg.connect(source, autocommit=True, row_factory=dict_row) as conn:
            report = install_capture(conn, args.schema)
    except (psycopg.Error, ValueError):
        parser.exit(2, "change capture installation failed; no partial installation committed\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

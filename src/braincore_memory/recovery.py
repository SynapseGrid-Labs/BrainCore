"""Privileged transactional recovery journal for compatible successor recovery.

Install with writers drained, then commit before enabling any accepted writes.
Recovery retains the canonical database with a compatible application; this journal
alone does not make an older schema or application safe. Payloads are private.
IDs are allocation order and must never be interpreted as commit watermarks.
"""

import argparse
import hashlib
import json
import os
from typing import Any

import psycopg
from psycopg import sql

from braincore_memory.legacy_capture import payload_expression, source_columns, source_keys

RECOVERY_SCHEMA = "braincore_recovery"


def install_recovery(conn: psycopg.Connection[Any], schemas: list[str]) -> dict[str, Any]:
    """Atomically register all base tables; fail closed on changed table definitions."""
    if (
        not schemas
        or len(set(schemas)) != len(schemas)
        or any(name in {RECOVERY_SCHEMA, "pg_catalog", "information_schema"} for name in schemas)
    ):
        raise ValueError("invalid recovery source schemas")
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='5s'")
        mappings = {}
        for schema in sorted(schemas):
            keys = source_keys(conn, schema)
            for table in keys:
                conn.execute(
                    sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
                        sql.Identifier(schema, table)
                    )
                )
            mappings[schema] = {"keys": keys, "columns": source_columns(conn, schema)}
        signature = hashlib.sha256(json.dumps(mappings, sort_keys=True).encode()).hexdigest()
        conn.execute("CREATE SCHEMA IF NOT EXISTS braincore_recovery")
        conn.execute("REVOKE ALL ON SCHEMA braincore_recovery FROM PUBLIC")
        conn.execute("""CREATE TABLE IF NOT EXISTS braincore_recovery.configuration (
            singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
            signature text NOT NULL, installed_at timestamptz NOT NULL DEFAULT now())""")
        existing = conn.execute("SELECT signature FROM braincore_recovery.configuration").fetchone()
        if existing and existing["signature"] != signature:
            raise ValueError("recovery table definitions changed; drain writers and review renewal")
        conn.execute("""CREATE TABLE IF NOT EXISTS braincore_recovery.changes (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_schema text NOT NULL, source_table text NOT NULL,
            operation text NOT NULL CHECK(operation IN ('INSERT','UPDATE','DELETE')),
            old_payload jsonb, new_payload jsonb,
            transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
            captured_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
        conn.execute("""CREATE OR REPLACE FUNCTION braincore_recovery.reject_removal()
            RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
            BEGIN RAISE EXCEPTION 'captured data cannot be truncated'; END;$$""")
        conn.execute("""DROP TRIGGER IF EXISTS recovery_immutable ON braincore_recovery.changes""")
        conn.execute("""CREATE TRIGGER recovery_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
            ON braincore_recovery.changes FOR EACH STATEMENT
            EXECUTE FUNCTION braincore_recovery.reject_removal()""")
        for schema, mapping in mappings.items():
            for table in mapping["keys"]:
                relation = sql.Identifier(schema, table)
                function = sql.Identifier(
                    RECOVERY_SCHEMA,
                    "capture_" + hashlib.sha256((schema + "." + table).encode()).hexdigest()[:24],
                )
                body = sql.SQL("""DECLARE before_row jsonb; after_row jsonb;
                    BEGIN
                    IF current_setting('synchronous_commit') = 'off' THEN
                      RAISE EXCEPTION 'accepted recovery writes require durable commit'; END IF;
                    IF TG_TABLE_SCHEMA <> {schema} OR TG_TABLE_NAME <> {table} THEN
                      RAISE EXCEPTION 'invalid recovery trigger relation'; END IF;
                    IF TG_OP <> 'INSERT' THEN before_row := {before}; END IF;
                    IF TG_OP <> 'DELETE' THEN after_row := {after}; END IF;
                    INSERT INTO braincore_recovery.changes
                      (source_schema,source_table,operation,old_payload,new_payload)
                      VALUES(TG_TABLE_SCHEMA,TG_TABLE_NAME,TG_OP,before_row,after_row);
                    RETURN NULL; END;""").format(
                    schema=sql.Literal(schema),
                    table=sql.Literal(table),
                    before=payload_expression("old", mapping["columns"][table]),
                    after=payload_expression("new", mapping["columns"][table]),
                )
                conn.execute(
                    sql.SQL("""CREATE OR REPLACE FUNCTION {}() RETURNS trigger
                    LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS {}""").format(
                        function, sql.Literal(body.as_string(conn))
                    )
                )
                conn.execute(sql.SQL("REVOKE ALL ON FUNCTION {}() FROM PUBLIC").format(function))
                for name in ("braincore_recovery_row", "braincore_recovery_truncate"):
                    conn.execute(
                        sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                            sql.Identifier(name), relation
                        )
                    )
                conn.execute(
                    sql.SQL("""CREATE TRIGGER braincore_recovery_row
                    AFTER INSERT OR UPDATE OR DELETE ON {} FOR EACH ROW
                    EXECUTE FUNCTION {}()""").format(relation, function)
                )
                conn.execute(
                    sql.SQL("""CREATE TRIGGER braincore_recovery_truncate
                    BEFORE TRUNCATE ON {} FOR EACH STATEMENT
                    EXECUTE FUNCTION braincore_recovery.reject_removal()""").format(relation)
                )
        conn.execute(
            """INSERT INTO braincore_recovery.configuration(signature) VALUES(%s)
            ON CONFLICT(singleton) DO NOTHING""",
            (signature,),
        )
    return {
        "gate": "PASS",
        "table_count": sum(len(m["keys"]) for m in mappings.values()),
        "signature": signature,
        "recovery_policy": "retain compatible canonical database",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", action="append", required=True)
    parser.add_argument("--acknowledge-writers-drained", action="store_true")
    args = parser.parse_args()
    if not args.acknowledge_writers_drained:
        parser.exit(2, "writer drain acknowledgement is required before installation\n")
    database = os.environ.get("BRAINCORE_DATABASE_URL")
    if not database:
        parser.exit(2, "BRAINCORE_DATABASE_URL is required\n")
    from psycopg.rows import dict_row

    try:
        with psycopg.connect(database, autocommit=True, row_factory=dict_row) as conn:
            report = install_recovery(conn, args.schema)
    except (psycopg.Error, ValueError):
        parser.exit(2, "recovery installation failed; no partial installation committed\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

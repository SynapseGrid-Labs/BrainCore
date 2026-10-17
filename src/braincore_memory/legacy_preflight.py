"""Read-only legacy ownership preflight; this is not a data converter.

The report contains schema names and aggregate counts, never source payloads,
credentials, or object identifiers. Unowned records and conflicting ownership
require an explicit reviewed mapping before a converter can promote them.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


def inspect_source(conn: psycopg.Connection[Any], schema: str = "preserve") -> dict[str, Any]:
    """Inspect one consistent read-only snapshot, including composite FK edges."""
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '30s'")
        tables = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=%s ORDER BY tablename",
            (schema,),
        ).fetchall()
        inventory: dict[str, Any] = {}
        blockers: list[dict[str, Any]] = []
        for table_row in tables:
            table = table_row["tablename"]
            columns = conn.execute(
                """SELECT column_name,udt_schema,udt_name,is_nullable
                   FROM information_schema.columns
                   WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
                (schema, table),
            ).fetchall()
            relation = sql.Identifier(schema, table)
            cursor = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(relation))
            row = cursor.fetchone()
            assert row is not None
            row_count = row["n"]
            column_names = {column["column_name"] for column in columns}
            owner_column = next((x for x in ("tenant", "tenant_id") if x in column_names), None)
            item: dict[str, Any] = {
                "columns": columns,
                "rows": row_count,
                "owner_column": owner_column,
            }
            if owner_column:
                # Counts only: even tenant names are excluded from the exported report.
                ownership = conn.execute(
                    sql.SQL(
                        "SELECT count(DISTINCT {owner}) AS owners, "
                        "count(*) FILTER (WHERE {owner} IS NULL OR "
                        "btrim({owner}::text)='') AS missing FROM {table}"
                    ).format(owner=sql.Identifier(owner_column), table=relation)
                ).fetchone()
                assert ownership is not None
                item["owner_count"] = ownership["owners"]
                item["missing_owner_rows"] = ownership["missing"]
                if ownership["missing"]:
                    blockers.append(
                        {"kind": "missing_owner", "table": table, "rows": ownership["missing"]}
                    )
            elif row_count:
                blockers.append(
                    {"kind": "ownership_mapping_required", "table": table, "rows": row_count}
                )
            inventory[table] = item
        edges = conn.execute(
            """SELECT child.relname AS child_table,parent.relname AS parent_table,
                      fk.conname AS constraint_name,
                      array_agg(ca.attname ORDER BY keys.ordinality) AS child_columns,
                      array_agg(pa.attname ORDER BY keys.ordinality) AS parent_columns
               FROM pg_constraint fk
               JOIN pg_class child ON child.oid=fk.conrelid
               JOIN pg_class parent ON parent.oid=fk.confrelid
               JOIN pg_namespace ns ON ns.oid=child.relnamespace
               JOIN pg_namespace pn ON pn.oid=parent.relnamespace
               CROSS JOIN LATERAL unnest(fk.conkey,fk.confkey)
                   WITH ORDINALITY AS keys(child_key,parent_key,ordinality)
               JOIN pg_attribute ca ON ca.attrelid=child.oid AND ca.attnum=keys.child_key
               JOIN pg_attribute pa ON pa.attrelid=parent.oid AND pa.attnum=keys.parent_key
               WHERE fk.contype='f' AND ns.nspname=%s AND pn.nspname=%s
               GROUP BY child.relname,parent.relname,fk.conname ORDER BY 1,2,3""",
            (schema, schema),
        ).fetchall()
        for edge in edges:
            child = inventory[edge["child_table"]]
            parent = inventory[edge["parent_table"]]
            if not child["owner_column"] or not parent["owner_column"]:
                continue
            join = sql.SQL(" AND ").join(
                sql.SQL("s.{}=t.{}").format(sql.Identifier(a), sql.Identifier(b))
                for a, b in zip(edge["child_columns"], edge["parent_columns"], strict=True)
            )
            conflict_row = conn.execute(
                sql.SQL(
                    "SELECT count(*) AS n FROM {child} s JOIN {parent} t ON {join} "
                    "WHERE s.{child_owner}::text IS DISTINCT FROM t.{parent_owner}::text"
                ).format(
                    child=sql.Identifier(schema, edge["child_table"]),
                    parent=sql.Identifier(schema, edge["parent_table"]),
                    join=join,
                    child_owner=sql.Identifier(child["owner_column"]),
                    parent_owner=sql.Identifier(parent["owner_column"]),
                )
            ).fetchone()
            assert conflict_row is not None
            conflict_count = conflict_row["n"]
            edge["ownership_conflicts"] = conflict_count
            if conflict_count:
                blockers.append(
                    {"kind": "cross_owner_relationship", **edge, "rows": conflict_count}
                )
        if not inventory:
            blockers.append({"kind": "missing_source_schema"})
        canonical = json.dumps({"tables": inventory, "relationships": edges}, sort_keys=True)
        return {
            "format_version": 1,
            "source_schema": schema,
            "schema_inventory_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
            "tables": inventory,
            "relationships": edges,
            "table_count": len(inventory),
            "row_count": sum(table["rows"] for table in inventory.values()),
            "blockers": blockers,
            "ownership_gate": "FAIL" if blockers else "PASS",
            "migration_gate": "NOT RUN",
            "limitations": [
                "Ownership mapping is required for populated tables without tenant columns.",
                "This preflight does not import records or authorize cross-tenant evidence.",
                "The inventory fingerprint is not a per-object payload reconciliation hash.",
                "Client parity, scoped visibility, delta capture and recovery are separate gates.",
            ],
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="preserve")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = os.environ.get("BRAINCORE_LEGACY_DATABASE_URL")
    if not source:
        parser.exit(2, "BRAINCORE_LEGACY_DATABASE_URL is required\n")
    if args.output.exists():
        parser.exit(2, "output already exists; choose a new receipt path\n")
    try:
        with psycopg.connect(source, row_factory=dict_row, autocommit=True) as conn:
            report = inspect_source(conn, args.schema)
        # Refuse to overwrite an existing receipt even if another process creates it.
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2, sort_keys=True)
            output.write("\n")
    except (psycopg.Error, OSError):
        parser.exit(2, "preflight failed; no migration was performed\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("table_count", "row_count", "ownership_gate", "migration_gate")
            }
        )
    )
    raise SystemExit(1 if report["blockers"] else 0)


if __name__ == "__main__":
    main()

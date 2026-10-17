import json
import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory.config import get_settings
from braincore_memory.legacy_preflight import inspect_source


@pytest.fixture
def legacy_source() -> Iterator[tuple[psycopg.Connection[dict[str, object]], str]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires the disposable integration database")
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row, autocommit=True
    ) as conn:
        database = conn.execute("SELECT current_database() AS name").fetchone()
        assert database is not None and database["name"] == "braincore_test"
        schema = "legacy_preflight_" + uuid4().hex
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            yield conn, schema
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize("child_owner,conflicts", [("alpha", 0), ("beta", 1)])
def test_composite_relationship_ownership_and_redaction(
    legacy_source: tuple[psycopg.Connection[dict[str, object]], str],
    child_owner: str,
    conflicts: int,
) -> None:
    conn, schema = legacy_source
    conn.execute(
        sql.SQL(
            "CREATE TABLE {} (group_id int, id int, tenant text, payload text, "
            "PRIMARY KEY(group_id,id))"
        ).format(sql.Identifier(schema, "artifact"))
    )
    conn.execute(
        sql.SQL(
            "CREATE TABLE {} (id int PRIMARY KEY, parent_group int, parent_id int, "
            "tenant text, FOREIGN KEY(parent_group,parent_id) REFERENCES {}(group_id,id))"
        ).format(sql.Identifier(schema, "segment"), sql.Identifier(schema, "artifact"))
    )
    conn.execute(
        sql.SQL(
            "INSERT INTO {} VALUES (1,1,'alpha','sensitive-payload'),(2,1,'beta','other-payload')"
        ).format(sql.Identifier(schema, "artifact"))
    )
    conn.execute(
        sql.SQL("INSERT INTO {} VALUES (1,1,1,%s)").format(sql.Identifier(schema, "segment")),
        (child_owner,),
    )
    report = inspect_source(conn, schema)
    assert report["table_count"] == 2
    assert report["row_count"] == 3
    assert report["relationships"][0]["ownership_conflicts"] == conflicts
    assert report["ownership_gate"] == ("FAIL" if conflicts else "PASS")
    serialized = json.dumps(report)
    assert "sensitive-payload" not in serialized
    assert '"alpha"' not in serialized and '"beta"' not in serialized
    # Read-only inspection changes neither rows nor the caller's session policy.
    assert (
        conn.execute(
            sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(schema, "segment"))
        ).fetchone()["n"]
        == 1
    )
    assert conn.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"] == "off"


def test_missing_and_inherited_owners_block_without_inventing_tenant(
    legacy_source: tuple[psycopg.Connection[dict[str, object]], str],
) -> None:
    conn, schema = legacy_source
    conn.execute(
        sql.SQL("CREATE TABLE {} (id int,tenant text)").format(sql.Identifier(schema, "memory"))
    )
    conn.execute(
        sql.SQL("INSERT INTO {} VALUES (1,NULL),(2,'')").format(sql.Identifier(schema, "memory"))
    )
    conn.execute(
        sql.SQL("CREATE TABLE {} (id int,payload text)").format(sql.Identifier(schema, "audit"))
    )
    conn.execute(
        sql.SQL("INSERT INTO {} VALUES (1,'private-audit-event')").format(
            sql.Identifier(schema, "audit")
        )
    )
    report = inspect_source(conn, schema)
    assert report["ownership_gate"] == "FAIL"
    assert {b["kind"] for b in report["blockers"]} == {
        "missing_owner",
        "ownership_mapping_required",
    }
    assert report["tables"]["memory"]["missing_owner_rows"] == 2
    assert "private-audit-event" not in json.dumps(report)
    assert report["migration_gate"] == "NOT RUN"


def test_absent_or_untrusted_schema_never_becomes_success(
    legacy_source: tuple[psycopg.Connection[dict[str, object]], str],
) -> None:
    conn, schema = legacy_source
    report = inspect_source(conn, schema + "'; DROP SCHEMA public CASCADE; --")
    assert report["ownership_gate"] == "FAIL"
    assert report["blockers"] == [{"kind": "missing_source_schema"}]
    assert conn.execute("SELECT to_regnamespace('public') AS ns").fetchone()["ns"] is not None

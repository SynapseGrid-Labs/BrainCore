from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from braincore_memory.config import get_settings
from braincore_memory.recovery import RECOVERY_SCHEMA, install_recovery


@pytest.fixture(scope="session")
def isolated_database() -> None:
    # These tests use the disposable database's isolated capture schema only;
    # do not reapply unrelated migrations or truncate canonical tenant state.
    pass


def test_recovery_is_atomic_private_immutable_and_preserves_sql_null(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    try:
        report = install_recovery(c, [schema])
        table = sql.Identifier(schema, "records")
        c.execute(sql.SQL("INSERT INTO {} VALUES(1,1,NULL)").format(table))
        with pytest.raises(ValueError), c.transaction():
            c.execute(sql.SQL("UPDATE {} SET payload='aborted'").format(table))
            raise ValueError("aborted")
        c.execute(sql.SQL("UPDATE {} SET payload='accepted'").format(table))
        assert install_recovery(c, [schema]) == report
        rows = c.execute("SELECT * FROM braincore_recovery.changes ORDER BY id").fetchall()
        assert len(rows) == 2
        with pytest.raises(psycopg.errors.RaiseException), c.transaction():
            c.execute("SET LOCAL synchronous_commit='off'")
            c.execute(sql.SQL("INSERT INTO {} VALUES(1,4,'unsafe commit')").format(table))
        assert rows[0]["new_payload"]["__braincore_sql_null_columns"] == ["payload"]
        assert rows[1]["old_payload"]["payload"] is None
        assert rows[1]["new_payload"]["payload"] == "accepted"
        assert not c.execute(
            "SELECT has_schema_privilege('public',%s,'USAGE') AS access", (RECOVERY_SCHEMA,)
        ).fetchone()["access"]
        with pytest.raises(psycopg.errors.RaiseException):
            c.execute("DELETE FROM braincore_recovery.changes")
        with pytest.raises(psycopg.errors.RaiseException):
            c.execute(sql.SQL("TRUNCATE {}").format(table))
        role = sql.Identifier("recovery_writer_" + uuid4().hex)
        c.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(role))
        try:
            c.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(schema), role)
            )
            c.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON {} TO {}").format(table, role))
            c.execute(sql.SQL("SET ROLE {}").format(role))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                c.execute(sql.SQL("ALTER TABLE {} ADD COLUMN forbidden text").format(table))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                c.execute(sql.SQL("DROP TABLE {}").format(table))
        finally:
            c.execute("RESET ROLE")
            c.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(table, role))
            c.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(sql.Identifier(schema), role)
            )
            c.execute(sql.SQL("DROP ROLE {}").format(role))
        # Privileged maintenance must keep writers drained after this DDL.
        # A failed reinstall does not certify the old trigger for new writes.
        c.execute(sql.SQL("ALTER TABLE {} ADD COLUMN added text").format(table))
        with pytest.raises(ValueError, match="changed"):
            install_recovery(c, [schema])
        retained = c.execute("SELECT * FROM braincore_recovery.changes ORDER BY id").fetchall()
        assert retained == rows
    finally:
        c.execute("DROP SCHEMA IF EXISTS braincore_recovery CASCADE")


def test_recovery_groups_transactions_without_commit_watermarks(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    try:
        install_recovery(c, [schema])
        relation = sql.Identifier(schema, "records")
        with psycopg.connect(get_settings().database_url.get_secret_value()) as late:
            late.execute(sql.SQL("INSERT INTO {} VALUES(1,1,'late')").format(relation))
            with c.transaction():
                c.execute(sql.SQL("INSERT INTO {} VALUES(1,2,'early')").format(relation))
                c.execute(sql.SQL("INSERT INTO {} VALUES(1,3,'related')").format(relation))
            early = c.execute("SELECT id,transaction_id FROM braincore_recovery.changes").fetchall()
            assert len(early) == 2 and early[0]["transaction_id"] == early[1]["transaction_id"]
            late.commit()
        rows = c.execute("SELECT id FROM braincore_recovery.changes ORDER BY id").fetchall()
        assert len(rows) == 3 and rows[0]["id"] < min(r["id"] for r in early)
    finally:
        c.execute("DROP SCHEMA IF EXISTS braincore_recovery CASCADE")

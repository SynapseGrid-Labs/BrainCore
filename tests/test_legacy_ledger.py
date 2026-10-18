from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory import legacy_ledger
from braincore_memory.config import get_settings
from braincore_memory.legacy_capture import install_capture, pending_changes
from braincore_memory.legacy_ledger import copy_source, reconcile, register_source, replay_changes


@pytest.fixture
def ledger_target(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row, autocommit=True
    ) as c:
        yield c


def test_complete_copy_preserves_precision_and_duplicate_payloads_without_mutation(
    capture_source: tuple[psycopg.Connection[Any], str],
    ledger_target: psycopg.Connection[Any],
) -> None:
    source, schema = capture_source
    relation = sql.Identifier(schema, "records")
    source.execute(
        sql.SQL("ALTER TABLE {} ADD COLUMN amount numeric(38,22), ADD COLUMN detail jsonb").format(
            relation
        )
    )
    source.execute(
        sql.SQL("""INSERT INTO {} VALUES
        (1,1,'identical',1234567890123456.1234567890123456789012,'{{"nested":[null,true,1.2]}}'),
        (1,2,'identical',1234567890123456.1234567890123456789012,'{{"nested":[null,true,1.2]}}')""").format(
            relation
        )
    )
    source_id = uuid4()
    result = copy_source(source, ledger_target, source_id, schema, batch_size=1)
    assert result["row_count"] == 2 and result["native_import_gate"] == "NOT RUN"
    result = reconcile(source, ledger_target, source_id, schema)
    assert result["source_accounting_gate"] == "PASS"
    rows = ledger_target.execute(
        """SELECT payload::text AS payload FROM braincore.legacy_record_versions
                                    WHERE source_id=%s""",
        (source_id,),
    ).fetchall()
    assert len(rows) == 2 and all(
        "1234567890123456.1234567890123456789012" in r["payload"] for r in rows
    )
    copy_source(source, ledger_target, source_id, schema, batch_size=2)
    assert (
        ledger_target.execute(
            """SELECT count(*) AS n FROM braincore.legacy_record_versions
                                    WHERE source_id=%s""",
            (source_id,),
        ).fetchone()["n"]
        == 2
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        ledger_target.execute(
            "UPDATE braincore.legacy_record_versions SET payload='{}' WHERE source_id=%s",
            (source_id,),
        )
    assert (
        source.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(relation)).fetchone()["n"]
        == 2
    )


def test_delta_retry_after_commit_preserves_deleted_and_corrected_history(
    capture_source: tuple[psycopg.Connection[Any], str],
    ledger_target: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, schema = capture_source
    install_capture(source, schema)
    relation = sql.Identifier(schema, "records")
    source.execute(
        sql.SQL("INSERT INTO {} VALUES (1,1,'original'),(1,2,'later deleted')").format(relation)
    )
    source_id = uuid4()
    copy_source(source, ledger_target, source_id, schema, batch_size=1)
    source.execute(sql.SQL("UPDATE {} SET id=3,payload='corrected' WHERE id=1").format(relation))
    source.execute(sql.SQL("DELETE FROM {} WHERE id=2").format(relation))
    acknowledge = legacy_ledger.acknowledge_changes

    def crash_after_target_commit(*args: Any) -> None:
        raise RuntimeError("simulated crash before source acknowledgement")

    monkeypatch.setattr(legacy_ledger, "acknowledge_changes", crash_after_target_commit)
    with pytest.raises(RuntimeError, match="crash"):
        replay_changes(source, ledger_target, source_id)
    assert len(pending_changes(source, str(source_id))) == 4
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "PASS"
    monkeypatch.setattr(legacy_ledger, "acknowledge_changes", acknowledge)
    replay_changes(source, ledger_target, source_id)
    assert pending_changes(source, str(source_id)) == []
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "PASS"
    versions = ledger_target.execute(
        """SELECT payload->>'payload' AS text
                                        FROM braincore.legacy_record_versions WHERE source_id=%s""",
        (source_id,),
    ).fetchall()
    assert sorted(r["text"] for r in versions) == ["corrected", "later deleted", "original"]
    copy_source(source, ledger_target, source_id, schema)
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "PASS"
    assert (
        ledger_target.execute(
            "SELECT count(*) AS n FROM braincore.legacy_record_heads "
            "WHERE source_id=%s AND deleted",
            (source_id,),
        ).fetchone()["n"]
        == 2
    )


def test_partial_batch_retry_reconciles_and_source_identity_is_bound(
    capture_source: tuple[psycopg.Connection[Any], str],
    ledger_target: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, schema = capture_source
    source.execute(
        sql.SQL("INSERT INTO {} VALUES (1,1,'first'),(1,2,'second')").format(
            sql.Identifier(schema, "records")
        )
    )
    source_id = uuid4()
    stage = legacy_ledger.stage_records
    calls = 0

    def fail_second_batch(*args: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("interrupted copy")
        stage(*args)

    monkeypatch.setattr(legacy_ledger, "stage_records", fail_second_batch)
    with pytest.raises(RuntimeError, match="interrupted"):
        copy_source(source, ledger_target, source_id, schema, batch_size=1)
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "FAIL"
    monkeypatch.setattr(legacy_ledger, "stage_records", stage)
    copy_source(source, ledger_target, source_id, schema, batch_size=1)
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "PASS"
    with pytest.raises(ValueError, match="identity"):
        register_source(ledger_target, source_id, "another_schema", {"records": ["part", "id"]})


def test_sql_null_and_json_null_remain_distinct_during_copy_and_delta(
    capture_source: tuple[psycopg.Connection[Any], str],
    ledger_target: psycopg.Connection[Any],
) -> None:
    source, schema = capture_source
    relation = sql.Identifier(schema, "records")
    source.execute(sql.SQL("ALTER TABLE {} ADD COLUMN detail jsonb").format(relation))
    source.execute(
        sql.SQL("INSERT INTO {} VALUES (1,1,'SQL null',NULL),(1,2,'JSON null','null')").format(
            relation
        )
    )
    install_capture(source, schema)
    source_id = uuid4()
    copy_source(source, ledger_target, source_id, schema)
    rows = ledger_target.execute(
        """SELECT payload->>'payload' AS label,
                                            payload->'__braincore_sql_null_columns' AS nulls
                                     FROM braincore.legacy_record_versions WHERE source_id=%s""",
        (source_id,),
    ).fetchall()
    by_label = {r["label"]: r["nulls"] for r in rows}
    assert "detail" in by_label["SQL null"] and "detail" not in by_label["JSON null"]
    source.execute(sql.SQL("UPDATE {} SET detail='null' WHERE id=1").format(relation))
    source.execute(sql.SQL("UPDATE {} SET detail=NULL WHERE id=2").format(relation))
    replay_changes(source, ledger_target, source_id)
    assert reconcile(source, ledger_target, source_id, schema)["source_accounting_gate"] == "PASS"
    changes = source.execute("""SELECT old_payload->'__braincore_sql_null_columns' AS before,
                                         new_payload->'__braincore_sql_null_columns' AS after
                                  FROM braincore_legacy_capture.changes ORDER BY id""").fetchall()
    assert "detail" in changes[0]["before"] and "detail" not in changes[0]["after"]
    assert "detail" not in changes[1]["before"] and "detail" in changes[1]["after"]

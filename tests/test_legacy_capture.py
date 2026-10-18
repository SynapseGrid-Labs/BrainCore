from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory.config import get_settings
from braincore_memory.legacy_capture import (
    CAPTURE_SCHEMA,
    acknowledge_changes,
    install_capture,
    pending_changes,
    source_keys,
    stage_committed_transactions,
)


def test_capture_is_transactional_complete_and_resumable(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    report = install_capture(c, schema)
    assert report["capture_gate"] == "PASS" and report["table_count"] == 1
    relation = sql.Identifier(schema, "records")
    c.execute(sql.SQL("INSERT INTO {} VALUES (1,1,'original')").format(relation))
    with pytest.raises(ValueError), c.transaction():
        c.execute(sql.SQL("UPDATE {} SET payload='uncommitted'").format(relation))
        raise ValueError("simulate aborted write")
    c.execute(sql.SQL("UPDATE {} SET id=2,payload='corrected'").format(relation))
    c.execute(sql.SQL("DELETE FROM {} WHERE id=2").format(relation))
    subscriber = str(uuid4())
    changes = pending_changes(c, subscriber)
    assert [r["operation"] for r in changes] == ["INSERT", "UPDATE", "DELETE"]
    assert '"id": 1' in changes[1]["old_key"] and '"id": 2' in changes[1]["new_key"]
    assert "uncommitted" not in str(changes)
    assert "original" in changes[1]["old_payload"] and "corrected" in changes[1]["new_payload"]
    acknowledge_changes(c, subscriber, [changes[0]["id"]])
    acknowledge_changes(c, subscriber, [changes[0]["id"]])
    assert len(pending_changes(c, subscriber)) == 2
    assert install_capture(c, schema) == report
    assert len(pending_changes(c, subscriber)) == 2
    assert len(pending_changes(c, str(uuid4()))) == 3
    with pytest.raises(psycopg.errors.RaiseException):
        c.execute(sql.SQL("TRUNCATE {}").format(relation))


def test_late_commit_below_delivered_identifier_is_never_skipped(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    install_capture(c, schema)
    subscriber = str(uuid4())
    relation = sql.Identifier(schema, "records")
    # A lower sequence ID can commit after a higher ID on a different row.
    with psycopg.connect(get_settings().database_url.get_secret_value()) as late:
        late.execute(sql.SQL("INSERT INTO {} VALUES (1,1,'late commit')").format(relation))
        c.execute(sql.SQL("INSERT INTO {} VALUES (1,2,'early commit')").format(relation))
        first = pending_changes(c, subscriber)
        assert len(first) == 1 and "early commit" in first[0]["new_payload"]
        acknowledge_changes(c, subscriber, [first[0]["id"]])
        late.commit()
    second = pending_changes(c, subscriber)
    assert len(second) == 1 and "late commit" in second[0]["new_payload"]
    assert second[0]["id"] < first[0]["id"]
    acknowledge_changes(c, subscriber, [second[0]["id"]])
    assert pending_changes(c, subscriber) == []


def test_capture_fails_closed_for_changed_or_keyless_sources(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    c.execute(sql.SQL("CREATE TABLE {} (payload text)").format(sql.Identifier(schema, "keyless")))
    with pytest.raises(ValueError, match="primary key"):
        install_capture(c, schema)
    assert c.execute("SELECT to_regnamespace(%s) AS ns", (CAPTURE_SCHEMA,)).fetchone()["ns"] is None
    c.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(schema, "keyless")))
    install_capture(c, schema)
    c.execute(
        sql.SQL("CREATE TABLE {} (id int PRIMARY KEY)").format(sql.Identifier(schema, "added"))
    )
    with pytest.raises(ValueError, match="changed"):
        install_capture(c, schema)
    assert pending_changes(c, str(uuid4())) == []


def test_large_transaction_streaming_resume_never_acknowledges_partial_delivery(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    from braincore_memory.legacy_ledger import register_source

    source, schema = capture_source
    install_capture(source, schema)
    source.execute(
        sql.SQL(
            "INSERT INTO {} SELECT 1,g,'synthetic transaction' FROM generate_series(1,20001) g"
        ).format(sql.Identifier(schema, "records"))
    )
    source_id, run_id = uuid4(), uuid4()
    subscriber = str(uuid4())
    emitted: list[dict[str, Any]] = []

    def stop_after_durable_page(receipt: dict[str, Any]) -> None:
        emitted.append(receipt)
        raise RuntimeError("simulated interruption after durable staging")

    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as target:
        register_source(target, source_id, schema, source_keys(source, schema))
        with pytest.raises(RuntimeError, match="simulated interruption"), source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                selection_limit=1,
                run_id=run_id,
                page_receipt=stop_after_durable_page,
            )
        assert len(emitted) == 1 and len(emitted[0]["event_ids"]) == 1000
        assert target.execute(
            "SELECT copied_rows,completed_at FROM braincore.legacy_copy_runs WHERE id=%s",
            (run_id,),
        ).fetchone() == {"copied_rows": 1000, "completed_at": None}
        assert source.execute(
            "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries "
            "WHERE subscriber_id=%s::uuid",
            (subscriber,),
        ).fetchone() == {"n": 0}
        event_ids: list[int] = []

        def collect(receipt: dict[str, Any]) -> None:
            assert len(receipt["event_ids"]) <= 1000
            assert len(receipt["changed_heads"]) <= 2000
            assert "payload" not in receipt
            event_ids.extend(receipt["event_ids"])

        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            result = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                selection_limit=1,
                run_id=run_id,
                page_receipt=collect,
            )
        assert result["events"] == 20_001 and result["pages"] == 21
        assert result["transaction_count"] == 1 and result["delta_staging_gate"] == "PASS"
        assert result["source_capture_acknowledged"] is False
        assert len(event_ids) == len(set(event_ids)) == 20_001
        assert target.execute(
            "SELECT count(*) AS n FROM braincore.legacy_record_heads WHERE source_id=%s",
            (source_id,),
        ).fetchone() == {"n": 20_001}
        assert source.execute(
            "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries "
            "WHERE subscriber_id=%s::uuid",
            (subscriber,),
        ).fetchone() == {"n": 0}
        # The independent mapping consumer owns acknowledgement after its gates.
        acknowledge_changes(source, subscriber, event_ids)
        assert pending_changes(source, subscriber) == []


def test_complete_transaction_staging_preserves_key_moves_deletes_and_prior_acks(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    from braincore_memory.legacy_ledger import register_source

    source, schema = capture_source
    install_capture(source, schema)
    relation = sql.Identifier(schema, "records")
    with source.transaction():
        source.execute(sql.SQL("INSERT INTO {} VALUES(1,1,'original')").format(relation))
        source.execute(sql.SQL("UPDATE {} SET id=2,payload='corrected'").format(relation))
        source.execute(sql.SQL("DELETE FROM {} WHERE id=2").format(relation))
    subscriber, source_id, run_id = str(uuid4()), uuid4(), uuid4()
    first = pending_changes(source, subscriber)
    acknowledge_changes(source, subscriber, [first[0]["id"]])
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as target:
        register_source(target, source_id, schema, source_keys(source, schema))
        receipts: list[dict[str, Any]] = []
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            result = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                page_size=1,
                run_id=run_id,
                page_receipt=receipts.append,
            )
        assert result["events"] == 3 and len(receipts) == 3
        assert [page["event_ids"][0] for page in receipts] == [r["id"] for r in first]
        heads = target.execute(
            "SELECT record_key->>'id' AS id,deleted,last_change_id "
            "FROM braincore.legacy_record_heads WHERE source_id=%s ORDER BY record_key",
            (source_id,),
        ).fetchall()
        assert heads == [
            {"id": "1", "deleted": True, "last_change_id": first[1]["id"]},
            {"id": "2", "deleted": True, "last_change_id": first[2]["id"]},
        ]
        # Completed runs can reconstruct metadata spools without changing receipts.
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            replay = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                page_size=1,
                run_id=run_id,
            )
        assert replay["events"] == 3 and len(pending_changes(source, subscriber)) == 2
        with source.transaction(), pytest.raises(ValueError, match="page size"):
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                page_size=2,
                run_id=run_id,
            )


def test_staging_keeps_lower_allocation_late_commits_eligible(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    from braincore_memory.legacy_ledger import register_source

    source, schema = capture_source
    install_capture(source, schema)
    relation = sql.Identifier(schema, "records")
    subscriber, source_id = str(uuid4()), uuid4()
    with (
        psycopg.connect(
            get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
        ) as target,
        psycopg.connect(get_settings().database_url.get_secret_value()) as late,
    ):
        register_source(target, source_id, schema, source_keys(source, schema))
        late.execute(sql.SQL("INSERT INTO {} VALUES(1,1,'late commit')").format(relation))
        source.execute(sql.SQL("INSERT INTO {} VALUES(1,2,'early commit')").format(relation))
        first: list[dict[str, Any]] = []
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            result = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                page_receipt=first.append,
            )
        assert result["events"] == 1
        acknowledge_changes(source, subscriber, first[0]["event_ids"])
        late.commit()
        second: list[dict[str, Any]] = []
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            result = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                page_receipt=second.append,
            )
        assert result["events"] == 1
        assert second[0]["event_ids"][0] < first[0]["event_ids"][0]
        assert pending_changes(source, subscriber)[0]["id"] == second[0]["event_ids"][0]


def test_staging_rejects_wrong_source_transaction_and_changed_page_receipt(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    from braincore_memory.legacy_ledger import register_source

    source, schema = capture_source
    install_capture(source, schema)
    source.execute(
        sql.SQL("INSERT INTO {} VALUES(1,1,'synthetic')").format(sql.Identifier(schema, "records"))
    )
    subscriber, source_id, run_id = str(uuid4()), uuid4(), uuid4()
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as target:
        register_source(target, source_id, schema, source_keys(source, schema))
        with pytest.raises(ValueError, match="active independent"):
            stage_committed_transactions(source, target, source_id, subscriber, schema=schema)
        with source.transaction(), pytest.raises(ValueError, match="READ ONLY REPEATABLE READ"):
            stage_committed_transactions(source, target, source_id, subscriber, schema=schema)
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                run_id=run_id,
            )
        target.execute(
            "UPDATE braincore.legacy_copy_batches SET batch_hash=%s WHERE run_id=%s",
            (bytes(32), run_id),
        )
        with pytest.raises(ValueError, match="no longer matches"), source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                run_id=run_id,
            )
        assert len(pending_changes(source, subscriber)) == 1


def test_paged_ack_is_idempotent_and_rolls_back_late_invalid_event(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    c, schema = capture_source
    install_capture(c, schema)
    c.execute(
        sql.SQL("INSERT INTO {} SELECT 1,n,'ack fixture' FROM generate_series(1,2001) n").format(
            sql.Identifier(schema, "records")
        )
    )
    ids = [
        r["id"]
        for r in c.execute("SELECT id FROM braincore_legacy_capture.changes ORDER BY id").fetchall()
    ]
    assert len(ids) == 2001
    subscriber = str(uuid4())
    # A failure after two valid pages must not leave any partial acknowledgment.
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        acknowledge_changes(c, subscriber, ids[:2000] + [-1])
    assert (
        c.execute(
            "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries WHERE subscriber_id=%s",
            (subscriber,),
        ).fetchone()["n"]
        == 0
    )
    acknowledge_changes(c, subscriber, ids + [ids[0]])
    acknowledge_changes(c, subscriber, ids)
    assert (
        c.execute(
            "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries WHERE subscriber_id=%s",
            (subscriber,),
        ).fetchone()["n"]
        == 2001
    )
    assert pending_changes(c, subscriber) == []
    assert len(pending_changes(c, str(uuid4()), limit=10)) == 10

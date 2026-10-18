import hashlib
import json
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory.config import get_settings
from braincore_memory.legacy_capture import (
    acknowledge_changes,
    committed_transaction_page_query,
    install_capture,
    source_keys,
    stage_committed_transactions,
)
from braincore_memory.legacy_ledger import register_source


def _ordered_events_query() -> str:
    return (
        "SELECT c.id,c.source_table,c.operation,c.old_key::text,c.new_key::text,"
        "c.old_payload::text,c.new_payload::text "
        "FROM braincore_legacy_capture.changes c "
        "WHERE c.transaction_id=ANY(%s::xid8[]) ORDER BY c.id"
    )


def _safe_plan(
    source: psycopg.Connection[Any],
    query: str,
    parameters: tuple[Any, ...],
    *,
    proven_large_transaction: bool = False,
) -> None:
    plan = source.execute("EXPLAIN (FORMAT JSON) " + query, parameters, prepare=False).fetchone()[
        "QUERY PLAN"
    ][0]["Plan"]
    nodes = [plan]
    for node in nodes:
        nodes.extend(node.get("Plans", []))
    assert not any(
        node["Node Type"] == "Seq Scan" and node.get("Relation Name") == "changes" for node in nodes
    )
    assert not any(node["Node Type"] in {"Sort", "Incremental Sort"} for node in nodes)
    assert any(node.get("Index Name") == "changes_transaction_id_id_fixture" for node in nodes) or (
        proven_large_transaction
        and any(
            node["Node Type"] == "Index Scan" and node.get("Index Name") == "changes_pkey"
            for node in nodes
        )
    )


def test_capture_transaction_query_limit() -> None:
    with pytest.raises(ValueError, match="selection must not exceed 1000"):
        committed_transaction_page_query([str(x) for x in range(1001)], after_id=None, page_size=1)


@pytest.mark.parametrize("transaction_id", ["", "-1", "+1", " 1", "١", str(2**64)])
def test_capture_transaction_query_invalid_id_fails_closed(transaction_id: str) -> None:
    with pytest.raises(ValueError, match="unsigned xid8 decimal strings"):
        committed_transaction_page_query([transaction_id], after_id=None, page_size=1)


def test_numeric_transaction_aliases_preserve_any_set_semantics(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    source, schema = capture_source
    install_capture(source, schema)
    source.execute("""INSERT INTO braincore_legacy_capture.changes
        (id,transaction_id,source_table,operation,new_key,new_payload)
        OVERRIDING SYSTEM VALUE
        SELECT g,g::text::xid8,'records','INSERT',jsonb_build_object('part',1,'id',g),
          jsonb_build_object('payload','same transaction aliases')
        FROM generate_series(0,2) g""")
    for transactions in (["0", "00"], ["1", "01"], ["1", "01", "2", "002"]):
        expected = source.execute(_ordered_events_query(), (transactions,)).fetchall()
        for after in (None, 0, 1, 2):
            query, parameters = committed_transaction_page_query(
                transactions, after_id=after, page_size=1
            )
            assert (
                source.execute(query, parameters, prepare=False).fetchall()
                == [row for row in expected if after is None or row["id"] > after][:1]
            )


def test_merged_transaction_pages_use_index_after_growth(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    source, schema = capture_source
    install_capture(source, schema)
    # 1000 sparse selected transactions, a million unrelated changes, and a
    # separate large transaction exercise planner skew and the maximum selector.
    source.execute("""INSERT INTO braincore_legacy_capture.changes
        (id,transaction_id,source_table,operation,new_key,new_payload)
        OVERRIDING SYSTEM VALUE
        SELECT g,(100+((g+2000)/2))::text::xid8,'records','INSERT',
          jsonb_build_object('part',1,'id',g),jsonb_build_object('payload',repeat(md5(g::text),4))
        FROM generate_series(-2000,-1) g""")
    source.execute("""INSERT INTO braincore_legacy_capture.changes
        (id,transaction_id,source_table,operation,new_key,new_payload)
        OVERRIDING SYSTEM VALUE
        SELECT g,'999999'::xid8,'records','INSERT',jsonb_build_object('part',1,'id',g),
          jsonb_build_object('payload',repeat(md5(g::text),4))
        FROM generate_series(10000,1009999) g""")
    source.execute("""INSERT INTO braincore_legacy_capture.changes
        (id,transaction_id,source_table,operation,new_key,new_payload)
        OVERRIDING SYSTEM VALUE
        SELECT g,'999998'::xid8,'records','INSERT',jsonb_build_object('part',1,'id',g),
          jsonb_build_object('payload',repeat(md5(g::text),4))
        FROM generate_series(2000000,2110000) g""")
    source.execute(
        "CREATE INDEX changes_transaction_id_id_fixture "
        "ON braincore_legacy_capture.changes(transaction_id,id)"
    )
    source.execute("ANALYZE braincore_legacy_capture.changes")
    transactions = [str(x) for x in range(100, 1100)]
    expected = source.execute(_ordered_events_query(), (transactions,)).fetchall()
    assert len(expected) == 2000 and expected[0]["id"] == -2000
    with source.transaction():
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source.execute("SET LOCAL statement_timeout='5s'")
        for after in (None, -1001, -1):
            query, parameters = committed_transaction_page_query(
                transactions, after_id=after, page_size=156
            )
            _safe_plan(source, query, parameters)
            rows = source.execute(query, parameters, prepare=False).fetchall()
            assert rows == [r for r in expected if after is None or r["id"] > after][:156]
        actual = []
        after = None
        while True:
            query, parameters = committed_transaction_page_query(
                transactions, after_id=after, page_size=156
            )
            _safe_plan(source, query, parameters)
            rows = source.execute(query, parameters, prepare=False).fetchall()
            if not rows:
                break
            actual.extend(rows)
            after = rows[-1]["id"]
        assert actual == expected
        for after in (None, 2050000, 2110000):
            query, parameters = committed_transaction_page_query(
                ["999998"], after_id=after, page_size=156
            )
            _safe_plan(source, query, parameters, proven_large_transaction=True)
            rows = source.execute(query, parameters, prepare=False).fetchall()
            expected_start = 2000000 if after is None else after + 1
            assert [r["id"] for r in rows] == list(
                range(expected_start, min(expected_start + 156, 2110001))
            )
        # Set semantics are preserved if a caller repeats transaction IDs.
        unique_query, unique_parameters = committed_transaction_page_query(
            transactions[:3], after_id=None, page_size=156
        )
        duplicate_query, duplicate_parameters = committed_transaction_page_query(
            transactions[:3] + transactions[:3], after_id=None, page_size=156
        )
        assert source.execute(duplicate_query, duplicate_parameters, prepare=False).fetchall() == (
            source.execute(unique_query, unique_parameters, prepare=False).fetchall()
        )


def test_old_ordered_prefix_resumes_merged_transactions_with_same_digest(
    capture_source: tuple[psycopg.Connection[Any], str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import braincore_memory.legacy_capture as capture

    source, schema = capture_source
    install_capture(source, schema)
    source.execute(
        "ALTER SEQUENCE braincore_legacy_capture.changes_id_seq MINVALUE -10 RESTART WITH -10"
    )
    records = sql.Identifier(schema, "records")
    # Allocation order and transaction order intentionally differ. The first
    # selected transaction spans a committed event from the second transaction.
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row
    ) as writer:
        writer.execute(sql.SQL("INSERT INTO {} VALUES (1,1,'a-first')").format(records))
        source.execute(sql.SQL("INSERT INTO {} VALUES (1,2,'b-middle')").format(records))
        writer.execute(sql.SQL("INSERT INTO {} VALUES (1,3,'a-last')").format(records))
    subscriber, source_id, run_id = str(uuid4()), uuid4(), uuid4()
    acknowledge_changes(source, subscriber, [-10])
    original_ack_count = source.execute(
        "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries WHERE subscriber_id=%s",
        (subscriber,),
    ).fetchone()["n"]
    source.execute(
        sql.SQL("INSERT INTO {} VALUES (1,4,'later excluded transaction')").format(records)
    )
    new_query = capture.committed_transaction_page_query

    def old_ordered_page(
        transactions: list[str], *, after_id: int | None, page_size: int
    ) -> tuple[str, tuple[Any, ...]]:
        # Emulate the original globally ordered cursor's fetchmany page. Its
        # durable page hash depends on row content/order, rather than SQL shape.
        predicate = "c.transaction_id=ANY(%s::xid8[])"
        parameters: tuple[Any, ...] = (transactions,)
        if after_id is not None:
            predicate += " AND c.id>%s"
            parameters += (after_id,)
        return (
            _ordered_events_query().split("WHERE ")[0]
            + "WHERE "
            + predicate
            + " ORDER BY c.id LIMIT %s",
            parameters + (page_size,),
        )

    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as target:
        register_source(target, source_id, schema, source_keys(source, schema))
        monkeypatch.setattr(capture, "committed_transaction_page_query", old_ordered_page)
        calls = 0

        def interrupt_after_first(query: str, parameters: tuple[Any, ...]) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("interrupt original ordered stream")

        with pytest.raises(RuntimeError, match="interrupt original"), source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                selection_limit=2,
                page_size=2,
                run_id=run_id,
                page_query_guard=interrupt_after_first,
            )
        before = target.execute(
            "SELECT snapshot_id,copied_rows,completed_at FROM braincore.legacy_copy_runs "
            "WHERE id=%s",
            (run_id,),
        ).fetchone()
        assert before["copied_rows"] == 2 and before["completed_at"] is None
        header = json.loads(before["snapshot_id"])
        assert len(header["transaction_ids"]) == 2
        selected = source.execute(_ordered_events_query(), (header["transaction_ids"],)).fetchall()
        assert [row["id"] for row in selected] == [-10, -9, -8]
        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with source.cursor(name="original_capture_ordered_cursor") as stream:
                stream.execute(_ordered_events_query(), (header["transaction_ids"],))
                original_cursor_prefix = stream.fetchmany(2)
        assert original_cursor_prefix == selected[:2]
        expected_hash = hashlib.sha256()
        for row in original_cursor_prefix:
            expected_hash.update(json.dumps(row, sort_keys=True).encode())
            expected_hash.update(b"\n")
        first_hash = target.execute(
            "SELECT batch_hash FROM braincore.legacy_copy_batches "
            "WHERE run_id=%s AND batch_index=0",
            (run_id,),
        ).fetchone()["batch_hash"]
        assert bytes(first_hash) == expected_hash.digest()
        monkeypatch.setattr(capture, "committed_transaction_page_query", new_query)
        source.execute(
            sql.SQL("INSERT INTO {} VALUES (1,5,'new commit after interruption')").format(records)
        )
        for _replay in range(2):
            emitted: list[int] = []
            with source.transaction():
                source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                report = stage_committed_transactions(
                    source,
                    target,
                    source_id,
                    subscriber,
                    schema=schema,
                    selection_limit=2,
                    page_size=2,
                    run_id=run_id,
                    page_receipt=lambda page, emitted=emitted: emitted.extend(page["event_ids"]),
                )
            assert report["events"] == 3 and emitted == [-10, -9, -8]
            assert report["initial_snapshot"] == header["snapshot"]
            after = target.execute(
                "SELECT snapshot_id,copied_rows FROM braincore.legacy_copy_runs WHERE id=%s",
                (run_id,),
            ).fetchone()
            assert after == {"snapshot_id": before["snapshot_id"], "copied_rows": 3}
            assert bytes(
                target.execute(
                    "SELECT batch_hash FROM braincore.legacy_copy_batches "
                    "WHERE run_id=%s AND batch_index=0",
                    (run_id,),
                ).fetchone()["batch_hash"]
            ) == bytes(first_hash)
        assert (
            source.execute(
                "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries "
                "WHERE subscriber_id=%s",
                (subscriber,),
            ).fetchone()["n"]
            == original_ack_count
            == 1
        )

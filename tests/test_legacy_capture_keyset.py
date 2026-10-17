from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory.config import get_settings
from braincore_memory.legacy_capture import (
    committed_transaction_page_query,
    install_capture,
    pending_changes,
    source_keys,
    stage_committed_transactions,
)
from braincore_memory.legacy_ledger import register_source


def test_keyset_pages_retain_full_order_including_negative_ids(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    source, schema = capture_source
    install_capture(source, schema)
    records = sql.Identifier(schema, "records")
    source.execute(
        "ALTER SEQUENCE braincore_legacy_capture.changes_id_seq MINVALUE -10 RESTART WITH -10"
    )
    source.execute(
        sql.SQL("INSERT INTO {} SELECT 1,g,'first' FROM generate_series(1,7) g").format(records)
    )
    source.execute(sql.SQL("INSERT INTO {} VALUES (1,8,'other transaction')").format(records))
    selected = source.execute(
        "SELECT DISTINCT transaction_id::text AS xid "
        "FROM braincore_legacy_capture.changes ORDER BY xid"
    ).fetchall()
    txids = [row["xid"] for row in selected]
    expected = [
        row["id"]
        for row in source.execute(
            "SELECT id FROM braincore_legacy_capture.changes "
            "WHERE transaction_id=ANY(%s::xid8[]) ORDER BY id",
            (txids,),
        ).fetchall()
    ]
    actual: list[int] = []
    after_id = None
    with source.transaction():
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        while True:
            query, parameters = committed_transaction_page_query(
                txids, after_id=after_id, page_size=3
            )
            rows = source.execute(query, parameters, prepare=False).fetchall()
            if not rows:
                break
            assert len(rows) <= 3
            actual.extend(row["id"] for row in rows)
            after_id = rows[-1]["id"]
    assert actual == expected and actual[0] == -10 and len(actual) == 8


@pytest.mark.parametrize("page_size", [0, 1001])
def test_keyset_page_limit_fails_closed(page_size: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 1000"):
        committed_transaction_page_query(["42"], after_id=None, page_size=page_size)


def test_page_guard_failure_retains_receipt_and_resumes_same_run_without_ack(
    capture_source: tuple[psycopg.Connection[Any], str],
) -> None:
    source, schema = capture_source
    install_capture(source, schema)
    records = sql.Identifier(schema, "records")
    source.execute(
        sql.SQL(
            "INSERT INTO {} SELECT 1,g,'complete transaction' FROM generate_series(1,1200) g"
        ).format(records)
    )
    source.execute(
        sql.SQL("INSERT INTO {} VALUES (1,1201,'later unselected transaction')").format(records)
    )
    subscriber, source_id, run_id = str(uuid4()), uuid4(), uuid4()
    selected_ids = [row["id"] for row in pending_changes(source, subscriber, limit=1000)]
    all_changes = source.execute(
        "SELECT id FROM braincore_legacy_capture.changes ORDER BY id"
    ).fetchall()
    selected_complete_ids = [row["id"] for row in all_changes[:1200]]
    assert len(selected_ids) == 1000
    guarded_parameters: list[tuple[Any, ...]] = []

    def fail_second_page(query: str, parameters: tuple[Any, ...]) -> None:
        guarded_parameters.append(parameters)
        assert query.endswith("ORDER BY c.id LIMIT %s") and parameters[-1] == 400
        if len(guarded_parameters) == 2:
            raise RuntimeError("unsafe plan after growth")

    with psycopg.connect(
        get_settings().database_url.get_secret_value(),
        autocommit=True,
        row_factory=dict_row,
    ) as target:
        register_source(target, source_id, schema, source_keys(source, schema))
        with (
            pytest.raises(RuntimeError, match="unsafe plan after growth"),
            source.transaction(),
        ):
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                selection_limit=1000,
                page_size=400,
                run_id=run_id,
                page_query_guard=fail_second_page,
            )
        assert target.execute(
            "SELECT copied_rows,completed_at FROM braincore.legacy_copy_runs WHERE id=%s",
            (run_id,),
        ).fetchone() == {"copied_rows": 400, "completed_at": None}
        assert (
            source.execute(
                "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries "
                "WHERE subscriber_id=%s::uuid",
                (subscriber,),
            ).fetchone()["n"]
            == 0
        )
        emitted: list[int] = []
        guarded_parameters.clear()

        def approve_page(query: str, parameters: tuple[Any, ...]) -> None:
            assert query.endswith("ORDER BY c.id LIMIT %s") and parameters[-1] == 400
            guarded_parameters.append(parameters)

        with source.transaction():
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            report = stage_committed_transactions(
                source,
                target,
                source_id,
                subscriber,
                schema=schema,
                selection_limit=1000,
                page_size=400,
                run_id=run_id,
                page_receipt=lambda page: emitted.extend(page["event_ids"]),
                page_query_guard=approve_page,
            )
        assert report["events"] == 1200 and report["pages"] == 3
        assert emitted == selected_complete_ids and len(set(emitted)) == 1200
        assert len(guarded_parameters) == 4  # Three complete pages plus terminal empty query.
        assert len(guarded_parameters[0]) == 2  # First page has no artificial lower bound.
        assert [parameters[1] for parameters in guarded_parameters[1:]] == [
            400,
            800,
            1200,
        ]
        assert (
            target.execute(
                "SELECT copied_rows FROM braincore.legacy_copy_runs WHERE id=%s",
                (run_id,),
            ).fetchone()["copied_rows"]
            == 1200
        )
        assert (
            source.execute(
                "SELECT count(*) AS n FROM braincore_legacy_capture.deliveries "
                "WHERE subscriber_id=%s::uuid",
                (subscriber,),
            ).fetchone()["n"]
            == 0
        )

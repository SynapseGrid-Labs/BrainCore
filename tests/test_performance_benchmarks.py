from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
import respx

from benchmarks.performance import api_workload, finish_report
from benchmarks.performance_core import (
    cache_metadata,
    evaluate_gates,
    percentile,
    retrieval_scores,
    run_load,
    summarize,
)
from benchmarks.performance_fixture import (
    FixtureConfig,
    check_capacity,
    fixture_rows,
    guard_database,
    mode_settings,
    query_cases,
    query_sql,
    validate_truth,
)


def test_fixture_is_repeatable_streaming_and_has_hot_and_tail_tenants() -> None:
    config = FixtureConfig(rows=1000, dimensions=4, tenants=10, hot_tenant_fraction=0.8)
    first = list(fixture_rows(config))
    assert first == list(fixture_rows(config))
    assert first != list(fixture_rows(replace(config, seed=config.seed + 1)))
    assert 700 < sum(row[1] == 0 for row in first) < 900
    assert {row[1] for row in first} == set(range(10))
    assert len(json.loads(first[0][3])) == 4


def test_large_scale_requires_capacity_acknowledgements() -> None:
    config = FixtureConfig(rows=100_000_000)
    with pytest.raises(ValueError, match="100M requires"):
        check_capacity(config, False, None, None)
    with pytest.raises(ValueError, match="100M requires"):
        check_capacity(config, True, 1, 64)
    with pytest.raises(ValueError, match="100M requires"):
        check_capacity(config, True, float("inf"), 64)
    capacity = check_capacity(config, True, 1024, 64)
    assert capacity["capacity_verified"] is False
    assert capacity["raw_vector_bytes"] == 100_000_000 * 384 * 4


def test_query_matrix_includes_hot_tail_selective_and_empty_filters() -> None:
    cases = query_cases(FixtureConfig(dimensions=4), 10, 10, [1, 0.1, 0.01, 0.001, 0])
    assert {case["scope_threshold"] for case in cases} == {10000, 1000, 100, 10, 0}
    assert sum(case["tenant_id"] == 0 for case in cases) == 5
    assert cases == query_cases(FixtureConfig(dimensions=4), 10, 10, [1, 0.1, 0.01, 0.001, 0])


def test_recall_uses_min_k_eligible_and_empty_sets_have_no_ratio() -> None:
    small = retrieval_scores([1, 2], [1, 2, 3], eligible=3, k=10)
    assert small["recall_at_k"] == pytest.approx(2 / 3)
    assert small["fill_rate"] == pytest.approx(2 / 3)
    empty = retrieval_scores([], [], eligible=0, k=10)
    assert empty["recall_at_k"] is None
    assert empty["fill_rate"] is None
    assert empty["empty_result_correct"] is True
    wrong_empty = retrieval_scores([1], [], eligible=0, k=10)
    assert wrong_empty["invalid_result"] is True
    assert wrong_empty["empty_result_correct"] is False
    assert retrieval_scores([1, 1], [1, 2], eligible=2, k=10)["invalid_result"] is True


def test_ground_truth_rejects_incomplete_expected_sets_and_wrong_corpus() -> None:
    with pytest.raises(ValueError, match="exactly min"):
        retrieval_scores([1], [1], eligible=10, k=5)
    fixture = {"corpus_sha256": "a", "config": {"dimensions": 4}}
    with pytest.raises(ValueError, match="different corpus"):
        validate_truth({"fixture_sha256": "b"}, fixture)


def test_exact_and_ann_keep_identical_predicates_and_index_compatible_order() -> None:
    exact, hnsw = query_sql("exact"), query_sql("hnsw")
    assert exact.split("ORDER BY")[0] == hnsw.split("ORDER BY")[0]
    assert "ORDER BY embedding <=> %s::vector LIMIT" in hnsw
    assert "ORDER BY embedding <=> %s::vector, id LIMIT" in exact
    assert mode_settings("exact", 100, 20000)["enable_indexscan"] == "off"
    assert mode_settings("iterative", 100, 20000)["hnsw.iterative_scan"] == "strict_order"


def test_database_guard_rejects_application_database_before_mutation() -> None:
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = ("braincore",)
    with pytest.raises(ValueError, match="database name"):
        guard_database(conn, True)
    conn.execute.assert_called_once_with("SELECT current_database()")
    conn.reset_mock()
    with pytest.raises(ValueError, match="acknowledge"):
        guard_database(conn, False)
    conn.execute.assert_not_called()
    conn.execute.return_value.fetchone.return_value = ("braincore_benchmark_smoke",)
    guard_database(conn, True)


async def test_closed_loop_uses_all_workers_without_drops() -> None:
    active = 0
    maximum = 0

    async def work(worker: int, sequence: int) -> dict[str, Any]:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.003)
        active -= 1
        return {"error": None, "degraded": False}

    samples, elapsed = await run_load(work, 18, 3, None, 1)
    assert maximum == 3
    assert len(samples) == 18
    assert all(sample["error"] is None for sample in samples)
    assert elapsed > 0


async def test_open_loop_overload_records_drops_and_scheduled_response_time() -> None:
    async def work(worker: int, sequence: int) -> dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"error": None, "degraded": False}

    samples, elapsed = await run_load(work, 100, 1, 100000, 1)
    summary = summarize(samples, elapsed, 100000)
    assert len(samples) == 100
    assert summary["requests_dropped"] > 0
    assert summary["requests_succeeded"] > 0
    assert summary["requests_failed"] == 0
    assert summary["requests_attempted"] + summary["requests_dropped"] == 100
    assert all(
        sample["response_ms"] >= sample["service_ms"] for sample in samples if not sample["error"]
    )


async def test_load_errors_export_type_without_sensitive_exception_text() -> None:
    async def work(worker: int, sequence: int) -> dict[str, Any]:
        raise RuntimeError("private-endpoint.example/secret")

    samples, elapsed = await run_load(work, 3, 1, None, 1)
    assert all(sample["error"] == "RuntimeError" for sample in samples)
    assert "private-endpoint" not in json.dumps(samples)
    assert summarize(samples, elapsed, None)["response_latency_ms"]["p99"] is None


def test_summary_separates_degraded_and_failed_latency_from_healthy() -> None:
    def sample(
        latency: float, *, error: str | None = None, degraded: bool = False
    ) -> dict[str, Any]:
        return {
            "response_ms": latency,
            "service_ms": latency,
            "queue_delay_ms": 0,
            "error": error,
            "degraded": degraded,
        }

    summary = summarize(
        [sample(10), sample(20), sample(1, degraded=True), sample(2, error="TimeoutError")], 2, 10
    )
    assert summary["response_latency_ms"] == {
        "count": 2,
        "p50": 10,
        "p95": 20,
        "p99": 20,
        "max": 20,
    }
    assert summary["requests_degraded"] == summary["requests_failed"] == 1
    assert summary["achieved_qps"] == 1.5
    assert summary["healthy_qps"] == 1
    assert summary["error_rate"] == summary["degraded_rate"] == 0.25
    assert percentile([], 99) is None


def test_gates_never_pass_unknown_metrics_or_missing_sample_evidence() -> None:
    summary = {
        "requests_succeeded": 1000,
        "error_rate": 0,
        "degraded_rate": 0,
        "recall_at_k_mean": None,
        "response_latency_ms": {"p95": 20},
    }
    profile = {
        "minimum_samples": 1000,
        "error_rate_max": 0,
        "degraded_rate_max": 0,
        "recall_min": 0.99,
    }
    result = evaluate_gates(summary, profile)
    assert result["status"] == "insufficient_evidence"
    assert result["unknown_metrics"] == ["recall_min"]
    assert evaluate_gates(summary, None)["status"] == "not_evaluated"
    assert evaluate_gates({**summary, "plan_verified": False}, None)["status"] == "fail"
    profile.pop("recall_min")
    assert evaluate_gates(summary, profile)["status"] == "pass"
    assert evaluate_gates({**summary, "error_rate": 0.01}, profile)["status"] == "fail"


def test_cache_conditions_cannot_silently_claim_cold_or_warm() -> None:
    with pytest.raises(ValueError, match="full workload warmup"):
        cache_metadata("warm", 0, None)
    with pytest.raises(ValueError, match="evidence"):
        cache_metadata("cold_start", 0, None)
    with pytest.raises(ValueError, match="only permitted"):
        cache_metadata("cold_start", 1, "a" * 64)
    cold = cache_metadata("cold_start", 0, "a" * 64)
    assert cold["control"] == "operator_attested"
    assert cold["verified_os_cache_state"] is False


@respx.mock
async def test_http_mode_records_disabled_embedding_as_degraded_without_content() -> None:
    respx.post("http://127.0.0.1:8787/v1/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": "example", "content": "private query content"}],
                "streams": {"vector": {"state": "disabled"}, "full_text": {"state": "ok"}},
                "warnings": ["private-provider.example"],
            },
        )
    )
    args = argparse.Namespace(
        timeout_ms=1000, concurrency=1, warmups=0, requests=2, rate=None, queue_size=1
    )
    samples, elapsed = await api_workload(
        args, [{"query": "synthetic query"}], "http://127.0.0.1:8787", "test-only"
    )
    assert summarize(samples, elapsed, None)["requests_degraded"] == 2
    assert "private" not in json.dumps(samples)
    assert all(sample["recall_at_k"] is None for sample in samples)


def test_artifact_manifest_records_hashes_without_host_paths(tmp_path: Path) -> None:
    finish_report(tmp_path, {"kind": "test"}, [{"count": 1}], [{"sequence": 0}])
    manifest = json.loads((tmp_path / "run-manifest.json").read_text())
    assert set(manifest["artifact_sha256"]) == {"samples.jsonl", "summary.json"}
    assert all(len(value) == 64 for value in manifest["artifact_sha256"].values())
    assert "hostname" not in manifest["provenance"]
    assert str(tmp_path) not in (tmp_path / "run-manifest.json").read_text()
    assert all(not Path(name).is_absolute() for name in manifest["provenance"]["source_sha256"])


@pytest.mark.parametrize(
    ("changes", "failed_target"),
    [
        ({"recall_at_k_mean": 0.5}, "recall_min"),
        ({"fill_rate_mean": 0.5}, "fill_min"),
        ({"error_rate": 0.1}, "error_rate_max"),
        ({"response_latency_ms": {"p95": 200, "p99": 300}}, "p95_ms_max"),
    ],
)
def test_per_filter_gates_reject_bad_bucket_hidden_by_passing_aggregate(
    changes: dict[str, Any],
    failed_target: str,
) -> None:
    healthy = {
        "requests_succeeded": 10_000,
        "error_rate": 0,
        "degraded_rate": 0,
        "recall_at_k_mean": 1,
        "fill_rate_mean": 1,
        "response_latency_ms": {"p95": 10, "p99": 20},
    }
    summary = {**healthy, "by_filter": {"large": healthy, "small": {**healthy, **changes}}}
    profile = {
        "minimum_samples": 10_000,
        "error_rate_max": 0.01,
        "degraded_rate_max": 0,
        "recall_min": 0.99,
        "fill_min": 0.99,
        "p95_ms_max": 50,
    }
    assert evaluate_gates(summary, profile)["status"] == "pass"
    result = evaluate_gates(summary, {**profile, "per_filter": {"minimum_samples": 1}})
    assert result["status"] == "fail"
    assert result["failures"] == ["per_filter:small"]
    assert failed_target in result["per_filter"]["small"]["failures"]


def test_bucket_sample_minimum_is_independent_of_aggregate_and_defaults_to_10000() -> None:
    bucket = {"requests_succeeded": 5000, "error_rate": 0, "degraded_rate": 0}
    summary = {
        **bucket,
        "requests_succeeded": 10000,
        "by_filter": {"hot": bucket, "tail": bucket},
    }
    result = evaluate_gates(
        summary, {"error_rate_max": 0, "degraded_rate_max": 0, "per_filter": {}}
    )
    assert result["status"] == "insufficient_evidence"
    assert result["unknown_metrics"] == ["per_filter:hot", "per_filter:tail"]
    assert result["per_filter"]["hot"]["unknown_metrics"] == ["minimum_samples"]


def test_declared_empty_bucket_uses_correctness_without_inventing_recall() -> None:
    healthy = {
        "requests_succeeded": 10000,
        "error_rate": 0,
        "degraded_rate": 0,
        "recall_at_k_mean": 1,
        "fill_rate_mean": 1,
    }
    empty = {
        **healthy,
        "recall_at_k_mean": None,
        "fill_rate_mean": None,
        "declared_cases": 1,
        "nonempty_cases": 0,
        "empty_set_failures": 0,
    }
    summary = {**healthy, "by_filter": {"nonempty": healthy, "empty": empty}}
    profile = {
        "error_rate_max": 0,
        "degraded_rate_max": 0,
        "recall_min": 0.99,
        "fill_min": 1,
        "per_filter": {},
    }
    assert evaluate_gates(summary, profile)["status"] == "pass"
    empty["empty_set_failures"] = 1
    assert evaluate_gates(summary, profile)["status"] == "fail"


def test_bucket_summary_keeps_dropped_arrivals_and_never_reached_declared_buckets() -> None:
    from benchmarks.performance_core import summarize_filters

    cases = [
        {"case_id": 0, "tenant_id": 0, "scope_threshold": 100, "eligible": 2},
        {"case_id": 1, "tenant_id": 1, "scope_threshold": 0, "eligible": 0},
    ]
    samples = [
        {
            "sequence": 0,
            "error": "load_generator_queue_full",
            "degraded": False,
            "response_ms": 0,
            "service_ms": None,
            "queue_delay_ms": None,
        }
    ]
    groups = summarize_filters(samples, cases, 1)
    assert samples[0]["case_id"] == 0
    assert samples[0]["eligible"] == 2
    assert groups["hot:scope_bucket<100"]["requests_dropped"] == 1
    assert groups["hot:scope_bucket<100"]["error_rate"] == 1
    assert groups["tail:scope_bucket<0"]["requests_offered"] == 0
    assert groups["tail:scope_bucket<0"]["declared_cases"] == 1


def test_per_filter_gate_without_filter_evidence_is_insufficient() -> None:
    summary = {"requests_succeeded": 10000, "error_rate": 0, "degraded_rate": 0}
    result = evaluate_gates(
        summary, {"error_rate_max": 0, "degraded_rate_max": 0, "per_filter": {}}
    )
    assert result["status"] == "insufficient_evidence"
    assert result["unknown_metrics"] == ["per_filter:no_filter_evidence"]

"""Measurement and scheduling primitives for the disposable performance harness."""

from __future__ import annotations

import asyncio
import math
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

Work = Callable[[int, int], Awaitable[dict[str, Any]]]


def percentile(values: Sequence[float], percent: float) -> float | None:
    """Nearest-rank percentile; never synthesize measurements for an empty set."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percent / 100 * len(ordered)) - 1)]


def distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values) if values else None,
    }


def retrieval_scores(
    returned: Sequence[int | str], expected: Sequence[int | str], eligible: int, k: int
) -> dict[str, Any]:
    """Recall denominator is min(k, eligible); empty sets have no recall ratio."""
    target = min(k, eligible)
    if eligible < 0 or k < 1 or len(set(expected)) != target:
        raise ValueError("ground truth must contain exactly min(k, eligible) unique IDs")
    unique = set(returned)
    return {
        "eligible": eligible,
        "target": target,
        "returned": len(returned),
        "recall_at_k": len(unique & set(expected)) / target if target else None,
        "fill_rate": min(len(unique), target) / target if target else None,
        "empty_result_correct": not returned if target == 0 else None,
        "invalid_result": len(unique) != len(returned) or len(returned) > target,
    }


def cache_metadata(
    condition: str, warmups: int, cold_evidence_sha256: str | None
) -> dict[str, Any]:
    if condition not in {"uncontrolled", "warm", "cold_start"}:
        raise ValueError("unknown cache condition")
    if condition == "warm" and warmups < 1:
        raise ValueError("warm runs require at least one full workload warmup")
    if condition != "warm" and warmups != 0:
        raise ValueError("warmups are only permitted for warm runs")
    if condition == "cold_start":
        if not cold_evidence_sha256 or not re.fullmatch("[0-9a-f]{64}", cold_evidence_sha256):
            raise ValueError("cold_start requires an external cache-reset evidence SHA-256")
    elif cold_evidence_sha256 is not None:
        raise ValueError("cold evidence only applies to cold_start")
    return {
        "condition": condition,
        "warmup_passes": warmups,
        "control": "operator_attested" if condition == "cold_start" else condition,
        "external_reset_evidence_sha256": cold_evidence_sha256,
        "verified_os_cache_state": False,
        "scope": "start of timed workload; later requests naturally warm caches",
    }


async def run_load(
    work: Work, requests: int, concurrency: int, rate: float | None, queue_size: int
) -> tuple[list[dict[str, Any]], float]:
    """Bounded open-loop arrivals; overload drops are recorded, never hidden by backpressure.

    A worker owns its connection. Response time starts at the scheduled arrival, which
    includes client queue and scheduler delay rather than suffering coordinated omission.
    """
    if (
        requests < 1
        or concurrency < 1
        or queue_size < 1
        or (rate is not None and (not math.isfinite(rate) or rate <= 0))
    ):
        raise ValueError("requests, concurrency, queue size and offered rate must be positive")
    queue: asyncio.Queue[tuple[int, float] | None] = asyncio.Queue(maxsize=queue_size)
    slots = asyncio.Semaphore(concurrency)
    results: list[dict[str, Any]] = []
    started = time.perf_counter()

    async def worker(worker_id: int) -> None:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                return
            sequence, scheduled = item
            began = time.perf_counter()
            try:
                value = await work(worker_id, sequence)
            except Exception as exc:
                # Exception text may contain credentials, URLs, paths or query contents.
                value = {"error": type(exc).__name__, "degraded": False}
            ended = time.perf_counter()
            value.update(
                {
                    "sequence": sequence,
                    "queue_delay_ms": (began - scheduled) * 1000,
                    "service_ms": (ended - began) * 1000,
                    "response_ms": (ended - scheduled) * 1000,
                }
            )
            results.append(value)
            queue.task_done()
            if rate is None:
                slots.release()

    workers = [asyncio.create_task(worker(index)) for index in range(concurrency)]
    try:
        for sequence in range(requests):
            scheduled = started + sequence / rate if rate else time.perf_counter()
            delay = scheduled - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            if rate:
                try:
                    queue.put_nowait((sequence, scheduled))
                except asyncio.QueueFull:
                    results.append(
                        {
                            "sequence": sequence,
                            "error": "load_generator_queue_full",
                            "degraded": False,
                            "service_ms": None,
                            "queue_delay_ms": None,
                            "response_ms": max(0.0, (time.perf_counter() - scheduled) * 1000),
                        }
                    )
            else:
                # Closed-loop mode supplies at most concurrency outstanding operations.
                await slots.acquire()
                await queue.put((sequence, time.perf_counter()))
        await queue.join()
    finally:
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
    elapsed = time.perf_counter() - started
    return sorted(results, key=lambda item: item["sequence"]), elapsed


def summarize(samples: list[dict[str, Any]], elapsed: float, rate: float | None) -> dict[str, Any]:
    healthy = [s for s in samples if not s.get("error") and not s.get("degraded")]
    degraded = [s for s in samples if not s.get("error") and s.get("degraded")]
    errors = [s for s in samples if s.get("error")]
    drops = [s for s in errors if s["error"] == "load_generator_queue_full"]
    recalls = [float(s["recall_at_k"]) for s in healthy if s.get("recall_at_k") is not None]
    fills = [float(s["fill_rate"]) for s in healthy if s.get("fill_rate") is not None]
    error_counts = dict(Counter(str(sample["error"]) for sample in errors))
    total = len(samples)
    return {
        "requests_offered": total,
        "requests_attempted": total - len(drops),
        "requests_succeeded": len(healthy),
        "requests_degraded": len(degraded),
        "requests_failed": len(errors) - len(drops),
        "requests_dropped": len(drops),
        "errors": error_counts,
        "error_rate": len(errors) / total if total else None,
        "degraded_rate": len(degraded) / total if total else None,
        "duration_seconds": elapsed,
        "offered_qps": rate if rate else None,
        "achieved_qps": (len(healthy) + len(degraded)) / elapsed if elapsed > 0 else None,
        "healthy_qps": len(healthy) / elapsed if elapsed > 0 else None,
        "actual_offered_qps": total / elapsed if elapsed > 0 else None,
        "response_latency_ms": distribution([float(s["response_ms"]) for s in healthy]),
        "service_latency_ms": distribution([float(s["service_ms"]) for s in healthy]),
        "queue_delay_ms": distribution(
            [float(s["queue_delay_ms"]) for s in samples if s.get("queue_delay_ms") is not None]
        ),
        "degraded_response_latency_ms": distribution([float(s["response_ms"]) for s in degraded]),
        "error_response_latency_ms": distribution([float(s["response_ms"]) for s in errors]),
        "recall_at_k_mean": sum(recalls) / len(recalls) if recalls else None,
        "fill_rate_mean": sum(fills) / len(fills) if fills else None,
        "recall_samples": len(recalls),
        "empty_set_samples": sum(s.get("empty_result_correct") is not None for s in healthy),
        "empty_set_failures": sum(s.get("empty_result_correct") is False for s in samples),
        "invalid_results": sum(bool(s.get("invalid_result")) for s in samples),
    }


def summarize_filters(
    samples: list[dict[str, Any]],
    cases: list[dict[str, Any]],
    elapsed: float,
) -> dict[str, dict[str, Any]]:
    """Attribute every offered request, including dropped/errors, to its declared bucket."""

    def label(case: dict[str, Any]) -> str:
        tenant_class = "hot" if case["tenant_id"] == 0 else "tail"
        return f"{tenant_class}:scope_bucket<{case['scope_threshold']}"

    groups: dict[str, list[dict[str, Any]]] = {label(case): [] for case in cases}
    declared = Counter(label(case) for case in cases)
    nonempty = Counter(label(case) for case in cases if case["eligible"] > 0)
    for sample in samples:
        case = cases[sample["sequence"] % len(cases)]
        sample.update(
            {
                "case_id": case["case_id"],
                "tenant_class": "hot" if case["tenant_id"] == 0 else "tail",
                "scope_threshold": case["scope_threshold"],
                "eligible": case["eligible"],
            }
        )
        groups[label(case)].append(sample)
    return {
        name: {
            **summarize(group, elapsed, None),
            "declared_cases": declared[name],
            "nonempty_cases": nonempty[name],
        }
        for name, group in sorted(groups.items())
    }


GATE_METRICS = {
    "p95_ms_max": ("response_latency_ms.p95", "max"),
    "p99_ms_max": ("response_latency_ms.p99", "max"),
    "error_rate_max": ("error_rate", "max"),
    "degraded_rate_max": ("degraded_rate", "max"),
    "recall_min": ("recall_at_k_mean", "min"),
    "fill_min": ("fill_rate_mean", "min"),
    "healthy_qps_min": ("healthy_qps", "min"),
    "embedding_lag_seconds_max": ("embedding_lag_seconds", "max"),
    "server_cpu_percent_max": ("server_cpu_percent", "max"),
}


def evaluate_gates(summary: dict[str, Any], profile: dict[str, Any] | None) -> dict[str, Any]:
    failures: list[str] = []
    unknown: list[str] = []
    if summary.get("empty_set_failures") or summary.get("invalid_results"):
        failures.append("retrieval_correctness")
    if summary.get("plan_verified") is False:
        failures.append("query_plan")
    if not profile:
        return {
            "status": "fail" if failures else "not_evaluated",
            "failures": failures,
            "reason": "explicit thresholds required; no default performance claim",
        }
    required = {"error_rate_max", "degraded_rate_max"}
    if not required.issubset(profile):
        raise ValueError("gate profile requires explicit error_rate_max and degraded_rate_max")
    if set(profile) - set(GATE_METRICS) - {"minimum_samples", "per_filter"}:
        raise ValueError("unknown gate metric")
    minimum = profile.get("minimum_samples", 10_000)
    if not isinstance(minimum, int) or isinstance(minimum, bool) or minimum < 1:
        raise ValueError("minimum_samples must be a positive integer")
    if summary["requests_succeeded"] < minimum:
        unknown.append("minimum_samples")
    for name, threshold in profile.items():
        if name in {"minimum_samples", "per_filter"}:
            continue
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, int | float)
            or not math.isfinite(threshold)
            or threshold < 0
        ):
            raise ValueError("gate thresholds must be finite non-negative numbers")
        path, direction = GATE_METRICS[name]
        value: Any = summary
        for key in path.split("."):
            value = value.get(key) if isinstance(value, dict) else None
        if value is None:
            unknown.append(name)
        elif (direction == "max" and value > threshold) or (
            direction == "min" and value < threshold
        ):
            failures.append(name)
    bucket_gates: dict[str, Any] = {}
    if "per_filter" in profile:
        overrides = profile["per_filter"]
        if not isinstance(overrides, dict) or "per_filter" in overrides:
            raise ValueError("per_filter must contain flat gate overrides")
        if set(overrides) - set(GATE_METRICS) - {"minimum_samples"}:
            raise ValueError("unknown per_filter gate metric")
        inherited = {
            name: value
            for name, value in profile.items()
            if name
            in {
                "p95_ms_max",
                "p99_ms_max",
                "error_rate_max",
                "degraded_rate_max",
                "recall_min",
                "fill_min",
            }
        }
        bucket_profile = {"minimum_samples": 10_000, **inherited, **overrides}
        buckets = summary.get("by_filter")
        if not isinstance(buckets, dict) or not buckets:
            unknown.append("per_filter:no_filter_evidence")
        else:
            for label, bucket in buckets.items():
                selected = dict(bucket_profile)
                # Empty sets have correctness checks, not invented recall/fill ratios.
                if bucket.get("declared_cases", 0) > 0 and bucket.get("nonempty_cases") == 0:
                    selected.pop("recall_min", None)
                    selected.pop("fill_min", None)
                result = evaluate_gates(bucket, selected)
                bucket_gates[label] = result
                if result["status"] == "fail":
                    failures.append(f"per_filter:{label}")
                elif result["status"] != "pass":
                    unknown.append(f"per_filter:{label}")
    return {
        "status": "fail" if failures else "insufficient_evidence" if unknown else "pass",
        "failures": failures,
        "unknown_metrics": unknown,
        "thresholds": profile,
        "per_filter": bucket_gates,
    }

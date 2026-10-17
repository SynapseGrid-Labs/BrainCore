"""CLI: reproducible disposable PostgreSQL and synthetic HTTP performance workloads."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import re
import resource
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import psycopg

from .performance_core import (
    cache_metadata,
    evaluate_gates,
    run_load,
    summarize,
    summarize_filters,
)
from .performance_fixture import (
    SCALES,
    FixtureConfig,
    check_capacity,
    create_fixture,
    database_metrics,
    execute_query,
    explain_plans,
    ground_truth,
    guard_database,
    mode_settings,
    read_fixture,
    validate_truth,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def provenance() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    paths = [
        "benchmarks/performance.py",
        "benchmarks/performance_core.py",
        "benchmarks/performance_fixture.py",
        "uv.lock",
    ]
    return {
        "python": platform.python_version(),
        "os": platform.system(),
        "architecture": platform.machine(),
        "dependencies": {name: importlib.metadata.version(name) for name in ("psycopg", "httpx")},
        "source_sha256": {name: sha256(root / name) for name in paths if (root / name).is_file()},
        "hostname_and_paths": "deliberately excluded",
    }


def client_resources() -> dict[str, float]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "cpu_seconds": usage.ru_utime + usage.ru_stime,
        "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
    }


def finish_report(
    output: Path,
    manifest: dict[str, Any],
    summaries: list[dict[str, Any]],
    samples: list[dict[str, Any]],
) -> None:
    raw = output / "samples.jsonl"
    raw.write_text(
        "".join(json.dumps(row, allow_nan=False) + "\n" for row in samples), encoding="utf-8"
    )
    write_json(output / "summary.json", summaries)
    manifest["artifact_sha256"] = {
        "samples.jsonl": sha256(raw),
        "summary.json": sha256(output / "summary.json"),
    }
    manifest["provenance"] = provenance()
    manifest["completed_at_utc"] = datetime.now(UTC).isoformat()
    write_json(output / "run-manifest.json", manifest)


def load_profile(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.gates is None:
        return None
    value = read_json(args.gates)
    if not isinstance(value, dict):
        raise ValueError("gate profile must be a JSON object")
    return value


def prepare_output(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    cache = cache_metadata(args.cache, args.warmups, args.cold_evidence_sha256)
    output: Path = args.output_dir
    output.mkdir(parents=True, exist_ok=False)
    return output, cache


async def sql_workload(
    args: argparse.Namespace,
    dsn: str,
    mode: str,
    cases: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], float]:
    connections: list[psycopg.AsyncConnection[Any]] = []
    try:
        for _ in range(args.concurrency):
            conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
            connections.append(conn)
            settings = mode_settings(mode, args.ef_search, args.max_scan_tuples)
            settings["statement_timeout"] = str(args.timeout_ms)
            for name, value in settings.items():
                await conn.execute("SELECT set_config(%s,%s,false)", (name, value))

        async def work(worker: int, sequence: int) -> dict[str, Any]:
            return await execute_query(connections[worker], cases[sequence % len(cases)], mode)

        for _ in range(args.warmups):
            for case_index in range(len(cases)):
                await asyncio.gather(
                    *(work(worker, case_index) for worker in range(args.concurrency))
                )
        return await run_load(work, args.requests, args.concurrency, args.rate, args.queue_size)
    finally:
        for conn in connections:
            await conn.close()


def run_sql(args: argparse.Namespace, conn: psycopg.Connection[Any], dsn: str) -> int:
    fixture = read_fixture(conn)
    cases = validate_truth(read_json(args.ground_truth), fixture)
    modes: list[str] = args.modes
    if args.cache == "cold_start" and len(modes) != 1:
        raise ValueError("cold_start requires one mode per externally reset run")
    profile = load_profile(args)
    output, cache = prepare_output(args)
    initial = database_metrics(conn)
    version = tuple(int(part) for part in initial["pgvector_version"].split(".")[:2])
    if "iterative" in modes and version < (0, 8):
        raise ValueError("iterative scans require pgvector >= 0.8")
    conn.commit()
    all_samples: list[dict[str, Any]] = []
    summaries = []
    mode_metadata = []
    for mode in modes:
        # EXPLAIN without ANALYZE does not execute the retrieval query.
        plans = explain_plans(conn, cases, mode, args.ef_search, args.max_scan_tuples)
        conn.commit()
        before = database_metrics(conn)
        conn.commit()
        resource_before = client_resources()
        samples, elapsed = asyncio.run(sql_workload(args, dsn, mode, cases))
        resource_after = client_resources()
        after = database_metrics(conn)
        conn.commit()
        summary = summarize(samples, elapsed, args.rate)
        summary.update(
            {
                "mode": mode,
                "plan_verified": all(plan["verified"] for plan in plans),
                "server_cpu_percent": after["server_cpu_percent"],
                "embedding_lag_seconds": after["embedding_lag_seconds"],
            }
        )
        for sample in samples:
            sample["mode"] = mode
        summary["by_filter"] = summarize_filters(samples, cases, elapsed)
        summary["gate"] = evaluate_gates(summary, profile)
        summaries.append(summary)
        all_samples.extend(samples)
        mode_metadata.append(
            {
                "mode": mode,
                "session_settings": mode_settings(mode, args.ef_search, args.max_scan_tuples),
                "plans": plans,
                "database_before": before,
                "database_after": after,
                "client_cpu_seconds": resource_after["cpu_seconds"]
                - resource_before["cpu_seconds"],
                "client_peak_rss_bytes": resource_after["peak_rss_bytes"],
                "client_resource_scope": "CPU includes setup/warmup; RSS is process lifetime peak",
            }
        )
    manifest = {
        "format_version": 1,
        "kind": "sql_precomputed_vector_microbenchmark",
        "started_at_utc": args.started_at,
        "fixture": fixture,
        "ground_truth_sha256": sha256(args.ground_truth),
        "cache": cache,
        "requests_per_mode": args.requests,
        "concurrency": args.concurrency,
        "schedule": "open_loop" if args.rate else "closed_loop",
        "offered_rate": args.rate,
        "queue_size": args.queue_size,
        "timeout_ms": args.timeout_ms,
        "mode_order": modes,
        "modes": mode_metadata,
        "instrumentation": (
            "one persistent connection per worker; connection setup and session settings excluded; "
            "service timing includes SQL roundtrip, fetching IDs and local recall calculation"
        ),
        "limitations": [
            "synthetic independent vectors; results do not predict embedding quality",
            "benchmark-only schema; excludes application, authentication, fusion and ingestion",
            "mode order fixed and recorded; randomize order across replicate runs",
            "cold_start is externally attested, not independently verified",
            "database counters may lag and include other activity in this disposable database",
            "HNSW build order and PostgreSQL scheduling may vary despite a deterministic corpus",
        ],
    }
    finish_report(output, manifest, summaries, all_samples)
    return (
        1 if any(s["gate"]["status"] in {"fail", "insufficient_evidence"} for s in summaries) else 0
    )


def load_api_cases(path: Path) -> list[dict[str, Any]]:
    cases = read_json(path)
    if not isinstance(cases, list) or not cases:
        raise ValueError("API workload must be a non-empty JSON array")
    for case in cases:
        if not isinstance(case, dict) or set(case) - {"query", "scope", "limit", "metadata_filter"}:
            raise ValueError("unsupported API workload fields")
        if not isinstance(case.get("query"), str) or not 1 <= len(case["query"]) <= 10_000:
            raise ValueError("API workload requires text queries")
        if not isinstance(case.get("limit", 10), int) or not 1 <= case.get("limit", 10) <= 100:
            raise ValueError("API limit must be 1..100")
        if "scope" in case and (
            not isinstance(case["scope"], dict)
            or set(case["scope"]) - {"project_id", "user_id", "agent_id", "run_id"}
        ):
            raise ValueError("invalid API scope")
    return list(cases)


async def api_workload(
    args: argparse.Namespace,
    cases: list[dict[str, Any]],
    url: str,
    api_key: str,
) -> tuple[list[dict[str, Any]], float]:
    async with httpx.AsyncClient(
        base_url=url,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=args.timeout_ms / 1000,
        limits=httpx.Limits(max_connections=args.concurrency),
    ) as client:

        async def work(worker: int, sequence: int) -> dict[str, Any]:
            index = sequence % len(cases)
            response = await client.post("/v1/search", json=cases[index])
            if response.is_error:
                return {
                    "case_id": index,
                    "error": f"http_{response.status_code}",
                    "degraded": False,
                }
            payload = response.json()
            streams = payload.get("streams")
            if not isinstance(streams, dict) or not isinstance(payload.get("results"), list):
                return {"case_id": index, "error": "invalid_api_response", "degraded": False}
            # Search with text requires vector retrieval. Disabled/missing vector is degraded.
            safe_states = {"ok", "ready", "failed", "degraded", "disabled", "pending"}
            states = {
                str(name): str(value.get("state"))
                if value.get("state") in safe_states
                else "unknown"
                for name, value in streams.items()
                if name in {"vector", "full_text", "temporal", "listing"}
                and isinstance(value, dict)
            }
            degraded = (
                states.get("vector") not in {"ok", "ready"}
                or any(value in {"failed", "degraded", "unknown"} for value in states.values())
                or bool(payload.get("warnings"))
            )
            return {
                "case_id": index,
                "error": None,
                "degraded": degraded,
                "streams": states,
                "warning_count": len(payload.get("warnings", [])),
                "returned": len(payload["results"]),
                "recall_at_k": None,
                "fill_rate": None,
            }

        for _ in range(args.warmups):
            for index in range(len(cases)):
                value = await work(0, index)
                if value.get("error"):
                    raise RuntimeError("API warmup failed")
        return await run_load(work, args.requests, args.concurrency, args.rate, args.queue_size)


def run_api(args: argparse.Namespace) -> int:
    if not args.acknowledge_disposable_api:
        raise ValueError("--acknowledge-disposable-api is required")
    url = os.environ[args.api_url_env]
    key = os.environ[args.api_key_env]
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(
            "API target must be a disposable loopback endpoint without URL credentials"
        )
    if args.embedding_profile and not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", args.embedding_profile
    ):
        raise ValueError("embedding profile must be a public label without a URL or path")
    cases = load_api_cases(args.workload)
    profile = load_profile(args)
    output, cache = prepare_output(args)
    before = client_resources()
    samples, elapsed = asyncio.run(api_workload(args, cases, url, key))
    after = client_resources()
    summary = summarize(samples, elapsed, args.rate)
    summary["gate"] = evaluate_gates(summary, profile)
    manifest = {
        "format_version": 1,
        "kind": "http_query_embedding_end_to_end",
        "started_at_utc": args.started_at,
        "workload_sha256": sha256(args.workload),
        "workload_cases": len(cases),
        "cache": cache,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "schedule": "open_loop" if args.rate else "closed_loop",
        "offered_rate": args.rate,
        "queue_size": args.queue_size,
        "timeout_ms": args.timeout_ms,
        "declared_embedding_profile": args.embedding_profile,
        "embedding_profile_verified": False,
        "client_cpu_seconds": after["cpu_seconds"] - before["cpu_seconds"],
        "client_peak_rss_bytes": after["peak_rss_bytes"],
        "client_resource_scope": "CPU includes setup/warmup; RSS is process lifetime peak",
        "unavailable_metrics": {
            "recall_at_k": "HTTP fusion results have no exact vector ground truth in this mode",
            "fill_rate": "eligible corpus size is not exposed by the search API",
            "server_cpu_percent": "external collector required",
            "index_bytes": "not exposed by search API",
            "embedding_lag_seconds": "read workload does not exercise asynchronous ingestion",
        },
        "precomputed_vector_http_mode": "unsupported by current public search API",
        "limitations": [
            "use synthetic data and a disposable API instance only",
            "embeddings include provider latency; provider provenance is operator declared",
            "request content, endpoints, credentials and raw error details are not exported",
        ],
    }
    finish_report(output, manifest, [summary], samples)
    return 1 if summary["gate"]["status"] in {"fail", "insufficient_evidence"} else 0


def add_workload_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument(
        "--rate", type=float, help="open-loop arrivals per second; omit for closed loop"
    )
    parser.add_argument("--queue-size", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=5000)
    parser.add_argument("--cache", choices=["uncontrolled", "warm", "cold_start"], default="warm")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--cold-evidence-sha256")
    parser.add_argument(
        "--gates", type=Path, help="explicit JSON thresholds; no built-in speed claims"
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("fixture", "ground-truth", "run"):
        command = commands.add_parser(name)
        command.add_argument("--dsn-env", default="BRAINCORE_BENCHMARK_DSN")
        command.add_argument("--acknowledge-disposable-database", action="store_true")
        if name == "fixture":
            command.add_argument("--scale", choices=list(SCALES), default="100k")
            command.add_argument(
                "--rows", type=int, help="override named scale for small smoke tests"
            )
            command.add_argument("--dimensions", type=int, default=384)
            command.add_argument("--tenants", type=int, default=100)
            command.add_argument("--hot-tenant-fraction", type=float, default=0.5)
            command.add_argument("--seed", type=int, default=20261002)
            command.add_argument("--hnsw-m", type=int, default=16)
            command.add_argument("--hnsw-ef-construction", type=int, default=64)
            command.add_argument("--allow-100m", action="store_true")
            command.add_argument("--storage-budget-gib", type=float)
            command.add_argument("--memory-budget-gib", type=float)
            command.add_argument("--output", type=Path, required=True)
        elif name == "ground-truth":
            command.add_argument("--queries", type=int, default=20)
            command.add_argument("--top-k", type=int, default=10)
            command.add_argument(
                "--selectivities", type=float, nargs="+", default=[1, 0.1, 0.01, 0.001, 0]
            )
            command.add_argument("--output", type=Path, required=True)
        else:
            command.add_argument("--ground-truth", required=True, type=Path)
            command.add_argument(
                "--modes",
                nargs="+",
                choices=["exact", "hnsw", "iterative"],
                default=["exact", "hnsw", "iterative"],
            )
            command.add_argument("--ef-search", type=int, default=100)
            command.add_argument("--max-scan-tuples", type=int, default=20_000)
            add_workload_args(command)
    api = commands.add_parser("api")
    api.add_argument("--api-url-env", default="BRAINCORE_BENCHMARK_API_URL")
    api.add_argument("--api-key-env", default="BRAINCORE_BENCHMARK_API_KEY")
    api.add_argument("--acknowledge-disposable-api", action="store_true")
    api.add_argument("--workload", required=True, type=Path)
    api.add_argument("--embedding-profile", help="public provider/model configuration label")
    add_workload_args(api)
    return root


def main() -> int:
    args = parser().parse_args()
    args.started_at = datetime.now(UTC).isoformat()
    if hasattr(args, "requests") and (
        args.requests < 1
        or args.concurrency < 1
        or args.queue_size < 1
        or args.timeout_ms < 1
        or (args.rate is not None and (not math.isfinite(args.rate) or args.rate <= 0))
    ):
        raise ValueError("load parameters and timeout must be positive and finite")
    if args.command == "api":
        return run_api(args)
    # Require the acknowledgement before opening even a read-only database connection.
    if not args.acknowledge_disposable_database:
        raise ValueError("--acknowledge-disposable-database is required")
    if args.command in {"fixture", "ground-truth"} and args.output.exists():
        raise ValueError("refusing to overwrite existing artifact")
    dsn = os.environ[args.dsn_env]
    with psycopg.connect(dsn) as conn:
        guard_database(conn, args.acknowledge_disposable_database)
        if args.command == "fixture":
            config = FixtureConfig(
                rows=args.rows if args.rows is not None else SCALES[args.scale],
                dimensions=args.dimensions,
                tenants=args.tenants,
                hot_tenant_fraction=args.hot_tenant_fraction,
                seed=args.seed,
                hnsw_m=args.hnsw_m,
                hnsw_ef_construction=args.hnsw_ef_construction,
            )
            capacity = check_capacity(
                config, args.allow_100m, args.storage_budget_gib, args.memory_budget_gib
            )
            value = create_fixture(conn, config, capacity)
        elif args.command == "ground-truth":
            fixture = read_fixture(conn)
            conn.commit()
            value = ground_truth(conn, fixture, args.queries, args.top_k, args.selectivities)
        else:
            return run_sql(args, conn, dsn)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.exists():
            raise ValueError("refusing to overwrite existing artifact")
        write_json(args.output, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Seeded, bounded-memory fixtures and exact PostgreSQL/pgvector comparisons.

This is deliberately a database microbenchmark, not the BrainCore schema or API.
Only an explicitly acknowledged, benchmark-named database can be modified.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any

import psycopg
from psycopg import sql

from .performance_core import retrieval_scores

SCALES = {"100k": 100_000, "1m": 1_000_000, "10m": 10_000_000, "100m": 100_000_000}
SCHEMA = "braincore_perf"
TABLE = "braincore_perf.memories"
BUCKETS = 10_000
FORMAT_VERSION = 1


@dataclass(frozen=True)
class FixtureConfig:
    rows: int = 100_000
    dimensions: int = 384
    tenants: int = 100
    hot_tenant_fraction: float = 0.5
    seed: int = 20261002
    hnsw_m: int = 16
    hnsw_ef_construction: int = 64

    def validate(self) -> None:
        if not 1 <= self.rows <= 100_000_000 or not 1 <= self.dimensions <= 2000:
            raise ValueError("rows must be 1..100M and dimensions 1..2000")
        if self.tenants < 2 or not 0 < self.hot_tenant_fraction < 1:
            raise ValueError("at least two tenants and a hot fraction strictly between 0 and 1")
        if not 2 <= self.hnsw_m <= 100 or not 4 <= self.hnsw_ef_construction <= 1000:
            raise ValueError("invalid HNSW build settings")
        if self.hnsw_ef_construction < 2 * self.hnsw_m:
            raise ValueError("ef_construction must be at least twice m")


def check_capacity(
    config: FixtureConfig, allow_100m: bool, storage_gib: float | None, memory_gib: float | None
) -> dict[str, Any]:
    config.validate()
    vector_bytes = config.rows * config.dimensions * 4
    if config.rows >= 100_000_000 and (
        not allow_100m
        or storage_gib is None
        or memory_gib is None
        or not math.isfinite(storage_gib)
        or not math.isfinite(memory_gib)
        or storage_gib * 1024**3 < vector_bytes * 3
        or memory_gib <= 0
    ):
        raise ValueError(
            "100M requires --allow-100m, --memory-budget-gib and "
            "--storage-budget-gib at least three times raw vector bytes; "
            "these are operator budgets, not a capacity guarantee"
        )
    return {
        "raw_vector_bytes": vector_bytes,
        "storage_budget_gib": storage_gib,
        "memory_budget_gib": memory_gib,
        "capacity_verified": False,
        "estimate_excludes": ["row_overhead", "WAL", "temporary_build_space", "HNSW_graph"],
    }


def vector_literal(rng: random.Random, dimensions: int) -> str:
    values = [round(rng.uniform(-1, 1), 7) for _ in range(dimensions)]
    if not any(values):
        values[0] = 1.0
    return "[" + ",".join(str(value) for value in values) + "]"


def fixture_rows(config: FixtureConfig) -> Iterator[tuple[int, int, int, str]]:
    config.validate()
    rng = random.Random(config.seed)  # noqa: S311 - deterministic synthetic workload
    for row_id in range(1, config.rows + 1):
        tenant = (
            0 if rng.random() < config.hot_tenant_fraction else rng.randrange(1, config.tenants)
        )
        yield row_id, tenant, rng.randrange(BUCKETS), vector_literal(rng, config.dimensions)


def guard_database(conn: psycopg.Connection[Any], acknowledged: bool) -> None:
    if not acknowledged:
        raise ValueError("--acknowledge-disposable-database is required")
    row = conn.execute("SELECT current_database()").fetchone()
    if row is None or not re.fullmatch(r"braincore_benchmark(?:_[a-z0-9_]+)?", row[0]):
        raise ValueError(
            "database name must be braincore_benchmark or braincore_benchmark_<suffix>"
        )


def create_fixture(
    conn: psycopg.Connection[Any], config: FixtureConfig, capacity: dict[str, Any]
) -> dict[str, Any]:
    config.validate()
    existing = conn.execute("SELECT to_regnamespace(%s)", (SCHEMA,)).fetchone()
    assert existing is not None
    if existing[0] is not None:
        raise ValueError("fixture schema already exists; use a new disposable benchmark database")
    started = time.perf_counter()
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.execute("CREATE SCHEMA braincore_perf")
    conn.execute(
        sql.SQL(
            "CREATE TABLE braincore_perf.memories ("
            "id bigint PRIMARY KEY, tenant_id integer NOT NULL, scope_bucket integer NOT NULL, "
            "state text NOT NULL DEFAULT 'active', embedding vector({}) NOT NULL)"
        ).format(sql.Literal(config.dimensions))
    )
    digest = hashlib.sha256()
    with conn.cursor().copy(
        "COPY braincore_perf.memories (id,tenant_id,scope_bucket,embedding) FROM STDIN"
    ) as copy:
        for row in fixture_rows(config):
            copy.write_row(row)
            digest.update(("\t".join(map(str, row)) + "\n").encode())
    load_seconds = time.perf_counter() - started
    index_started = time.perf_counter()
    conn.execute(
        sql.SQL(
            "CREATE INDEX memories_embedding_hnsw ON braincore_perf.memories "
            "USING hnsw (embedding vector_cosine_ops) WITH (m={}, ef_construction={})"
        ).format(sql.Literal(config.hnsw_m), sql.Literal(config.hnsw_ef_construction))
    )
    index_seconds = time.perf_counter() - index_started
    conn.execute("ANALYZE braincore_perf.memories")
    row_count = conn.execute("SELECT count(*) FROM braincore_perf.memories").fetchone()
    assert row_count is not None
    actual_rows = row_count[0]
    if actual_rows != config.rows:
        raise RuntimeError("fixture row count mismatch")
    manifest = {
        "format_version": FORMAT_VERSION,
        "kind": "synthetic_pgvector_fixture",
        "generator": "python_random_mt19937_uniform_v1",
        "config": asdict(config),
        "corpus_sha256": digest.hexdigest(),
        "capacity": capacity,
        "actual_rows": actual_rows,
        "load_seconds": load_seconds,
        "index_build_seconds": index_seconds,
        "distribution": {
            "tenant_0": "hot tenant with configured probability",
            "other_tenants": "uniform among remaining tenants",
            "scope_bucket": "uniform integer 0..9999; independent of vectors and tenants",
            "vectors": "independent uniform components [-1,1], rounded to seven decimals",
            "state": "all active; vectors precomputed and non-null",
        },
    }
    conn.execute("CREATE TABLE braincore_perf.fixture_manifest (document jsonb NOT NULL)")
    conn.execute(
        "INSERT INTO braincore_perf.fixture_manifest VALUES (%s::jsonb)", (json.dumps(manifest),)
    )
    conn.commit()
    return manifest


def read_fixture(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    row = conn.execute("SELECT document FROM braincore_perf.fixture_manifest").fetchone()
    if row is None or row[0].get("format_version") != FORMAT_VERSION:
        raise ValueError("missing or unsupported fixture manifest")
    return dict(row[0])


def query_cases(
    config: FixtureConfig, count: int, k: int, selectivities: list[float]
) -> list[dict[str, Any]]:
    if count < 1 or not 1 <= k <= 1000 or not selectivities:
        raise ValueError("positive query count, k 1..1000, and selectivities are required")
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in selectivities):
        raise ValueError("selectivities must be finite fractions in [0,1]")
    rng = random.Random(config.seed + 1)  # noqa: S311
    cases = []
    for index in range(count):
        selectivity = selectivities[(index // 2) % len(selectivities)]
        cases.append(
            {
                "case_id": index,
                "tenant_id": 0 if index % 2 == 0 else 1 + index % (config.tenants - 1),
                "scope_threshold": math.floor(selectivity * BUCKETS),
                "requested_scope_selectivity": selectivity,
                "vector": vector_literal(rng, config.dimensions),
                "k": k,
            }
        )
    return cases


def query_sql(mode: str) -> str:
    if mode not in {"exact", "hnsw", "iterative"}:
        raise ValueError("unsupported retrieval mode")
    if mode == "exact":
        return (
            "SELECT id FROM braincore_perf.memories "
            "WHERE tenant_id=%s AND scope_bucket<%s AND state='active' AND embedding IS NOT NULL "
            "ORDER BY embedding <=> %s::vector, id LIMIT %s"
        )
    return (
        "SELECT id FROM braincore_perf.memories "
        "WHERE tenant_id=%s AND scope_bucket<%s AND state='active' AND embedding IS NOT NULL "
        "ORDER BY embedding <=> %s::vector LIMIT %s"
    )


def query_params(case: dict[str, Any]) -> tuple[Any, ...]:
    return case["tenant_id"], case["scope_threshold"], case["vector"], case["k"]


def mode_settings(mode: str, ef_search: int, max_scan_tuples: int) -> dict[str, str]:
    query_sql(mode)
    if not 1 <= ef_search <= 1000 or max_scan_tuples < 1:
        raise ValueError("ef_search must be 1..1000 and max_scan_tuples positive")
    return {
        "enable_indexscan": "off" if mode == "exact" else "on",
        "enable_bitmapscan": "off",
        "enable_seqscan": "on" if mode == "exact" else "off",
        "hnsw.ef_search": str(ef_search),
        "hnsw.iterative_scan": "strict_order" if mode == "iterative" else "off",
        "hnsw.max_scan_tuples": str(max_scan_tuples),
        "hnsw.scan_mem_multiplier": "1",
    }


def ground_truth(
    conn: psycopg.Connection[Any],
    fixture: dict[str, Any],
    count: int,
    k: int,
    selectivities: list[float],
) -> dict[str, Any]:
    cases = query_cases(FixtureConfig(**fixture["config"]), count, k, selectivities)
    with conn.transaction():
        for name, value in mode_settings("exact", 40, 20_000).items():
            conn.execute("SELECT set_config(%s,%s,true)", (name, value))
        for case in cases:
            eligible = conn.execute(
                "SELECT count(*) FROM braincore_perf.memories WHERE tenant_id=%s "
                "AND scope_bucket<%s AND state='active' AND embedding IS NOT NULL",
                (case["tenant_id"], case["scope_threshold"]),
            ).fetchone()
            assert eligible is not None
            case["eligible"] = eligible[0]
            case["expected_ids"] = [
                row[0] for row in conn.execute(query_sql("exact"), query_params(case)).fetchall()
            ]
            retrieval_scores(case["expected_ids"], case["expected_ids"], case["eligible"], k)
    return {
        "format_version": FORMAT_VERSION,
        "fixture_sha256": fixture["corpus_sha256"],
        "basis": "exact cosine over identical tenant/scope/state predicates; ties by id",
        "cases": cases,
    }


def validate_truth(truth: dict[str, Any], fixture: dict[str, Any]) -> list[dict[str, Any]]:
    if truth.get("fixture_sha256") != fixture["corpus_sha256"]:
        raise ValueError("ground truth belongs to a different corpus")
    if truth.get("format_version") != FORMAT_VERSION or not truth.get("cases"):
        raise ValueError("missing or unsupported ground truth")
    cases: list[dict[str, Any]] = truth["cases"]
    config = FixtureConfig(**fixture["config"])
    seen: set[int] = set()
    for case in cases:
        if (
            not isinstance(case["case_id"], int)
            or case["case_id"] in seen
            or not 0 <= case["tenant_id"] < config.tenants
            or not 0 <= case["scope_threshold"] <= BUCKETS
        ):
            raise ValueError("invalid query case")
        seen.add(case["case_id"])
        vector = json.loads(case["vector"])
        if len(vector) != config.dimensions or not all(math.isfinite(v) for v in vector):
            raise ValueError("invalid query vector")
        retrieval_scores(case["expected_ids"], case["expected_ids"], case["eligible"], case["k"])
    return cases


def explain_plans(
    conn: psycopg.Connection[Any],
    cases: list[dict[str, Any]],
    mode: str,
    ef_search: int,
    max_scan_tuples: int,
) -> list[dict[str, Any]]:
    output = []
    with conn.transaction():
        for name, value in mode_settings(mode, ef_search, max_scan_tuples).items():
            conn.execute("SELECT set_config(%s,%s,true)", (name, value))
        for case in cases:
            plan_row = conn.execute(
                "EXPLAIN (FORMAT JSON) " + query_sql(mode), query_params(case)
            ).fetchone()
            assert plan_row is not None
            plan = plan_row[0][0]["Plan"]
            nodes: list[dict[str, Any]] = []

            def visit(node: dict[str, Any], output_nodes: list[dict[str, Any]]) -> None:
                output_nodes.append(
                    {
                        "node_type": node["Node Type"],
                        "index_name": node.get("Index Name"),
                        "estimated_rows": node.get("Plan Rows"),
                    }
                )
                for child in node.get("Plans", []):
                    visit(child, output_nodes)

            visit(plan, nodes)
            used_hnsw = any(node["index_name"] == "memories_embedding_hnsw" for node in nodes)
            output.append(
                {
                    "case_id": case["case_id"],
                    "nodes": nodes,
                    "verified": not used_hnsw if mode == "exact" else used_hnsw,
                }
            )
    return output


async def execute_query(
    conn: psycopg.AsyncConnection[Any],
    case: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    # Worker connections have session settings and statement_timeout configured once.
    cursor = await conn.execute(query_sql(mode), query_params(case))
    ids = [row[0] for row in await cursor.fetchall()]
    return {
        "case_id": case["case_id"],
        "tenant_class": "hot" if case["tenant_id"] == 0 else "tail",
        "scope_threshold": case["scope_threshold"],
        "error": None,
        "degraded": False,
        **retrieval_scores(ids, case["expected_ids"], case["eligible"], case["k"]),
    }


def database_metrics(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    version = conn.execute(
        "SELECT current_setting('server_version_num'), "
        "(SELECT extversion FROM pg_extension WHERE extname='vector')"
    ).fetchone()
    sizes = conn.execute(
        "SELECT pg_relation_size('braincore_perf.memories'), "
        "pg_indexes_size('braincore_perf.memories'), "
        "pg_total_relation_size('braincore_perf.memories'), "
        "pg_relation_size('braincore_perf.memories_embedding_hnsw')"
    ).fetchone()
    stats = conn.execute(
        "SELECT blks_read,blks_hit,temp_bytes,deadlocks,xact_commit,xact_rollback "
        "FROM pg_stat_database WHERE datname=current_database()"
    ).fetchone()
    settings = conn.execute(
        "SELECT name,setting,unit FROM pg_settings WHERE name = ANY(%s)",
        (
            [
                "shared_buffers",
                "effective_cache_size",
                "work_mem",
                "maintenance_work_mem",
                "max_parallel_workers_per_gather",
                "max_connections",
                "jit",
            ],
        ),
    ).fetchall()
    assert version is not None and sizes is not None and stats is not None
    return {
        "postgres_version_num": version[0],
        "pgvector_version": version[1],
        "heap_bytes": sizes[0],
        "index_bytes": sizes[1],
        "total_relation_bytes": sizes[2],
        "hnsw_index_bytes": sizes[3],
        "database_counters": dict(
            zip(
                ["blocks_read", "blocks_hit", "temp_bytes", "deadlocks", "commits", "rollbacks"],
                stats,
                strict=True,
            )
        ),
        "postgres_settings": {
            name: {"value": value, "unit": unit} for name, value, unit in settings
        },
        "server_cpu_percent": None,
        "server_peak_rss_bytes": None,
        "embedding_lag_seconds": None,
        "replication_lag_seconds": None,
        "unavailable": {
            "server_cpu_percent": "not exposed by core PostgreSQL statistics",
            "server_peak_rss_bytes": "requires external server resource collection",
            "embedding_lag_seconds": "fixture vectors precomputed; no ingestion worker exercised",
            "replication_lag_seconds": "no replica topology assumed",
        },
    }

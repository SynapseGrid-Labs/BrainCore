import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from fastapi import HTTPException
from psycopg import sql
from psycopg.types.json import Jsonb

from braincore_memory import legacy_search
from braincore_memory.auth import key_digest
from braincore_memory.config import Settings
from braincore_memory.legacy_clients import configure_legacy_read, hydrate_client_attributes
from braincore_memory.legacy_lexical_projection import backfill_projection
from braincore_memory.legacy_search import (
    KINDS,
    candidate_query,
    configure_authenticated_readers,
    select_candidates,
)

from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source


def _credential(
    conn: psycopg.Connection[Any],
    tenant: UUID,
    *,
    roles: list[str] | None = None,
    scope: dict[str, str] | None = None,
    revoked: bool = False,
    expired: bool = False,
) -> str:
    key = "bck_legacy_test_" + uuid4().hex
    conn.execute(
        """INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope,
           revoked_at,expires_at) VALUES(%s,%s,%s,%s,%s,%s,
             CASE WHEN %s THEN now() ELSE NULL END,
             CASE WHEN %s THEN now()-interval '1 day' ELSE NULL END)""",
        (
            tenant,
            key,
            key,
            key_digest(key),
            roles or ["admin"],
            Jsonb(scope) if scope is not None else None,
            revoked,
            expired,
        ),
    )
    return key


def _seed(conn: psycopg.Connection[Any], tenant: UUID, kind: str, **overrides: Any) -> UUID:
    memory = uuid4()
    project = overrides.pop("project_id", "project/literal_%/nested")
    fields = {
        "id": memory,
        "tenant_id": tenant,
        "object_type": kind,
        "content": "signal",
        "dedupe_key": hashlib.sha256(memory.bytes).digest(),
        "project_id": project,
        "scope": Jsonb({"project_id": project}),
        "state": "active",
        "valid_from": datetime(2000, 1, 1, tzinfo=UTC),
        **overrides,
    }
    conn.execute(
        sql.SQL("INSERT INTO braincore.memories({}) VALUES({})").format(
            sql.SQL(",").join(sql.Identifier(name) for name in fields),
            sql.SQL(",").join(sql.Placeholder() for _ in fields),
        ),
        list(fields.values()),
    )
    return memory


@pytest.fixture
def search_source(typed_source: dict[str, Any]) -> Iterator[dict[str, Any]]:
    conn, source_id = typed_source["target"], typed_source["source_id"]
    assert hydrate_client_attributes(conn, source_id)["attribute_gate"] == "PASS"
    tenants = {
        row["original_tenant"]: row["tenant_id"]
        for row in conn.execute(
            "SELECT original_tenant,tenant_id FROM braincore.legacy_tenant_map WHERE source_id=%s",
            (source_id,),
        ).fetchall()
    }
    row = conn.execute(
        "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id", (uuid4().hex,)
    ).fetchone()
    tenants["unrelated"] = row["id"]
    credentials = {name: _credential(conn, tenant) for name, tenant in tenants.items()}
    assert backfill_projection(typed_source["dsn"], batch_size=2)["ready"] is True
    yield {**typed_source, "tenants": tenants, "credentials": credentials}


def _preserve_candidates(
    conn: psycopg.Connection[Any],
    kind: str,
    *,
    query: str | None = None,
    vector: str | None = None,
    scope: str | None = None,
    as_of: str | None = None,
    limit: int = 30,
    include_inactive: bool = False,
) -> list[dict[str, Any]]:
    clauses: list[sql.Composable] = [
        sql.SQL("m.tenant_id=ANY(braincore.compat_tenants())"),
        sql.SQL("braincore.compat_permits(m.scope)"),
        sql.SQL("m.object_type=%s"),
    ]
    parameters: list[Any] = [kind]
    if kind == "fact":
        clauses.append(sql.SQL("v.current_status='active'"))
    if scope is not None:
        clauses.append(sql.SQL("v.scope_path LIKE %s"))
        parameters.append(scope + "%")
    if as_of is not None and kind in {"fact", "memory"}:
        clauses.extend(
            [
                sql.SQL("(v.valid_from IS NULL OR v.valid_from<=%s::timestamptz)"),
                sql.SQL("(v.valid_to IS NULL OR v.valid_to>%s::timestamptz)"),
            ]
        )
        parameters.extend([as_of, as_of])
    if query is not None:
        score = sql.SQL("ts_rank_cd(v.fts,plainto_tsquery('english',%s))")
        clauses.append(sql.SQL("v.fts @@ plainto_tsquery('english',%s)"))
        parameters.append(query)
        first = query
        direction = sql.SQL("DESC")
    else:
        score = sql.SQL("v.embedding <=> %s::vector")
        clauses.append(sql.SQL("v.embedding IS NOT NULL"))
        first = vector
        direction = sql.SQL("ASC")
    statement = sql.SQL("""SELECT m.id,{score} AS score FROM {view} v
      JOIN braincore.memories m ON COALESCE(m.legacy_external_id,m.id)=v.{external}
      WHERE {where} ORDER BY {score} {direction} LIMIT %s""").format(
        score=score,
        view=sql.Identifier("preserve", kind),
        external=sql.Identifier(kind + "_id"),
        where=sql.SQL(" AND ").join(clauses),
        direction=direction,
    )
    return conn.execute(statement, [first, *parameters, first, limit]).fetchall()


def test_explicit_authenticated_readers_include_only_requested_tenants_and_reset(
    search_source: dict[str, Any],
) -> None:
    conn, tenants, credentials = (
        search_source["target"],
        search_source["tenants"],
        search_source["credentials"],
    )
    ids = {name: _seed(conn, tenant, "memory") for name, tenant in tenants.items()}
    with conn.transaction():
        readers = configure_authenticated_readers(conn, [credentials["alpha"], credentials["beta"]])
        visible = set(select_candidates(conn, "memory", query="signal"))
        assert visible == {ids["alpha"], ids["beta"]}
        assert conn.execute("SELECT braincore.compat_tenants() AS ids").fetchone()["ids"] == [
            tenants["alpha"],
            tenants["beta"],
        ]
        configure_legacy_read(conn, readers[0])
        assert select_candidates(conn, "memory", query="signal") == [ids["alpha"]]
        assert conn.execute("SELECT braincore.compat_tenants() AS ids").fetchone()["ids"] == [
            tenants["alpha"]
        ]
    project = "project/literal_%/nested"
    scoped = [
        _credential(conn, tenants[name], roles=["read"], scope={"project_id": project})
        for name in ("alpha", "beta")
    ]
    with conn.transaction():
        configure_authenticated_readers(conn, scoped)
        assert set(select_candidates(conn, "memory", query="signal")) == {ids["alpha"], ids["beta"]}


def test_invalid_revoked_expired_write_only_and_mismatched_scopes_are_refused(
    search_source: dict[str, Any],
) -> None:
    conn, tenants = search_source["target"], search_source["tenants"]
    valid = search_source["credentials"]["alpha"]
    for invalid in (
        "unregistered_key",
        _credential(conn, tenants["alpha"], revoked=True),
        _credential(conn, tenants["alpha"], expired=True),
    ):
        with pytest.raises(ValueError, match="unavailable"), conn.transaction():
            configure_authenticated_readers(conn, [valid, invalid])
    only_write = _credential(conn, tenants["alpha"], roles=["write"], scope={"user_id": "alice"})
    with pytest.raises(HTTPException), conn.transaction():
        configure_authenticated_readers(conn, [only_write])
    mismatched = [
        _credential(conn, tenants[name], roles=["read"], scope={"user_id": user})
        for name, user in (("alpha", "alice"), ("beta", "bob"))
    ]
    with pytest.raises(ValueError, match="different reader scopes"), conn.transaction():
        configure_authenticated_readers(conn, mismatched)
    for credentials in ([], [valid] * 5):
        with pytest.raises(ValueError, match="one to four"), conn.transaction():
            configure_authenticated_readers(conn, credentials)


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_projected_fts_matches_compatibility_view_rank_and_eligibility(
    search_source: dict[str, Any],
    kind: str,
) -> None:
    conn, tenants = search_source["target"], search_source["tenants"]
    for repetitions in (5, 3, 1):
        _seed(conn, tenants["alpha"], kind, content=" ".join(["signal"] * repetitions))
    for fields in (
        {"state": "suppressed", "content": "signal " * 9},
        {"valid_from": datetime(2100, 1, 1, tzinfo=UTC), "content": "signal " * 8},
        {"valid_until": datetime(2010, 1, 1, tzinfo=UTC), "content": "signal " * 7},
        {"project_id": "elsewhere", "content": "signal " * 6},
        {"legacy_fts_eligible": False, "content": "signal " * 10},
        {
            "legacy_source_id": search_source["source_id"],
            "legacy_record_key": Jsonb({"test": "unready"}),
            "legacy_external_id": uuid4(),
            "content": "signal " * 11,
        },
    ):
        _seed(conn, tenants["alpha"], kind, **fields)
    _seed(conn, tenants["unrelated"], kind, content="signal " * 12)
    for conditions in (
        {"query": "signal", "limit": 2},
        {"query": "signal", "scope": "project/", "limit": 30},
        {"query": "signal", "as_of": "2020-01-01T00:00:00Z", "limit": 30},
        {"query": "signal", "as_of": "2020-01-01T00:00:00Z", "include_inactive": True},
    ):
        with conn.transaction():
            configure_authenticated_readers(
                conn,
                [search_source["credentials"]["alpha"]],
                include_inactive=conditions.get("include_inactive", False),
            )
            statement, parameters = candidate_query(kind, **conditions)
            actual = conn.execute(statement, parameters).fetchall()
            expected = _preserve_candidates(conn, kind, **conditions)
            assert actual == expected
            assert select_candidates(conn, kind, **conditions) == [row["id"] for row in expected]


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_current_override_fts_and_bounded_hydration_preserve_external_identity(
    search_source: dict[str, Any],
    kind: str,
) -> None:
    conn, tenant = search_source["target"], search_source["tenants"]["alpha"]
    memory = _seed(conn, tenant, kind, content="canonical signal", legacy_attributes_ready=True)
    conn.execute(
        """UPDATE braincore.memories SET legacy_content_hash=public.digest(content,'sha256'),
           legacy_search_vector=to_tsvector('english','oldneedle') WHERE id=%s""",
        (memory,),
    )
    conn.execute(
        """INSERT INTO braincore.legacy_client_attributes
           (memory_id,tenant_id,attributes,content_hash,search_vector,current_override)
           SELECT id,tenant_id,'{}'::jsonb,public.digest(content,'sha256'),
             to_tsvector('english','newneedle'),true FROM braincore.memories WHERE id=%s""",
        (memory,),
    )
    with conn.transaction():
        configure_authenticated_readers(conn, [search_source["credentials"]["alpha"]])
        for query in ("oldneedle", "newneedle", "canonical signal"):
            actual = select_candidates(conn, kind, query=query)
            expected = [row["id"] for row in _preserve_candidates(conn, kind, query=query)]
            assert actual == expected
            assert (memory in actual) is (query == "newneedle")
        imported = conn.execute(
            "SELECT id FROM braincore.memories WHERE object_type=%s AND legacy_external_id=%s",
            (kind, search_source["same_external"]),
        ).fetchone()["id"]
        unrelated = _seed(conn, search_source["tenants"]["unrelated"], kind)
        suppressed = _seed(conn, tenant, kind, state="suppressed")
        unready = _seed(
            conn,
            tenant,
            kind,
            legacy_source_id=search_source["source_id"],
            legacy_record_key=Jsonb({"test": "hydrate-unready"}),
            legacy_external_id=uuid4(),
        )
        outside_scope = _seed(conn, tenant, kind, project_id="elsewhere")
        helper = sql.Identifier("braincore", "compat_hydrate_" + kind)
        rows = conn.execute(
            sql.SQL("SELECT * FROM {}(%s::uuid[])").format(helper),
            ([imported, unrelated, suppressed, unready],),
        ).fetchall()
        assert len(rows) == 1 and rows[0][kind + "_id"] == search_source["same_external"]
        assert (
            conn.execute(sql.SQL("SELECT * FROM {}(%s::uuid[])").format(helper), ([],)).fetchall()
            == []
        )

        scoped_key = _credential(
            conn, tenant, roles=["read"], scope={"project_id": "project/literal_%/nested"}
        )
        configure_authenticated_readers(conn, [scoped_key])
        rows = conn.execute(
            sql.SQL("SELECT * FROM {}(%s::uuid[])").format(helper),
            ([imported, outside_scope],),
        ).fetchall()
        assert len(rows) == 1 and rows[0][kind + "_id"] == search_source["same_external"]


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_vector_query_shape_keeps_index_ordering(kind: str) -> None:
    statement, _ = candidate_query(kind, vector="[1]", limit=2)
    ordering = statement.as_string().split("ORDER BY", 1)[1]
    assert "embedding" in ordering and "<=> %s::vector ASC LIMIT %s" in ordering
    assert "," not in ordering


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_vector_candidates_match_tiny_exact_eligible_truth_and_use_bare_order(
    search_source: dict[str, Any],
    kind: str,
) -> None:
    conn, tenants = search_source["target"], search_source["tenants"]
    query_vector = "[" + ",".join(["1"] + ["0"] * 383) + "]"
    first = "[" + ",".join(["0.8", "0.6"] + ["0"] * 382) + "]"
    second = "[" + ",".join(["0.6", "0.8"] + ["0"] * 382) + "]"
    winners = [
        _seed(conn, tenants["alpha"], kind, embedding=vector, embedding_state="ready")
        for vector in (first, second)
    ]
    for tenant, fields in (
        (tenants["unrelated"], {}),
        (tenants["alpha"], {"state": "suppressed"}),
        (tenants["alpha"], {"project_id": "elsewhere"}),
        (tenants["alpha"], {"embedding_state": "pending"}),
        (
            tenants["alpha"],
            {
                "legacy_source_id": search_source["source_id"],
                "legacy_record_key": Jsonb({"test": "vector-unready"}),
                "legacy_external_id": uuid4(),
            },
        ),
    ):
        _seed(
            conn, tenant, kind, **{"embedding": query_vector, "embedding_state": "ready", **fields}
        )
    conditions = {"vector": query_vector, "scope": "project/", "limit": 2}
    with conn.transaction():
        configure_authenticated_readers(conn, [search_source["credentials"]["alpha"]])
        statement, _ = candidate_query(kind, **conditions)
        text = statement.as_string()
        assert "scoped_eligible AS MATERIALIZED" in text and "LIMIT 1001" in text
        assert "ORDER BY (CASE WHEN m.object_type =" in text
        assert "::vector(384) <=> %s::vector ASC LIMIT %s" in text
        exact = _preserve_candidates(conn, kind, **conditions)
        assert [row["id"] for row in exact] == winners
        assert select_candidates(conn, kind, **conditions) == winners


_QUERY_VECTOR = "[" + ",".join(["1"] + ["0"] * 383) + "]"


def _vector_rows(
    conn: psycopg.Connection[Any], tenant: UUID, kind: str, project: str, count: int
) -> list[UUID]:
    rows = conn.execute(
        """INSERT INTO braincore.memories
           (id,tenant_id,object_type,content,dedupe_key,project_id,legacy_scope_path,
            scope,state,valid_from,embedding,embedding_state)
           SELECT id,%s,%s,'bounded vector fixture',public.digest(id::text,'sha256'),
             %s,'different/source/path',jsonb_build_object('project_id',%s::text),
             'active','2000-01-01',
             (ARRAY[cos(angle)::real,sin(angle)::real]||array_fill(0::real,ARRAY[382]))::vector,
             'ready'
           FROM (SELECT gen_random_uuid() AS id,0.01+n::float8/(%s+2) AS angle
                 FROM generate_series(1,%s) n) fixture
           RETURNING id""",
        (tenant, kind, project, project, count, count),
    ).fetchall()
    return [row["id"] for row in rows]


def _plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from _plan_nodes(child)


def test_scoped_vector_query_bounds_exact_set_and_shares_ann_eligibility() -> None:
    statement, parameters = candidate_query(
        "memory", vector="[1]", scope="project/", as_of="2020-01-01", limit=9
    )
    text = statement.as_string()
    assert "scoped_eligible AS MATERIALIZED" in text
    assert "LIMIT 1001" in text
    assert "(SELECT n FROM eligible_count)<=1000" in text
    assert "(SELECT n FROM eligible_count)>1000" in text
    assert text.count('"m"."project_id" LIKE %s') == 2
    assert text.count("braincore.compat_permits") == 2
    assert text.count("braincore.compat_tenants()") == 2
    literal_type = (
        sql.SQL("{} = {}")
        .format(sql.Identifier("m", "object_type"), sql.Literal("memory"))
        .as_string()
    )
    assert text.count(literal_type) == 2
    assert parameters.count("project/%") == 2
    assert "ORDER BY (CASE WHEN m.object_type =" in text
    assert "::vector(384) <=> %s::vector ASC LIMIT %s" in text


def test_sparse_scoped_vector_fills_exact_order_and_uses_project_id(
    search_source: dict[str, Any],
) -> None:
    conn, tenant = search_source["target"], search_source["tenants"]["alpha"]
    project = "bounded/sparse/"
    eligible = _vector_rows(conn, tenant, "segment", project, 81)
    _vector_rows(conn, search_source["tenants"]["unrelated"], "segment", project, 10)
    _vector_rows(conn, tenant, "fact", project, 10)
    _vector_rows(conn, tenant, "segment", "elsewhere/", 10)
    with conn.transaction():
        configure_authenticated_readers(conn, [search_source["credentials"]["alpha"]])
        expected = conn.execute(
            "SELECT id FROM braincore.memories WHERE id=ANY(%s::uuid[]) "
            "ORDER BY embedding <=> %s::vector LIMIT 9",
            (eligible, _QUERY_VECTOR),
        ).fetchall()
        actual = select_candidates(conn, "segment", vector=_QUERY_VECTOR, scope=project, limit=9)
        assert actual == [row["id"] for row in expected]
        assert len(actual) == 9
        assert select_candidates(
            conn, "segment", vector=_QUERY_VECTOR, scope=project, limit=100
        ) == [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM braincore.memories WHERE id=ANY(%s::uuid[]) "
                "ORDER BY embedding <=> %s::vector",
                (eligible, _QUERY_VECTOR),
            ).fetchall()
        ]
        assert (
            select_candidates(conn, "segment", vector=_QUERY_VECTOR, scope="nonexistent/", limit=9)
            == []
        )
        index = conn.execute(
            "SELECT pg_get_indexdef(indexrelid) AS definition,indisvalid,indisready "
            "FROM pg_index WHERE indexrelid='braincore.memories_compat_project_scope_idx'::regclass"
        ).fetchone()
        assert index["indisvalid"] and index["indisready"]
        assert "project_id text_pattern_ops" in index["definition"]
        assert "legacy_scope_path" not in index["definition"]


@pytest.mark.parametrize("count", [1000, 1001])
def test_scoped_vector_exact_threshold_executes_only_selected_branch(
    search_source: dict[str, Any], count: int
) -> None:
    conn, tenant = search_source["target"], search_source["tenants"]["alpha"]
    project = "bounded/threshold/"
    _vector_rows(conn, tenant, "memory", project, count)
    with conn.transaction():
        configure_authenticated_readers(conn, [search_source["credentials"]["alpha"]])
        statement, parameters = candidate_query(
            "memory", vector=_QUERY_VECTOR, scope=project, limit=9
        )
        # Only the disposable fixture executes this bounded query. Actual loops
        # prove the sentinel activates ANN rather than sorting a truncated set.
        plan = conn.execute(
            sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) ") + statement, parameters
        ).fetchone()["QUERY PLAN"][0]["Plan"]
        exact = [node for node in _plan_nodes(plan) if node.get("Alias") == "e"]
        assert len(exact) == 1
        assert exact[0]["Actual Loops"] == (1 if count == 1000 else 0)
        eligible = [
            node for node in _plan_nodes(plan) if node.get("Subplan Name") == "CTE scoped_eligible"
        ]
        assert len(eligible) == 1 and eligible[0]["Actual Rows"] == count
        assert (
            len(select_candidates(conn, "memory", vector=_QUERY_VECTOR, scope=project, limit=9))
            == 9
        )


@pytest.mark.parametrize("kind", ["fact", "memory"])
def test_scoped_vector_preserves_authorization_history_and_projection_readiness(
    search_source: dict[str, Any], kind: str
) -> None:
    conn, tenants = search_source["target"], search_source["tenants"]
    project = "bounded/history/"
    eligible = _vector_rows(conn, tenants["alpha"], kind, project, 1)[0]
    inactive = _seed(
        conn,
        tenants["alpha"],
        kind,
        project_id=project,
        state="suppressed",
        embedding=_QUERY_VECTOR,
        embedding_state="ready",
    )
    for fields in (
        {"valid_until": datetime(2010, 1, 1, tzinfo=UTC)},
        {"valid_from": datetime(2030, 1, 1, tzinfo=UTC)},
        {"embedding_state": "pending"},
        {
            "legacy_source_id": search_source["source_id"],
            "legacy_record_key": Jsonb({"test": "adaptive-unready"}),
            "legacy_external_id": uuid4(),
        },
    ):
        _seed(
            conn,
            tenants["alpha"],
            kind,
            **{
                "project_id": project,
                "embedding": _QUERY_VECTOR,
                "embedding_state": "ready",
                **fields,
            },
        )
    _vector_rows(conn, tenants["beta"], kind, project, 1)
    _vector_rows(conn, tenants["alpha"], kind, "outside/auth/", 1)
    key = _credential(conn, tenants["alpha"], roles=["read"], scope={"project_id": project})
    with conn.transaction():
        configure_authenticated_readers(conn, [key], include_inactive=True)
        conditions = {
            "vector": _QUERY_VECTOR,
            "scope": "bounded/",
            "as_of": "2020-01-01",
            "limit": 9,
            "include_inactive": True,
        }
        actual = select_candidates(conn, kind, **conditions)
        assert set(actual) == ({eligible, inactive} if kind == "memory" else {eligible})
        configure_authenticated_readers(conn, [key])
        assert select_candidates(
            conn,
            kind,
            vector=_QUERY_VECTOR,
            scope="bounded/",
            as_of="2020-01-01",
            limit=9,
        ) == [eligible]


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_typed_vector_partial_index_supports_generic_prepared_plan_and_sparse_fill(
    search_source: dict[str, Any], kind: str
) -> None:
    conn, tenant = search_source["target"], search_source["tenants"]["alpha"]
    eligible = _vector_rows(conn, tenant, kind, "typed/", 81)
    other_kind = "fact" if kind != "fact" else "memory"
    _vector_rows(conn, tenant, other_kind, "typed/", 1000)
    for owner, fields in (
        (search_source["tenants"]["unrelated"], {}),
        (tenant, {"state": "suppressed"}),
        (tenant, {"embedding_state": "pending"}),
        (tenant, {"project_id": "outside/auth/"}),
        (
            tenant,
            {
                "legacy_source_id": search_source["source_id"],
                "legacy_record_key": Jsonb({"test": "typed-unready"}),
                "legacy_external_id": uuid4(),
            },
        ),
    ):
        _seed(
            conn,
            owner,
            kind,
            **{
                "project_id": "typed/",
                "embedding": _QUERY_VECTOR,
                "embedding_state": "ready",
                **fields,
            },
        )
    key = _credential(conn, tenant, roles=["read"], scope={"project_id": "typed/"})
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        conn.execute("ANALYZE braincore.memories")  # Disposable fixture only.
        conn.execute("SET LOCAL plan_cache_mode=force_generic_plan")
        conn.execute("SET LOCAL enable_seqscan=off")
        conn.execute("SET LOCAL enable_bitmapscan=off")
        conn.execute("SET LOCAL enable_sort=off")
        conn.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
        conn.execute("SET LOCAL hnsw.ef_search=1000")
        statement, values = candidate_query(kind, vector=_QUERY_VECTOR, limit=9)
        literal_type = (
            sql.SQL("{} = {}")
            .format(sql.Identifier("m", "object_type"), sql.Literal(kind))
            .as_string()
        )
        assert literal_type in statement.as_string()
        parts = statement.as_string().split("%s")
        prepared = "".join(
            part + (f"${i + 1}" if i < len(parts) - 1 else "") for i, part in enumerate(parts)
        )
        name = sql.Identifier("typed_vector_" + uuid4().hex)
        conn.execute(sql.SQL("PREPARE {} AS {}").format(name, sql.SQL(prepared)))
        try:
            execution = sql.SQL("EXECUTE {} ({})").format(
                name, sql.SQL(",").join(sql.Literal(value) for value in values)
            )
            plan = conn.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + execution).fetchone()[
                "QUERY PLAN"
            ][0]["Plan"]
            indexes = {node["Index Name"] for node in _plan_nodes(plan) if "Index Name" in node}
            assert "memories_embedding_" + kind + "_idx" in indexes
            assert "memories_embedding_idx" not in indexes
            actual = [row["id"] for row in conn.execute(execution).fetchall()]
            expected = [
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM braincore.memories WHERE id=ANY(%s::uuid[]) "
                    "ORDER BY embedding <=> %s::vector LIMIT 9",
                    (eligible, _QUERY_VECTOR),
                ).fetchall()
            ]
            assert actual == expected and len(actual) == 9
        finally:
            conn.execute(sql.SQL("DEALLOCATE {}").format(name))


@pytest.mark.parametrize("override", [None, 81])
def test_compatibility_vector_settings_use_default_or_override_and_reset(
    search_source: dict[str, Any], override: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BRAINCORE_SEARCH_HNSW_EF_SEARCH", raising=False)
    settings = (
        Settings(_env_file=None)
        if override is None
        else Settings(_env_file=None, search_hnsw_ef_search=override)
    )
    monkeypatch.setattr(legacy_search, "get_settings", lambda: settings)
    conn = search_source["target"]
    conn.execute("SELECT '[1,0,0]'::vector")
    baseline = conn.execute("SELECT current_setting('hnsw.ef_search') AS ef").fetchone()["ef"]
    with conn.transaction():
        configure_authenticated_readers(conn, [search_source["credentials"]["alpha"]])
        select_candidates(conn, "segment", vector=_QUERY_VECTOR, limit=9)
        assert conn.execute("SELECT current_setting('hnsw.ef_search') AS ef").fetchone()[
            "ef"
        ] == str(1000 if override is None else override)
    assert (
        conn.execute("SELECT current_setting('hnsw.ef_search') AS ef").fetchone()["ef"] == baseline
    )

"""Indexed candidate selection for explicitly authenticated compatibility readers."""

import json
from collections.abc import Sequence
from typing import Any, Literal
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .auth import Principal, key_digest
from .config import get_settings
from .legacy_clients import configure_legacy_read

LegacyKind = Literal["fact", "memory", "segment", "episode"]
KINDS = {"fact", "memory", "segment", "episode"}


def configure_authenticated_readers(
    conn: psycopg.Connection[Any], credentials: Sequence[str], *, include_inactive: bool = False
) -> tuple[Principal, ...]:
    """Validate every key; refuse differing scopes rather than widening access."""
    if not 1 <= len(credentials) <= 4:
        raise ValueError("one to four explicit reader credentials required")
    readers: list[Principal] = []
    for credential in credentials:
        row = conn.execute(
            "SELECT id,tenant_id,roles,scope FROM braincore.api_keys WHERE digest=%s "
            "AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now())",
            (key_digest(credential),),
        ).fetchone()
        if row is None:
            raise ValueError("compatibility reader credential unavailable")
        principal = Principal(row["id"], row["tenant_id"], frozenset(row["roles"]), row["scope"])
        principal.require("read")
        readers.append(principal)
    if any(p.scope != readers[0].scope for p in readers):
        raise ValueError("different reader scopes cannot be aggregated")
    if any(("admin" in p.roles) != ("admin" in readers[0].roles) for p in readers):
        raise ValueError("different reader scope privileges cannot be aggregated")
    configure_legacy_read(conn, readers[0], include_inactive=include_inactive)
    conn.execute(
        "SELECT set_config('braincore.compat_read_tenants',%s,true)",
        (json.dumps(list(dict.fromkeys(str(p.tenant_id) for p in readers))),),
    )
    return tuple(readers)


def candidate_query(
    object_type: LegacyKind,
    *,
    query: str | None = None,
    vector: str | None = None,
    scope: str | None = None,
    as_of: str | None = None,
    limit: int = 30,
    include_inactive: bool = False,
) -> tuple[sql.Composed, list[Any]]:
    """Preserve legacy rank/path/validity semantics and per-type candidate limits."""
    if object_type not in KINDS or not 1 <= limit <= 300:
        raise ValueError("invalid legacy candidate type or budget")
    if (query is None) == (vector is None):
        raise ValueError("exactly one lexical query or embedding is required")
    alias = "l" if query is not None else "m"

    def field(name: str) -> sql.Identifier:
        return sql.Identifier(alias, name)

    clauses: list[sql.Composable] = [
        sql.SQL("{} = ANY(braincore.compat_tenants())").format(field("tenant_id")),
        sql.SQL("braincore.compat_permits({})").format(field("scope")),
        # A trusted finite type literal lets generic prepared plans prove the
        # matching partial vector-index predicate. Authorization remains bound.
        sql.SQL("{} = {}").format(field("object_type"), sql.Literal(object_type)),
    ]
    values: list[Any] = []
    # Historical facts still require current_status=active in the original
    # stream; historical other types allow inactive rows through the views.
    if object_type == "fact" or not include_inactive:
        clauses.append(sql.SQL("{}='active'").format(field("state")))
    if scope is not None:
        clauses.append(sql.SQL("{} LIKE %s").format(field("project_id")))
        values.append(scope + "%")
    if as_of is not None and object_type in {"fact", "memory"}:
        clauses.extend(
            [
                sql.SQL("({} IS NULL OR {}<=%s::timestamptz)").format(
                    field("valid_from"), field("valid_from")
                ),
                sql.SQL("({} IS NULL OR {}>%s::timestamptz)").format(
                    field("valid_until"), field("valid_until")
                ),
            ]
        )
        values.extend([as_of, as_of])
    score: sql.Composable
    if query is not None:
        relation = sql.SQL("braincore.memories_lexical l")
        clauses.extend(
            [sql.SQL("l.compat_ready"), sql.SQL("l.compat_fts @@ plainto_tsquery('english',%s)")]
        )
        values.append(query)
        score = sql.SQL("ts_rank_cd(l.compat_fts,plainto_tsquery('english',%s))")
        first_value = query
        direction = sql.SQL("DESC")
    else:
        relation = sql.SQL("braincore.memories m")
        clauses.extend(
            [
                sql.SQL("m.embedding_state='ready' AND m.embedding IS NOT NULL"),
                sql.SQL(
                    "(m.legacy_source_id IS NULL OR m.legacy_attributes_ready OR EXISTS "
                    "(SELECT 1 FROM braincore.legacy_client_attributes x "
                    "WHERE x.memory_id=m.id AND x.tenant_id=m.tenant_id))"
                ),
            ]
        )
        # The typed CASE expression matches only the corresponding type graph,
        # so the planner cannot choose the global HNSW postfilter path instead.
        score = sql.SQL(
            "(CASE WHEN m.object_type = {kind} THEN m.embedding ELSE NULL END)"
            "::vector(384) <=> %s::vector"
        ).format(kind=sql.Literal(object_type))
        assert vector is not None
        first_value = vector
        direction = sql.SQL("ASC")
    statement = sql.SQL(
        "SELECT {id} AS id,{score} AS score FROM {relation} "
        "WHERE {where} ORDER BY {score} {direction} LIMIT %s"
    ).format(
        id=field("id"),
        score=score,
        relation=relation,
        where=sql.SQL(" AND ").join(clauses),
        direction=direction,
    )
    if vector is not None and scope is not None:
        return _scoped_vector_query(
            sql.SQL(" AND ").join(clauses), values, vector, limit, indexed_distance=score
        )
    return statement, [first_value, *values, first_value, limit]


def _scoped_vector_query(
    eligibility: sql.Composed,
    values: list[Any],
    vector: str,
    limit: int,
    *,
    indexed_distance: sql.Composable,
) -> tuple[sql.Composed, list[Any]]:
    # One snapshot decides whether ALL eligible rows fit the bounded exact path.
    # An overflow sentinel is never used as an approximate candidate pre-limit.
    # Both branches use the very same authenticated eligibility expression.
    statement = sql.SQL("""
      WITH scoped_eligible AS MATERIALIZED (
        SELECT m.id,m.embedding FROM braincore.memories m
        WHERE {eligibility} LIMIT 1001
      ), eligible_count AS MATERIALIZED (
        SELECT count(*) AS n FROM scoped_eligible
      )
      SELECT id,score FROM (
        (SELECT e.id,e.embedding <=> %s::vector AS score FROM scoped_eligible e
         WHERE (SELECT n FROM eligible_count)<=1000
         ORDER BY e.embedding <=> %s::vector ASC LIMIT %s)
        UNION ALL
        (SELECT m.id,{indexed_distance} AS score FROM braincore.memories m
         WHERE {eligibility} AND (SELECT n FROM eligible_count)>1000
         ORDER BY {indexed_distance} ASC LIMIT %s)
      ) candidates ORDER BY score ASC
    """).format(eligibility=eligibility, indexed_distance=indexed_distance)
    return statement, [*values, vector, vector, limit, vector, *values, vector, limit]


def select_candidates(
    conn: psycopg.Connection[Any], object_type: LegacyKind, **conditions: Any
) -> list[UUID]:
    if conditions.get("query") is not None:
        ready = conn.execute(
            "SELECT ready FROM braincore.legacy_lexical_projection_state WHERE singleton"
        ).fetchone()
        if not ready or not ready["ready"]:
            raise ValueError("legacy lexical projection unavailable")
    else:
        version = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname='vector'"
        ).fetchone()
        if not version or tuple(int(x) for x in version["extversion"].split(".")[:2]) < (0, 8):
            raise ValueError("iterative vector capability unavailable")
        conn.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
        conn.execute(
            "SELECT set_config('hnsw.ef_search',%s,true)",
            (str(get_settings().search_hnsw_ef_search),),
        )
    statement, values = candidate_query(object_type, **conditions)
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(statement, values)
        return [row["id"] for row in cursor.fetchall()]


def _require_projection(conn: psycopg.Connection[Any]) -> None:
    conn.execute("SELECT braincore.require_legacy_search_projection()")


def _fact_filters(
    scope: str | None, as_of: str | None, alias: str
) -> tuple[sql.Composed, list[Any]]:
    def field(name: str) -> sql.Identifier:
        return sql.Identifier(alias, name)

    clauses: list[sql.Composable] = [sql.SQL("{}='active'").format(field("current_status"))]
    values: list[Any] = []
    if as_of is not None:
        clauses.extend(
            [
                sql.SQL("({} IS NULL OR {}<=%s::timestamptz)").format(
                    field("valid_from"), field("valid_from")
                ),
                sql.SQL("({} IS NULL OR {}>%s::timestamptz)").format(
                    field("valid_to"), field("valid_to")
                ),
            ]
        )
        values.extend([as_of, as_of])
    if scope is not None:
        clauses.append(sql.SQL("{} LIKE %s").format(field("scope_path")))
        values.append(scope + "%")
    return sql.SQL(" AND ").join(clauses), values


def compatibility_relation(object_type: Literal["fact", "entity"]) -> sql.Composed:
    """Trusted query fragment with explicit visibility before indexed joins.

    Security-barrier views remain available for arbitrary SQL clients. The
    fixed application query uses the same guards directly so PostgreSQL can
    push equality joins into the compact relation's subject/external indexes.
    Callers must first configure authenticated readers and check readiness.
    """
    if object_type == "entity":
        fields = sql.SQL(
            "l.id AS native_id,l.tenant_id,l.compat_external_id AS entity_id,"
            "l.compat_entity_name AS canonical_name,l.compat_aliases AS aliases"
        )
    elif object_type == "fact":
        fields = sql.SQL(
            "l.id AS native_id,l.tenant_id,l.compat_external_id AS fact_id,"
            "l.compat_subject_entity_id AS subject_entity_id,"
            "l.compat_object_entity_id AS object_entity_id,l.compat_episode_id AS episode_id,"
            "l.compat_confidence AS confidence,l.compat_last_seen_at AS last_seen_at,"
            "l.valid_from,l.valid_until AS valid_to,l.project_id AS scope_path,"
            "l.compat_current_status AS current_status"
        )
    else:
        raise ValueError("invalid compatibility relation")
    return sql.SQL(
        "SELECT {fields} FROM braincore.memories_lexical l WHERE l.object_type={kind} "
        "AND l.tenant_id=ANY(braincore.compat_tenants()) AND braincore.compat_permits(l.scope) "
        "AND (l.state='active' OR "
        "current_setting('braincore.compat_include_inactive',true)='true') "
        "AND l.compat_ready"
    ).format(fields=fields, kind=sql.Literal(object_type))


def select_structured_candidates(
    conn: psycopg.Connection[Any],
    query: str,
    *,
    scope: str | None = None,
    as_of: str | None = None,
    limit: int = 30,
) -> list[UUID]:
    """Choose facts before wide legacy hydration, preserving original name/rank rules."""
    if not 1 <= limit <= 300:
        raise ValueError("invalid structured candidate budget")
    _require_projection(conn)
    where, values = _fact_filters(scope, as_of, "f")
    statement = sql.SQL(
        "WITH matched_entities AS MATERIALIZED ("
        "SELECT entity_id FROM ({entities}) e "
        "WHERE canonical_name ILIKE %s OR aliases::text ILIKE %s LIMIT 20) "
        "SELECT f.native_id AS id FROM ({facts}) f "
        "JOIN matched_entities me ON f.subject_entity_id=me.entity_id "
        "WHERE {where} ORDER BY f.confidence DESC,f.last_seen_at DESC NULLS LAST LIMIT %s"
    ).format(
        where=where, entities=compatibility_relation("entity"), facts=compatibility_relation("fact")
    )
    pattern = "%" + query + "%"
    return [r["id"] for r in conn.execute(statement, [pattern, pattern, *values, limit])]


def select_related_candidates(
    conn: psycopg.Connection[Any],
    *,
    fact_ids: Sequence[str] = (),
    episode_ids: Sequence[str] = (),
    scope: str | None = None,
    as_of: str | None = None,
    limit: int = 20,
) -> list[UUID]:
    """Bound legacy relation expansion before payload hydration; retain unordered LIMIT."""
    if (
        bool(fact_ids) == bool(episode_ids)
        or not 1 <= limit <= 300
        or len(fact_ids) + len(episode_ids) > 1200
    ):
        raise ValueError("one bounded relation input is required")
    _require_projection(conn)
    where, values = _fact_filters(scope, as_of, "f2")
    if fact_ids:
        statement = sql.SQL(
            "SELECT DISTINCT f2.native_id AS id FROM ({facts}) f1 "
            "JOIN ({facts}) f2 "
            "ON f2.subject_entity_id=f1.subject_entity_id AND f2.fact_id<>f1.fact_id "
            "WHERE f1.fact_id=ANY(%s::uuid[]) AND {where} LIMIT %s"
        ).format(where=where, facts=compatibility_relation("fact"))
        identifiers = [UUID(x) for x in fact_ids]
    else:
        statement = sql.SQL(
            "SELECT f2.native_id AS id FROM ({facts}) f2 "
            "WHERE f2.episode_id=ANY(%s::uuid[]) AND {where} LIMIT %s"
        ).format(where=where, facts=compatibility_relation("fact"))
        identifiers = [UUID(x) for x in episode_ids]
    return [r["id"] for r in conn.execute(statement, [identifiers, *values, limit])]


def resolve_visible_fact_ids(conn: psycopg.Connection[Any], fact_ids: Sequence[str]) -> list[UUID]:
    """Resolve only authenticated visible fact aliases for evidence hydration."""
    if len(fact_ids) > 1200:
        raise ValueError("evidence input exceeds legacy search budget")
    _require_projection(conn)
    return [
        r["id"]
        for r in conn.execute(
            sql.SQL("SELECT native_id AS id FROM ({facts}) f WHERE fact_id=ANY(%s::uuid[])").format(
                facts=compatibility_relation("fact")
            ),
            ([UUID(x) for x in fact_ids],),
        )
    ]

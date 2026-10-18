import hashlib
import json
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from psycopg import AsyncConnection, Error, sql
from psycopg.errors import FeatureNotSupported

from .auth import Principal
from .config import Settings
from .embedding import EmbeddingResult
from .models import IngestRequest, RememberRequest, SearchRequest


def canonical_hash(value: Any) -> bytes:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    ).encode()
    return hashlib.sha256(payload).digest()


def _operation_request_hash(kind: str, body: dict[str, Any]) -> bytes:
    value = {"kind": kind, "body": body}
    try:
        # Preserve existing hashes byte-for-byte for all UTF-8-compatible bodies.
        return canonical_hash(value)
    except UnicodeEncodeError:
        # Raw batches include malformed items, even though their text is never
        # stored. Escape those code points for stable replay/conflict detection.
        # The NUL prefix cannot occur in canonical JSON and separates this domain
        # from every valid request, including strings containing literal escapes.
        payload = json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=True
        ).encode("ascii")
        return hashlib.sha256(b"\x00braincore-invalid-unicode-request\x00" + payload).digest()


def memory_dedupe_hash(
    content: str, scope: dict[str, Any], reference: dict[str, Any] | None = None
) -> bytes:
    value: dict[str, Any] = {"content": content, "scope": scope}
    if reference is not None:
        value["reference"] = reference
    return canonical_hash(value)


def scope_values(request: RememberRequest) -> tuple[str | None, ...]:
    scope = request.scope
    return scope.project_id, scope.user_id, scope.agent_id, scope.run_id


async def begin_idempotent_operation(
    conn: AsyncConnection[dict[str, object]],
    principal: Principal,
    key: str,
    body: dict[str, Any],
    kind: str,
    count: int,
) -> tuple[UUID, bool]:
    if not key or len(key) > 200:
        raise HTTPException(status_code=422, detail="invalid_idempotency_key")
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"{principal.tenant_id}:{principal.key_id}:{key}",),
    )
    await conn.execute(
        """DELETE FROM braincore.idempotency_records
           WHERE tenant_id=%s AND actor_key_id=%s AND key=%s AND expires_at <= now()""",
        (principal.tenant_id, principal.key_id, key),
    )
    request_hash = _operation_request_hash(kind, body)
    existing_cursor = await conn.execute(
        """SELECT request_hash, operation_id FROM braincore.idempotency_records
           WHERE tenant_id = %s AND actor_key_id=%s AND key = %s
             AND expires_at > now() FOR UPDATE""",
        (principal.tenant_id, principal.key_id, key),
    )
    existing = await existing_cursor.fetchone()
    if existing:
        stored_hash = existing["request_hash"]
        if not isinstance(stored_hash, bytes | bytearray | memoryview):
            raise RuntimeError("database returned an invalid request hash")
        if bytes(stored_hash) != request_hash:
            raise HTTPException(status_code=409, detail="idempotency_conflict")
        return UUID(str(existing["operation_id"])), True

    cursor = await conn.execute(
        """INSERT INTO braincore.operations (tenant_id, actor_key_id, kind, accepted_items)
           VALUES (%s, %s, %s, %s) RETURNING id""",
        (principal.tenant_id, principal.key_id, kind, count),
    )
    operation_id = UUID(str((await cursor.fetchone())["id"]))  # type: ignore[index]
    await conn.execute(
        """INSERT INTO braincore.idempotency_records
           (tenant_id, actor_key_id, key, request_hash, operation_id)
           VALUES (%s, %s, %s, %s, %s)""",
        (principal.tenant_id, principal.key_id, key, request_hash, operation_id),
    )
    return operation_id, False


async def remember_one(
    conn: AsyncConnection[dict[str, object]],
    principal: Principal,
    operation_id: UUID,
    item_index: int,
    request: RememberRequest,
) -> UUID:
    content_hash = hashlib.sha256(request.content.encode()).digest()
    dedupe_hash = memory_dedupe_hash(
        request.content,
        request.scope.model_dump(),
        request.reference.model_dump() if request.reference else None,
    )
    artifact_cursor = await conn.execute(
        """
        INSERT INTO braincore.artifacts
          (tenant_id, source_type, source_uri, content, content_sha256, scope, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            principal.tenant_id,
            request.evidence.source_type,
            request.evidence.source_uri,
            request.content,
            content_hash,
            json.dumps(request.scope.model_dump(exclude_none=True)),
            json.dumps(request.evidence.metadata),
        ),
    )
    artifact_id = (await artifact_cursor.fetchone())["id"]  # type: ignore[index]
    project_id, user_id, agent_id, run_id = scope_values(request)
    reference_namespace = request.reference.namespace if request.reference else None
    reference_key = request.reference.key if request.reference else None
    existing: dict[str, object] | None = None
    if request.reference:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (
                f"{principal.tenant_id}:reference:"
                f"{'/'.join(request.reference.namespace)}:{request.reference.key}",
            ),
        )
        existing = await (
            await conn.execute(
                """SELECT id,content,project_id,user_id,agent_id,run_id,object_type,
                          legacy_source_id,legacy_attributes_ready
                   FROM braincore.memories
                   WHERE tenant_id=%s AND reference_namespace=%s AND reference_key=%s
                     AND project_id IS NOT DISTINCT FROM %s
                     AND user_id IS NOT DISTINCT FROM %s
                     AND agent_id IS NOT DISTINCT FROM %s
                     AND run_id IS NOT DISTINCT FROM %s
                   FOR UPDATE""",
                (
                    principal.tenant_id,
                    reference_namespace,
                    reference_key,
                    project_id,
                    user_id,
                    agent_id,
                    run_id,
                ),
            )
        ).fetchone()
    if existing is None:
        if request.reference:
            conflict = await (
                await conn.execute(
                    "SELECT id FROM braincore.memories WHERE tenant_id=%s AND dedupe_key=%s",
                    (principal.tenant_id, dedupe_hash),
                )
            ).fetchone()
            if conflict is not None:
                raise HTTPException(status_code=409, detail="memory_conflict")
            memory_cursor = await conn.execute(
                """
                INSERT INTO braincore.memories
                  (tenant_id,content,dedupe_key,project_id,user_id,agent_id,run_id,
                   scope,reference_namespace,reference_key,valid_from,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    principal.tenant_id,
                    request.content,
                    dedupe_hash,
                    project_id,
                    user_id,
                    agent_id,
                    run_id,
                    json.dumps(request.scope.model_dump(exclude_none=True)),
                    reference_namespace,
                    reference_key,
                    request.valid_from or datetime.now(UTC),
                    json.dumps(request.metadata),
                ),
            )
        else:
            memory_cursor = await conn.execute(
                """
                INSERT INTO braincore.memories
                  (tenant_id,content,dedupe_key,project_id,user_id,agent_id,run_id,
                   scope,valid_from,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id,dedupe_key) DO UPDATE SET updated_at=now()
                RETURNING id
                """,
                (
                    principal.tenant_id,
                    request.content,
                    dedupe_hash,
                    project_id,
                    user_id,
                    agent_id,
                    run_id,
                    json.dumps(request.scope.model_dump(exclude_none=True)),
                    request.valid_from or datetime.now(UTC),
                    json.dumps(request.metadata),
                ),
            )
        memory_id = UUID(str((await memory_cursor.fetchone())["id"]))  # type: ignore[index]
    else:
        memory_id = UUID(str(existing["id"]))
        existing_scope = {
            field: existing[field] for field in ("project_id", "user_id", "agent_id", "run_id")
        }
        if existing_scope != request.scope.model_dump():
            raise HTTPException(status_code=409, detail="reference_scope_conflict")
        content_changed = existing["content"] != request.content
        if content_changed and (
            existing["object_type"] == "fact"
            or (
                existing["object_type"] in {"memory", "episode"}
                and (
                    existing["legacy_source_id"] is not None or existing["legacy_attributes_ready"]
                )
            )
        ):
            raise HTTPException(status_code=409, detail="structured_fact_revision_required")
        if content_changed:
            conflict = await (
                await conn.execute(
                    """SELECT id FROM braincore.memories
                       WHERE tenant_id=%s AND dedupe_key=%s AND id<>%s""",
                    (principal.tenant_id, dedupe_hash, memory_id),
                )
            ).fetchone()
            if conflict is not None:
                raise HTTPException(status_code=409, detail="memory_conflict")
            prior = await (
                await conn.execute(
                    """SELECT id FROM braincore.memory_revisions
                       WHERE tenant_id=%s AND memory_id=%s
                       ORDER BY created_at DESC LIMIT 1""",
                    (principal.tenant_id, memory_id),
                )
            ).fetchone()
            await conn.execute(
                """INSERT INTO braincore.memory_revisions
                   (tenant_id,memory_id,prior_revision_id,content,reason)
                   VALUES (%s,%s,%s,%s,'external reference update')""",
                (principal.tenant_id, memory_id, prior["id"] if prior else None, request.content),
            )
        await conn.execute(
            """UPDATE braincore.memories
               SET content=%s,dedupe_key=%s,project_id=%s,user_id=%s,agent_id=%s,run_id=%s,
                   scope=%s,metadata=%s,valid_from=COALESCE(%s,valid_from),
                   embedding_state='pending',updated_at=now(),
                   embedding=CASE WHEN %s THEN NULL ELSE embedding END,
                   embedding_model=CASE WHEN %s THEN NULL ELSE embedding_model END,
                   embedding_preprocessing_revision=CASE WHEN %s THEN NULL
                     ELSE embedding_preprocessing_revision END
               WHERE id=%s AND tenant_id=%s""",
            (
                request.content,
                dedupe_hash,
                project_id,
                user_id,
                agent_id,
                run_id,
                json.dumps(request.scope.model_dump(exclude_none=True)),
                json.dumps(request.metadata),
                request.valid_from,
                content_changed,
                content_changed,
                content_changed,
                memory_id,
                principal.tenant_id,
            ),
        )
    await conn.execute(
        """INSERT INTO braincore.memory_revisions
           (tenant_id, memory_id, content, reason)
           SELECT %s, %s, %s, 'initial statement'
           WHERE NOT EXISTS (
             SELECT 1 FROM braincore.memory_revisions
             WHERE tenant_id = %s AND memory_id = %s
           )""",
        (
            principal.tenant_id,
            memory_id,
            request.content,
            principal.tenant_id,
            memory_id,
        ),
    )
    await conn.execute(
        """INSERT INTO braincore.memory_evidence (tenant_id,memory_id,artifact_id,quote)
           VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (
            principal.tenant_id,
            memory_id,
            artifact_id,
            request.evidence.quote or request.content,
        ),
    )
    item_cursor = await conn.execute(
        """INSERT INTO braincore.operation_items
           (operation_id, tenant_id, item_index, state, memory_id)
           VALUES (%s,%s,%s,'queued',%s) RETURNING id""",
        (operation_id, principal.tenant_id, item_index, memory_id),
    )
    item_id = (await item_cursor.fetchone())["id"]  # type: ignore[index]
    await conn.execute(
        """INSERT INTO braincore.jobs
           (tenant_id, operation_id, operation_item_id, kind, payload)
           VALUES (%s,%s,%s,'embed_memory',%s)""",
        (principal.tenant_id, operation_id, item_id, json.dumps({"memory_id": str(memory_id)})),
    )
    await conn.execute(
        """INSERT INTO braincore.audit_events
           (tenant_id, actor_key_id, action, target_type, target_id)
           VALUES (%s,%s,'memory.remember','memory',%s)""",
        (principal.tenant_id, principal.key_id, memory_id),
    )
    return memory_id


async def enqueue_ingestion_one(
    conn: AsyncConnection[dict[str, object]],
    principal: Principal,
    operation_id: UUID,
    item_index: int,
    request: IngestRequest,
) -> None:
    content_hash = hashlib.sha256(request.content.encode()).digest()
    artifact_cursor = await conn.execute(
        """
        INSERT INTO braincore.artifacts
          (tenant_id, source_type, source_uri, content, content_sha256, scope, metadata)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        (
            principal.tenant_id,
            request.source_type,
            request.source_uri,
            request.content,
            content_hash,
            json.dumps(request.scope.model_dump(exclude_none=True)),
            json.dumps(request.metadata),
        ),
    )
    artifact_id = (await artifact_cursor.fetchone())["id"]  # type: ignore[index]
    item_cursor = await conn.execute(
        """INSERT INTO braincore.operation_items
           (operation_id,tenant_id,item_index,state)
           VALUES (%s,%s,%s,'queued') RETURNING id""",
        (operation_id, principal.tenant_id, item_index),
    )
    item_id = (await item_cursor.fetchone())["id"]  # type: ignore[index]
    payload = {
        "artifact_id": str(artifact_id),
        "scope": request.scope.model_dump(),
        "metadata": request.metadata,
    }
    await conn.execute(
        """INSERT INTO braincore.jobs
           (tenant_id,operation_id,operation_item_id,kind,payload)
           VALUES (%s,%s,%s,'extract_ingestion',%s)""",
        (principal.tenant_id, operation_id, item_id, json.dumps(payload)),
    )
    await conn.execute(
        """INSERT INTO braincore.audit_events
           (tenant_id,actor_key_id,action,target_type,target_id)
           VALUES (%s,%s,'ingestion.accept','artifact',%s)""",
        (principal.tenant_id, principal.key_id, artifact_id),
    )


@dataclass(frozen=True)
class SearchOutcome:
    payload: dict[str, Any]
    timings_ms: dict[str, float]
    degraded: bool


_MEMORY_COLUMNS = sql.SQL("""m.id,m.content,m.project_id,m.user_id,m.agent_id,m.run_id,m.scope,
    m.reference_namespace,m.reference_key,m.state,m.trust,m.valid_from,m.valid_until,
    m.embedding_state,m.metadata,m.created_at,m.updated_at,m.object_type,
    m.legacy_external_id,m.legacy_scope_path""")


def _search_filter(principal: Principal, body: SearchRequest) -> tuple[sql.Composed, list[object]]:
    principal.require("read")
    scope = body.scope.model_dump() if body.scope is not None else principal.scope
    principal.require_scope(scope)
    values: list[object] = [principal.tenant_id]
    clauses: list[sql.Composable] = [sql.SQL("m.tenant_id = %s")]
    if scope:
        for field in ("project_id", "user_id", "agent_id", "run_id"):
            if scope.get(field) is not None:
                clauses.append(sql.SQL("m.{} = %s").format(sql.Identifier(field)))
                values.append(scope[field])
    if body.object_type is not None:
        clauses.append(sql.SQL("m.object_type = %s"))
        values.append(body.object_type)
    else:
        # Legacy entities support structured relations; the four source result
        # types retain their original unfiltered retrieval contract.
        clauses.append(sql.SQL("m.object_type <> 'entity'"))
    if body.legacy_scope_prefix is not None:
        scope_prefix = (
            body.legacy_scope_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        clauses.append(sql.SQL("m.legacy_scope_path LIKE %s"))
        values.append(scope_prefix + "%")
    if body.reference_namespace_prefix:
        prefix = body.reference_namespace_prefix
        if any(not part or len(part) > 200 for part in prefix):
            raise HTTPException(status_code=422, detail="validation_failed")
        clauses.append(sql.SQL("m.reference_namespace[1:cardinality(%s::text[])] = %s::text[]"))
        values.extend([prefix, prefix])
    if body.metadata_filter is not None:
        clauses.append(sql.SQL("m.metadata @> %s::jsonb"))
        values.append(json.dumps(body.metadata_filter))
    if not body.include_inactive:
        clauses.append(sql.SQL("m.state = 'active'"))
    if not body.include_inactive or body.as_of is not None:
        if body.as_of is None:
            clauses.append(
                sql.SQL(
                    "(m.valid_from IS NULL OR m.valid_from <= statement_timestamp()) "
                    "AND (m.valid_until IS NULL OR m.valid_until > statement_timestamp())"
                )
            )
        else:
            clauses.append(
                sql.SQL(
                    "(m.valid_from IS NULL OR m.valid_from <= %s) "
                    "AND (m.valid_until IS NULL OR m.valid_until > %s)"
                )
            )
            values.extend([body.as_of, body.as_of])
    return sql.SQL(" AND ").join(clauses), values


def _memory_identifier(value: object) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


async def _candidate_ids(
    conn: AsyncConnection[dict[str, object]], query: sql.Composed, values: list[object]
) -> list[UUID]:
    rows = await (await conn.execute(query, values)).fetchall()
    return [_memory_identifier(row["id"]) for row in rows]


def _vector_query(
    where: sql.Composed,
    values: list[object],
    vector_literal: str,
    limit: int,
    settings: Settings,
    *,
    mode: str,
    object_type: str | None = None,
) -> tuple[sql.Composed, list[object]]:
    object_types = ("entity", "fact", "memory", "segment", "episode")
    if object_type is not None and object_type not in object_types:
        raise ValueError("Unknown vector object type")
    eligible = sql.SQL("""{where} AND m.embedding IS NOT NULL AND m.embedding_state = 'ready'
        AND m.embedding_model = %s AND m.embedding_preprocessing_revision = %s""").format(
        where=where
    )
    eligible_values = [
        *values,
        settings.embedding_model,
        settings.embedding_preprocessing_revision,
    ]
    if mode == "exact":
        # Materialization separates filtering from distance ordering. The planner
        # cannot turn the outer scan into an approximate vector index scan.
        query = sql.SQL("""
            WITH eligible AS MATERIALIZED (
                SELECT m.id,m.updated_at,m.embedding
                FROM braincore.memories m WHERE {eligible}
            )
            SELECT id FROM eligible
            ORDER BY embedding <=> %s::vector, updated_at DESC, id
            LIMIT %s
        """).format(eligible=eligible)
        return query, [*eligible_values, vector_literal, limit]
    if mode not in {"planner", "iterative_hnsw"}:
        raise ValueError("Unknown vector search mode")
    distance: sql.Composable = sql.SQL("m.embedding <=> %s::vector")
    if object_type is not None:
        # The type expression cannot use the global graph. Its partial graph
        # has the identical expression; the trusted type equality ensures the
        # CASE value is exactly the canonical embedding for every eligible row.
        eligible = sql.SQL("{eligible} AND m.object_type = {kind}").format(
            eligible=eligible, kind=sql.Literal(object_type)
        )
        distance = sql.SQL(
            "(CASE WHEN m.object_type = {kind} THEN m.embedding ELSE NULL END)"
            "::vector(384) <=> %s::vector"
        ).format(kind=sql.Literal(object_type))
    # Untyped requests retain their existing global nearest-neighbor contract.
    # Timestamp/ID ties are ordered only within the bounded candidate set.
    query = sql.SQL("""
        WITH nearest AS MATERIALIZED (
            SELECT m.id,m.updated_at,{distance} AS distance
            FROM braincore.memories m WHERE {eligible}
            ORDER BY {distance}
            LIMIT %s
        )
        SELECT id FROM nearest ORDER BY distance, updated_at DESC, id
    """).format(eligible=eligible, distance=distance)
    return query, [vector_literal, *eligible_values, vector_literal, limit]


async def check_full_text_capability(
    conn: AsyncConnection[dict[str, object]], settings: Settings
) -> None:
    if settings.search_full_text_mode == "canonical":
        return
    row = await (
        await conn.execute(
            "SELECT ready FROM braincore.lexical_projection_state WHERE singleton = true"
        )
    ).fetchone()
    if row is None or row["ready"] is not True:
        raise FeatureNotSupported("Full-text projection backfill must complete before search")


def _full_text_query(
    where: sql.Composed,
    values: list[object],
    query_text: str,
    limit: int,
    settings: Settings,
) -> tuple[sql.Composed, list[object]]:
    # Both relations expose identical filter/vector/tie fields. No pre-ranking
    # truncation: rank the complete eligible match set before the candidate LIMIT.
    relation = "memories_lexical" if settings.search_full_text_mode == "projection" else "memories"
    query = sql.SQL("""
        SELECT m.id FROM braincore.{relation} m
        WHERE {where} AND m.legacy_fts_eligible
          AND m.search_vector @@ plainto_tsquery('english', %s)
        ORDER BY ts_rank_cd(m.search_vector, plainto_tsquery('english', %s)) DESC,
                 m.updated_at DESC, m.id
        LIMIT %s
    """).format(relation=sql.Identifier(relation), where=where)
    return query, [*values, query_text, query_text, limit]


async def check_search_capability(
    conn: AsyncConnection[dict[str, object]], settings: Settings
) -> None:
    if settings.search_vector_mode == "exact":
        return
    # Unknown custom GUC names can be accepted even by an unsupported extension.
    version = await (
        await conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
    ).fetchone()
    try:
        supported = version is not None and tuple(
            int(part) for part in str(version["extversion"]).split(".")[:2]
        ) >= (0, 8)
    except ValueError:
        supported = False
    if not supported:
        raise FeatureNotSupported("Iterative vector search requires pgvector 0.8 or newer")


async def _configure_iterative_search(
    conn: AsyncConnection[dict[str, object]], settings: Settings
) -> None:
    await check_search_capability(conn, settings)
    await conn.execute(
        """SELECT set_config('hnsw.iterative_scan', 'strict_order', true),
                  set_config('hnsw.ef_search', %s, true),
                  set_config('hnsw.max_scan_tuples', %s, true),
                  set_config('hnsw.scan_mem_multiplier', %s, true)""",
        (
            str(settings.search_hnsw_ef_search),
            str(settings.search_hnsw_max_scan_tuples),
            str(settings.search_hnsw_scan_mem_multiplier),
        ),
    )


async def _vector_candidates(
    conn: AsyncConnection[dict[str, object]],
    where: sql.Composed,
    values: list[object],
    vector: list[float],
    limit: int,
    settings: Settings,
    *,
    object_type: str | None = None,
) -> tuple[list[UUID], str | None]:
    vector_literal = f"[{','.join(str(value) for value in vector)}]"
    mode = settings.search_vector_mode
    query, parameters = _vector_query(
        where, values, vector_literal, limit, settings, mode=mode, object_type=object_type
    )
    # A failed ANN capability/settings/query must never trigger an unbounded
    # exact scan. The caller contains the error and discloses a failed stream.
    async with conn.transaction():
        if mode != "exact":
            await _configure_iterative_search(conn, settings)
        return await _candidate_ids(conn, query, parameters), None


def _incompatible_vector_query(
    where: sql.Composed, values: list[object], settings: Settings
) -> tuple[sql.Composed, list[object]]:
    # Disjoint ranges can seek the identity index even when every vector matches.
    # IS DISTINCT FROM would require scanning the entire healthy eligible set.
    model = settings.embedding_model
    revision = settings.embedding_preprocessing_revision
    ranges: list[tuple[str, list[object]]] = [
        ("m.embedding_model IS NULL", []),
        ("m.embedding_model < %s", [model]),
        ("m.embedding_model > %s", [model]),
        ("m.embedding_model = %s AND m.embedding_preprocessing_revision IS NULL", [model]),
        ("m.embedding_model = %s AND m.embedding_preprocessing_revision < %s", [model, revision]),
        ("m.embedding_model = %s AND m.embedding_preprocessing_revision > %s", [model, revision]),
    ]
    queries: list[sql.Composed] = []
    parameters: list[object] = []
    for identity_range, range_values in ranges:
        queries.append(
            sql.SQL("""SELECT 1 FROM braincore.memories m
                WHERE {where} AND m.embedding IS NOT NULL AND m.embedding_state = 'ready'
                  AND {identity_range}""").format(
                where=where, identity_range=sql.SQL(identity_range)
            )
        )
        parameters.extend([*values, *range_values])
    # These applicable rows cannot participate in vector retrieval yet. Keep
    # them in a separate partial index so healthy coverage remains a bounded
    # empty-index probe, rather than a scan over every compatible memory.
    queries.append(
        sql.SQL("""SELECT 1 FROM braincore.memories m
            WHERE {where} AND (m.embedding_state IN ('pending','failed')
                OR (m.embedding_state = 'ready' AND m.embedding IS NULL))""").format(where=where)
    )
    parameters.extend(values)
    query = sql.SQL("SELECT EXISTS ({ranges}) AS incompatible").format(
        ranges=sql.SQL(" UNION ALL ").join(queries)
    )
    return query, parameters


SearchConnectionFactory = Callable[
    [], AbstractAsyncContextManager[AsyncConnection[dict[str, object]]]
]


async def search_memories(
    connect: SearchConnectionFactory,
    principal: Principal,
    body: SearchRequest,
    embedding: EmbeddingResult | Awaitable[EmbeddingResult] | None,
    settings: Settings,
    *,
    stage_observer: Callable[[str], None] | None = None,
) -> SearchOutcome:
    """Rank lean UUID candidates, then hydrate an eligible page in fusion order.

    Connection leases cover database work only. The caller may start embedding
    before calling this function; lexical retrieval overlaps it without holding
    a pooled database connection while awaiting the external provider.
    """

    def stage(name: str) -> None:
        if stage_observer is not None:
            stage_observer(name)

    stage("eligibility")
    started = perf_counter()
    timings = dict.fromkeys(
        (
            "eligibility",
            "connection_wait",
            "full_text",
            "embedding_wait",
            "coverage",
            "vector",
            "fusion",
            "hydration",
        ),
        0.0,
    )
    where, values = _search_filter(principal, body)
    timings["eligibility"] = (perf_counter() - started) * 1000
    if body.query is None:
        stage("connection_wait")
        pool_started = perf_counter()
        async with connect() as conn:
            timings["connection_wait"] += (perf_counter() - pool_started) * 1000
            stage("hydration")
            hydration_started = perf_counter()
            query = sql.SQL("""
                SELECT {columns}, 1.0::float AS score
                FROM braincore.memories m WHERE {where}
                ORDER BY m.updated_at DESC, m.id
                LIMIT %s OFFSET %s
            """).format(columns=_MEMORY_COLUMNS, where=where)
            async with conn.transaction():
                rows = await (
                    await conn.execute(query, [*values, body.limit, body.offset])
                ).fetchall()
            timings["hydration"] = (perf_counter() - hydration_started) * 1000
        timings["total"] = (perf_counter() - started) * 1000
        stage("complete")
        return SearchOutcome(
            {
                "results": rows,
                "streams": {
                    "full_text": {"state": "disabled"},
                    "vector": {"state": "disabled"},
                    "temporal": {"state": "ok"},
                    "listing": {"state": "ok"},
                },
                "warnings": [],
            },
            timings,
            False,
        )
    if embedding is None:
        raise ValueError("query search requires an embedding result or task")

    warnings: list[str] = []
    fts_state = "ok"
    candidate_limit = (body.limit + body.offset) * 3
    fts_query, fts_values = _full_text_query(where, values, body.query, candidate_limit, settings)
    stage("connection_wait")
    pool_started = perf_counter()
    async with connect() as conn:
        timings["connection_wait"] += (perf_counter() - pool_started) * 1000
        stage("full_text")
        stage_started = perf_counter()
        try:
            async with conn.transaction():
                await check_full_text_capability(conn, settings)
                fts_ids = await _candidate_ids(conn, fts_query, fts_values)
        except Error:
            fts_ids = []
            fts_state = "failed"
            warnings.append("Full-text search unavailable; this stream was skipped.")
        timings["full_text"] = (perf_counter() - stage_started) * 1000

    stage("embedding_wait")
    stage_started = perf_counter()
    embedded = embedding if isinstance(embedding, EmbeddingResult) else await embedding
    timings["embedding_wait"] = (perf_counter() - stage_started) * 1000
    vector_state = embedded.state
    vector_warnings = [embedded.warning] if embedded.warning else []
    vector_ids: list[UUID] = []
    stage("connection_wait")
    pool_started = perf_counter()
    async with connect() as conn, conn.transaction():
        timings["connection_wait"] += (perf_counter() - pool_started) * 1000
        coverage_ok = True
        if embedded.vector is not None and embedded.state == "ok":
            stage("coverage")
            stage_started = perf_counter()
            try:
                coverage_query, coverage_values = _incompatible_vector_query(
                    where, values, settings
                )
                async with conn.transaction():
                    coverage = await (
                        await conn.execute(coverage_query, coverage_values)
                    ).fetchone()
                if coverage is None:
                    raise RuntimeError("database returned no embedding coverage result")
                if coverage["incompatible"]:
                    vector_warnings.append(
                        "Some eligible memories have missing, failed, incompatible or "
                        "unidentified embeddings; "
                        "vector results are incomplete. An administrator must refresh embeddings."
                    )
            except Error:
                coverage_ok = False
                vector_state = "failed"
                vector_warnings.append(
                    "Embedding coverage could not be verified; vector search was skipped."
                )
            timings["coverage"] = (perf_counter() - stage_started) * 1000
            stage("vector")
            stage_started = perf_counter()
            if coverage_ok:
                try:
                    vector_ids, fallback_warning = await _vector_candidates(
                        conn,
                        where,
                        values,
                        embedded.vector,
                        candidate_limit,
                        settings,
                        object_type=body.object_type,
                    )
                    if fallback_warning:
                        vector_warnings.append(fallback_warning)
                except Error:
                    vector_state = "failed"
                    vector_warnings.append("Vector search unavailable; this stream was skipped.")
            timings["vector"] = (perf_counter() - stage_started) * 1000
        elif embedded.state == "ok":
            vector_state = "failed"
            vector_warnings.append("Embedder returned no vector; vector search was skipped.")
        warnings.extend(vector_warnings)

        stage("fusion")
        stage_started = perf_counter()
        fused: dict[UUID, float] = {}
        for ids in (fts_ids, vector_ids):
            for rank, memory_id in enumerate(ids, start=1):
                fused[memory_id] = fused.get(memory_id, 0.0) + 1.0 / (60 + rank)
        ranked_ids = sorted(fused, key=lambda memory_id: fused[memory_id], reverse=True)
        timings["fusion"] = (perf_counter() - stage_started) * 1000

        stage("hydration")
        stage_started = perf_counter()
        rows = []
        if ranked_ids:
            # Recheck eligibility before final pagination so concurrently removed
            # candidates do not crowd out eligible reserves. Ordinality preserves
            # fusion order without an array_position scan for every candidate.
            hydrate_query = sql.SQL("""
                SELECT {columns}
                FROM unnest(%s::uuid[]) WITH ORDINALITY AS ranked(id, position)
                JOIN braincore.memories m ON m.id = ranked.id
                WHERE {where}
                ORDER BY ranked.position
                LIMIT %s OFFSET %s
            """).format(columns=_MEMORY_COLUMNS, where=where)
            hydrated = await (
                await conn.execute(hydrate_query, [ranked_ids, *values, body.limit, body.offset])
            ).fetchall()
            rows = [{**row, "score": fused[_memory_identifier(row["id"])]} for row in hydrated]
        timings["hydration"] = (perf_counter() - stage_started) * 1000
    timings["total"] = (perf_counter() - started) * 1000
    stage("complete")
    return SearchOutcome(
        {
            "results": rows,
            "streams": {
                "full_text": {"state": fts_state},
                "vector": {
                    "state": vector_state,
                    "warning": " ".join(vector_warnings) if vector_warnings else None,
                },
                "temporal": {"state": "ok"},
            },
            "warnings": warnings,
        },
        timings,
        bool(warnings) or fts_state == "failed" or vector_state == "failed",
    )

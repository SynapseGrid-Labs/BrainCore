import asyncio
import json
import logging
import re
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from psycopg import DataError, Error, IntegrityError, sql
from psycopg.errors import FeatureNotSupported
from pydantic import ValidationError as PydanticValidationError

from . import service as memory_service
from .auth import (
    SESSION_COOKIE_NAME,
    Principal,
    authenticate,
    key_digest,
    require_admin,
    require_operation_read,
    require_read,
    require_write,
)
from .config import get_settings
from .db import close_pool, connection, open_pool
from .embedding import (
    EmbeddingResult,
    close_embedding_client,
    embed_one,
    open_embedding_client,
)
from .legacy_api import router as legacy_router
from .legacy_provenance import read_provenance
from .maintenance import StaleDemotionRequest, connected_demote, connected_status
from .models import (
    ApiKeyCreateRequest,
    EmbeddingRefreshAccepted,
    EmbeddingRefreshRequest,
    EvidenceGrantRequest,
    EvidenceGrantRevokeRequest,
    FeedbackRequest,
    IngestRequest,
    LifecycleRequest,
    MemoryResolveRequest,
    OperationAccepted,
    RawBatchRequest,
    RememberRequest,
    RevisionRequest,
    SearchRequest,
    SearchResponse,
)
from .request_validation import validation_error_details
from .service import (
    begin_idempotent_operation,
    canonical_hash,
    enqueue_ingestion_one,
    memory_dedupe_hash,
    remember_one,
)

logger = logging.getLogger("braincore_memory.api")
_degraded_searches: dict[UUID, int] = {}
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _request_id(value: str | None) -> str:
    if value is not None and _REQUEST_ID_PATTERN.fullmatch(value):
        return value
    return f"req_{secrets.token_hex(12)}"


async def _mutation_replay(
    conn: Any,
    principal: Principal,
    key: str,
    kind: str,
    body: dict[str, Any],
) -> dict[str, Any] | None:
    if not key or len(key) > 200:
        raise HTTPException(status_code=422, detail="invalid_idempotency_key")
    request_hash = canonical_hash({"kind": kind, "body": body})
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"{principal.tenant_id}:{principal.key_id}:mutation:{key}",),
    )
    await conn.execute(
        """DELETE FROM braincore.mutation_receipts
           WHERE tenant_id=%s AND actor_key_id=%s AND key=%s AND expires_at <= now()""",
        (principal.tenant_id, principal.key_id, key),
    )
    existing = await (
        await conn.execute(
            """SELECT kind,request_hash,response FROM braincore.mutation_receipts
               WHERE tenant_id=%s AND actor_key_id=%s AND key=%s
                 AND expires_at > now() FOR UPDATE""",
            (principal.tenant_id, principal.key_id, key),
        )
    ).fetchone()
    if existing is None:
        return None
    stored_hash = existing["request_hash"]
    if not isinstance(stored_hash, bytes | bytearray | memoryview):
        raise RuntimeError("database returned an invalid mutation request hash")
    if existing["kind"] != kind or bytes(stored_hash) != request_hash:
        raise HTTPException(status_code=409, detail="idempotency_conflict")
    response = existing["response"]
    if not isinstance(response, dict):
        raise RuntimeError("database returned an invalid mutation receipt")
    return {**response, "reused": True}


async def _store_mutation(
    conn: Any,
    principal: Principal,
    key: str,
    kind: str,
    body: dict[str, Any],
    response: dict[str, Any],
) -> None:
    await conn.execute(
        """INSERT INTO braincore.mutation_receipts
           (tenant_id,actor_key_id,key,kind,request_hash,response)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (
            principal.tenant_id,
            principal.key_id,
            key,
            kind,
            canonical_hash({"kind": kind, "body": body}),
            json.dumps(response, default=str),
        ),
    )


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await open_pool()
    try:
        await open_embedding_client()
        yield
    finally:
        await close_embedding_client()
        await close_pool()


app = FastAPI(
    title="BrainCore API",
    version="2.0.0",
    description="Evidence-native, self-hosted memory",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.include_router(legacy_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "X-CSRF-Token"],
)


def _secure_response(response: Response, request_id: str) -> Response:
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
    )
    return response


@app.middleware("http")
async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
    started = time.perf_counter()
    request_id = _request_id(request.headers.get("X-Request-ID"))
    request.state.request_id = request_id
    if request.method in {"POST", "PUT", "PATCH"}:
        content_length = request.headers.get("content-length")
        if (
            content_length
            and content_length.isdigit()
            and int(content_length) > get_settings().max_body_bytes
        ):
            return _secure_response(
                JSONResponse(
                    status_code=413,
                    content={
                        "error": {
                            "code": "request_too_large",
                            "message": "Request body exceeds the configured limit.",
                            "request_id": request_id,
                            "retryable": False,
                            "details": {},
                        }
                    },
                ),
                request_id,
            )
        buffered = bytearray()
        async for chunk in request.stream():
            buffered.extend(chunk)
            if len(buffered) > get_settings().max_body_bytes:
                return _secure_response(
                    JSONResponse(
                        status_code=413,
                        content={
                            "error": {
                                "code": "request_too_large",
                                "message": "Request body exceeds the configured limit.",
                                "request_id": request_id,
                                "retryable": False,
                                "details": {},
                            }
                        },
                    ),
                    request_id,
                )
        request._body = bytes(buffered)  # noqa: SLF001 - replay the bounded body downstream
    response = await call_next(request)
    _secure_response(response, request_id)
    logger.info(
        json.dumps(
            {
                "event": "braincore_request",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
            separators=(",", ":"),
        )
    )
    return response


@app.get("/openapi.json", include_in_schema=False)
async def authenticated_openapi(_: Principal = Depends(require_read)) -> JSONResponse:
    return JSONResponse(app.openapi())


@app.get("/docs", include_in_schema=False)
async def authenticated_docs(_: Principal = Depends(require_read)) -> HTMLResponse:
    return HTMLResponse(
        "<!doctype html><html><head><title>BrainCore API</title></head>"
        "<body><main><h1>BrainCore API</h1><p>The authenticated OpenAPI JSON is "
        '<a href="/openapi.json">available here</a>.</p></main></body></html>'
    )


@app.exception_handler(HTTPException)
async def braincore_http_error(request: Request, exc: HTTPException) -> JSONResponse:
    code = str(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": code.replace("_", " ").capitalize(),
                "request_id": getattr(request.state, "request_id", "unknown"),
                "retryable": exc.status_code in {429, 502, 503, 504},
                "details": {},
            }
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_failed",
                "message": "Request validation failed.",
                "request_id": getattr(request.state, "request_id", "unknown"),
                "retryable": False,
                "details": {"errors": validation_error_details(exc.errors())},
            }
        },
    )


@app.exception_handler(Exception)
async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(
        json.dumps(
            {
                "event": "unexpected_error",
                "request_id": request_id,
                "error_type": type(exc).__name__,
            },
            separators=(",", ":"),
        )
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "The request could not be completed.",
                "request_id": request_id,
                "retryable": False,
                "details": {},
            }
        },
    )


@app.get("/v1/health")
async def health(_: Principal = Depends(require_read)) -> dict[str, str]:
    return {"status": "ok", "version": "2.0.0"}


@app.get("/v1/readiness")
async def readiness(_: Principal = Depends(require_read)) -> dict[str, str]:
    async with connection() as conn:
        await conn.execute("SELECT 1")
        try:
            await memory_service.check_search_capability(conn, get_settings())
        except FeatureNotSupported:
            raise HTTPException(
                status_code=503, detail="iterative_vector_search_unavailable"
            ) from None
        try:
            await memory_service.check_full_text_capability(conn, get_settings())
        except Error:
            raise HTTPException(
                status_code=503, detail="full_text_projection_unavailable"
            ) from None
    return {"status": "ready"}


async def _system_counts(principal: Principal) -> dict[str, int]:
    settings = get_settings()
    async with connection() as conn:
        row = await (
            await conn.execute(
                """SELECT
                     (SELECT count(*) FROM braincore.jobs
                       WHERE tenant_id=%s AND state='queued')::int AS jobs_queued,
                     (SELECT count(*) FROM braincore.jobs
                       WHERE tenant_id=%s AND state='running')::int AS jobs_running,
                     (SELECT count(*) FROM braincore.jobs
                       WHERE tenant_id=%s AND state='dead_letter')::int AS jobs_dead_letter,
                     (SELECT count(*) FROM braincore.memories
                       WHERE tenant_id=%s AND embedding_state='pending')::int AS embeddings_pending,
                     (SELECT count(*) FROM braincore.memories
                       WHERE tenant_id=%s AND embedding_state='failed')::int AS embeddings_failed,
                     (SELECT count(*) FROM braincore.memories
                       WHERE tenant_id=%s AND embedding_state='not_applicable')::int
                       AS embeddings_not_applicable,
                     (SELECT count(*) FROM braincore.operations
                       WHERE tenant_id=%s AND state='partial')::int AS operations_partial,
                     (SELECT count(*) FROM braincore.memories
                       WHERE tenant_id=%s AND embedding_state='ready'
                         AND (embedding_model IS DISTINCT FROM %s
                           OR embedding_preprocessing_revision IS DISTINCT FROM %s))::int
                       AS embeddings_incompatible""",
                (principal.tenant_id,) * 8
                + (settings.embedding_model, settings.embedding_preprocessing_revision),
            )
        ).fetchone()
    if row is None:
        raise RuntimeError("system status query returned no record")
    counts: dict[str, int] = {}
    for key, value in row.items():
        if not isinstance(value, int):
            raise RuntimeError("system status query returned a non-integer count")
        counts[key] = value
    return counts


@app.get("/v1/admin/status")
async def system_status(principal: Principal = Depends(require_admin)) -> dict[str, Any]:
    return {"status": "ok", "counts": await _system_counts(principal)}


@app.get("/v1/admin/maintenance/status")
async def native_maintenance_status(
    include_stats: bool = False,
    embedding_checks: bool = False,
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    return await asyncio.to_thread(connected_status, principal, include_stats, embedding_checks)


@app.post("/v1/admin/maintenance/stale:demote")
async def native_demote_stale(
    body: StaleDemotionRequest,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    try:
        return await asyncio.to_thread(connected_demote, principal, body, idempotency_key)
    except ValueError as error:
        if str(error) == "idempotency_conflict":
            raise HTTPException(status_code=409, detail="idempotency_conflict") from None
        raise HTTPException(status_code=422, detail="invalid_maintenance_request") from None


@app.get("/v1/metrics", response_class=PlainTextResponse)
async def metrics(principal: Principal = Depends(require_admin)) -> PlainTextResponse:
    counts = await _system_counts(principal)
    lines = [
        "# HELP braincore_search_degraded_total Searches with degraded retrieval.",
        "# TYPE braincore_search_degraded_total counter",
        f"braincore_search_degraded_total {_degraded_searches.get(principal.tenant_id, 0)}",
    ]
    for name, value in counts.items():
        metric = f"braincore_{name}"
        lines.extend([f"# TYPE {metric} gauge", f"{metric} {value}"])
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.post("/v1/admin/api-keys", status_code=201)
async def create_api_key(
    body: ApiKeyCreateRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    raw = f"bck_{secrets.token_urlsafe(32)}"
    async with connection() as conn, conn.transaction():
        request_body = body.model_dump(mode="json")
        replay = await _mutation_replay(
            conn, principal, idempotency_key, "api_key.create", request_body
        )
        if replay is not None:
            raise HTTPException(status_code=409, detail="api_key_secret_not_replayable")
        record = await (
            await conn.execute(
                """INSERT INTO braincore.api_keys
                   (tenant_id,name,prefix,digest,roles,scope,expires_at)
                   SELECT %s,%s,%s,%s,%s,%s,%s
                   WHERE %s::timestamptz IS NULL OR %s::timestamptz > statement_timestamp()
                   RETURNING id,name,prefix,roles,scope,created_at,expires_at""",
                (
                    principal.tenant_id,
                    body.name,
                    raw[:12],
                    key_digest(raw),
                    sorted(body.roles),
                    json.dumps(body.scope.model_dump(exclude_none=True)) if body.scope else None,
                    body.expires_at,
                    body.expires_at,
                    body.expires_at,
                ),
            )
        ).fetchone()
        if record is None:
            # Recheck atomically using database time after any idempotency-lock wait.
            raise HTTPException(status_code=422, detail="api_key_expiry_not_future")
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id)
               VALUES (%s,%s,'api_key.create','api_key',%s)""",
            (principal.tenant_id, principal.key_id, record["id"] if record else None),
        )
        safe_record = dict(record)
        await _store_mutation(
            conn, principal, idempotency_key, "api_key.create", request_body, safe_record
        )
    return {**record, "api_key": raw, "reused": False}


@app.get("/v1/admin/api-keys")
async def list_api_keys(principal: Principal = Depends(require_admin)) -> dict[str, Any]:
    async with connection() as conn:
        rows = await (
            await conn.execute(
                """SELECT id,name,prefix,roles,scope,created_at,expires_at,last_used_at,revoked_at
                   FROM braincore.api_keys WHERE tenant_id=%s ORDER BY created_at DESC""",
                (principal.tenant_id,),
            )
        ).fetchall()
    return {"api_keys": rows}


@app.post("/v1/admin/api-keys/{key_id}/revoke")
async def revoke_api_key(
    key_id: UUID,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    if key_id == principal.key_id:
        raise HTTPException(status_code=409, detail="cannot_revoke_current_key")
    async with connection() as conn, conn.transaction():
        request_body = {"key_id": str(key_id)}
        replay = await _mutation_replay(
            conn, principal, idempotency_key, "api_key.revoke", request_body
        )
        if replay is not None:
            return replay
        record = await (
            await conn.execute(
                """UPDATE braincore.api_keys SET revoked_at=COALESCE(revoked_at,now())
                   WHERE id=%s AND tenant_id=%s RETURNING id,revoked_at""",
                (key_id, principal.tenant_id),
            )
        ).fetchone()
        if record is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        await conn.execute(
            """UPDATE braincore.dashboard_sessions SET revoked_at=COALESCE(revoked_at,now())
               WHERE api_key_id=%s AND tenant_id=%s""",
            (key_id, principal.tenant_id),
        )
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id)
               VALUES (%s,%s,'api_key.revoke','api_key',%s)""",
            (principal.tenant_id, principal.key_id, key_id),
        )
        await _store_mutation(
            conn, principal, idempotency_key, "api_key.revoke", request_body, dict(record)
        )
    return {**record, "reused": False}


@app.post("/v1/admin/api-keys/{key_id}/rotate")
async def rotate_api_key(
    key_id: UUID,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    if key_id == principal.key_id:
        raise HTTPException(status_code=409, detail="cannot_rotate_current_key")
    raw = f"bck_{secrets.token_urlsafe(32)}"
    async with connection() as conn, conn.transaction():
        request_body = {"key_id": str(key_id)}
        replay = await _mutation_replay(
            conn, principal, idempotency_key, "api_key.rotate", request_body
        )
        if replay is not None:
            raise HTTPException(status_code=409, detail="api_key_secret_not_replayable")
        record = await (
            await conn.execute(
                """UPDATE braincore.api_keys
                   SET prefix=%s,digest=%s,revoked_at=NULL,last_used_at=NULL
                   WHERE id=%s AND tenant_id=%s AND revoked_at IS NULL
                   RETURNING id,name,prefix,roles,scope,created_at,expires_at""",
                (raw[:12], key_digest(raw), key_id, principal.tenant_id),
            )
        ).fetchone()
        if record is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        await conn.execute(
            """UPDATE braincore.dashboard_sessions SET revoked_at=COALESCE(revoked_at,now())
               WHERE api_key_id=%s AND tenant_id=%s""",
            (key_id, principal.tenant_id),
        )
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id)
               VALUES (%s,%s,'api_key.rotate','api_key',%s)""",
            (principal.tenant_id, principal.key_id, key_id),
        )
        safe_record = dict(record)
        await _store_mutation(
            conn, principal, idempotency_key, "api_key.rotate", request_body, safe_record
        )
    return {**record, "api_key": raw, "reused": False}


@app.post("/v1/dashboard/session", status_code=201)
async def create_dashboard_session(
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(authenticate),
) -> dict[str, Any]:
    if principal.session_id is not None:
        raise HTTPException(status_code=400, detail="api_key_required")
    session_token = f"braincore_session_{secrets.token_urlsafe(32)}"
    csrf_token = f"braincore_csrf_{secrets.token_urlsafe(32)}"
    settings = get_settings()
    async with connection() as conn, conn.transaction():
        replay = await _mutation_replay(
            conn, principal, idempotency_key, "dashboard.session.create", {}
        )
        if replay is not None:
            raise HTTPException(status_code=409, detail="session_secret_not_replayable")
        record = await (
            await conn.execute(
                """INSERT INTO braincore.dashboard_sessions
                   (tenant_id,api_key_id,digest,csrf_digest,roles,scope,expires_at)
                   VALUES (%s,%s,%s,%s,%s,%s,now()+(%s * interval '1 second'))
                   RETURNING id,expires_at""",
                (
                    principal.tenant_id,
                    principal.key_id,
                    key_digest(session_token),
                    key_digest(csrf_token),
                    sorted(principal.roles),
                    json.dumps(principal.scope) if principal.scope else None,
                    settings.session_ttl_seconds,
                ),
            )
        ).fetchone()
        if record is None:
            raise RuntimeError("dashboard session creation returned no record")
        await _store_mutation(
            conn,
            principal,
            idempotency_key,
            "dashboard.session.create",
            {},
            {"session_id": record["id"], "expires_at": record["expires_at"]},
        )
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_token,
        max_age=settings.session_ttl_seconds,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="strict",
        path="/",
    )
    return {
        "session_id": record["id"],
        "expires_at": record["expires_at"],
        "csrf_token": csrf_token,
    }


@app.delete("/v1/dashboard/session", status_code=204)
async def delete_dashboard_session(
    response: Response,
    _: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(authenticate),
) -> Response:
    if principal.session_id is None:
        raise HTTPException(status_code=400, detail="dashboard_session_required")
    if not principal.csrf_valid:
        raise HTTPException(status_code=403, detail="csrf_failed")
    async with connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE braincore.dashboard_sessions SET revoked_at=COALESCE(revoked_at,now())
               WHERE id=%s AND tenant_id=%s""",
            (principal.session_id, principal.tenant_id),
        )
    settings = get_settings()
    response.delete_cookie(
        SESSION_COOKIE_NAME,
        path="/",
        httponly=True,
        secure=settings.cookie_secure,
        samesite="strict",
    )
    response.status_code = 204
    return response


@app.post("/v1/memories", response_model=OperationAccepted, status_code=202)
async def remember(
    body: RememberRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> OperationAccepted:
    principal.require_scope(body.scope)
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn, principal, idempotency_key, body.model_dump(mode="json"), "remember", 1
        )
        if not reused:
            await remember_one(conn, principal, operation_id, 0, body)
        state_cursor = await conn.execute(
            "SELECT state FROM braincore.operations WHERE id = %s AND tenant_id = %s",
            (operation_id, principal.tenant_id),
        )
        row = await state_cursor.fetchone()
    return OperationAccepted(operation_id=operation_id, state=str(row["state"]), reused=reused)  # type: ignore[index]


@app.post("/v1/ingestions", response_model=OperationAccepted, status_code=202)
async def ingest(
    body: IngestRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> OperationAccepted:
    principal.require_scope(body.scope)
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn, principal, idempotency_key, body.model_dump(mode="json"), "ingest", 1
        )
        if not reused:
            await enqueue_ingestion_one(conn, principal, operation_id, 0, body)
        state = await (
            await conn.execute(
                "SELECT state FROM braincore.operations WHERE id=%s AND tenant_id=%s",
                (operation_id, principal.tenant_id),
            )
        ).fetchone()
        if state is None:
            raise RuntimeError("accepted ingestion operation disappeared")
    return OperationAccepted(
        operation_id=operation_id,
        state=str(state["state"]),
        reused=reused,
    )


def _item_error(error: PydanticValidationError) -> dict[str, object]:
    return {
        "code": "validation_failed",
        "message": "Batch item validation failed.",
        "retryable": False,
        "details": {"errors": validation_error_details(error.errors())},
    }


async def _batch_operation(
    body: RawBatchRequest,
    idempotency_key: str,
    principal: Principal,
    model: type[RememberRequest] | type[IngestRequest],
    kind: str,
) -> OperationAccepted:
    validated: list[RememberRequest | IngestRequest | None] = []
    errors: list[dict[str, object] | None] = []
    for raw_item in body.items:
        try:
            validated.append(model.model_validate(raw_item))
            errors.append(None)
        except PydanticValidationError as error:
            validated.append(None)
            errors.append(_item_error(error))
    if all(item is None for item in validated):
        raise HTTPException(status_code=422, detail="validation_failed")
    for item in validated:
        if item is not None:
            principal.require_scope(item.scope)
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn,
            principal,
            idempotency_key,
            # Preserve malformed raw item keys for the request fingerprint;
            # JSON-mode serialization rejects surrogate keys before item outcomes.
            body.model_dump(),
            kind,
            len(body.items),
        )
        if not reused:
            for index, item in enumerate(validated):
                item_error = errors[index]
                if item is not None:
                    try:
                        # PostgreSQL errors abort their transaction. Roll back
                        # the entire item before storing its terminal outcome.
                        async with conn.transaction():
                            if isinstance(item, RememberRequest):
                                await remember_one(conn, principal, operation_id, index, item)
                            else:
                                await enqueue_ingestion_one(
                                    conn, principal, operation_id, index, item
                                )
                    except HTTPException as error:
                        if error.status_code != 409 or error.detail not in (
                            "memory_conflict",
                            "reference_scope_conflict",
                            "structured_fact_revision_required",
                        ):
                            raise
                        item_error = {
                            "code": error.detail,
                            "message": error.detail.replace("_", " ").capitalize(),
                            "retryable": False,
                            "details": {},
                        }
                    except (DataError, IntegrityError):
                        # Database diagnostics can contain source content and
                        # schema details. Only expose a stable, generic outcome.
                        # Connection/system failures still abort the request.
                        item_error = {
                            "code": "batch_item_failed",
                            "message": "Batch item could not be stored.",
                            "retryable": False,
                            "details": {},
                        }
                if item_error is not None:
                    await conn.execute(
                        """INSERT INTO braincore.operation_items
                           (operation_id,tenant_id,item_index,state,error,completed_at)
                           VALUES (%s,%s,%s,'failed',%s,now())""",
                        (operation_id, principal.tenant_id, index, json.dumps(item_error)),
                    )
            # Ready reference upserts can finish synchronously without a job.
            # Account for every terminal item, including validation failures.
            await conn.execute(
                """UPDATE braincore.operations operation
                   SET succeeded_items=items.succeeded,
                       failed_items=items.failed,
                       state=CASE WHEN items.succeeded+items.failed=operation.accepted_items
                                  THEN CASE WHEN items.failed=0 THEN 'succeeded'
                                            WHEN items.succeeded=0 THEN 'failed'
                                            ELSE 'partial' END
                                  ELSE operation.state END,
                       completed_at=CASE WHEN items.succeeded+items.failed=operation.accepted_items
                                         THEN now() ELSE operation.completed_at END
                   FROM (SELECT count(*) FILTER (WHERE state='succeeded')::int AS succeeded,
                                count(*) FILTER (WHERE state='failed')::int AS failed
                         FROM braincore.operation_items
                         WHERE operation_id=%s AND tenant_id=%s) items
                   WHERE operation.id=%s AND operation.tenant_id=%s""",
                (operation_id, principal.tenant_id, operation_id, principal.tenant_id),
            )
        state = await (
            await conn.execute(
                "SELECT state FROM braincore.operations WHERE id=%s AND tenant_id=%s",
                (operation_id, principal.tenant_id),
            )
        ).fetchone()
        if state is None:
            raise RuntimeError("accepted batch operation disappeared")
    return OperationAccepted(
        operation_id=operation_id,
        state=str(state["state"]),
        reused=reused,
    )


@app.post("/v1/memories:batch", response_model=OperationAccepted, status_code=202)
async def remember_batch(
    body: RawBatchRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> OperationAccepted:
    return await _batch_operation(
        body, idempotency_key, principal, RememberRequest, "remember_batch"
    )


@app.post("/v1/ingestions:batch", response_model=OperationAccepted, status_code=202)
async def ingest_batch(
    body: RawBatchRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> OperationAccepted:
    return await _batch_operation(body, idempotency_key, principal, IngestRequest, "ingest_batch")


def _scope_sql(scope: object | None, values: list[object]) -> list[str]:
    clauses: list[str] = []
    if scope:
        for field in ("project_id", "user_id", "agent_id", "run_id"):
            value = scope.get(field) if isinstance(scope, dict) else getattr(scope, field)
            if value is not None:
                clauses.append(f"m.{field} = %s")
                values.append(value)
    return clauses


@app.post("/v1/search", response_model=SearchResponse, response_model_exclude_unset=True)
async def search(
    body: SearchRequest,
    request: Request,
    principal: Principal = Depends(require_read),
) -> dict[str, Any]:
    # Authorize before starting any work outside PostgreSQL.
    if body.scope is not None:
        principal.require_scope(body.scope)
    started = time.perf_counter()
    settings = get_settings()
    embedding_ms = 0.0
    cache_hit = False
    active_stage = "eligibility"
    stage_started = started

    def observe_stage(name: str) -> None:
        nonlocal active_stage, stage_started
        active_stage = name
        stage_started = time.perf_counter()

    async def timed_embedding(query: str) -> EmbeddingResult:
        nonlocal embedding_ms, cache_hit
        embed_started = time.perf_counter()
        result = await embed_one(query, tenant_id=str(principal.tenant_id))
        embedding_ms = (time.perf_counter() - embed_started) * 1000
        cache_hit = result.cache_hit
        return result

    embedding_task = asyncio.create_task(timed_embedding(body.query)) if body.query else None
    try:
        outcome = await memory_service.search_memories(
            connection, principal, body, embedding_task, settings, stage_observer=observe_stage
        )
    except BaseException:
        logger.warning(
            json.dumps(
                {
                    "event": "braincore_search_aborted",
                    "request_id": request.state.request_id,
                    "stage": active_stage,
                    "stage_elapsed_ms": round((time.perf_counter() - stage_started) * 1000, 3),
                    "request_work_ms": round((time.perf_counter() - started) * 1000, 3),
                    "vector_mode": settings.search_vector_mode,
                },
                separators=(",", ":"),
            )
        )
        raise
    finally:
        if embedding_task is not None and not embedding_task.done():
            embedding_task.cancel()
            with suppress(asyncio.CancelledError):
                await embedding_task
    if outcome.degraded:
        _degraded_searches[principal.tenant_id] = _degraded_searches.get(principal.tenant_id, 0) + 1
    timings = {
        **outcome.timings_ms,
        "embedding": round(embedding_ms, 3),
        "request_work": round((time.perf_counter() - started) * 1000, 3),
    }
    outcome.payload["retrieval_status"] = "degraded" if outcome.degraded else "complete"
    if settings.search_diagnostics:
        outcome.payload["diagnostics"] = {
            "timings_ms": timings,
            "embedding_cache_hit": cache_hit,
        }
        logger.info(
            json.dumps(
                {
                    "event": "braincore_search",
                    "timings_ms": timings,
                    "embedding_cache_hit": cache_hit,
                    "degraded": outcome.degraded,
                    "result_count": len(outcome.payload["results"]),
                },
                separators=(",", ":"),
            )
        )
    return outcome.payload


@app.post("/v1/admin/embeddings:refresh", response_model=EmbeddingRefreshAccepted, status_code=202)
async def refresh_embeddings(
    body: EmbeddingRefreshRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> EmbeddingRefreshAccepted:
    """Queue a bounded, audited batch of missing, failed or incompatible embeddings."""
    settings = get_settings()
    request_body = {
        **body.model_dump(mode="json"),
        "model": settings.embedding_model,
        "preprocessing_revision": settings.embedding_preprocessing_revision,
    }
    async with connection() as conn, conn.transaction():
        # A fresh statement snapshot after this lock observes another refresh's
        # committed jobs, even when the two requests use different keys.
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"{principal.tenant_id}:embedding_refresh",),
        )
        operation_id, reused = await begin_idempotent_operation(
            conn, principal, idempotency_key, request_body, "embedding_refresh", 0
        )
        if not reused:
            # Page the tenant's UUID index before applying scope/repair filters.
            # LIMIT then bounds rows examined as well as jobs enqueued, even
            # when the whole corpus is healthy or the requested scope is tiny.
            scope_values: list[object] = []
            scope_clauses = _scope_sql(body.scope, scope_values)
            needs_refresh = sql.SQL(
                "({scope}) AND m.content ~ '[^[:space:]]' "
                "AND (m.embedding IS NULL OR m.embedding_state <> 'ready' "
                "OR m.embedding_model IS DISTINCT FROM %s "
                "OR m.embedding_preprocessing_revision IS DISTINCT FROM %s)"
            ).format(
                scope=sql.SQL(" AND ").join(map(sql.SQL, scope_clauses))
                if scope_clauses
                else sql.SQL("true")
            )
            after = sql.SQL(" AND m.id > %s") if body.after_id else sql.SQL("")
            values = [
                *scope_values,
                settings.embedding_model,
                settings.embedding_preprocessing_revision,
                principal.tenant_id,
            ]
            if body.after_id:
                values.append(body.after_id)
            query = sql.SQL(
                "SELECT m.id,({needs}) AS needs_refresh FROM braincore.memories m "
                "WHERE m.tenant_id=%s {after} ORDER BY m.id "
                "LIMIT %s"
            ).format(needs=needs_refresh, after=after)
            rows = await (await conn.execute(query, [*values, body.scan_limit])).fetchall()
            accepted_count = 0
            scanned_count = 0
            next_cursor = None
            rescan_recommended = False
            for row in rows:
                scanned_count += 1
                memory_id = row["id"]
                next_cursor = str(memory_id)
                if not row["needs_refresh"]:
                    continue
                # Lock only this page's eligible row, rechecking after concurrent
                # worker/correction changes; locked rows cannot extend the page.
                lock_query = sql.SQL(
                    "SELECT m.id FROM braincore.memories m WHERE ({needs}) "
                    "AND m.tenant_id=%s AND m.id=%s FOR UPDATE OF m SKIP LOCKED"
                ).format(needs=needs_refresh)
                locked = await (
                    await conn.execute(
                        lock_query,
                        [
                            *scope_values,
                            settings.embedding_model,
                            settings.embedding_preprocessing_revision,
                            principal.tenant_id,
                            memory_id,
                        ],
                    )
                ).fetchone()
                if locked is None:
                    rescan_recommended = True
                    continue
                active = await (
                    await conn.execute(
                        """SELECT 1 FROM braincore.jobs WHERE tenant_id=%s
                           AND kind='embed_memory' AND state IN ('queued','running')
                           AND payload->>'memory_id'=%s LIMIT 1""",
                        (principal.tenant_id, str(memory_id)),
                    )
                ).fetchone()
                if active is not None:
                    continue
                await conn.execute(
                    """UPDATE braincore.memories SET embedding=NULL,embedding_state='pending',
                       embedding_model=NULL,embedding_preprocessing_revision=NULL
                       WHERE tenant_id=%s AND id=%s""",
                    (principal.tenant_id, memory_id),
                )
                item = await (
                    await conn.execute(
                        """INSERT INTO braincore.operation_items
                           (operation_id,tenant_id,item_index,memory_id,result)
                           VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                        (
                            operation_id,
                            principal.tenant_id,
                            accepted_count,
                            memory_id,
                            json.dumps({"embedding_refresh": True}),
                        ),
                    )
                ).fetchone()
                if item is None:
                    raise RuntimeError("embedding refresh item was not created")
                await conn.execute(
                    """INSERT INTO braincore.jobs
                       (tenant_id,operation_id,operation_item_id,kind,payload)
                       VALUES (%s,%s,%s,'embed_memory',%s)""",
                    (
                        principal.tenant_id,
                        operation_id,
                        item["id"],
                        json.dumps({"memory_id": str(memory_id)}),
                    ),
                )
                accepted_count += 1
                if accepted_count == body.limit:
                    break
            if scanned_count == len(rows) and len(rows) < body.scan_limit:
                next_cursor = None
            await conn.execute(
                """UPDATE braincore.operations SET accepted_items=%s,
                   state=CASE WHEN %s=0 THEN 'succeeded' ELSE 'queued' END,
                   completed_at=CASE WHEN %s=0 THEN now() ELSE NULL END
                   WHERE tenant_id=%s AND id=%s""",
                (accepted_count, accepted_count, accepted_count, principal.tenant_id, operation_id),
            )
            await conn.execute(
                """INSERT INTO braincore.audit_events
                   (tenant_id,actor_key_id,action,target_type,target_id,details)
                   VALUES (%s,%s,'embedding.refresh','operation',%s,%s)""",
                (
                    principal.tenant_id,
                    principal.key_id,
                    operation_id,
                    json.dumps(
                        {
                            **request_body,
                            "accepted_items": accepted_count,
                            "scanned_items": len(rows),
                            "next_cursor": next_cursor,
                            "rescan_recommended": rescan_recommended,
                        }
                    ),
                ),
            )
        record = await (
            await conn.execute(
                "SELECT state FROM braincore.operations WHERE id=%s AND tenant_id=%s",
                (operation_id, principal.tenant_id),
            )
        ).fetchone()
        receipt = await (
            await conn.execute(
                """SELECT details FROM braincore.audit_events
                   WHERE tenant_id=%s AND target_id=%s AND action='embedding.refresh'
                     AND created_at=(SELECT created_at FROM braincore.operations
                                     WHERE tenant_id=%s AND id=%s)""",
                (principal.tenant_id, operation_id, principal.tenant_id, operation_id),
            )
        ).fetchone()
        if record is None or receipt is None or not isinstance(receipt["details"], dict):
            raise RuntimeError("embedding refresh operation disappeared")
        details = receipt["details"]
    return EmbeddingRefreshAccepted(
        operation_id=operation_id,
        state=str(record["state"]),
        reused=reused,
        scanned_items=int(details["scanned_items"]),
        next_cursor=details["next_cursor"],
        rescan_recommended=bool(details["rescan_recommended"]),
    )


async def _memory_or_404(memory_id: UUID, principal: Principal) -> dict[str, object]:
    async with connection() as conn:
        cursor = await conn.execute(
            """SELECT id,content,project_id,user_id,agent_id,run_id,scope,
                      reference_namespace,reference_key,state,trust,
                      valid_from,valid_until,embedding_state,metadata,created_at,updated_at,
                      object_type,legacy_external_id,legacy_scope_path
               FROM braincore.memories WHERE id = %s AND tenant_id = %s""",
            (memory_id, principal.tenant_id),
        )
        row = await cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="resource_not_found")
    if not principal.permits_scope(
        {field: row[field] for field in ("project_id", "user_id", "agent_id", "run_id")}
    ):
        raise HTTPException(status_code=404, detail="resource_not_found")
    return row


@app.post("/v1/memories:resolve")
async def resolve_memory(
    body: MemoryResolveRequest, principal: Principal = Depends(require_read)
) -> dict[str, object]:
    effective_scope = body.scope.model_dump() if body.scope else principal.scope
    if effective_scope is None:
        raise HTTPException(status_code=422, detail="scope_required")
    principal.require_scope(effective_scope)
    async with connection() as conn:
        row = await (
            await conn.execute(
                """SELECT id,content,project_id,user_id,agent_id,run_id,scope,
                          reference_namespace,reference_key,state,trust,valid_from,valid_until,
                          embedding_state,metadata,created_at,updated_at
                   FROM braincore.memories
                   WHERE tenant_id=%s AND reference_namespace=%s AND reference_key=%s
                     AND project_id IS NOT DISTINCT FROM %s
                     AND user_id IS NOT DISTINCT FROM %s
                     AND agent_id IS NOT DISTINCT FROM %s
                     AND run_id IS NOT DISTINCT FROM %s""",
                (
                    principal.tenant_id,
                    body.namespace,
                    body.key,
                    effective_scope.get("project_id"),
                    effective_scope.get("user_id"),
                    effective_scope.get("agent_id"),
                    effective_scope.get("run_id"),
                ),
            )
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="resource_not_found")
    if not principal.permits_scope(
        {field: row[field] for field in ("project_id", "user_id", "agent_id", "run_id")}
    ):
        raise HTTPException(status_code=404, detail="resource_not_found")
    return row


@app.get("/v1/memories/{memory_id}")
async def get_memory(
    memory_id: UUID, principal: Principal = Depends(require_read)
) -> dict[str, object]:
    return await _memory_or_404(memory_id, principal)


@app.get("/v1/memories/{memory_id}/explain")
async def explain(memory_id: UUID, principal: Principal = Depends(require_read)) -> dict[str, Any]:
    async with connection() as conn, conn.transaction():
        current = await (
            await conn.execute(
                """SELECT project_id,user_id,agent_id,run_id FROM braincore.memories
                   WHERE id=%s AND tenant_id=%s FOR SHARE""",
                (memory_id, principal.tenant_id),
            )
        ).fetchone()
        if current is None or not principal.permits_scope(current):
            raise HTTPException(status_code=404, detail="resource_not_found")
        evidence = await (
            await conn.execute(
                """SELECT e.artifact_id, e.quote, a.source_type, a.source_uri,
                          a.scope, a.created_at
                   FROM braincore.memory_evidence e
                   JOIN braincore.artifacts a
                     ON a.id=e.artifact_id AND a.tenant_id=e.artifact_tenant_id
                   JOIN braincore.memories m
                     ON m.id=e.memory_id AND m.tenant_id=e.tenant_id
                   LEFT JOIN braincore.artifact_access_grants g ON g.id=e.access_grant_id
                   WHERE e.memory_id=%s AND e.tenant_id=%s
                     AND (e.artifact_tenant_id=e.tenant_id OR
                          (g.revoked_at IS NULL
                           AND (g.expires_at IS NULL OR g.expires_at > statement_timestamp())
                           AND g.artifact_id=e.artifact_id
                           AND g.owner_tenant_id=e.artifact_tenant_id
                           AND g.grantee_tenant_id=e.tenant_id AND m.scope @> g.scope))""",
                (memory_id, principal.tenant_id),
            )
        ).fetchall()
        revisions = await (
            await conn.execute(
                """SELECT id, prior_revision_id, content, reason, created_at
                   FROM braincore.memory_revisions
                   WHERE memory_id = %s AND tenant_id = %s ORDER BY created_at""",
                (memory_id, principal.tenant_id),
            )
        ).fetchall()
        lifecycle = await (
            await conn.execute(
                """SELECT action, prior_state, next_state, reason, created_at
                   FROM braincore.lifecycle_events
                   WHERE memory_id = %s AND tenant_id = %s ORDER BY created_at""",
                (memory_id, principal.tenant_id),
            )
        ).fetchall()
        feedback_history = await (
            await conn.execute(
                """SELECT rating,comment,created_at
                   FROM braincore.feedback
                   WHERE memory_id=%s AND tenant_id=%s ORDER BY created_at""",
                (memory_id, principal.tenant_id),
            )
        ).fetchall()
        legacy_provenance = await read_provenance(conn, principal, memory_id)
    return {
        "memory_id": memory_id,
        "legacy_provenance": legacy_provenance,
        "evidence": evidence,
        "revisions": revisions,
        "lifecycle": lifecycle,
        "feedback": feedback_history,
    }


@app.get("/v1/operations/{operation_id}")
async def operation(
    operation_id: UUID, principal: Principal = Depends(require_operation_read)
) -> dict[str, Any]:
    async with connection() as conn:
        cursor = await conn.execute(
            """SELECT id,actor_key_id,kind,state,accepted_items,succeeded_items,failed_items,
                      warning_items,error,created_at,started_at,completed_at
               FROM braincore.operations WHERE id = %s AND tenant_id = %s""",
            (operation_id, principal.tenant_id),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        if "admin" not in principal.roles and UUID(str(row["actor_key_id"])) != principal.key_id:
            raise HTTPException(status_code=404, detail="resource_not_found")
        items = await (
            await conn.execute(
                """SELECT id,item_index,state,memory_id,result,error,created_at,completed_at
                   FROM braincore.operation_items
                   WHERE operation_id = %s AND tenant_id = %s ORDER BY item_index""",
                (operation_id, principal.tenant_id),
            )
        ).fetchall()
    return {**row, "items": items}


@app.post("/v1/memories/{memory_id}/revisions")
async def revise(
    memory_id: UUID,
    body: RevisionRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> dict[str, Any]:
    revision_id: object | None = None
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn,
            principal,
            idempotency_key,
            {"memory_id": str(memory_id), "body": body.model_dump()},
            "revise",
            1,
        )
        if not reused:
            current = await (
                await conn.execute(
                    "SELECT content,project_id,user_id,agent_id,run_id,object_type,"
                    "legacy_source_id,legacy_attributes_ready,reference_namespace,reference_key "
                    "FROM braincore.memories "
                    "WHERE id=%s AND tenant_id=%s FOR UPDATE",
                    (memory_id, principal.tenant_id),
                )
            ).fetchone()
            if current is None:
                raise HTTPException(status_code=404, detail="resource_not_found")
            if not principal.permits_scope(
                {field: current[field] for field in ("project_id", "user_id", "agent_id", "run_id")}
            ):
                raise HTTPException(status_code=404, detail="resource_not_found")
            if current["object_type"] == "fact" or (
                current["object_type"] in {"memory", "episode"}
                and (current["legacy_source_id"] is not None or current["legacy_attributes_ready"])
            ):
                raise HTTPException(status_code=409, detail="structured_fact_revision_required")
            dedupe_key = memory_dedupe_hash(
                body.content,
                {
                    field: current[field]
                    for field in ("project_id", "user_id", "agent_id", "run_id")
                },
                {"namespace": current["reference_namespace"], "key": current["reference_key"]}
                if current["reference_key"] is not None
                else None,
            )
            conflict = await (
                await conn.execute(
                    """SELECT id FROM braincore.memories
                       WHERE tenant_id=%s AND dedupe_key=%s AND id<>%s""",
                    (principal.tenant_id, dedupe_key, memory_id),
                )
            ).fetchone()
            if conflict is not None:
                raise HTTPException(status_code=409, detail="memory_conflict")
            prior = await (
                await conn.execute(
                    "SELECT id FROM braincore.memory_revisions "
                    "WHERE tenant_id=%s AND memory_id=%s ORDER BY created_at DESC LIMIT 1",
                    (principal.tenant_id, memory_id),
                )
            ).fetchone()
            revision = await (
                await conn.execute(
                    """INSERT INTO braincore.memory_revisions
                       (tenant_id,memory_id,prior_revision_id,content,reason)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (
                        principal.tenant_id,
                        memory_id,
                        prior["id"] if prior else None,
                        body.content,
                        body.reason,
                    ),
                )
            ).fetchone()
            if revision is None:
                raise RuntimeError("revision creation returned no record")
            revision_id = revision["id"]
            await conn.execute(
                "UPDATE braincore.memories "
                "SET content=%s,dedupe_key=%s,embedding=NULL,embedding_state='pending', "
                "embedding_model=NULL,embedding_preprocessing_revision=NULL,updated_at=now() "
                "WHERE id=%s AND tenant_id=%s",
                (body.content, dedupe_key, memory_id, principal.tenant_id),
            )
            item = await (
                await conn.execute(
                    """INSERT INTO braincore.operation_items
                   (operation_id,tenant_id,item_index,state,memory_id,result)
                   VALUES (%s,%s,0,'queued',%s,%s) RETURNING id""",
                    (
                        operation_id,
                        principal.tenant_id,
                        memory_id,
                        json.dumps({"revision_id": str(revision_id)}),
                    ),
                )
            ).fetchone()
            if item is None:
                raise RuntimeError("failed to create revision embedding item")
            await conn.execute(
                """INSERT INTO braincore.jobs
                   (tenant_id,operation_id,operation_item_id,kind,payload)
                   VALUES (%s,%s,%s,'embed_memory',%s)""",
                (
                    principal.tenant_id,
                    operation_id,
                    item["id"],
                    json.dumps({"memory_id": str(memory_id)}),
                ),
            )
            await conn.execute(
                """INSERT INTO braincore.audit_events
                   (tenant_id,actor_key_id,action,target_type,target_id,details)
                   VALUES (%s,%s,'memory.revise','memory',%s,%s)""",
                (
                    principal.tenant_id,
                    principal.key_id,
                    memory_id,
                    json.dumps({"revision_id": str(revision_id)}),
                ),
            )
        else:
            replay_item = await (
                await conn.execute(
                    """SELECT result FROM braincore.operation_items
                       WHERE operation_id=%s AND tenant_id=%s AND item_index=0""",
                    (operation_id, principal.tenant_id),
                )
            ).fetchone()
            replay_result = replay_item["result"] if replay_item else None
            if isinstance(replay_result, dict):
                revision_id = replay_result.get("revision_id")
    return {
        "operation_id": operation_id,
        "revision_id": revision_id,
        "reused": reused,
    }


@app.patch("/v1/memories/{memory_id}/lifecycle")
async def lifecycle(
    memory_id: UUID,
    body: LifecycleRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> dict[str, Any]:
    target = {
        "suppress": "suppressed",
        "retire": "retired",
        "restore": "active",
        "promote": "active",
    }[body.action]
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn,
            principal,
            idempotency_key,
            {"memory_id": str(memory_id), "body": body.model_dump()},
            "lifecycle",
            1,
        )
        if not reused:
            current = await (
                await conn.execute(
                    "SELECT state,project_id,user_id,agent_id,run_id FROM braincore.memories "
                    "WHERE id=%s AND tenant_id=%s FOR UPDATE",
                    (memory_id, principal.tenant_id),
                )
            ).fetchone()
            if current is None:
                raise HTTPException(status_code=404, detail="resource_not_found")
            if not principal.permits_scope(
                {field: current[field] for field in ("project_id", "user_id", "agent_id", "run_id")}
            ):
                raise HTTPException(status_code=404, detail="resource_not_found")
            trust = "corroborated" if body.action == "promote" else None
            await conn.execute(
                """UPDATE braincore.memories
                   SET state=%s,trust=COALESCE(%s,trust),updated_at=now()
                   WHERE id=%s AND tenant_id=%s""",
                (target, trust, memory_id, principal.tenant_id),
            )
            await conn.execute(
                """INSERT INTO braincore.lifecycle_events
                   (tenant_id,memory_id,action,prior_state,next_state,reason,actor_key_id)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (
                    principal.tenant_id,
                    memory_id,
                    body.action,
                    current["state"],
                    target,
                    body.reason,
                    principal.key_id,
                ),
            )
            await conn.execute(
                """INSERT INTO braincore.audit_events
                   (tenant_id,actor_key_id,action,target_type,target_id,details)
                   VALUES (%s,%s,'memory.lifecycle','memory',%s,%s)""",
                (
                    principal.tenant_id,
                    principal.key_id,
                    memory_id,
                    json.dumps({"action": body.action, "reason": body.reason}),
                ),
            )
            await conn.execute(
                """INSERT INTO braincore.operation_items
                   (operation_id,tenant_id,item_index,state,memory_id,completed_at)
                   VALUES (%s,%s,0,'succeeded',%s,now())""",
                (operation_id, principal.tenant_id, memory_id),
            )
            await conn.execute(
                "UPDATE braincore.operations "
                "SET state='succeeded',succeeded_items=1,completed_at=now() "
                "WHERE id=%s AND tenant_id=%s",
                (operation_id, principal.tenant_id),
            )
    return {"operation_id": operation_id, "memory_id": memory_id, "state": target, "reused": reused}


@app.post("/v1/memories/{memory_id}/feedback")
async def feedback(
    memory_id: UUID,
    body: FeedbackRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_write),
) -> dict[str, Any]:
    feedback_id: object | None = None
    async with connection() as conn, conn.transaction():
        operation_id, reused = await begin_idempotent_operation(
            conn,
            principal,
            idempotency_key,
            {"memory_id": str(memory_id), "body": body.model_dump()},
            "feedback",
            1,
        )
        if not reused:
            exists = await (
                await conn.execute(
                    "SELECT project_id,user_id,agent_id,run_id FROM braincore.memories "
                    "WHERE id=%s AND tenant_id=%s",
                    (memory_id, principal.tenant_id),
                )
            ).fetchone()
            if exists is None:
                raise HTTPException(status_code=404, detail="resource_not_found")
            if not principal.permits_scope(
                {field: exists[field] for field in ("project_id", "user_id", "agent_id", "run_id")}
            ):
                raise HTTPException(status_code=404, detail="resource_not_found")
            record = await (
                await conn.execute(
                    """INSERT INTO braincore.feedback
                       (tenant_id,memory_id,rating,comment,actor_key_id)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (principal.tenant_id, memory_id, body.rating, body.comment, principal.key_id),
                )
            ).fetchone()
            if record is None:
                raise RuntimeError("feedback creation returned no record")
            feedback_id = record["id"]
            await conn.execute(
                """INSERT INTO braincore.audit_events
                   (tenant_id,actor_key_id,action,target_type,target_id,details)
                   VALUES (%s,%s,'memory.feedback','memory',%s,%s)""",
                (
                    principal.tenant_id,
                    principal.key_id,
                    memory_id,
                    json.dumps({"feedback_id": str(feedback_id), "rating": body.rating}),
                ),
            )
            await conn.execute(
                """INSERT INTO braincore.operation_items
                   (operation_id,tenant_id,item_index,state,memory_id,result,completed_at)
                   VALUES (%s,%s,0,'succeeded',%s,%s,now())""",
                (
                    operation_id,
                    principal.tenant_id,
                    memory_id,
                    json.dumps({"feedback_id": str(feedback_id)}),
                ),
            )
            await conn.execute(
                "UPDATE braincore.operations "
                "SET state='succeeded',succeeded_items=1,completed_at=now() "
                "WHERE id=%s AND tenant_id=%s",
                (operation_id, principal.tenant_id),
            )
        else:
            replay_item = await (
                await conn.execute(
                    """SELECT result FROM braincore.operation_items
                       WHERE operation_id=%s AND tenant_id=%s AND item_index=0""",
                    (operation_id, principal.tenant_id),
                )
            ).fetchone()
            replay_result = replay_item["result"] if replay_item else None
            if isinstance(replay_result, dict):
                feedback_id = replay_result.get("feedback_id")
    return {
        "operation_id": operation_id,
        "feedback_id": feedback_id,
        "reused": reused,
    }


@app.post("/v1/operations/{operation_id}/retry", status_code=202)
async def retry_operation(
    operation_id: UUID,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    if not idempotency_key or len(idempotency_key) > 200:
        raise HTTPException(status_code=422, detail="invalid_idempotency_key")
    request_hash = canonical_hash({"kind": "operation_retry", "operation_id": str(operation_id)})
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"{principal.tenant_id}:{principal.key_id}:{idempotency_key}",),
        )
        await conn.execute(
            """DELETE FROM braincore.idempotency_records
               WHERE tenant_id=%s AND actor_key_id=%s AND key=%s AND expires_at <= now()""",
            (principal.tenant_id, principal.key_id, idempotency_key),
        )
        prior = await (
            await conn.execute(
                """SELECT request_hash,operation_id FROM braincore.idempotency_records
                   WHERE tenant_id=%s AND actor_key_id=%s AND key=%s
                     AND expires_at > now() FOR UPDATE""",
                (principal.tenant_id, principal.key_id, idempotency_key),
            )
        ).fetchone()
        if prior is not None:
            prior_hash = prior["request_hash"]
            if not isinstance(prior_hash, bytes | bytearray | memoryview):
                raise RuntimeError("database returned an invalid request hash")
            if bytes(prior_hash) != request_hash:
                raise HTTPException(status_code=409, detail="idempotency_conflict")
            return {"operation_id": prior["operation_id"], "state": "queued", "reused": True}
        operation_row = await (
            await conn.execute(
                """SELECT state FROM braincore.operations
                   WHERE id=%s AND tenant_id=%s FOR UPDATE""",
                (operation_id, principal.tenant_id),
            )
        ).fetchone()
        if operation_row is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        if operation_row["state"] not in {"partial", "failed", "dead_letter"}:
            raise HTTPException(status_code=409, detail="operation_not_retryable")
        jobs = await (
            await conn.execute(
                """UPDATE braincore.jobs
                   SET state='queued',attempts=0,available_at=now(),leased_until=NULL,
                       leased_by=NULL,last_error=NULL,completed_at=NULL
                   WHERE operation_id=%s AND tenant_id=%s
                     AND state IN ('dead_letter','failed')
                   RETURNING operation_item_id""",
                (operation_id, principal.tenant_id),
            )
        ).fetchall()
        item_ids = [row["operation_item_id"] for row in jobs if row["operation_item_id"]]
        if not item_ids:
            raise HTTPException(status_code=409, detail="operation_not_retryable")
        await conn.execute(
            """UPDATE braincore.operation_items
               SET state='queued',error=NULL,completed_at=NULL
               WHERE id=ANY(%s) AND tenant_id=%s""",
            (item_ids, principal.tenant_id),
        )
        await conn.execute(
            """UPDATE braincore.operations
               SET state='queued',error=NULL,completed_at=NULL
               WHERE id=%s AND tenant_id=%s""",
            (operation_id, principal.tenant_id),
        )
        await conn.execute(
            """INSERT INTO braincore.idempotency_records
               (tenant_id,actor_key_id,key,request_hash,operation_id)
               VALUES (%s,%s,%s,%s,%s)""",
            (
                principal.tenant_id,
                principal.key_id,
                idempotency_key,
                request_hash,
                operation_id,
            ),
        )
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id)
               VALUES (%s,%s,'operation.retry','operation',%s)""",
            (principal.tenant_id, principal.key_id, operation_id),
        )
    return {"operation_id": operation_id, "state": "queued", "reused": False}


_dashboard_candidates = (
    Path.cwd() / "dashboard" / "dist",
    Path(__file__).resolve().parents[2] / "dashboard" / "dist",
)
_dashboard_dir = next(
    (candidate for candidate in _dashboard_candidates if candidate.is_dir()),
    _dashboard_candidates[0],
)
if (_dashboard_dir / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=_dashboard_dir / "assets"), name="dashboard-assets")


@app.get("/", include_in_schema=False)
async def dashboard_index() -> Response:
    index = _dashboard_dir / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=404, detail="dashboard_not_built")
    return FileResponse(index)


@app.get("/{dashboard_path:path}", include_in_schema=False)
async def dashboard_fallback(dashboard_path: str) -> Response:
    if dashboard_path.startswith("v1/"):
        raise HTTPException(status_code=404, detail="resource_not_found")
    index = _dashboard_dir / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=404, detail="dashboard_not_built")
    return FileResponse(index)


@app.post("/v1/artifacts/{artifact_id}/grants", status_code=201)
async def grant_evidence(
    artifact_id: UUID,
    body: EvidenceGrantRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    if not body.reason.strip():
        raise HTTPException(status_code=422, detail="reason_required")
    if body.expires_at is not None and (
        body.expires_at.tzinfo is None or body.expires_at <= datetime.now(UTC)
    ):
        raise HTTPException(status_code=422, detail="future_expiry_required")
    if body.grantee_tenant_id == principal.tenant_id:
        raise HTTPException(status_code=422, detail="sharing_requires_another_tenant")
    payload = {"artifact_id": str(artifact_id), **body.model_dump(mode="json")}
    async with connection() as conn, conn.transaction():
        owned = await (
            await conn.execute(
                "SELECT id FROM braincore.artifacts WHERE id=%s AND tenant_id=%s FOR KEY SHARE",
                (artifact_id, principal.tenant_id),
            )
        ).fetchone()
        if owned is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        replay = await _mutation_replay(conn, principal, idempotency_key, "evidence.grant", payload)
        if replay is not None:
            return replay
        recipient = await (
            await conn.execute(
                "SELECT id FROM braincore.tenants WHERE id=%s", (body.grantee_tenant_id,)
            )
        ).fetchone()
        if recipient is None:
            raise HTTPException(status_code=422, detail="invalid_recipient")
        record = await (
            await conn.execute(
                """INSERT INTO braincore.artifact_access_grants
                   (artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason,actor_key_id,expires_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    artifact_id,
                    principal.tenant_id,
                    body.grantee_tenant_id,
                    json.dumps(body.scope.model_dump(exclude_none=True)),
                    body.reason,
                    principal.key_id,
                    body.expires_at,
                ),
            )
        ).fetchone()
        if record is None:
            raise RuntimeError("grant creation returned no record")
        grant_id = str(record["id"])
        response = {"grant_id": grant_id, "artifact_id": str(artifact_id), "reused": False}
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id,details)
               VALUES (%s,%s,'evidence.grant','artifact',%s,%s)""",
            (
                principal.tenant_id,
                principal.key_id,
                artifact_id,
                json.dumps({**payload, "grant_id": grant_id}),
            ),
        )
        await _store_mutation(conn, principal, idempotency_key, "evidence.grant", payload, response)
    return response


@app.post("/v1/evidence-grants/{grant_id}/revoke")
async def revoke_evidence_grant(
    grant_id: UUID,
    body: EvidenceGrantRevokeRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    if not body.reason.strip():
        raise HTTPException(status_code=422, detail="reason_required")
    payload = {"grant_id": str(grant_id), **body.model_dump(mode="json")}
    async with connection() as conn, conn.transaction():
        record = await (
            await conn.execute(
                """SELECT artifact_id,revoked_at FROM braincore.artifact_access_grants
                   WHERE id=%s AND owner_tenant_id=%s FOR UPDATE""",
                (grant_id, principal.tenant_id),
            )
        ).fetchone()
        if record is None:
            raise HTTPException(status_code=404, detail="resource_not_found")
        replay = await _mutation_replay(
            conn, principal, idempotency_key, "evidence.revoke", payload
        )
        if replay is not None:
            return replay
        if record["revoked_at"] is None:
            await conn.execute(
                "UPDATE braincore.artifact_access_grants SET revoked_at=now() WHERE id=%s",
                (grant_id,),
            )
        response = {"grant_id": str(grant_id), "revoked": True, "reused": False}
        await conn.execute(
            """INSERT INTO braincore.audit_events
               (tenant_id,actor_key_id,action,target_type,target_id,details)
               VALUES (%s,%s,'evidence.revoke','artifact',%s,%s)""",
            (principal.tenant_id, principal.key_id, record["artifact_id"], json.dumps(payload)),
        )
        await _store_mutation(
            conn, principal, idempotency_key, "evidence.revoke", payload, response
        )
    return response

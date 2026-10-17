"""Authenticated canonical mutations and atomic ingestion for legacy clients."""

import hashlib
from functools import partial
from typing import Annotated, Any, Literal
from uuid import UUID

import anyio
import psycopg
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .auth import Principal, require_write
from .config import get_settings
from .legacy_clients import mutate_legacy_object
from .legacy_types import FIELD_TYPES
from .legacy_writes import _finish, _prior_receipt, mutate_artifact, mutate_control

router = APIRouter()
CORE_TYPES = {"entity", "fact", "memory", "segment", "episode"}


class LegacyMutationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    object_type: str
    object_id: UUID | None = None
    fields: dict[str, Any] = Field(default_factory=dict, max_length=110)
    action: Literal["upsert", "suppress", "retire", "restore"] = "upsert"
    reason: str = Field(default="legacy client mutation", min_length=1, max_length=1000)

    @field_validator("object_type")
    @classmethod
    def supported_kind(cls, value: str) -> str:
        if value not in FIELD_TYPES:
            raise ValueError("unsupported legacy object type")
        return value


class LegacyBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[LegacyMutationRequest] = Field(min_length=1, max_length=20000)


def dispatch(
    conn: psycopg.Connection[Any], principal: Principal, body: LegacyMutationRequest, key: str
) -> dict[str, Any]:
    values: dict[str, Any] = dict(
        fields=body.fields,
        object_id=body.object_id,
        action=body.action,
        reason=body.reason,
        idempotency_key=key,
    )
    if body.object_type in CORE_TYPES:
        return mutate_legacy_object(conn, principal, object_type=body.object_type, **values)
    if body.object_type == "artifact":
        return mutate_artifact(conn, principal, **values)
    return mutate_control(conn, principal, object_type=body.object_type, **values)


def _mutate(principal: Principal, body: LegacyMutationRequest, key: str) -> dict[str, Any]:
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), row_factory=dict_row
    ) as conn:
        return dispatch(conn, principal, body, key)


def _batch(principal: Principal, body: LegacyBatchRequest, key: str) -> dict[str, Any]:
    encoded = body.model_dump(mode="json")
    with (
        psycopg.connect(
            get_settings().database_url.get_secret_value(), row_factory=dict_row
        ) as conn,
        conn.transaction(),
    ):
        digest, prior = _prior_receipt(conn, principal, key, {"batch": encoded})
        if prior is not None:
            return prior
        aliases: dict[str, str] = {}
        results = []
        for index, original in enumerate(body.items):
            fields = dict(original.fields)
            # Logical dedupe may return an older display UUID. Later references
            # to an originally preallocated UUID must resolve in this transaction.
            for name, value in fields.items():
                if FIELD_TYPES[original.object_type].get(name) == "uuid" and value is not None:
                    fields[name] = aliases.get(str(value), value)
                elif FIELD_TYPES[original.object_type].get(name) == "uuid[]" and isinstance(
                    value, list
                ):
                    fields[name] = [aliases.get(str(element), element) for element in value]
            item = original.model_copy(update={"fields": fields})
            child_key = (
                "legacy-batch:"
                + hashlib.sha256(
                    (str(principal.key_id) + ":" + key + ":" + str(index)).encode()
                ).hexdigest()
            )
            result = dispatch(conn, principal, item, child_key)
            results.append(result)
            if original.object_id is not None:
                aliases[str(original.object_id)] = str(result["object_id"])
        response = {"results": results}
        return _finish(conn, principal, key, digest, "batch", principal.key_id, response)


async def _run(
    request: Request, callback: Any, principal: Principal, body: Any, key: str
) -> dict[str, Any]:
    if not hasattr(request.app.state, "legacy_mutation_limiter"):
        request.app.state.legacy_mutation_limiter = anyio.CapacityLimiter(4)
    try:
        return await anyio.to_thread.run_sync(
            partial(callback, principal, body, key),
            limiter=request.app.state.legacy_mutation_limiter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except psycopg.IntegrityError:
        raise HTTPException(status_code=409, detail="legacy_constraint_conflict") from None


@router.post("/v1/legacy/objects:mutate")
async def legacy_mutate(
    request: Request,
    body: LegacyMutationRequest,
    principal: Annotated[Principal, Depends(require_write)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
) -> dict[str, Any]:
    return await _run(request, _mutate, principal, body, idempotency_key)


@router.post("/v1/legacy/objects:batch")
async def legacy_batch(
    request: Request,
    body: LegacyBatchRequest,
    principal: Annotated[Principal, Depends(require_write)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
) -> dict[str, Any]:
    return await _run(request, _batch, principal, body, idempotency_key)

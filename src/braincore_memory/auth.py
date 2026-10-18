import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request, Security, status
from fastapi.security import APIKeyCookie, HTTPAuthorizationCredentials, HTTPBearer

from .db import connection

bearer_scheme = HTTPBearer(auto_error=False, description="Tenant-bound BrainCore API key")
SESSION_COOKIE_NAME = "braincore_session"
session_scheme = APIKeyCookie(name=SESSION_COOKIE_NAME, auto_error=False)


@dataclass(frozen=True)
class Principal:
    key_id: UUID
    tenant_id: UUID
    roles: frozenset[str]
    scope: dict[str, str] | None = None
    session_id: UUID | None = None
    csrf_valid: bool = True

    def require(self, role: str) -> None:
        if role not in self.roles and "admin" not in self.roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="permission_denied")

    def permits_scope(self, candidate: object) -> bool:
        if self.scope is None:
            return "admin" in self.roles
        if hasattr(candidate, "model_dump"):
            values = candidate.model_dump()
        elif isinstance(candidate, dict):
            values = candidate
        else:
            return False
        return all(values.get(field) == value for field, value in self.scope.items())

    def require_scope(self, candidate: object) -> None:
        if not self.permits_scope(candidate):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="scope_forbidden")


def key_digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


async def authenticate(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
    dashboard_cookie: str | None = Security(session_scheme),
    x_csrf_token: str | None = Header(default=None, alias="X-CSRF-Token"),
) -> Principal:
    raw = (
        credentials.credentials.strip()
        if credentials and credentials.scheme.lower() == "bearer"
        else None
    )
    session_token = (
        dashboard_cookie or request.cookies.get(SESSION_COOKIE_NAME) if raw is None else None
    )
    if raw is None and session_token is None:
        raise HTTPException(status_code=401, detail="authentication_required")
    digest = key_digest(raw or session_token or "")
    async with connection() as conn, conn.transaction():
        if raw is not None:
            row = await conn.execute(
                """SELECT id,tenant_id,roles,scope,NULL::uuid AS session_id,
                          NULL::bytea AS csrf_digest
                   FROM braincore.api_keys
                   WHERE digest=%s AND revoked_at IS NULL
                     AND (expires_at IS NULL OR expires_at > now())""",
                (digest,),
            )
        else:
            row = await conn.execute(
                """SELECT session.api_key_id AS id,session.tenant_id,session.roles,
                          session.scope,session.id AS session_id,session.csrf_digest
                   FROM braincore.dashboard_sessions session
                   JOIN braincore.api_keys api_key
                     ON api_key.id=session.api_key_id
                    AND api_key.tenant_id=session.tenant_id
                   WHERE session.digest=%s
                     AND session.revoked_at IS NULL AND session.expires_at > now()
                     AND api_key.revoked_at IS NULL
                     AND (api_key.expires_at IS NULL OR api_key.expires_at > now())""",
                (digest,),
            )
        record = await row.fetchone()
        if record is None:
            raise HTTPException(status_code=401, detail="authentication_required")
        if raw is not None:
            await conn.execute(
                "UPDATE braincore.api_keys SET last_used_at=%s WHERE id=%s AND tenant_id=%s",
                (datetime.now(UTC), record["id"], record["tenant_id"]),
            )
        else:
            await conn.execute(
                "UPDATE braincore.dashboard_sessions SET last_used_at=%s "
                "WHERE id=%s AND tenant_id=%s",
                (datetime.now(UTC), record["session_id"], record["tenant_id"]),
            )
    roles_value = record["roles"]
    if not isinstance(roles_value, list):
        raise RuntimeError("database returned invalid API-key roles")
    csrf_digest = record["csrf_digest"]
    scope_value = record["scope"]
    if scope_value is not None and not isinstance(scope_value, dict):
        raise RuntimeError("database returned invalid API-key scope")
    csrf_valid = raw is not None or (
        isinstance(csrf_digest, bytes | bytearray | memoryview)
        and x_csrf_token is not None
        and constant_time_equal(bytes(csrf_digest), key_digest(x_csrf_token))
    )
    return Principal(
        key_id=UUID(str(record["id"])),
        tenant_id=UUID(str(record["tenant_id"])),
        roles=frozenset(str(role) for role in roles_value),
        scope={str(key): str(value) for key, value in scope_value.items()}
        if scope_value is not None
        else None,
        session_id=UUID(str(record["session_id"])) if record["session_id"] else None,
        csrf_valid=csrf_valid,
    )


def require_read(principal: Principal = Depends(authenticate)) -> Principal:
    principal.require("read")
    return principal


def require_operation_read(principal: Principal = Depends(authenticate)) -> Principal:
    """Allow writers to poll their own outcomes; the route enforces actor isolation."""
    if "write" not in principal.roles:
        principal.require("read")
    return principal


def require_write(principal: Principal = Depends(authenticate)) -> Principal:
    principal.require("write")
    if principal.session_id is not None and not principal.csrf_valid:
        raise HTTPException(status_code=403, detail="csrf_failed")
    return principal


def require_admin(request: Request, principal: Principal = Depends(authenticate)) -> Principal:
    principal.require("admin")
    if (
        request.method not in {"GET", "HEAD", "OPTIONS"}
        and principal.session_id is not None
        and not principal.csrf_valid
    ):
        raise HTTPException(status_code=403, detail="csrf_failed")
    return principal


def constant_time_equal(left: bytes, right: bytes) -> bool:
    return hmac.compare_digest(left, right)

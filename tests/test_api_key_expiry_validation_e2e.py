"""Disposable HTTP/SQL contracts for API-key expiry and secret handling."""

from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import httpx
import pytest

from braincore_memory import models
from braincore_memory.auth import key_digest
from braincore_memory.db import connection

from .conftest import auth_headers
from .test_memory_input_contracts_e2e import write_counts


async def key_snapshot() -> dict[str, Any]:
    async with connection() as conn:
        keys = await (
            await conn.execute(
                "SELECT to_jsonb(k)-'last_used_at' AS value FROM braincore.api_keys k ORDER BY id"
            )
        ).fetchall()
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM braincore.audit_events) AS audits,"
                "(SELECT count(*) FROM braincore.mutation_receipts) AS mutations,"
                "(SELECT count(*) FROM braincore.dashboard_sessions) AS sessions"
            )
        ).fetchone()
    assert counts is not None
    return {"keys": keys, "counts": counts, "writes": await write_counts()}


def set_model_clock(monkeypatch: pytest.MonkeyPatch, instant: datetime) -> None:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr(models, "datetime", FrozenDatetime)


@pytest.mark.parametrize(
    "invalid_kind",
    ["past", "equal", "equal-offset", "naive", "date", "malformed", "overflow", "utc-range"],
)
async def test_invalid_expiry_leaves_no_effects_and_same_key_corrected_retry_works(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, invalid_kind: str
) -> None:
    now = datetime.now(UTC)
    set_model_clock(monkeypatch, now)
    invalid = {
        "past": (now - timedelta(seconds=1)).isoformat(),
        "equal": now.isoformat(),
        "equal-offset": now.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat(),
        "naive": (now + timedelta(days=1)).replace(tzinfo=None).isoformat(),
        "date": (now + timedelta(days=1)).date().isoformat(),
        "malformed": "not-a-timestamp",
        "overflow": "10000-01-01T00:00:00Z",
        "utc-range": "9999-12-31T23:59:59-01:00",
    }[invalid_kind]
    headers = auth_headers(idempotency=f"synthetic-expiry-{uuid4()}")
    body = {"name": f"synthetic-expiry-{uuid4()}", "roles": ["admin"], "expires_at": invalid}
    before = await key_snapshot()
    rejected = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert rejected.status_code == 422, rejected.text
    assert rejected.json()["error"]["code"] == "validation_failed"
    assert "api_key" not in rejected.json()
    assert await key_snapshot() == before

    body["expires_at"] = (now + timedelta(days=1)).isoformat()
    created = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert created.status_code == 201, created.text
    secret = created.json()["api_key"]
    assert (await client.get("/v1/admin/api-keys", headers=auth_headers(secret))).status_code == 200
    after = await key_snapshot()
    replay = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert replay.status_code == 409
    assert replay.json()["error"]["code"] == "api_key_secret_not_replayable"
    assert secret not in replay.text
    assert await key_snapshot() == after
    async with connection() as conn:
        receipt = await (
            await conn.execute(
                "SELECT response FROM braincore.mutation_receipts WHERE key=%s",
                (headers["Idempotency-Key"],),
            )
        ).fetchone()
    assert receipt is not None and "api_key" not in receipt["response"]


@pytest.mark.parametrize("expiry_mode", ["omitted", "null", "positive-offset", "negative-offset"])
async def test_nonexpiring_and_future_offset_keys_preserve_roles_scope_and_authentication(
    client: httpx.AsyncClient, expiry_mode: str
) -> None:
    body: dict[str, Any] = {
        "name": f"synthetic-scoped-expiry-{uuid4()}",
        "roles": ["read", "write"],
        "scope": {"user_id": "alice"},
    }
    expected = None
    if expiry_mode == "null":
        body["expires_at"] = None
    elif expiry_mode in {"positive-offset", "negative-offset"}:
        offset = timezone(
            timedelta(hours=5, minutes=30)
            if expiry_mode == "positive-offset"
            else timedelta(hours=-7)
        )
        expected = (datetime.now(UTC) + timedelta(days=1)).astimezone(offset)
        body["expires_at"] = expected.isoformat()
    result = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"scoped-expiry-{uuid4()}"),
        json=body,
    )
    assert result.status_code == 201, result.text
    record = result.json()
    assert record["roles"] == ["read", "write"] and record["scope"] == {"user_id": "alice"}
    if expected is None:
        assert record["expires_at"] is None
    else:
        assert datetime.fromisoformat(record["expires_at"]) == expected
    async with connection() as conn:
        stored = await (
            await conn.execute(
                "SELECT digest,expires_at,roles,scope FROM braincore.api_keys WHERE id=%s",
                (record["id"],),
            )
        ).fetchone()
    assert stored is not None
    verifier = bytes(stored["digest"])
    assert len(verifier) == 32 and verifier != record["api_key"].encode("utf-8")
    assert stored["expires_at"] == expected
    assert stored["roles"] == ["read", "write"] and stored["scope"] == {"user_id": "alice"}
    headers = auth_headers(record["api_key"])
    denied = await client.get("/v1/admin/api-keys", headers=headers)
    assert denied.status_code == 403
    secret = record["api_key"]
    tampered = secret[:-1] + ("A" if secret[-1] != "A" else "B")
    assert tampered != secret
    invalid = await client.get("/v1/admin/api-keys", headers=auth_headers(tampered))
    assert invalid.status_code == 401
    assert invalid.json()["error"]["code"] == "authentication_required"
    for user, status in [("alice", 202), ("other", 403)]:
        response = await client.post(
            "/v1/memories",
            headers=auth_headers(record["api_key"], idempotency=f"expiry-scope-{uuid4()}"),
            json={"content": f"Synthetic expiry scope {uuid4()}.", "scope": {"user_id": user}},
        )
        assert response.status_code == status, response.text


async def test_database_clock_rejects_expiry_that_aged_after_request_validation(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    # A slow request or lagging application clock must not bypass the insert-time guard.
    set_model_clock(monkeypatch, now - timedelta(days=2))
    headers = auth_headers(idempotency=f"expiry-clock-{uuid4()}")
    body = {
        "name": "synthetic-database-clock",
        "roles": ["admin"],
        "expires_at": (now - timedelta(days=1)).isoformat(),
    }
    before = await key_snapshot()
    rejected = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert rejected.status_code == 422, rejected.text
    assert rejected.json()["error"]["code"] == "api_key_expiry_not_future"
    assert await key_snapshot() == before
    body["expires_at"] = (now + timedelta(days=1)).isoformat()
    created = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert created.status_code == 201, created.text


async def test_expired_replay_validates_before_secret_replay_and_has_no_new_effects(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    headers = auth_headers(idempotency=f"expiry-replay-{uuid4()}")
    body = {
        "name": "synthetic-aged-replay",
        "roles": ["admin"],
        "expires_at": (now + timedelta(days=1)).isoformat(),
    }
    created = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert created.status_code == 201, created.text
    before = await key_snapshot()
    set_model_clock(monkeypatch, now + timedelta(days=2))
    replay = await client.post("/v1/admin/api-keys", headers=headers, json=body)
    assert replay.status_code == 422 and replay.json()["error"]["code"] == "validation_failed"
    assert created.json()["api_key"] not in replay.text
    assert await key_snapshot() == before


@pytest.mark.parametrize(
    "roles,scope",
    [(["admin"], {"user_id": "alice"}), (["read"], None), ([], None), (["unknown"], None)],
)
async def test_future_expiry_does_not_bypass_role_and_scope_validation(
    client: httpx.AsyncClient, roles: list[str], scope: dict[str, str] | None
) -> None:
    before = await key_snapshot()
    result = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"expiry-invalid-role-{uuid4()}"),
        json={
            "name": "synthetic-invalid-role",
            "roles": roles,
            "scope": scope,
            "expires_at": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
        },
    )
    assert result.status_code == 422, result.text
    assert await key_snapshot() == before


async def test_future_keys_stay_tenant_bound_and_expired_keys_stop_authenticating(
    client: httpx.AsyncClient,
) -> None:
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    result = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"expiry-tenant-{uuid4()}"),
        json={
            "name": f"synthetic-tenant-expiry-{uuid4()}",
            "roles": ["admin"],
            "expires_at": future,
        },
    )
    assert result.status_code == 201, result.text
    created = result.json()
    foreign_secret = f"bck_{uuid4().hex}_{uuid4().hex}"
    async with connection() as conn:
        tenant = await (
            await conn.execute(
                "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id",
                (f"synthetic-expiry-tenant-{uuid4()}",),
            )
        ).fetchone()
        assert tenant is not None
        await conn.execute(
            "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,expires_at) "
            "VALUES (%s,'synthetic-foreign-expiry',%s,%s,ARRAY['admin'],%s)",
            (tenant["id"], foreign_secret[:12], key_digest(foreign_secret), future),
        )
    own = await client.get("/v1/admin/api-keys", headers=auth_headers(created["api_key"]))
    foreign = await client.get("/v1/admin/api-keys", headers=auth_headers(foreign_secret))
    assert own.status_code == foreign.status_code == 200
    assert created["id"] in {item["id"] for item in own.json()["api_keys"]}
    assert created["id"] not in {item["id"] for item in foreign.json()["api_keys"]}
    async with connection() as conn:
        await conn.execute(
            "UPDATE braincore.api_keys SET expires_at=statement_timestamp() WHERE id=%s",
            (created["id"],),
        )
    expired = await client.get("/v1/admin/api-keys", headers=auth_headers(created["api_key"]))
    assert expired.status_code == 401
    assert expired.json()["error"]["code"] == "authentication_required"

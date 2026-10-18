"""Real authentication, worker outcomes and isolation in the disposable database."""

from typing import Any
from uuid import uuid4

import httpx
import pytest
from psycopg.types.json import Jsonb

from braincore_memory.api import app
from braincore_memory.auth import key_digest
from braincore_memory.db import connection

from .conftest import auth_headers
from .test_e2e import run_worker_once


async def create_key(client: httpx.AsyncClient, roles: list[str]) -> dict[str, Any]:
    response = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"outcome-key-{uuid4()}"),
        json={
            "name": f"synthetic-outcome-{uuid4()}",
            "roles": roles,
            **({} if "admin" in roles else {"scope": {"user_id": "alice"}}),
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def submit(client: httpx.AsyncClient, key: str) -> str:
    result = await client.post(
        "/v1/memories",
        headers=auth_headers(key, idempotency=f"outcome-{uuid4()}"),
        json={"content": f"Synthetic operation {uuid4()}.", "scope": {"user_id": "alice"}},
    )
    assert result.status_code == 202, result.text
    return str(result.json()["operation_id"])


@pytest.mark.parametrize("endpoint", ["memories", "ingestions"])
@pytest.mark.parametrize("batch", [False, True])
async def test_write_only_can_observe_accepted_terminal_and_per_item_outcomes(
    client: httpx.AsyncClient, endpoint: str, batch: bool
) -> None:
    key = (await create_key(client, ["write"]))["api_key"]
    item = {"content": f"Synthetic outcome fact {uuid4()}.", "scope": {"user_id": "alice"}}
    body = {"items": [item, {"content": "Invalid item without a scope."}]} if batch else item
    path = f"/v1/{endpoint}" + (":batch" if batch else "")
    headers = auth_headers(key, idempotency=f"outcome-submit-{uuid4()}")
    accepted = await client.post(path, headers=headers, json=body)
    assert accepted.status_code == 202, accepted.text
    operation_id = accepted.json()["operation_id"]
    queued = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers(key))
    assert queued.status_code == 200, queued.text
    assert queued.json()["state"] == "queued"
    assert queued.json()["items"][0]["state"] == "queued"
    if batch:
        assert queued.json()["items"][1]["state"] == "failed"
        assert queued.json()["items"][1]["error"]["code"] == "validation_failed"
    run_worker_once()
    terminal = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers(key))
    assert terminal.status_code == 200, terminal.text
    final = terminal.json()
    assert final["state"] == ("partial" if batch else "succeeded")
    assert final["succeeded_items"] == 1 and final["failed_items"] == int(batch)
    assert final["completed_at"] is not None
    assert final["items"][0]["state"] == "succeeded"
    assert final["items"][0]["completed_at"] is not None
    if endpoint == "ingestions":
        assert final["items"][0]["result"]["memory_ids"]
    else:
        assert final["items"][0]["memory_id"]
    replay = await client.post(path, headers=headers, json=body)
    assert replay.status_code == 202 and replay.json()["reused"] is True
    assert replay.json()["operation_id"] == operation_id
    admin_view = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert admin_view.status_code == 200 and admin_view.json() == final


@pytest.mark.parametrize("observer_roles", [["write"], ["read"], ["read", "write"]])
async def test_other_actor_same_scope_cannot_observe_operation(
    client: httpx.AsyncClient, observer_roles: list[str]
) -> None:
    owner = await create_key(client, ["write"])
    observer = await create_key(client, observer_roles)
    operation_id = await submit(client, owner["api_key"])
    result = await client.get(
        f"/v1/operations/{operation_id}", headers=auth_headers(observer["api_key"])
    )
    assert result.status_code == 404
    assert result.json()["error"]["code"] == "resource_not_found"
    assert "actor_key_id" not in result.json() and "items" not in result.json()


@pytest.mark.parametrize("roles", [["write"], ["admin"]])
async def test_cross_tenant_outcomes_stay_hidden_even_from_admin(
    client: httpx.AsyncClient, roles: list[str]
) -> None:
    owner = await create_key(client, ["write"])
    operation_id = await submit(client, owner["api_key"])
    foreign_key = f"bck_{uuid4().hex}_{uuid4().hex}"
    async with connection() as conn:
        tenant = await (
            await conn.execute(
                "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id",
                (f"synthetic-outcome-tenant-{uuid4()}",),
            )
        ).fetchone()
        assert tenant is not None
        await conn.execute(
            "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope) "
            "VALUES (%s,'synthetic-outcome-key',%s,%s,%s,%s)",
            (
                tenant["id"],
                foreign_key[:12],
                key_digest(foreign_key),
                roles,
                None if "admin" in roles else Jsonb({"user_id": "alice"}),
            ),
        )
    result = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers(foreign_key))
    assert result.status_code == 404
    assert result.json()["error"]["code"] == "resource_not_found"


async def test_existing_read_role_can_still_observe_own_operation(
    client: httpx.AsyncClient,
) -> None:
    owner = await create_key(client, ["read", "write"])
    operation_id = await submit(client, owner["api_key"])
    async with connection() as conn:
        await conn.execute(
            "UPDATE braincore.api_keys SET roles=ARRAY['read'] WHERE id=%s", (owner["id"],)
        )
    result = await client.get(
        f"/v1/operations/{operation_id}", headers=auth_headers(owner["api_key"])
    )
    assert result.status_code == 200, result.text
    blocked = await client.post(
        "/v1/memories",
        headers=auth_headers(owner["api_key"], idempotency=f"readonly-{uuid4()}"),
        json={"content": "Read role cannot write.", "scope": {"user_id": "alice"}},
    )
    assert blocked.status_code == 403


async def test_write_only_session_polling_preserves_csrf_and_read_boundaries(
    client: httpx.AsyncClient,
) -> None:
    owner = await create_key(client, ["write"])
    operation_id = await submit(client, owner["api_key"])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as browser:
        session = await browser.post(
            "/v1/dashboard/session",
            headers=auth_headers(owner["api_key"], idempotency=f"outcome-session-{uuid4()}"),
        )
        assert session.status_code == 201, session.text
        result = await browser.get(f"/v1/operations/{operation_id}")
        assert result.status_code == 200, result.text
        denied = await browser.post(
            "/v1/memories",
            headers={"Idempotency-Key": f"outcome-no-csrf-{uuid4()}"},
            json={"content": "Mutation needs CSRF.", "scope": {"user_id": "alice"}},
        )
        assert denied.status_code == 403 and denied.json()["error"]["code"] == "csrf_failed"
        allowed = await browser.post(
            "/v1/memories",
            headers={
                "Idempotency-Key": f"outcome-with-csrf-{uuid4()}",
                "X-CSRF-Token": session.json()["csrf_token"],
            },
            json={"content": f"Synthetic CSRF write {uuid4()}.", "scope": {"user_id": "alice"}},
        )
        assert allowed.status_code == 202, allowed.text
        assert (
            await browser.get(f"/v1/operations/{allowed.json()['operation_id']}")
        ).status_code == 200
        for method, path, body in [
            ("POST", "/v1/search", {"query": "synthetic"}),
            ("GET", "/v1/health", None),
            ("GET", "/openapi.json", None),
            ("GET", f"/v1/memories/{uuid4()}", None),
            ("POST", f"/v1/operations/{operation_id}/retry", None),
        ]:
            denied = await browser.request(method, path, json=body)
            assert denied.status_code == 403, denied.text


@pytest.mark.parametrize("invalid", ["missing", "unknown", "revoked", "expired"])
async def test_invalid_credentials_cannot_observe_outcomes(
    client: httpx.AsyncClient, invalid: str
) -> None:
    owner = await create_key(client, ["write"])
    operation_id = await submit(client, owner["api_key"])
    if invalid in {"revoked", "expired"}:
        async with connection() as conn:
            if invalid == "revoked":
                await conn.execute(
                    "UPDATE braincore.api_keys SET revoked_at=now() WHERE id=%s", (owner["id"],)
                )
            else:
                await conn.execute(
                    "UPDATE braincore.api_keys SET expires_at=now()-interval '1 second' "
                    "WHERE id=%s",
                    (owner["id"],),
                )
    headers = (
        {}
        if invalid == "missing"
        else auth_headers(f"unknown_{uuid4().hex}" if invalid == "unknown" else owner["api_key"])
    )
    result = await client.get(f"/v1/operations/{operation_id}", headers=headers)
    assert result.status_code == 401
    assert result.json()["error"]["code"] == "authentication_required"

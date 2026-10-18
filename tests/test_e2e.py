import asyncio
import os
import shutil
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
import pytest

from braincore_memory.api import app
from braincore_memory.config import get_settings

from .conftest import ADMIN_KEY, SECOND_KEY, auth_headers, create_second_tenant

ROOT = Path(__file__).resolve().parents[1]


def run_worker_once(environment_overrides: dict[str, str] | None = None) -> None:
    environment = {
        **os.environ,
        "BRAINCORE_WORKER_ONCE": "1",
        **(environment_overrides or {}),
    }
    bun = shutil.which("bun")
    if bun is None:
        raise RuntimeError("bun executable is required for worker integration tests")
    subprocess.run(  # noqa: S603 - executable is resolved locally; arguments are constant
        [bun, "worker/src/index.ts"],
        cwd=ROOT,
        env=environment,
        check=True,
        timeout=30,
    )


async def remember(client: httpx.AsyncClient, content: str, user_id: str = "alice") -> str:
    response = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"remember-{uuid4()}"),
        json={"content": content, "scope": {"user_id": user_id}},
    )
    assert response.status_code == 202, response.text
    operation_id = response.json()["operation_id"]
    run_worker_once()
    operation = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert operation.json()["state"] == "succeeded"
    return operation.json()["items"][0]["memory_id"]


async def test_direct_memory_idempotency_explain_revision_and_lifecycle(
    client: httpx.AsyncClient,
) -> None:
    body = {"content": "Alice prefers Python 3.12.", "scope": {"user_id": "alice"}}
    headers = auth_headers(idempotency="stable-key")
    first = await client.post("/v1/memories", headers=headers, json=body)
    replay = await client.post("/v1/memories", headers=headers, json=body)
    conflict = await client.post(
        "/v1/memories",
        headers=headers,
        json={"content": "different", "scope": {"user_id": "alice"}},
    )
    assert first.status_code == 202
    assert replay.json()["reused"] is True
    assert conflict.status_code == 409
    unknown_field = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency="unknown-field"),
        json={**body, "unsupported": True},
    )
    assert unknown_field.status_code == 422
    assert unknown_field.json()["error"]["code"] == "validation_failed"
    run_worker_once()
    operation = await client.get(
        f"/v1/operations/{first.json()['operation_id']}", headers=auth_headers()
    )
    memory_id = operation.json()["items"][0]["memory_id"]
    explanation = await client.get(f"/v1/memories/{memory_id}/explain", headers=auth_headers())
    assert explanation.json()["evidence"][0]["quote"] == body["content"]

    revision = await client.post(
        f"/v1/memories/{memory_id}/revisions",
        headers=auth_headers(idempotency="revision-key"),
        json={"content": "Alice prefers Rust.", "reason": "User corrected preference."},
    )
    assert revision.status_code == 200
    revision_replay = await client.post(
        f"/v1/memories/{memory_id}/revisions",
        headers=auth_headers(idempotency="revision-key"),
        json={"content": "Alice prefers Rust.", "reason": "User corrected preference."},
    )
    assert revision_replay.json()["reused"] is True
    assert revision_replay.json()["revision_id"] == revision.json()["revision_id"]
    run_worker_once()
    memory = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
    assert memory.json()["content"] == "Alice prefers Rust."
    assert memory.json()["embedding_state"] == "ready"

    feedback = await client.post(
        f"/v1/memories/{memory_id}/feedback",
        headers=auth_headers(idempotency="feedback-key"),
        json={"rating": 1, "comment": "Accurate."},
    )
    feedback_replay = await client.post(
        f"/v1/memories/{memory_id}/feedback",
        headers=auth_headers(idempotency="feedback-key"),
        json={"rating": 1, "comment": "Accurate."},
    )
    assert feedback_replay.json()["reused"] is True
    assert feedback_replay.json()["feedback_id"] == feedback.json()["feedback_id"]

    suppressed = await client.patch(
        f"/v1/memories/{memory_id}/lifecycle",
        headers=auth_headers(idempotency="suppress-key"),
        json={"action": "suppress", "reason": "No longer relevant."},
    )
    assert suppressed.json()["state"] == "suppressed"
    search = await client.post(
        "/v1/search",
        headers=auth_headers(),
        json={"query": "Rust", "scope": {"user_id": "alice"}},
    )
    assert all(item["id"] != memory_id for item in search.json()["results"])


async def test_ingestion_and_mixed_batch_terminal_states(client: httpx.AsyncClient) -> None:
    ingestion = await client.post(
        "/v1/ingestions",
        headers=auth_headers(idempotency="ingestion-key"),
        json={
            "content": "Alice moved to Phoenix. Alice owns a blue bicycle.",
            "scope": {"user_id": "alice"},
        },
    )
    run_worker_once()
    ingestion_operation = await client.get(
        f"/v1/operations/{ingestion.json()['operation_id']}", headers=auth_headers()
    )
    assert ingestion_operation.json()["state"] == "succeeded"
    assert len(ingestion_operation.json()["items"][0]["result"]["memory_ids"]) == 2

    batch = await client.post(
        "/v1/memories:batch",
        headers=auth_headers(idempotency="mixed-batch-key"),
        json={
            "items": [
                {"content": "Alice likes green tea.", "scope": {"user_id": "alice"}},
                {"content": "This item has no scope."},
            ]
        },
    )
    run_worker_once()
    batch_operation = await client.get(
        f"/v1/operations/{batch.json()['operation_id']}", headers=auth_headers()
    )
    assert batch_operation.json()["state"] == "partial"
    assert [item["state"] for item in batch_operation.json()["items"]] == [
        "succeeded",
        "failed",
    ]


async def test_cross_tenant_object_and_search_isolation(client: httpx.AsyncClient) -> None:
    memory_id = await remember(client, "Private tenant fact.")
    create_second_tenant()
    hidden = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers(SECOND_KEY))
    assert hidden.status_code == 404
    search = await client.post(
        "/v1/search",
        headers=auth_headers(SECOND_KEY),
        json={"query": "Private tenant fact", "scope": {"user_id": "alice"}},
    )
    assert search.status_code == 200
    assert search.json()["results"] == []


async def test_scope_bound_key_is_enforced_across_every_memory_interface(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"scope-key-{uuid4()}"),
        json={
            "name": f"alice-only-{uuid4()}",
            "roles": ["read", "write"],
            "scope": {"user_id": "alice"},
        },
    )
    assert created.status_code == 201, created.text
    alice_key = created.json()["api_key"]

    alice_write = await client.post(
        "/v1/memories",
        headers=auth_headers(alice_key, idempotency=f"alice-write-{uuid4()}"),
        json={"content": "Alice-only scoped fact.", "scope": {"user_id": "alice"}},
    )
    assert alice_write.status_code == 202, alice_write.text
    run_worker_once()
    own_operation = await client.get(
        f"/v1/operations/{alice_write.json()['operation_id']}", headers=auth_headers(alice_key)
    )
    assert own_operation.status_code == 200

    bob_write = await client.post(
        "/v1/memories",
        headers=auth_headers(alice_key, idempotency=f"bob-write-{uuid4()}"),
        json={"content": "Forbidden Bob write.", "scope": {"user_id": "bob"}},
    )
    assert bob_write.status_code == 403
    assert bob_write.json()["error"]["code"] == "scope_forbidden"

    bob_memory_id = await remember(client, "Bob's private scoped fact.", user_id="bob")
    bob_reference = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"bob-reference-{uuid4()}"),
        json={
            "content": "Bob reference value.",
            "scope": {"user_id": "bob"},
            "reference": {"namespace": ["bob"], "key": "private"},
        },
    )
    assert bob_reference.status_code == 202
    run_worker_once()
    forbidden_search = await client.post(
        "/v1/search",
        headers=auth_headers(alice_key),
        json={"query": "private", "scope": {"user_id": "bob"}},
    )
    assert forbidden_search.status_code == 403
    implicit_search = await client.post(
        "/v1/search", headers=auth_headers(alice_key), json={"query": None}
    )
    assert implicit_search.status_code == 200
    assert all(item["user_id"] == "alice" for item in implicit_search.json()["results"])

    for path in (
        f"/v1/memories/{bob_memory_id}",
        f"/v1/memories/{bob_memory_id}/explain",
    ):
        assert (await client.get(path, headers=auth_headers(alice_key))).status_code == 404
    resolved = await client.post(
        "/v1/memories:resolve",
        headers=auth_headers(alice_key),
        json={"namespace": ["bob"], "key": "private"},
    )
    assert resolved.status_code == 404
    same_reference_new_scope = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"same-reference-new-scope-{uuid4()}"),
        json={
            "content": "Alice's independent reference value.",
            "scope": {"user_id": "alice"},
            "reference": {"namespace": ["bob"], "key": "private"},
        },
    )
    assert same_reference_new_scope.status_code == 202
    run_worker_once()
    alice_resolved = await client.post(
        "/v1/memories:resolve",
        headers=auth_headers(alice_key),
        json={"namespace": ["bob"], "key": "private"},
    )
    assert alice_resolved.status_code == 200
    assert alice_resolved.json()["user_id"] == "alice"

    revision = await client.post(
        f"/v1/memories/{bob_memory_id}/revisions",
        headers=auth_headers(alice_key, idempotency=f"scope-revise-{uuid4()}"),
        json={"content": "Forbidden revision.", "reason": "Scope probe."},
    )
    lifecycle = await client.patch(
        f"/v1/memories/{bob_memory_id}/lifecycle",
        headers=auth_headers(alice_key, idempotency=f"scope-life-{uuid4()}"),
        json={"action": "suppress", "reason": "Scope probe."},
    )
    feedback = await client.post(
        f"/v1/memories/{bob_memory_id}/feedback",
        headers=auth_headers(alice_key, idempotency=f"scope-feedback-{uuid4()}"),
        json={"rating": -1, "comment": "Scope probe."},
    )
    assert [revision.status_code, lifecycle.status_code, feedback.status_code] == [404, 404, 404]

    hidden_operation = await client.get(
        f"/v1/operations/{bob_reference.json()['operation_id']}",
        headers=auth_headers(alice_key),
    )
    assert hidden_operation.status_code == 404
    retry_forbidden = await client.post(
        f"/v1/operations/{alice_write.json()['operation_id']}/retry",
        headers=auth_headers(alice_key, idempotency=f"scope-retry-{uuid4()}"),
    )
    assert retry_forbidden.status_code == 403


async def test_identical_content_preserves_scope_specific_evidence(
    client: httpx.AsyncClient,
) -> None:
    memory_ids: dict[str, str] = {}
    for user_id in ("alice", "bob"):
        accepted = await client.post(
            "/v1/memories",
            headers=auth_headers(idempotency=f"evidence-scope-{user_id}-{uuid4()}"),
            json={
                "content": "The shared wording is intentionally identical.",
                "scope": {"user_id": user_id},
                "evidence": {
                    "source_type": "test",
                    "source_uri": f"https://example.invalid/{user_id}",
                },
            },
        )
        assert accepted.status_code == 202
        run_worker_once()
        operation = await client.get(
            f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
        )
        memory_ids[user_id] = operation.json()["items"][0]["memory_id"]

    assert memory_ids["alice"] != memory_ids["bob"]
    corroborating = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"corroborating-evidence-{uuid4()}"),
        json={
            "content": "The shared wording is intentionally identical.",
            "scope": {"user_id": "alice"},
            "evidence": {
                "source_type": "test",
                "source_uri": "https://example.invalid/alice-second",
            },
        },
    )
    run_worker_once()
    corroborating_operation = await client.get(
        f"/v1/operations/{corroborating.json()['operation_id']}", headers=auth_headers()
    )
    assert corroborating_operation.json()["items"][0]["memory_id"] == memory_ids["alice"]
    for user_id, memory_id in memory_ids.items():
        explanation = await client.get(f"/v1/memories/{memory_id}/explain", headers=auth_headers())
        evidence = explanation.json()["evidence"]
        expected_uris = {f"https://example.invalid/{user_id}"}
        if user_id == "alice":
            expected_uris.add("https://example.invalid/alice-second")
        assert {item["source_uri"] for item in evidence} == expected_uris
        assert all(item["scope"] == {"user_id": user_id} for item in evidence)


async def test_validation_uses_stable_error_envelope(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency="invalid-key"),
        json={"content": "missing scope"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"
    assert response.json()["error"]["request_id"].startswith("req_")


async def test_expired_idempotency_and_mutation_receipts_can_be_safely_reused(
    client: httpx.AsyncClient,
) -> None:
    from braincore_memory.db import connection

    operation_key = f"expiring-operation-{uuid4()}"
    first = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=operation_key),
        json={"content": "First expiring request.", "scope": {"run_id": "ttl"}},
    )
    assert first.status_code == 202
    run_worker_once()
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE braincore.idempotency_records SET expires_at=now()-interval '1 second' "
            "WHERE key=%s",
            (operation_key,),
        )
    reused_after_expiry = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=operation_key),
        json={"content": "Second expiring request.", "scope": {"run_id": "ttl"}},
    )
    assert reused_after_expiry.status_code == 202
    assert reused_after_expiry.json()["reused"] is False
    run_worker_once()

    mutation_key = f"expiring-mutation-{uuid4()}"
    first_key = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=mutation_key),
        json={"name": f"ttl-one-{uuid4()}", "roles": ["read"], "scope": {"run_id": "ttl"}},
    )
    assert first_key.status_code == 201
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE braincore.mutation_receipts SET expires_at=now()-interval '1 second' "
            "WHERE key=%s",
            (mutation_key,),
        )
    second_key = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=mutation_key),
        json={"name": f"ttl-two-{uuid4()}", "roles": ["read"], "scope": {"run_id": "ttl"}},
    )
    assert second_key.status_code == 201
    assert second_key.json()["reused"] is False


async def test_openapi_and_docs_require_authentication_and_large_errors_are_hardened(
    client: httpx.AsyncClient,
) -> None:
    assert (await client.get("/openapi.json")).status_code == 401
    assert (await client.get("/docs")).status_code == 401
    schema = await client.get("/openapi.json", headers=auth_headers())
    assert schema.status_code == 200
    assert schema.json()["info"]["version"] == "2.0.0"
    docs = await client.get("/docs", headers=auth_headers())
    assert docs.status_code == 200

    too_large = await client.post(
        "/v1/memories",
        headers={
            **auth_headers(idempotency="too-large"),
            "Content-Length": str(get_settings().max_body_bytes + 1),
        },
        content=b"{}",
    )
    assert too_large.status_code == 413
    assert too_large.headers["x-content-type-options"] == "nosniff"
    assert too_large.headers["cache-control"] == "no-store"

    async def oversized_chunks() -> AsyncIterator[bytes]:
        for _ in range(get_settings().max_body_bytes // 65_536 + 1):
            yield b"x" * 65_536

    chunked = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency="too-large-chunked"),
        content=oversized_chunks(),
    )
    assert chunked.status_code == 413
    assert chunked.json()["error"]["code"] == "request_too_large"


async def test_api_key_administration_and_dashboard_csrf(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency="create-integration-key"),
        json={
            "name": "integration-key",
            "roles": ["read", "write"],
            "scope": {"user_id": "alice"},
        },
    )
    assert created.status_code == 201
    issued_key = created.json()["api_key"]
    key_id = created.json()["id"]
    replayed_create = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency="create-integration-key"),
        json={
            "name": "integration-key",
            "roles": ["read", "write"],
            "scope": {"user_id": "alice"},
        },
    )
    assert replayed_create.status_code == 409
    assert replayed_create.json()["error"]["code"] == "api_key_secret_not_replayable"
    conflicting_create = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency="create-integration-key"),
        json={"name": "different-key", "roles": ["read"], "scope": {"user_id": "alice"}},
    )
    assert conflicting_create.status_code == 409
    listed = await client.get("/v1/admin/api-keys", headers=auth_headers())
    listed_key = next(item for item in listed.json()["api_keys"] if item["id"] == key_id)
    assert "api_key" not in listed_key
    assert "digest" not in listed_key
    assert listed_key["scope"] == {"user_id": "alice"}
    assert (await client.get("/v1/health", headers=auth_headers(issued_key))).status_code == 200
    assert (await client.get("/v1/admin/status", headers=auth_headers())).status_code == 200
    assert (
        await client.get("/v1/admin/status", headers=auth_headers(issued_key))
    ).status_code == 403
    metrics = await client.get("/v1/metrics", headers=auth_headers())
    assert metrics.status_code == 200
    assert "braincore_jobs_queued" in metrics.text

    session_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )
    try:
        session = await session_client.post(
            "/v1/dashboard/session",
            headers=auth_headers(ADMIN_KEY, idempotency="dashboard-session"),
        )
        assert session.status_code == 201
        assert session_client.cookies.get("braincore_session")
        assert "bck_" not in str(session_client.cookies)
        created_cookie = session.headers["set-cookie"]
        assert "HttpOnly" in created_cookie
        assert "SameSite=strict" in created_cookie
        assert "Path=/" in created_cookie
        replayed_session = await session_client.post(
            "/v1/dashboard/session",
            headers=auth_headers(ADMIN_KEY, idempotency="dashboard-session"),
        )
        assert replayed_session.status_code == 409
        assert replayed_session.json()["error"]["code"] == "session_secret_not_replayable"
        csrf_token = session.json()["csrf_token"]
        cookie_admin_status = await session_client.get("/v1/admin/status")
        assert cookie_admin_status.status_code == 200
        cookie_read = await session_client.post(
            "/v1/search", json={"query": "anything", "scope": {"user_id": "alice"}}
        )
        assert cookie_read.status_code == 200
        blocked_write = await session_client.post(
            "/v1/memories",
            headers={"Idempotency-Key": "csrf-blocked"},
            json={"content": "blocked write", "scope": {"user_id": "alice"}},
        )
        assert blocked_write.status_code == 403
        allowed_write = await session_client.post(
            "/v1/memories",
            headers={"Idempotency-Key": "csrf-allowed", "X-CSRF-Token": csrf_token},
            json={"content": "allowed write", "scope": {"user_id": "alice"}},
        )
        assert allowed_write.status_code == 202
        logout = await session_client.delete(
            "/v1/dashboard/session",
            headers={
                "X-CSRF-Token": csrf_token,
                "Idempotency-Key": "dashboard-logout",
            },
        )
        assert logout.status_code == 204
        deleted_cookie = logout.headers["set-cookie"]
        assert "braincore_session=" in deleted_cookie
        assert "HttpOnly" in deleted_cookie
        assert "SameSite=strict" in deleted_cookie
        assert "Path=/" in deleted_cookie
        assert "Max-Age=0" in deleted_cookie
    finally:
        await session_client.aclose()

    rotated = await client.post(
        f"/v1/admin/api-keys/{key_id}/rotate",
        headers=auth_headers(idempotency="rotate-integration-key"),
    )
    rotated_key = rotated.json()["api_key"]
    assert (await client.get("/v1/health", headers=auth_headers(issued_key))).status_code == 401
    assert (await client.get("/v1/health", headers=auth_headers(rotated_key))).status_code == 200
    replayed_rotation = await client.post(
        f"/v1/admin/api-keys/{key_id}/rotate",
        headers=auth_headers(idempotency="rotate-integration-key"),
    )
    assert replayed_rotation.status_code == 409
    assert replayed_rotation.json()["error"]["code"] == "api_key_secret_not_replayable"

    revoked = await client.post(
        f"/v1/admin/api-keys/{key_id}/revoke",
        headers=auth_headers(idempotency="revoke-integration-key"),
    )
    assert revoked.status_code == 200
    assert (await client.get("/v1/health", headers=auth_headers(rotated_key))).status_code == 401


async def test_dashboard_session_stops_when_parent_api_key_expires(
    client: httpx.AsyncClient,
) -> None:
    from braincore_memory.db import connection

    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"create-expiring-key-{uuid4()}"),
        json={
            "name": "expiring-dashboard-key",
            "roles": ["read", "write"],
            "scope": {"user_id": "alice"},
            "expires_at": "2099-01-01T00:00:00Z",
        },
    )
    assert created.status_code == 201
    key_id = created.json()["id"]
    session_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )
    try:
        session = await session_client.post(
            "/v1/dashboard/session",
            headers=auth_headers(
                created.json()["api_key"], idempotency=f"expiring-session-{uuid4()}"
            ),
        )
        assert session.status_code == 201
        assert (await session_client.get("/v1/health")).status_code == 200
        async with connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE braincore.api_keys SET expires_at=now()-interval '1 second' WHERE id=%s",
                (key_id,),
            )
        assert (await session_client.get("/v1/health")).status_code == 401
    finally:
        await session_client.aclose()


def test_bootstrap_environment_rotation_replaces_prior_admin_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("set BRAINCORE_E2E=1 to run database integration tests")
    from braincore_memory.auth import key_digest
    from braincore_memory.config import get_settings
    from braincore_memory.migrate import bootstrap_admin

    rotated_key = "bck_rotated_bootstrap_0123456789abcdefghijklmnopqrstuvwxyz"
    monkeypatch.setenv("BRAINCORE_BOOTSTRAP_ADMIN_KEY", rotated_key)
    get_settings.cache_clear()
    bootstrap_admin()
    settings = get_settings()
    with psycopg.connect(settings.database_url.get_secret_value()) as conn, conn.cursor() as cursor:
        cursor.execute(
            """SELECT digest,revoked_at FROM braincore.api_keys
               WHERE name='bootstrap' ORDER BY created_at DESC"""
        )
        rows = cursor.fetchall()
    assert sum(row[1] is None for row in rows) == 1
    assert bytes(rows[0][0]) == key_digest(rotated_key)

    monkeypatch.setenv("BRAINCORE_BOOTSTRAP_ADMIN_KEY", ADMIN_KEY)
    get_settings.cache_clear()
    bootstrap_admin()
    with (
        psycopg.connect(get_settings().database_url.get_secret_value()) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT digest FROM braincore.api_keys WHERE name='bootstrap' AND revoked_at IS NULL"
        )
        restored = cursor.fetchone()
    assert restored is not None and bytes(restored[0]) == key_digest(ADMIN_KEY)
    get_settings.cache_clear()


async def test_langgraph_store_preserves_keys_revisions_search_and_suppression(
    client: httpx.AsyncClient,
) -> None:
    pytest.importorskip("langgraph")
    from braincore_memory.client import AsyncBrainCore
    from braincore_memory.langgraph import BrainCoreStore

    bun = shutil.which("bun")
    if bun is None:
        raise RuntimeError("bun executable is required for LangGraph integration tests")
    worker = await asyncio.create_subprocess_exec(  # noqa: S603 - executable is resolved locally
        bun,
        "worker/src/index.ts",
        cwd=ROOT,
        env=os.environ,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    async_sdk = AsyncBrainCore(api_key=ADMIN_KEY, client=client)
    store = BrainCoreStore(api_key="unused", async_client=async_sdk)
    namespace = ("alice", "memories")
    try:
        await store.aput(
            namespace, "preference", {"memory": "Alice likes oolong tea.", "kind": "preference"}
        )
        item = await store.aget(namespace, "preference")
        assert item is not None
        assert item.value["kind"] == "preference"

        matches = await store.asearch(namespace, query="oolong", filter={"kind": "preference"})
        assert [match.key for match in matches] == ["preference"]

        await store.aput(
            namespace, "preference", {"memory": "Alice likes green tea.", "kind": "preference"}
        )
        updated = await store.aget(namespace, "preference")
        assert updated is not None
        assert updated.value["memory"] == "Alice likes green tea."
        resolved = await client.post(
            "/v1/memories:resolve",
            headers=auth_headers(),
            json={
                "namespace": list(namespace),
                "key": "preference",
                "scope": {"user_id": "alice"},
            },
        )
        explanation = await client.get(
            f"/v1/memories/{resolved.json()['id']}/explain", headers=auth_headers()
        )
        assert len(explanation.json()["revisions"]) == 2

        await store.adelete(namespace, "preference")
        assert await store.aget(namespace, "preference") is None
        suppressed = await client.post(
            "/v1/memories:resolve",
            headers=auth_headers(),
            json={
                "namespace": list(namespace),
                "key": "preference",
                "scope": {"user_id": "alice"},
            },
        )
        assert suppressed.json()["state"] == "suppressed"
    finally:
        await store.aclose()
        store.close()
        worker.terminate()
        try:
            await asyncio.wait_for(worker.wait(), timeout=5)
        except TimeoutError:
            worker.kill()
            await asyncio.wait_for(worker.wait(), timeout=5)


async def test_worker_lease_recovery_dead_letter_retry_and_duplicate_delivery(
    client: httpx.AsyncClient,
) -> None:
    from braincore_memory.db import connection

    accepted = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency="lease-recovery-memory"),
        json={
            "content": "Lease recovery must not lose this memory.",
            "scope": {"run_id": "failure-test"},
        },
    )
    operation_id = accepted.json()["operation_id"]
    async with connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE braincore.jobs SET state='running',leased_by='dead-worker',
                      leased_until=now()-interval '1 second'
               WHERE operation_id=%s""",
            (operation_id,),
        )
        await conn.execute(
            "UPDATE braincore.operation_items SET state='running' WHERE operation_id=%s",
            (operation_id,),
        )
        await conn.execute(
            "UPDATE braincore.operations SET state='running' WHERE id=%s",
            (operation_id,),
        )
    run_worker_once()
    recovered = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert recovered.json()["state"] == "succeeded"
    memory_id = recovered.json()["items"][0]["memory_id"]

    async with connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE braincore.jobs SET state='dead_letter',attempts=max_attempts,
                      last_error='{"code":"forced_test_failure"}'::jsonb,completed_at=now()
               WHERE operation_id=%s""",
            (operation_id,),
        )
        await conn.execute(
            """UPDATE braincore.operation_items SET state='partial',
                      error='{"code":"forced_test_failure"}'::jsonb,completed_at=now()
               WHERE operation_id=%s""",
            (operation_id,),
        )
        await conn.execute(
            "UPDATE braincore.operations SET state='partial',completed_at=now() WHERE id=%s",
            (operation_id,),
        )
        await conn.execute(
            "UPDATE braincore.memories SET embedding_state='failed' WHERE id=%s",
            (memory_id,),
        )
    retried = await client.post(
        f"/v1/operations/{operation_id}/retry",
        headers=auth_headers(idempotency="retry-dead-letter"),
    )
    assert retried.status_code == 202
    assert retried.json()["state"] == "queued"
    replay = await client.post(
        f"/v1/operations/{operation_id}/retry",
        headers=auth_headers(idempotency="retry-dead-letter"),
    )
    assert replay.json()["reused"] is True
    run_worker_once()
    completed = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert completed.json()["state"] == "succeeded"

    async with connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE braincore.jobs SET state='queued',available_at=now(),completed_at=NULL
               WHERE operation_id=%s""",
            (operation_id,),
        )
    run_worker_once()
    async with connection() as conn:
        count = await (
            await conn.execute(
                "SELECT count(*)::int AS count FROM braincore.memories WHERE id=%s", (memory_id,)
            )
        ).fetchone()
    assert count is not None and count["count"] == 1


async def test_terminal_extraction_failure_is_explicit_and_recoverable(
    client: httpx.AsyncClient,
) -> None:
    from braincore_memory.db import connection

    accepted = await client.post(
        "/v1/ingestions",
        headers=auth_headers(idempotency=f"terminal-extraction-{uuid4()}"),
        json={
            "content": "This durable statement survives provider failure.",
            "scope": {"run_id": "extraction-failure"},
        },
    )
    assert accepted.status_code == 202
    operation_id = accepted.json()["operation_id"]
    async with connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE braincore.jobs SET max_attempts=1 WHERE operation_id=%s",
            (operation_id,),
        )
    run_worker_once(
        {
            "BRAINCORE_LLM_BASE_URL": "http://127.0.0.1:9",
            "BRAINCORE_LLM_MODEL": "unreachable-provider",
        }
    )
    failed = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert failed.status_code == 200
    assert failed.json()["state"] == "dead_letter"
    assert failed.json()["items"][0]["state"] == "failed"
    assert failed.json()["items"][0]["error"]["code"] == "extraction_failed"
    async with connection() as conn:
        artifact_count = await (
            await conn.execute(
                """SELECT count(*)::int AS count
                   FROM braincore.artifacts a
                   JOIN braincore.tenants t ON t.id=a.tenant_id
                   WHERE t.name='default'"""
            )
        ).fetchone()
    assert artifact_count is not None and artifact_count["count"] >= 1

    retried = await client.post(
        f"/v1/operations/{operation_id}/retry",
        headers=auth_headers(idempotency=f"retry-extraction-{uuid4()}"),
    )
    assert retried.status_code == 202
    run_worker_once({"BRAINCORE_LLM_BASE_URL": "", "BRAINCORE_LLM_MODEL": ""})
    recovered = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert recovered.json()["state"] == "succeeded"


def test_migration_failure_rolls_back_and_restart_is_idempotent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("set BRAINCORE_E2E=1 to run database integration tests")
    import braincore_memory.migrate as migrate
    from braincore_memory.config import get_settings

    migration = tmp_path / "999_restart_probe.sql"
    migration.write_text(
        "CREATE TABLE braincore.interrupted_migration_probe(id integer); SELECT 1 / 0;",
        encoding="utf-8",
    )
    monkeypatch.setattr(migrate, "migration_root", lambda: tmp_path)
    with pytest.raises(psycopg.errors.DivisionByZero):
        migrate.apply_migrations()

    database_url = get_settings().database_url.get_secret_value()
    with psycopg.connect(database_url) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT to_regclass('braincore.interrupted_migration_probe')")
        assert cursor.fetchone()[0] is None
        cursor.execute(
            "SELECT count(*) FROM braincore.schema_migrations WHERE version=%s",
            (migration.name,),
        )
        assert cursor.fetchone()[0] == 0

    migration.write_text(
        "CREATE TABLE braincore.interrupted_migration_probe(id integer);",
        encoding="utf-8",
    )
    migrate.apply_migrations()
    migrate.apply_migrations()
    with psycopg.connect(database_url) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT to_regclass('braincore.interrupted_migration_probe')")
        assert cursor.fetchone()[0] == "braincore.interrupted_migration_probe"
        cursor.execute("DROP TABLE braincore.interrupted_migration_probe")
        cursor.execute(
            "DELETE FROM braincore.schema_migrations WHERE version=%s", (migration.name,)
        )
        conn.commit()

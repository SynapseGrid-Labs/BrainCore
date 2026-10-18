"""Correctness checks against the disposable E2E database, never production data."""

import json
from uuid import uuid4

import httpx
import psycopg
import pytest

from braincore_memory.config import get_settings

from .conftest import auth_headers
from .test_e2e import remember, run_worker_once


async def test_embedding_refresh_is_bounded_audited_and_idempotent(
    client: httpx.AsyncClient,
) -> None:
    scope = f"refresh-{uuid4()}"
    first = await remember(client, "Quartz has a crystalline structure.", scope)
    second = await remember(client, "Quartz can be used in oscillators.", scope)
    outsider = await remember(client, "Unrelated embedding remains unchanged.", str(uuid4()))
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        # Emulate a pre-migration vector: its identity must not be invented.
        conn.execute(
            """UPDATE braincore.memories SET embedding_model=NULL,
               embedding_preprocessing_revision=NULL WHERE id=ANY(%s::uuid[])""",
            ([first, second, outsider],),
        )
    status = await client.get("/v1/admin/status", headers=auth_headers())
    assert status.json()["counts"]["embeddings_incompatible"] >= 3
    key = f"refresh-{uuid4()}"
    body = {"scope": {"user_id": scope}, "limit": 1}
    accepted = await client.post(
        "/v1/admin/embeddings:refresh", headers=auth_headers(idempotency=key), json=body
    )
    assert accepted.status_code == 202, accepted.text
    replay = await client.post(
        "/v1/admin/embeddings:refresh", headers=auth_headers(idempotency=key), json=body
    )
    assert replay.json()["reused"] is True
    assert replay.json()["operation_id"] == accepted.json()["operation_id"]
    conflict = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=key),
        json={**body, "limit": 2},
    )
    assert conflict.status_code == 409
    operation_id = accepted.json()["operation_id"]
    operation = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert len(operation.json()["items"]) == 1
    selected = operation.json()["items"][0]["memory_id"]
    assert selected in {first, second}
    # A second batch skips the currently queued item even before a worker runs.
    followup = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"refresh-{uuid4()}"),
        json={**body, "limit": 100},
    )
    following = await client.get(
        f"/v1/operations/{followup.json()['operation_id']}", headers=auth_headers()
    )
    assert {item["memory_id"] for item in following.json()["items"]} == (
        {first, second} - {selected}
    )
    run_worker_once()
    completed = await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())
    assert completed.json()["state"] == "succeeded"
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        rows = conn.execute(
            """SELECT id,embedding_model,embedding_preprocessing_revision,embedding_state
               FROM braincore.memories WHERE id=ANY(%s::uuid[])""",
            ([first, second, outsider],),
        ).fetchall()
        for memory_id, model, revision, state in rows:
            if str(memory_id) == outsider:
                assert model is None and revision is None
            else:
                assert (model, revision, state) == ("feature-hash-v1", "token-v2", "ready")
        assert (
            conn.execute(
                """SELECT count(*) FROM braincore.audit_events
               WHERE action='embedding.refresh' AND target_id=%s""",
                (operation_id,),
            ).fetchone()[0]
            == 1
        )
    explanation = await client.get(f"/v1/memories/{first}/explain", headers=auth_headers())
    assert explanation.json()["evidence"][0]["quote"] == "Quartz has a crystalline structure."
    empty = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"refresh-{uuid4()}"),
        json=body,
    )
    assert empty.json()["state"] == "succeeded"


async def test_embedding_refresh_requires_admin_and_respects_tenant(
    client: httpx.AsyncClient,
) -> None:
    scope = f"refresh-auth-{uuid4()}"
    own = await remember(client, "Refresh authorization fixture.", scope)
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        tenant = conn.execute(
            "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id", (str(uuid4()),)
        ).fetchone()[0]
        foreign = conn.execute(
            """INSERT INTO braincore.memories
               (tenant_id,content,dedupe_key,user_id,scope,embedding,embedding_state)
               SELECT %s,content,dedupe_key,user_id,scope,embedding,'ready'
               FROM braincore.memories WHERE id=%s RETURNING id""",
            (tenant, own),
        ).fetchone()[0]
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"key-{uuid4()}"),
        json={"name": scope, "roles": ["read", "write"], "scope": {"user_id": scope}},
    )
    limited = created.json()["api_key"]
    denied = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(limited, idempotency=f"refresh-{uuid4()}"),
        json={"limit": 1},
    )
    assert denied.status_code == 403
    accepted = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"refresh-{uuid4()}"),
        json={"scope": {"user_id": scope}},
    )
    assert accepted.json()["state"] == "succeeded"  # Only the foreign row needs refresh.
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        row = conn.execute(
            "SELECT embedding_state,embedding_model FROM braincore.memories WHERE id=%s",
            (foreign,),
        ).fetchone()
        assert row == ("ready", None)


@pytest.mark.parametrize("mode", ["planner", "exact", "iterative_hnsw"])
async def test_filtered_search_and_cached_queries_obey_committed_visibility(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "search_vector_mode", mode)
    monkeypatch.setattr(settings, "search_diagnostics", True)
    scope = f"quartz-{uuid4()}"
    ids = [
        await remember(client, f"Quartz fixture {label}.", scope)
        for label in ("visible", "future", "expired", "suppressed", "metadata", "incompatible")
    ]
    visible, future, expired, suppressed, wrong_metadata, incompatible = ids
    with psycopg.connect(settings.database_url.get_secret_value()) as conn:
        conn.execute(
            "UPDATE braincore.memories SET metadata=%s WHERE id=ANY(%s::uuid[])",
            (json.dumps({"color": "blue"}), ids),
        )
        conn.execute(
            "UPDATE braincore.memories SET valid_from=now()+interval '1 day' WHERE id=%s",
            (future,),
        )
        conn.execute(
            """UPDATE braincore.memories SET valid_from=now()-interval '2 days',
               valid_until=now()-interval '1 day' WHERE id=%s""",
            (expired,),
        )
        conn.execute("UPDATE braincore.memories SET state='suppressed' WHERE id=%s", (suppressed,))
        conn.execute(
            "UPDATE braincore.memories SET metadata='{}'::jsonb WHERE id=%s", (wrong_metadata,)
        )
        conn.execute(
            "UPDATE braincore.memories SET embedding_model='unknown-model' WHERE id=%s",
            (incompatible,),
        )
    body = {"query": "quartz", "scope": {"user_id": scope}, "metadata_filter": {"color": "blue"}}
    first = await client.post("/v1/search", headers=auth_headers(), json=body)
    assert first.status_code == 200, first.text
    payload = first.json()
    # The incompatible vector is excluded, but its valid lexical content remains searchable.
    assert {item["id"] for item in payload["results"]} == {visible, incompatible}
    scores = {item["id"]: item["score"] for item in payload["results"]}
    assert scores[visible] > scores[incompatible]
    assert payload["streams"]["vector"]["state"] == "ok"
    assert "connection_wait" in payload["diagnostics"]["timings_ms"]
    changed = await client.patch(
        f"/v1/memories/{visible}/lifecycle",
        headers=auth_headers(idempotency=f"suppress-{uuid4()}"),
        json={"action": "suppress", "reason": "Committed visibility fixture."},
    )
    assert changed.status_code == 200
    repeated = await client.post("/v1/search", headers=auth_headers(), json=body)
    assert repeated.json()["diagnostics"]["embedding_cache_hit"] is True
    assert {item["id"] for item in repeated.json()["results"]} == {incompatible}
    revised = await client.post(
        f"/v1/memories/{incompatible}/revisions",
        headers=auth_headers(idempotency=f"revise-{uuid4()}"),
        json={
            "content": "Amethyst after an audited correction.",
            "reason": "Correct stale content.",
        },
    )
    assert revised.status_code == 200
    with psycopg.connect(settings.database_url.get_secret_value()) as conn:
        assert conn.execute(
            """SELECT embedding IS NULL,embedding_model,embedding_preprocessing_revision
               FROM braincore.memories WHERE id=%s""",
            (incompatible,),
        ).fetchone() == (True, None, None)
    pending = await client.post("/v1/search", headers=auth_headers(), json=body)
    assert pending.json()["results"] == []


async def test_refresh_cursor_bounds_scans_and_replays_page_receipts(
    client: httpx.AsyncClient,
) -> None:
    scope = f"paged-{uuid4()}"
    missing = {await remember(client, f"Paged repair fixture {n}.", scope) for n in range(3)}
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        conn.execute(
            "UPDATE braincore.memories SET embedding_model=NULL WHERE id=ANY(%s::uuid[])",
            (list(missing),),
        )
    found: set[str] = set()
    after = None
    cursors: set[str] = set()
    for _ in range(1000):
        body = {"scope": {"user_id": scope}, "limit": 1, "scan_limit": 2, "after_id": after}
        headers = auth_headers(idempotency=f"page-{uuid4()}")
        response = await client.post("/v1/admin/embeddings:refresh", headers=headers, json=body)
        assert response.status_code == 202, response.text
        page = response.json()
        assert 0 <= page["scanned_items"] <= 2
        operation = await client.get(
            f"/v1/operations/{page['operation_id']}", headers=auth_headers()
        )
        ids = {item["memory_id"] for item in operation.json()["items"]}
        assert len(ids) <= 1 and not (found & ids)
        found |= ids
        replay = await client.post("/v1/admin/embeddings:refresh", headers=headers, json=body)
        assert replay.json() == {**page, "reused": True}
        after = page["next_cursor"]
        if after is None:
            break
        assert after not in cursors
        cursors.add(after)
    else:
        pytest.fail("bounded refresh pagination did not terminate")
    assert found == missing


async def test_concurrent_refreshes_do_not_duplicate_active_jobs(client: httpx.AsyncClient) -> None:
    import asyncio

    scope = f"concurrent-refresh-{uuid4()}"
    memory = await remember(client, "Concurrent refresh fixture.", scope)
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        conn.execute("UPDATE braincore.memories SET embedding_model=NULL WHERE id=%s", (memory,))
    responses = await asyncio.gather(
        *(
            client.post(
                "/v1/admin/embeddings:refresh",
                headers=auth_headers(idempotency=f"concurrent-refresh-{uuid4()}"),
                json={"scope": {"user_id": scope}},
            )
            for _ in range(4)
        )
    )
    items = []
    for response in responses:
        assert response.status_code == 202, response.text
        operation = await client.get(
            f"/v1/operations/{response.json()['operation_id']}", headers=auth_headers()
        )
        items.extend(operation.json()["items"])
    assert [item["memory_id"] for item in items] == [memory]


async def test_refresh_discloses_skipped_locked_rows_and_allows_a_new_pass(
    client: httpx.AsyncClient,
) -> None:
    scope = f"locked-refresh-{uuid4()}"
    memory = await remember(client, "Locked repair candidate.", scope)
    dsn = get_settings().database_url.get_secret_value()
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE braincore.memories SET embedding_model=NULL WHERE id=%s", (memory,))
    with psycopg.connect(dsn) as locker:
        locker.execute("SELECT id FROM braincore.memories WHERE id=%s FOR UPDATE", (memory,))
        response = await client.post(
            "/v1/admin/embeddings:refresh",
            headers=auth_headers(idempotency=f"locked-refresh-{uuid4()}"),
            json={"scope": {"user_id": scope}},
        )
        assert response.status_code == 202, response.text
        assert response.json()["rescan_recommended"] is True
        assert response.json()["state"] == "succeeded"
        assert response.json()["next_cursor"] is None
    retry = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"unlocked-refresh-{uuid4()}"),
        json={"scope": {"user_id": scope}},
    )
    assert retry.json()["rescan_recommended"] is False
    assert retry.json()["state"] == "queued"


async def test_refresh_preserves_blank_imports_without_creating_impossible_jobs(
    client: httpx.AsyncClient,
) -> None:
    scope = f"blank-import-{uuid4()}"
    empty = await remember(client, "Source text will be empty.", scope)
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        conn.execute(
            """UPDATE braincore.memories SET content='',embedding=NULL,
               embedding_model=NULL,embedding_preprocessing_revision=NULL,
               embedding_state='not_applicable' WHERE id=%s""",
            (empty,),
        )
    status = await client.get("/v1/admin/status", headers=auth_headers())
    assert status.json()["counts"]["embeddings_not_applicable"] >= 1
    response = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"blank-refresh-{uuid4()}"),
        json={"scope": {"user_id": scope}, "limit": 100},
    )
    assert response.status_code == 202, response.text
    operation = await client.get(
        f"/v1/operations/{response.json()['operation_id']}", headers=auth_headers()
    )
    assert operation.json()["state"] == "succeeded"
    assert operation.json()["items"] == []
    with psycopg.connect(get_settings().database_url.get_secret_value()) as conn:
        assert conn.execute(
            "SELECT content,embedding_state FROM braincore.memories WHERE id=%s", (empty,)
        ).fetchone() == ("", "not_applicable")

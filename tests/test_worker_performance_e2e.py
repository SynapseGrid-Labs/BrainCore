"""Lease/content races exercised only through the opt-in disposable E2E database."""

import asyncio
import json
import os
import re
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest

from braincore_memory.config import get_settings

from .conftest import auth_headers
from .test_e2e import ROOT


def safe_worker_stderr(stderr: bytes) -> str:
    text = stderr.decode(errors="replace")
    for name, value in os.environ.items():
        if value and any(
            token in name for token in ("PASSWORD", "SECRET", "API_KEY", "DATABASE_URL")
        ):
            text = text.replace(value, "[redacted]")
    text = re.sub(r"(postgres(?:ql)?://)[^\s]+", r"\1[redacted]", text)
    return text[-4096:]


async def sql(statement: str, parameters: tuple[object, ...] = ()) -> list[tuple[Any, ...]]:
    async with await psycopg.AsyncConnection.connect(
        get_settings().database_url.get_secret_value(), autocommit=True
    ) as connection:
        cursor = await connection.execute(statement, parameters)  # type: ignore[arg-type]
        return await cursor.fetchall() if cursor.description else []


async def pending_memory(client: httpx.AsyncClient) -> tuple[str, str, str]:
    response = await client.post(
        "/v1/memories",
        headers=auth_headers(idempotency=f"race-{uuid4()}"),
        json={"content": "Original durable evidence.", "scope": {"user_id": f"race-{uuid4()}"}},
    )
    assert response.status_code == 202, response.text
    operation_id = response.json()["operation_id"]
    rows = await sql(
        "SELECT id,payload->>'memory_id' FROM braincore.jobs WHERE operation_id=%s",
        (operation_id,),
    )
    job_id, memory_id = map(str, rows[0])
    await sql("UPDATE braincore.jobs SET max_attempts=1 WHERE id=%s", (job_id,))
    return operation_id, memory_id, job_id


@asynccontextmanager
async def blocked_embedder(
    status: int = 200,
) -> AsyncIterator[tuple[str, asyncio.Event, asyncio.Event]]:
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
            content_length = next(
                int(line.split(":", 1)[1])
                for line in headers.split("\r\n")
                if line.lower().startswith("content-length:")
            )
            request = json.loads(await reader.readexactly(content_length))
            entered.set()
            await release.wait()
            body = json.dumps(
                {
                    "dimensions": 384,
                    "model": "feature-hash-v1",
                    "preprocessing_revision": "token-v1",
                    "embeddings": [[1.0] + [0.0] * 383 for _ in request["inputs"]],
                }
            ).encode()
            writer.write(
                f"HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", entered, release
    finally:
        release.set()
        server.close()
        await server.wait_closed()


@asynccontextmanager
async def worker(embedder_url: str) -> AsyncIterator[asyncio.subprocess.Process]:
    bun = shutil.which("bun")
    assert bun is not None, "bun is required for worker integration tests"
    process = await asyncio.create_subprocess_exec(
        bun,
        "worker/src/index.ts",
        cwd=ROOT,
        env={
            **os.environ,
            "BRAINCORE_WORKER_ONCE": "1",
            "BRAINCORE_EMBEDDER_URL": embedder_url,
            "BRAINCORE_EMBEDDING_MODEL": "feature-hash-v1",
            "BRAINCORE_EMBEDDING_PREPROCESSING_REVISION": "token-v1",
            "BRAINCORE_EMBEDDING_TIMEOUT_SECONDS": "30",
            "BRAINCORE_LLM_BASE_URL": "",
            "BRAINCORE_LLM_MODEL": "",
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield process
    except BaseException as failure:
        returncode = process.returncode
        if returncode is None:
            process.kill()
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
            failure.add_note(
                f"worker pid={process.pid}, returncode_before_cleanup={returncode}, "
                f"returncode_after_cleanup={process.returncode}; "
                f"stderr tail: {safe_worker_stderr(stderr)}"
            )
        except Exception as diagnostic_error:
            failure.add_note(f"worker output unavailable: {type(diagnostic_error).__name__}")
        try:
            jobs = await asyncio.wait_for(
                sql(
                    "SELECT state,attempts,max_attempts,leased_by IS NOT NULL,"
                    "leased_until<now() FROM braincore.jobs ORDER BY created_at DESC LIMIT 10"
                ),
                timeout=5,
            )
            failure.add_note(f"recent synthetic jobs (state,attempts,max,leased,expired): {jobs}")
        except Exception as diagnostic_error:
            failure.add_note(f"job diagnostics unavailable: {type(diagnostic_error).__name__}")
        raise
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def finished(process: asyncio.subprocess.Process) -> None:
    _, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
    assert process.returncode == 0, safe_worker_stderr(stderr)


@pytest.mark.parametrize(("provider_status", "expected_state"), [(200, "ready"), (503, "failed")])
async def test_derived_embedding_refresh_preserves_semantic_updated_at(
    client: httpx.AsyncClient, provider_status: int, expected_state: str
) -> None:
    _, memory_id, job_id = await pending_memory(client)
    await sql(
        "UPDATE braincore.memories SET updated_at='2001-02-03T04:05:06Z' WHERE id=%s",
        (memory_id,),
    )
    if provider_status == 503:
        await sql("UPDATE braincore.jobs SET max_attempts=1 WHERE id=%s", (job_id,))
    before = await sql("SELECT updated_at FROM braincore.memories WHERE id=%s", (memory_id,))
    async with blocked_embedder(provider_status) as (url, _, release), worker(url) as process:
        release.set()
        await finished(process)
    after = await sql(
        "SELECT embedding_state,updated_at FROM braincore.memories WHERE id=%s",
        (memory_id,),
    )
    assert after == [(expected_state, before[0][0])]
    correction = await client.post(
        f"/v1/memories/{memory_id}/revisions",
        headers=auth_headers(idempotency=f"timestamp-revision-{uuid4()}"),
        json={
            "content": "Corrected evidence advances semantic time.",
            "reason": "Timestamp regression.",
        },
    )
    assert correction.status_code == 200, correction.text
    corrected = await sql("SELECT updated_at FROM braincore.memories WHERE id=%s", (memory_id,))
    assert corrected[0][0] > before[0][0]


@pytest.mark.parametrize("provider_status", [200, 503])
async def test_corrected_content_rejects_inflight_success_and_failure(
    client: httpx.AsyncClient, provider_status: int
) -> None:
    operation_id, memory_id, job_id = await pending_memory(client)
    async with blocked_embedder(provider_status) as (url, entered, release), worker(url) as process:
        await asyncio.wait_for(entered.wait(), timeout=5)
        revision = await client.post(
            f"/v1/memories/{memory_id}/revisions",
            headers=auth_headers(idempotency=f"race-revise-{uuid4()}"),
            json={
                "content": "Corrected durable evidence.",
                "reason": "Concurrent correction test.",
            },
        )
        assert revision.status_code == 200, revision.text
        # Hold the replacement job so the old attempt is the only possible writer.
        await sql(
            "UPDATE braincore.jobs SET available_at=now()+interval '1 hour' "
            "WHERE payload->>'memory_id'=%s AND id<>%s AND state='queued'",
            (memory_id, job_id),
        )
        release.set()
        await finished(process)
    rows = await sql(
        "SELECT content,embedding_state,embedding FROM braincore.memories WHERE id=%s",
        (memory_id,),
    )
    assert rows == [("Corrected durable evidence.", "pending", None)]
    operation = (await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())).json()
    assert operation["state"] == "partial"
    assert operation["items"][0]["state"] == "partial"
    # Complete the replacement work with a successful provider for clean test isolation.
    await sql(
        "UPDATE braincore.jobs SET available_at=now() WHERE payload->>'memory_id'=%s "
        "AND state='queued'",
        (memory_id,),
    )
    async with blocked_embedder() as (url, _, release), worker(url) as process:
        release.set()
        await finished(process)
    assert (await sql("SELECT embedding_state FROM braincore.memories WHERE id=%s", (memory_id,)))[
        0
    ][0] == "ready"


@pytest.mark.parametrize("provider_status", [200, 503])
async def test_reclaimed_lease_prevents_old_attempt_from_mutating_results(
    client: httpx.AsyncClient, provider_status: int
) -> None:
    _, memory_id, job_id = await pending_memory(client)
    async with blocked_embedder(provider_status) as (url, entered, release), worker(url) as process:
        await asyncio.wait_for(entered.wait(), timeout=5)
        await sql(
            "UPDATE braincore.jobs SET attempts=attempts+1,leased_by='replacement-test-worker',"
            "leased_until=now()+interval '10 minutes' WHERE id=%s",
            (job_id,),
        )
        release.set()
        await finished(process)
        assert await sql(
            "SELECT state,leased_by,attempts FROM braincore.jobs WHERE id=%s", (job_id,)
        ) == [("running", "replacement-test-worker", 2)]
        assert await sql(
            "SELECT embedding_state,embedding FROM braincore.memories WHERE id=%s", (memory_id,)
        ) == [("pending", None)]
        # Recovery owns terminalization once the replacement lease expires.
        await sql(
            "UPDATE braincore.jobs SET leased_until=now()-interval '1 second' WHERE id=%s",
            (job_id,),
        )
        async with worker(url) as recovery:
            await finished(recovery)
    assert await sql("SELECT state FROM braincore.jobs WHERE id=%s", (job_id,)) == [
        ("dead_letter",)
    ]
    assert await sql(
        "SELECT embedding_state FROM braincore.memories WHERE id=%s", (memory_id,)
    ) == [("failed",)]


async def test_newer_vector_for_unchanged_content_is_not_overwritten(
    client: httpx.AsyncClient,
) -> None:
    _, memory_id, _ = await pending_memory(client)
    newer_vector = "[0,1," + ",".join(["0"] * 382) + "]"
    async with blocked_embedder() as (url, entered, release), worker(url) as process:
        await asyncio.wait_for(entered.wait(), timeout=5)
        await sql(
            "UPDATE braincore.memories SET embedding=%s::vector,embedding_state='ready',"
            "embedding_model='newer-model',embedding_preprocessing_revision='token-v2' WHERE id=%s",
            (newer_vector, memory_id),
        )
        release.set()
        await finished(process)
    assert await sql(
        "SELECT embedding::text,embedding_model,embedding_preprocessing_revision "
        "FROM braincore.memories WHERE id=%s",
        (memory_id,),
    ) == [(newer_vector, "newer-model", "token-v2")]


async def test_expired_last_attempt_is_terminal_without_provider_reexecution(
    client: httpx.AsyncClient,
) -> None:
    operation_id, memory_id, job_id = await pending_memory(client)
    await sql(
        "UPDATE braincore.jobs j SET state='running',attempts=max_attempts,"
        "leased_by='expired-test-worker',leased_until=now()-interval '1 second',"
        "payload=j.payload || jsonb_build_object("
        "'embedding_content_hash',encode(sha256(convert_to(m.content,'UTF8')),'hex'),"
        "'embedding_dedupe_key',encode(m.dedupe_key,'hex'),"
        "'embedding_memory_version',m.xmin::text) "
        "FROM braincore.memories m WHERE j.id=%s AND m.id=%s",
        (job_id, memory_id),
    )
    async with blocked_embedder() as (url, entered, release), worker(url) as process:
        release.set()
        await finished(process)
        assert not entered.is_set()
    operation = (await client.get(f"/v1/operations/{operation_id}", headers=auth_headers())).json()
    assert operation["state"] == "partial"
    assert operation["items"][0]["state"] == "partial"
    assert await sql("SELECT state FROM braincore.jobs WHERE id=%s", (job_id,)) == [
        ("dead_letter",)
    ]
    assert await sql(
        "SELECT embedding_state FROM braincore.memories WHERE id=%s", (memory_id,)
    ) == [("failed",)]

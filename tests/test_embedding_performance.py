import asyncio
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from braincore_memory.config import Settings
from braincore_memory.embedder import EmbedRequest
from braincore_memory.embedding import EmbeddingClient, validate_embeddings


def payload(**overrides: Any) -> dict[str, Any]:
    return {
        "dimensions": 384,
        "model": "feature-hash-v1",
        "preprocessing_revision": "token-v2",
        "embeddings": [[1.0] + [0.0] * 383],
        **overrides,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"dimensions": 768},
        {"model": "other-model"},
        {"preprocessing_revision": "token-v1"},
        {"embeddings": []},
        {"embeddings": [[1.0] * 383]},
        {"embeddings": [[0.0] * 384]},
        {"embeddings": [[float("nan")] + [0.0] * 383]},
        {"embeddings": [[float("inf")] + [0.0] * 383]},
        {"embeddings": [[1e40] + [0.0] * 383]},
        {"embeddings": [[1e-50] + [0.0] * 383]},
        {"embeddings": [[True] + [0.0] * 383]},
        {"embeddings": [["1"] + [0.0] * 383]},
    ],
)
def test_rejects_incompatible_and_invalid_vectors(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        validate_embeddings(
            payload(**change), count=1, model="feature-hash-v1", preprocessing_revision="token-v2"
        )


def test_validates_entire_batch() -> None:
    batch = payload(embeddings=[[1.0] * 384, [0.0] * 384])
    with pytest.raises(ValueError):
        validate_embeddings(
            batch, count=2, model="feature-hash-v1", preprocessing_revision="token-v2"
        )


async def test_cache_partition_expiry_capacity_and_mutation_safety() -> None:
    calls = 0
    now = [10.0]

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=payload())

    settings = Settings(embedding_cache_max_entries=2, embedding_cache_ttl_seconds=5)
    client = EmbeddingClient(settings, transport=httpx.MockTransport(respond), clock=lambda: now[0])
    try:
        first = await client.embed_one("shared query", tenant_id="tenant-a")
        assert first.vector is not None
        first.vector[0] = 99
        cached = await client.embed_one("shared query", tenant_id="tenant-a")
        assert cached.cache_hit and cached.vector is not None and cached.vector[0] == 1
        await client.embed_one("shared query", tenant_id="tenant-b")
        assert calls == 2
        await client.embed_one("other query", tenant_id="tenant-a")
        assert calls == 3
        assert not (await client.embed_one("shared query", tenant_id="tenant-a")).cache_hit
        assert calls == 4  # least recently used entry was evicted
        now[0] += 6
        assert not (await client.embed_one("shared query", tenant_id="tenant-a")).cache_hit
        assert calls == 5
    finally:
        await client.aclose()


async def test_model_and_preprocessing_changes_do_not_reuse_cached_vectors() -> None:
    calls = 0
    settings = Settings()

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=payload(
                model=settings.embedding_model,
                preprocessing_revision=settings.embedding_preprocessing_revision,
            ),
        )

    client = EmbeddingClient(settings, transport=httpx.MockTransport(respond))
    try:
        await client.embed_one("query", tenant_id="tenant")
        settings.embedding_model = "other-model"
        assert not (await client.embed_one("query", tenant_id="tenant")).cache_hit
        settings.embedding_preprocessing_revision = "token-v1"
        assert not (await client.embed_one("query", tenant_id="tenant")).cache_hit
        assert calls == 3
    finally:
        await client.aclose()


@pytest.mark.parametrize("status,body", [(503, {}), (200, payload(embeddings=[[0.0] * 384]))])
async def test_failed_queries_are_never_cached(status: int, body: dict[str, Any]) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status if calls == 1 else 200, json=body if calls == 1 else payload())

    client = EmbeddingClient(Settings(), transport=httpx.MockTransport(respond))
    try:
        assert (await client.embed_one("query", tenant_id="tenant")).state == "failed"
        recovered = await client.embed_one("query", tenant_id="tenant")
        assert recovered.state == "ok" and not recovered.cache_hit
        assert (await client.embed_one("query", tenant_id="tenant")).cache_hit
        assert calls == 2
    finally:
        await client.aclose()


async def test_client_reuses_transport_with_bounded_concurrency() -> None:
    active = peak = calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak, calls
        calls += 1
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.005)
            return httpx.Response(200, json=payload())
        finally:
            active -= 1

    client = EmbeddingClient(
        Settings(embedding_max_concurrency=2), transport=httpx.MockTransport(respond)
    )
    try:
        results = await asyncio.gather(
            *(client.embed_one(f"query {index}", tenant_id="tenant") for index in range(9))
        )
        assert all(result.state == "ok" for result in results)
        assert calls == 9 and peak == 2
    finally:
        await client.aclose()


async def test_deadline_includes_waiting_for_a_slot_and_releases_it() -> None:
    release = asyncio.Event()
    entered = asyncio.Event()
    calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return httpx.Response(200, json=payload())

    client = EmbeddingClient(
        Settings(embedding_max_concurrency=1, embedding_timeout_seconds=0.02),
        transport=httpx.MockTransport(respond),
    )
    try:
        first = asyncio.create_task(client.embed_one("one", tenant_id="tenant"))
        await entered.wait()
        second = asyncio.create_task(client.embed_one("two", tenant_id="tenant"))
        results = await asyncio.gather(first, second)
        assert all(result.state == "failed" for result in results)
        release.set()
        assert (await client.embed_one("two", tenant_id="tenant")).state == "ok"
        assert calls <= 3
    finally:
        await client.aclose()


async def test_cancelled_request_releases_its_concurrency_slot() -> None:
    entered = asyncio.Event()
    released = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        entered.set()
        await released.wait()
        return httpx.Response(200, json=payload())

    client = EmbeddingClient(
        Settings(embedding_max_concurrency=1), transport=httpx.MockTransport(respond)
    )
    try:
        pending = asyncio.create_task(client.embed_one("one", tenant_id="tenant"))
        await entered.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        released.set()
        assert (await client.embed_one("two", tenant_id="tenant")).state == "ok"
    finally:
        await client.aclose()


def test_embedder_bounds_batch_work_and_rejects_requested_model_mismatch() -> None:
    with pytest.raises(ValidationError):
        EmbedRequest(inputs=["a" * 100_001])
    with pytest.raises(ValidationError):
        EmbedRequest(inputs=["a" * 100_000] * 6)
    with pytest.raises(ValidationError):
        EmbedRequest(inputs=["query"], model="other-model")  # type: ignore[arg-type]

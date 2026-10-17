import asyncio
import hashlib
import math
import struct
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from .config import Settings, get_settings

DIMENSIONS = 384
FLOAT32_MAX = 3.4028234663852886e38
CacheKey = tuple[str, str, str, str]


@dataclass(frozen=True)
class EmbeddingResult:
    vector: list[float] | None
    state: str
    warning: str | None = None
    cache_hit: bool = False


def validate_embeddings(
    body: Any, *, count: int, model: str, preprocessing_revision: str
) -> list[list[float]]:
    """Validate the whole provider response before any vector can be used."""
    if (
        not isinstance(body, dict)
        or body.get("dimensions") != DIMENSIONS
        or body.get("model", body.get("algorithm")) != model
        or body.get("preprocessing_revision") != preprocessing_revision
        or not isinstance(body.get("embeddings"), list)
        or len(body["embeddings"]) != count
    ):
        raise ValueError("Embedder returned incompatible vectors.")
    vectors: list[list[float]] = []
    for raw in body["embeddings"]:
        if not isinstance(raw, list) or len(raw) != DIMENSIONS:
            raise ValueError("Embedder returned an invalid vector.")
        vector: list[float] = []
        for value in raw:
            if isinstance(value, bool) or not isinstance(value, float | int):
                raise ValueError("Embedder returned a non-numeric vector.")
            number = float(value)
            if not math.isfinite(number) or abs(number) > FLOAT32_MAX:
                raise ValueError("Embedder returned a non-finite vector.")
            # pgvector stores float32 values; reject vectors which round entirely to zero.
            vector.append(struct.unpack("!f", struct.pack("!f", number))[0])
        if not any(vector):
            raise ValueError("Embedder returned a zero vector.")
        vectors.append(vector)
    return vectors


class EmbeddingClient:
    """Reuse HTTP connections and cache only successful, tenant-bound query vectors."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._slots = asyncio.Semaphore(settings.embedding_max_concurrency)
        self._cache: OrderedDict[CacheKey, tuple[float, tuple[float, ...]]] = OrderedDict()
        self._http = httpx.AsyncClient(
            timeout=settings.embedding_timeout_seconds,
            limits=httpx.Limits(
                max_connections=settings.embedding_max_concurrency,
                max_keepalive_connections=settings.embedding_max_concurrency,
            ),
            transport=transport,
        )

    async def aclose(self) -> None:
        self._cache.clear()
        await self._http.aclose()

    def _cached(self, key: CacheKey) -> EmbeddingResult | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires, vector = entry
        if expires <= self._clock():
            del self._cache[key]
            return None
        self._cache.move_to_end(key)
        return EmbeddingResult(list(vector), "ok", cache_hit=True)

    async def embed_one(self, text: str, *, tenant_id: str) -> EmbeddingResult:
        if not tenant_id:
            raise ValueError("A tenant identity is required for query embeddings.")
        settings = self._settings
        model = settings.embedding_model
        revision = settings.embedding_preprocessing_revision
        key = (tenant_id, hashlib.sha256(text.encode()).hexdigest(), model, revision)
        cached = self._cached(key)
        if cached is not None:
            return cached
        try:
            # The deadline includes time waiting for a connection/concurrency slot.
            async with asyncio.timeout(settings.embedding_timeout_seconds), self._slots:
                cached = self._cached(key)
                if cached is not None:
                    return cached
                response = await self._http.post(
                    f"{settings.embedder_url.rstrip('/')}/embed",
                    json={
                        "inputs": [text],
                        "model": model,
                        "preprocessing_revision": revision,
                    },
                )
                response.raise_for_status()
                vector = validate_embeddings(
                    response.json(), count=1, model=model, preprocessing_revision=revision
                )[0]
            if settings.embedding_cache_max_entries and settings.embedding_cache_ttl_seconds:
                self._cache[key] = (
                    self._clock() + settings.embedding_cache_ttl_seconds,
                    tuple(vector),
                )
                self._cache.move_to_end(key)
                while len(self._cache) > settings.embedding_cache_max_entries:
                    self._cache.popitem(last=False)
            return EmbeddingResult(vector, "ok")
        except (httpx.HTTPError, TimeoutError, KeyError, TypeError, ValueError, OverflowError):
            return EmbeddingResult(
                None, "failed", "Embedder unavailable or incompatible; vector search was skipped."
            )


_client: EmbeddingClient | None = None


async def open_embedding_client() -> None:
    global _client
    if _client is None:
        _client = EmbeddingClient(get_settings())


async def close_embedding_client() -> None:
    global _client
    if _client is not None:
        client, _client = _client, None
        await client.aclose()


async def embed_one(text: str, *, tenant_id: str) -> EmbeddingResult:
    await open_embedding_client()
    assert _client is not None
    return await _client.embed_one(text, tenant_id=tenant_id)

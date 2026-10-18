import hashlib
import math
import struct

import httpx
import pytest

from braincore_memory.config import Settings
from braincore_memory.embedder import app, feature_hash, stable_tokens
from braincore_memory.embedding import EmbeddingClient, validate_embeddings


@pytest.mark.parametrize(
    "text",
    [
        "今天天气很好",
        "これは日本語の文章です",
        "Привет мир",
        "مرحبا بالعالم",
        "हिन्दी पाठ",
        "Café cafe\u0301",
        "🧠✨",
        "!!!",
        "\u0301\u0308",
    ],
)
async def test_unicode_embeddings_are_nonzero_valid_and_revision_bound(text: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://embedder"
    ) as client:
        response = await client.post("/embed", json={"inputs": [text]})
        assert response.status_code == 200
        body = response.json()
        vectors = validate_embeddings(
            body, count=1, model="feature-hash-v1", preprocessing_revision="token-v2"
        )
        assert vectors[0] == pytest.approx(feature_hash(text), abs=1e-7)
        assert math.sqrt(sum(value * value for value in vectors[0])) == pytest.approx(1)


def test_fixed_codepoint_features_include_marks_and_preserve_documented_boundaries() -> None:
    assert feature_hash("CAFE") == feature_hash("cafe")
    assert feature_hash("Café") != feature_hash("CAFE\u0301")
    assert stable_tokens("हिन्दी पाठ") == list("हिन्दीपाठ")
    assert stable_tokens("مرحبا بالعالم") == list("مرحبابالعالم")
    assert stable_tokens("hello\u2003🧠") == ["hello", "🧠"]


def test_unicode_vector_golden_is_identical_on_supported_python_runtimes() -> None:
    vector = feature_hash("Привет мир 今天天气很好 हि 🧠")
    packed = struct.pack("<384f", *vector)
    assert (
        hashlib.sha256(packed).hexdigest()
        == "9fd8a484ad40c725dbad00f29e0f3b505a8712e2ad470030eb05dea416cd9ef3"
    )


async def test_original_ascii_revision_remains_explicit_and_unchanged() -> None:
    expected = [0.0] * 384
    for bucket, sign in [(268, -1), (65, 1), (321, 1), (221, 1), (32, 1)]:
        expected[bucket] = sign / math.sqrt(5)
    assert feature_hash("Alice likes tea.", "token-v1") == expected
    assert feature_hash("Alice likes tea.") == expected
    assert not any(feature_hash("今天天气很好", "token-v1"))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://embedder"
    ) as client:
        legacy = await client.post(
            "/embed", json={"inputs": ["Alice likes tea."], "preprocessing_revision": "token-v1"}
        )
        assert legacy.json()["preprocessing_revision"] == "token-v1"
        assert legacy.json()["embeddings"] == [expected]
        assert (await client.get("/health")).json()["supported_preprocessing_revisions"] == [
            "token-v1",
            "token-v2",
        ]
        assert (
            await client.post(
                "/embed", json={"inputs": ["query"], "preprocessing_revision": "token-v3"}
            )
        ).status_code == 422


async def test_real_provider_query_cache_is_partitioned_by_preprocessing_revision() -> None:
    settings = Settings(embedding_preprocessing_revision="token-v1")
    client = EmbeddingClient(settings, transport=httpx.ASGITransport(app=app))
    try:
        first = await client.embed_one("Alice likes tea.", tenant_id="synthetic-tenant")
        assert first.state == "ok" and not first.cache_hit
        assert (await client.embed_one("Alice likes tea.", tenant_id="synthetic-tenant")).cache_hit
        settings.embedding_preprocessing_revision = "token-v2"
        updated = await client.embed_one("Alice likes tea.", tenant_id="synthetic-tenant")
        assert updated.state == "ok" and not updated.cache_hit
        assert (await client.embed_one("今天天气很好", tenant_id="synthetic-tenant")).state == "ok"
    finally:
        await client.aclose()

"""Embedding regressions through the real worker and opt-in disposable database."""

from uuid import uuid4

import httpx
import psycopg
import pytest

from braincore_memory.config import get_settings
from braincore_memory.embedder import feature_hash

from .conftest import auth_headers
from .test_e2e import remember, run_worker_once


@pytest.mark.parametrize("size", [100_000, 100_001, 150_001, 500_000])
async def test_long_ingestion_finishes_with_complete_verbatim_evidence(
    client: httpx.AsyncClient, size: int
) -> None:
    content = "".join(chr(ord("a") + index // 100_000) for index in range(size - 8)) + "the tail"
    scope = f"long-ingestion-{uuid4()}"
    accepted = await client.post(
        "/v1/ingestions",
        headers=auth_headers(idempotency=f"long-ingestion-{uuid4()}"),
        json={"content": content, "scope": {"user_id": scope}},
    )
    assert accepted.status_code == 202, accepted.text
    run_worker_once({"BRAINCORE_LLM_BASE_URL": "", "BRAINCORE_LLM_MODEL": ""})
    operation = await client.get(
        f"/v1/operations/{accepted.json()['operation_id']}", headers=auth_headers()
    )
    assert operation.json()["state"] == "succeeded", operation.text
    item = operation.json()["items"][0]
    assert item["state"] == "succeeded"
    chunks = []
    for memory_id in item["result"]["memory_ids"]:
        memory = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
        assert memory.status_code == 200
        chunk = memory.json()["content"]
        assert 8 <= len(chunk) <= 100_000
        assert memory.json()["embedding_state"] == "ready"
        explanation = await client.get(f"/v1/memories/{memory_id}/explain", headers=auth_headers())
        assert explanation.json()["evidence"][0]["quote"] == chunk
        chunks.append(chunk)
    assert "".join(chunks) == content


@pytest.mark.parametrize(
    "content",
    ["今天天气很好", "これは日本語の文章です", "Привет мир", "مرحبا بالعالم", "🧠✨"],
)
async def test_unicode_memories_and_queries_use_healthy_local_vectors(
    client: httpx.AsyncClient, content: str
) -> None:
    scope = f"unicode-{uuid4()}"
    memory_id = await remember(client, content, scope)
    result = await client.post(
        "/v1/search", headers=auth_headers(), json={"query": content, "scope": {"user_id": scope}}
    )
    assert result.status_code == 200, result.text
    assert result.json()["streams"]["vector"] == {"state": "ok", "warning": None}
    assert [item["id"] for item in result.json()["results"]] == [memory_id]
    with psycopg.connect(get_settings().database_url.get_secret_value()) as connection:
        row = connection.execute(
            "SELECT embedding_state,embedding_model,embedding_preprocessing_revision "
            "FROM braincore.memories WHERE id=%s",
            (memory_id,),
        ).fetchone()
    assert row == ("ready", "feature-hash-v1", "token-v2")


async def test_direct_memory_accepts_maximum_supplementary_unicode_content(
    client: httpx.AsyncClient,
) -> None:
    content = "🧠" * 100_000
    scope = f"supplementary-{uuid4()}"
    memory_id = await remember(client, content, scope)
    memory = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
    assert memory.json()["content"] == content
    assert memory.json()["embedding_state"] == "ready"


async def test_legacy_revision_is_excluded_until_refresh_without_relabeling_evidence(
    client: httpx.AsyncClient,
) -> None:
    content = "An older durable statement."
    scope = f"legacy-preprocessing-{uuid4()}"
    memory_id = await remember(client, content, scope)
    legacy_vector = "[" + ",".join(map(str, feature_hash(content, "token-v1"))) + "]"
    with psycopg.connect(get_settings().database_url.get_secret_value()) as connection:
        connection.execute(
            "UPDATE braincore.memories SET embedding=%s::vector,"
            "embedding_preprocessing_revision='token-v1' WHERE id=%s",
            (legacy_vector, memory_id),
        )
    query = {"query": "今天天气很好", "scope": {"user_id": scope}}
    incompatible = await client.post("/v1/search", headers=auth_headers(), json=query)
    assert incompatible.status_code == 200, incompatible.text
    assert incompatible.json()["results"] == []
    assert "incompatible" in incompatible.json()["streams"]["vector"]["warning"]
    before = await client.get(f"/v1/memories/{memory_id}/explain", headers=auth_headers())
    refreshed = await client.post(
        "/v1/admin/embeddings:refresh",
        headers=auth_headers(idempotency=f"unicode-refresh-{uuid4()}"),
        json={"scope": {"user_id": scope}, "limit": 100, "scan_limit": 10000},
    )
    assert refreshed.status_code == 202, refreshed.text
    run_worker_once()
    operation = await client.get(
        f"/v1/operations/{refreshed.json()['operation_id']}", headers=auth_headers()
    )
    assert operation.json()["state"] == "succeeded"
    assert [item["memory_id"] for item in operation.json()["items"]] == [memory_id]
    compatible = await client.post("/v1/search", headers=auth_headers(), json=query)
    assert compatible.json()["streams"]["vector"] == {"state": "ok", "warning": None}
    assert [item["id"] for item in compatible.json()["results"]] == [memory_id]
    after = await client.get(f"/v1/memories/{memory_id}/explain", headers=auth_headers())
    assert after.json()["evidence"] == before.json()["evidence"]
    assert after.json()["revisions"] == before.json()["revisions"]
    memory = await client.get(f"/v1/memories/{memory_id}", headers=auth_headers())
    assert memory.json()["content"] == content
    with psycopg.connect(get_settings().database_url.get_secret_value()) as connection:
        assert connection.execute(
            "SELECT embedding_preprocessing_revision FROM braincore.memories WHERE id=%s",
            (memory_id,),
        ).fetchone() == ("token-v2",)

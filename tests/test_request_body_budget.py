"""Exercise encoded request budgets through the production HTTP middleware, without a DB."""

import json
from collections.abc import AsyncIterator
from contextlib import nullcontext
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from pydantic import ValidationError

from braincore_memory import AsyncBrainCore, api
from braincore_memory.auth import Principal
from braincore_memory.config import Settings
from braincore_memory.models import IngestRequest, RawBatchRequest


@pytest.fixture
def body_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.delenv("BRAINCORE_MAX_BODY_BYTES", raising=False)
    settings = Settings(_env_file=None)
    monkeypatch.setattr(api, "get_settings", lambda: settings)
    return settings


@pytest.fixture
def ingestion_handler(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    # Keep the real route, model validation and middleware; replace only auth/storage.
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    monkeypatch.setitem(api.app.dependency_overrides, api.require_write, lambda: principal)
    conn = AsyncMock()
    conn.transaction = Mock(return_value=nullcontext())
    conn.execute.return_value.fetchone.return_value = {"state": "queued"}
    monkeypatch.setattr(api, "connection", lambda: nullcontext(conn))
    monkeypatch.setattr(api, "begin_idempotent_operation", AsyncMock(return_value=(uuid4(), False)))
    handler = AsyncMock()
    monkeypatch.setattr(api, "enqueue_ingestion_one", handler)
    return handler


@pytest_asyncio.fixture
async def wire_client(
    body_settings: Settings, ingestion_handler: AsyncMock
) -> AsyncIterator[httpx.AsyncClient]:
    # ASGITransport does not start the database-opening application lifespan.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        yield client


def ingestion_payload(character: str, size: int = 500_000) -> dict[str, object]:
    return {
        "content": character * size,
        "scope": {field: "🧠" * 200 for field in ("project_id", "user_id", "agent_id", "run_id")},
        "source_type": "🧠" * 80,
        "source_uri": "🧠" * 2048,
        "metadata": {"synthetic": True},
    }


@pytest.mark.parametrize("character", ["界", "🧠"], ids=["cjk", "non-bmp"])
@pytest.mark.parametrize("ensure_ascii", [False, True], ids=["utf8", "escaped"])
@pytest.mark.parametrize("streamed", [False, True], ids=["content-length", "streamed"])
async def test_maximum_unicode_ingestion_reaches_handler(
    wire_client: httpx.AsyncClient,
    ingestion_handler: AsyncMock,
    character: str,
    ensure_ascii: bool,
    streamed: bool,
) -> None:
    payload = ingestion_payload(character)
    encoded = json.dumps(payload, ensure_ascii=ensure_ascii).encode("utf-8")
    assert len(encoded) > 1_048_576  # Every case reproduced the old 1 MiB rejection.

    async def chunks() -> AsyncIterator[bytes]:
        for offset in range(0, len(encoded), 65_536):
            yield encoded[offset : offset + 65_536]

    response = await wire_client.post(
        "/v1/ingestions",
        headers={"Content-Type": "application/json", "Idempotency-Key": "unicode-wire"},
        content=chunks() if streamed else encoded,
    )
    assert response.status_code == 202, response.text
    ingestion_handler.assert_awaited_once()
    assert ingestion_handler.await_args.args[-1].model_dump(mode="json") == payload


async def test_async_sdk_maximum_unicode_ingestion_uses_http_budget(
    wire_client: httpx.AsyncClient, ingestion_handler: AsyncMock
) -> None:
    content = "🧠" * 500_000
    client = AsyncBrainCore(client=wire_client)
    response = await client.ingest(content, {"user_id": "synthetic"})
    assert response["state"] == "queued"
    ingestion_handler.assert_awaited_once()
    assert ingestion_handler.await_args.args[-1].content == content


@pytest.mark.parametrize("character", ["界", "🧠"], ids=["cjk", "non-bmp"])
async def test_ingestion_character_limit_remains_enforced(
    wire_client: httpx.AsyncClient, ingestion_handler: AsyncMock, character: str
) -> None:
    response = await wire_client.post(
        "/v1/ingestions",
        headers={"Idempotency-Key": "over-character-limit"},
        json=ingestion_payload(character, size=500_001),
    )
    assert response.status_code == 422
    ingestion_handler.assert_not_awaited()


def test_body_budget_default_and_environment_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRAINCORE_MAX_BODY_BYTES", raising=False)
    assert Settings(_env_file=None).max_body_bytes == 8_388_608
    monkeypatch.setenv("BRAINCORE_MAX_BODY_BYTES", "1048576")
    assert Settings(_env_file=None).max_body_bytes == 1_048_576
    for invalid in ("1023", "33554433"):
        monkeypatch.setenv("BRAINCORE_MAX_BODY_BYTES", invalid)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)


async def test_lower_configured_budget_still_rejects_valid_unicode(
    wire_client: httpx.AsyncClient,
    ingestion_handler: AsyncMock,
    body_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRAINCORE_MAX_BODY_BYTES", "1048576")
    body_settings.max_body_bytes = Settings(_env_file=None).max_body_bytes
    response = await wire_client.post(
        "/v1/ingestions",
        headers={"Idempotency-Key": "lower-budget"},
        json=ingestion_payload("🧠"),
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    ingestion_handler.assert_not_awaited()


async def test_over_budget_content_length_rejects_before_reading_body(
    wire_client: httpx.AsyncClient, ingestion_handler: AsyncMock, body_settings: Settings
) -> None:
    async def unread_body() -> AsyncIterator[bytes]:
        raise AssertionError("an oversized Content-Length must reject before reading the body")
        yield b""  # pragma: no cover

    response = await wire_client.post(
        "/v1/ingestions",
        headers={"Content-Length": str(body_settings.max_body_bytes + 1)},
        content=unread_body(),
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"] == "no-store"
    ingestion_handler.assert_not_awaited()


@pytest.mark.parametrize("content_length", [None, "1", "invalid", "-1"])
async def test_stream_budget_rejects_missing_or_untrusted_content_length(
    wire_client: httpx.AsyncClient,
    ingestion_handler: AsyncMock,
    body_settings: Settings,
    content_length: str | None,
) -> None:
    # Valid JSON can exceed the total byte budget without exceeding any field limit.
    payload = {"content": "synthetic", "scope": {"user_id": "test"}, "metadata": {}}
    encoded = json.dumps(payload).encode() + b" " * body_settings.max_body_bytes
    consumed = 0

    async def chunks() -> AsyncIterator[bytes]:
        nonlocal consumed
        for offset in range(0, len(encoded), 65_536):
            chunk = encoded[offset : offset + 65_536]
            consumed += len(chunk)
            yield chunk
        raise AssertionError("the middleware must stop reading once the cap is exceeded")

    headers = {"Content-Type": "application/json", "Idempotency-Key": "over-budget"}
    if content_length is not None:
        headers["Content-Length"] = content_length
    response = await wire_client.post("/v1/ingestions", headers=headers, content=chunks())
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    assert consumed > body_settings.max_body_bytes
    assert consumed <= body_settings.max_body_bytes + 65_536
    ingestion_handler.assert_not_awaited()


async def test_batch_item_limits_do_not_bypass_total_byte_budget(
    wire_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = ingestion_payload("🧠")
    IngestRequest.model_validate(payload)
    batch = {"items": [payload, payload]}
    RawBatchRequest.model_validate(batch)
    handler = AsyncMock()
    monkeypatch.setattr(api, "_batch_operation", handler)
    response = await wire_client.post(
        "/v1/ingestions:batch",
        headers={"Content-Type": "application/json", "Idempotency-Key": "batch-budget"},
        content=json.dumps(batch, ensure_ascii=True).encode(),
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    handler.assert_not_awaited()


@pytest.mark.parametrize("extra_bytes", [0, 1], ids=["exact-cap", "over-cap"])
async def test_encoded_byte_budget_boundary(
    wire_client: httpx.AsyncClient,
    ingestion_handler: AsyncMock,
    body_settings: Settings,
    extra_bytes: int,
) -> None:
    body_settings.max_body_bytes = 1024
    encoded = json.dumps({"content": "🧠", "scope": {"user_id": "test"}}).encode()
    encoded += b" " * (body_settings.max_body_bytes + extra_bytes - len(encoded))
    response = await wire_client.post(
        "/v1/ingestions",
        headers={"Content-Type": "application/json", "Idempotency-Key": "boundary"},
        content=encoded,
    )
    assert response.status_code == (413 if extra_bytes else 202)
    assert ingestion_handler.await_count == (0 if extra_bytes else 1)

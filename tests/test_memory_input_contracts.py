"""Native content and explicit LangGraph client-mode contracts."""

import asyncio
import json
from typing import Any
from uuid import uuid4

import httpx
import pytest
from langgraph.store.base import GetOp, ListNamespacesOp, PutOp, SearchOp
from pydantic import ValidationError

from braincore_memory import api
from braincore_memory.auth import Principal
from braincore_memory.client import AsyncBrainCore, BrainCore
from braincore_memory.embedder import feature_hash
from braincore_memory.embedding import validate_embeddings
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.models import IngestRequest, RememberRequest, RevisionRequest

BLANK_CONTENT = [" ", "\t\r\n", "\u2003\u00a0"]


@pytest.mark.parametrize("content", BLANK_CONTENT)
@pytest.mark.parametrize("model", [RememberRequest, RevisionRequest])
def test_native_memory_models_reject_whitespace_without_changing_ingestion(
    content: str, model: type[RememberRequest] | type[RevisionRequest]
) -> None:
    body = (
        {"content": content, "scope": {"user_id": "alice"}}
        if model is RememberRequest
        else {"content": content, "reason": "Synthetic correction"}
    )
    with pytest.raises(ValidationError):
        model.model_validate(body)
    assert IngestRequest(content=content, scope={"user_id": "alice"}).content == content


@pytest.mark.parametrize("content", [" \t🧠 界 café\n ", "\x01", '  {"style":"concise"}  '])
def test_nonblank_content_is_preserved_verbatim(content: str) -> None:
    assert RememberRequest(content=content, scope={"user_id": "alice"}).content == content
    assert RevisionRequest(content=content, reason="Synthetic correction").content == content


@pytest.mark.parametrize("content", ["!!!", "---", "\x01", " \t🧠 界 café\n ", "Words --- and !!!"])
def test_supported_nonblank_content_keeps_healthy_token_v2_vectors(content: str) -> None:
    vector = feature_hash(content)
    validated = validate_embeddings(
        {
            "dimensions": 384,
            "model": "feature-hash-v1",
            "preprocessing_revision": "token-v2",
            "embeddings": [vector],
        },
        count=1,
        model="feature-hash-v1",
        preprocessing_revision="token-v2",
    )
    assert any(validated[0])


@pytest.mark.parametrize("content", BLANK_CONTENT)
@pytest.mark.parametrize("revision", [False, True])
async def test_blank_native_http_requests_reject_before_any_storage(
    monkeypatch: pytest.MonkeyPatch, content: str, revision: bool
) -> None:
    principal = Principal(uuid4(), uuid4(), frozenset({"admin"}))
    monkeypatch.setitem(api.app.dependency_overrides, api.require_write, lambda: principal)

    def forbidden_connection() -> Any:
        raise AssertionError("blank native writes must not access storage")

    monkeypatch.setattr(api, "connection", forbidden_connection)
    body = (
        {"content": content, "reason": "Synthetic correction"}
        if revision
        else {"content": content, "scope": {"user_id": "alice"}}
    )
    path = f"/v1/memories/{uuid4()}/revisions" if revision else "/v1/memories"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(path, headers={"Idempotency-Key": "blank"}, json=body)
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_failed"


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    requests: list[httpx.Request] = []

    def respond(_: object, request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"error": {"code": "not_found"}})

    async def arespond(transport: object, request: httpx.Request) -> httpx.Response:
        return respond(transport, request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", respond)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", arespond)
    return requests


@pytest.mark.parametrize(
    "base_url,key",
    [("http://127.0.0.1:8787", None), ("https://memory.example.invalid:9443", "synthetic-key")],
)
async def test_owned_store_modes_use_the_same_explicit_configuration(
    no_network: list[httpx.Request], base_url: str, key: str | None
) -> None:
    store = BrainCoreStore(base_url=base_url, api_key=key)
    sync_raw, async_raw = store.client._client, store.async_client._client
    try:
        assert store.get(("alice", "preferences"), "missing") is None
        assert await store.aget(("alice", "preferences"), "missing") is None
        assert len(no_network) == 2
        for request in no_network:
            assert str(request.url) == base_url + "/v1/memories:resolve"
            assert request.headers.get("Authorization") == (f"Bearer {key}" if key else None)
    finally:
        store.close()
        await store.aclose()
    assert sync_raw.is_closed and async_raw.is_closed


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("sdk_key", [None, "synthetic-sdk-key"])
async def test_one_injected_client_retains_transport_auth_and_rejects_other_mode(
    no_network: list[httpx.Request], asynchronous: bool, sdk_key: str | None
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"error": {"code": "not_found"}})

    transport = httpx.MockTransport(respond)
    kwargs = {
        "base_url": "https://injected.example.invalid:9443",
        "headers": {"Authorization": "Bearer synthetic-httpx-key", "X-Custom": "preserved"},
        "transport": transport,
    }
    sync_raw = httpx.Client(**kwargs)
    async_raw = httpx.AsyncClient(**kwargs)
    injected = (
        AsyncBrainCore(api_key=sdk_key, client=async_raw)
        if asynchronous
        else BrainCore(api_key=sdk_key, client=sync_raw)
    )
    store = (
        BrainCoreStore(async_client=injected) if asynchronous else BrainCoreStore(client=injected)
    )
    try:
        if asynchronous:
            assert await store.aget(("alice", "preferences"), "missing") is None
        else:
            assert store.get(("alice", "preferences"), "missing") is None
        operations = [
            GetOp(("alice",), "missing"),
            PutOp(("alice",), "key", {"style": "concise"}),
            SearchOp(("alice",), query="synthetic"),
            ListNamespacesOp(),
            PutOp(("alice",), "key", None),
        ]
        for operation in operations:
            with pytest.raises(ValueError, match="client" if asynchronous else "async_client"):
                if asynchronous:
                    store.batch([operation])
                else:
                    await store.abatch([operation])
        assert not no_network, (
            "an injected-only store must never construct/use a default counterpart"
        )
        assert len(requests) == 1
        assert str(requests[0].url) == "https://injected.example.invalid:9443/v1/memories:resolve"
        assert requests[0].headers["Authorization"] == f"Bearer {sdk_key or 'synthetic-httpx-key'}"
        assert requests[0].headers["X-Custom"] == "preserved"
        assert json.loads(requests[0].content)["scope"] == {"user_id": "alice"}
        store.close()
        await store.aclose()
        assert not sync_raw.is_closed and not async_raw.is_closed
    finally:
        await asyncio.to_thread(sync_raw.close)
        await async_raw.aclose()


async def test_paired_injected_clients_keep_each_transport_and_caller_ownership() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"error": {"code": "not_found"}})

    with httpx.Client(
        transport=httpx.MockTransport(respond), base_url="https://sync.example.invalid"
    ) as sync_raw:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond), base_url="https://async.example.invalid"
        ) as async_raw:
            store = BrainCoreStore(
                client=BrainCore(api_key="sync-key", client=sync_raw),
                async_client=AsyncBrainCore(api_key="async-key", client=async_raw),
            )
            assert store.get(("alice",), "missing") is None
            assert await store.aget(("alice",), "missing") is None
            store.close()
            await store.aclose()
            assert not sync_raw.is_closed and not async_raw.is_closed
    assert [(request.url.host, request.headers["Authorization"]) for request in requests] == [
        ("sync.example.invalid", "Bearer sync-key"),
        ("async.example.invalid", "Bearer async-key"),
    ]


@pytest.mark.parametrize(
    "value", [{}, {"content": " \t "}, {"style": "concise", "nested": {"enabled": True}}]
)
def test_langgraph_metadata_only_values_remain_valid_native_content(value: dict[str, Any]) -> None:
    item = {"content": BrainCoreStore._content(value), "scope": {"user_id": "alice"}}
    assert json.loads(RememberRequest.model_validate(item).content) == value

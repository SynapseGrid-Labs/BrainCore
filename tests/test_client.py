import httpx
import pytest

from braincore_memory import AsyncBrainCore, BrainCore, BrainCoreError


def test_sync_client_generates_idempotency_and_maps_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Idempotency-Key"]
        assert request.headers["Authorization"] == "Bearer key"
        return httpx.Response(202, json={"operation_id": "op-1", "state": "queued"})

    transport = httpx.MockTransport(handler)
    raw_client = httpx.Client(transport=transport, base_url="http://test")
    client = BrainCore(api_key="key", client=raw_client)
    assert client.remember("Alice likes tea", {"user_id": "alice"})["operation_id"] == "op-1"
    raw_client.close()


def test_sync_client_raises_stable_error() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            409,
            json={
                "error": {
                    "code": "idempotency_conflict",
                    "message": "Conflict",
                    "details": {},
                }
            },
        )
    )
    raw_client = httpx.Client(transport=transport, base_url="http://test")
    client = BrainCore(client=raw_client)
    with pytest.raises(BrainCoreError, match="Conflict") as raised:
        client.search("query", {"user_id": "alice"})
    assert raised.value.code == "idempotency_conflict"
    raw_client.close()


async def test_async_client_batch_facade() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/memories:batch"
        return httpx.Response(202, json={"operation_id": "op-2", "state": "queued"})

    transport = httpx.MockTransport(handler)
    raw_client = httpx.AsyncClient(transport=transport, base_url="http://test")
    client = AsyncBrainCore(client=raw_client)
    response = await client.remember_batch(
        [{"content": "Alice likes tea", "scope": {"user_id": "alice"}}]
    )
    assert response["operation_id"] == "op-2"
    await raw_client.aclose()


def test_sync_admin_key_facade() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/admin/api-keys"
        assert request.headers["Idempotency-Key"]
        return httpx.Response(201, json={"id": "key-1", "api_key": "bck_secret"})

    raw_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")
    client = BrainCore(client=raw_client)
    response = client.create_api_key("agent", ["read", "write"], {"agent_id": "agent-1"})
    assert response["id"] == "key-1"
    raw_client.close()


async def test_async_admin_key_facade() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/admin/api-keys/key-1/rotate"
        assert request.headers["Idempotency-Key"]
        return httpx.Response(200, json={"id": "key-1", "api_key": "bck_rotated"})

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://test")
    client = AsyncBrainCore(client=raw_client)
    response = await client.rotate_api_key("key-1")
    assert response["api_key"] == "bck_rotated"
    await raw_client.aclose()

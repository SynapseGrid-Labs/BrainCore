import httpx
import pytest
import respx

from braincore_memory import AsyncBrainCore, BrainCore, BrainCoreError


def test_respx_intercepts_sdk_owned_sync_transport() -> None:
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post("https://braincore.example/v1/search").mock(
            return_value=httpx.Response(200, json={"results": []})
        )
        with BrainCore(base_url="https://braincore.example", api_key="synthetic-key") as client:
            assert client.search("tea", {"user_id": "alice"}) == {"results": []}
        assert route.calls.last.request.headers["Authorization"] == "Bearer synthetic-key"


async def test_respx_intercepts_sdk_owned_async_transport_and_error() -> None:
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post("https://braincore.example/v1/memories").mock(
            return_value=httpx.Response(
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
        async with AsyncBrainCore(
            base_url="https://braincore.example",
            api_key="synthetic-key",
        ) as client:
            with pytest.raises(BrainCoreError) as raised:
                await client.remember("tea", {"user_id": "alice"}, idempotency_key="synthetic-op")
        assert raised.value.code == "idempotency_conflict"
        assert route.calls.last.request.headers["Idempotency-Key"] == "synthetic-op"

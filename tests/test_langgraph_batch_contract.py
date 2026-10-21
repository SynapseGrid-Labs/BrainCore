"""LangGraph adapter contracts exercised through the actual HTTP clients/models."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import MappingProxyType
from typing import Any

import httpx
import pytest
from langgraph.store.base import GetOp, ListNamespacesOp, MatchCondition, Op, PutOp, SearchOp

from braincore_memory.client import AsyncBrainCore, BrainCore
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.models import RawBatchRequest, RememberRequest, SearchRequest


def puts(count: int, *, start: int = 0) -> list[PutOp]:
    return [
        PutOp(("user-42", "preferences"), f"key-{index}", {"content": f"value-{index}"})
        for index in range(start, start + count)
    ]


class ContractAPI:
    def __init__(self, *, failure_batch: int | None = None, failure_state: str = "partial"):
        self.events: list[tuple[str, Any]] = []
        self.batches: list[list[dict[str, Any]]] = []
        self.records: dict[tuple[tuple[str, ...], str], dict[str, Any]] = {}
        self.failure_batch = failure_batch
        self.failure_state = failure_state

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        path = request.url.path
        if path == "/v1/memories:batch":
            batch = RawBatchRequest.model_validate(body)
            for item in batch.items:
                RememberRequest.model_validate(item)
            self.batches.append(batch.items)
            number = len(self.batches)
            self.events.append(("batch", [item["reference"]["key"] for item in batch.items]))
            for item in batch.items:
                reference = item["reference"]
                self.records[(tuple(reference["namespace"]), reference["key"])] = {
                    "id": reference["key"],
                    "state": "active",
                    "reference_namespace": reference["namespace"],
                    "reference_key": reference["key"],
                    "metadata": item["metadata"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                }
            return httpx.Response(202, json={"operation_id": str(number), "state": "queued"})
        if path.startswith("/v1/operations/"):
            number = int(path.rsplit("/", 1)[1])
            self.events.append(("wait", number))
            state = self.failure_state if number == self.failure_batch else "succeeded"
            return httpx.Response(200, json={"state": state})
        if path == "/v1/memories:resolve":
            self.events.append(("resolve", body["key"]))
            assert body["scope"] == {"user_id": body["namespace"][0]}
            record = self.records.get((tuple(body["namespace"]), body["key"]))
            return httpx.Response(200 if record else 404, json=record or {})
        if path.endswith("/lifecycle"):
            key = path.split("/")[-2]
            action = body["action"]
            record = next(record for record in self.records.values() if record["id"] == key)
            record["state"] = "suppressed" if action == "suppress" else "active"
            self.events.append(("lifecycle", action))
            return httpx.Response(200, json={"operation_id": "0"})
        if path == "/v1/search":
            SearchRequest.model_validate(body)
            self.events.append(("search", body))
            return httpx.Response(200, json={"results": list(self.records.values())[:1]})
        raise AssertionError(f"unexpected request: {request.method} {path}")


@asynccontextmanager
async def store_for(handler: Any) -> AsyncIterator[BrainCoreStore]:
    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="http://test") as sync_raw:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as async_raw:
            yield BrainCoreStore(
                client=BrainCore(client=sync_raw), async_client=AsyncBrainCore(client=async_raw)
            )


async def run_batch(store: BrainCoreStore, operations: list[Op], *, asynchronous: bool) -> Any:
    return await store.abatch(operations) if asynchronous else store.batch(operations)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("count", [100, 101, 200, 205])
async def test_put_batches_respect_http_limit_and_keep_every_reference(
    asynchronous: bool, count: int
) -> None:
    api = ContractAPI()
    async with store_for(api) as store:
        results = await run_batch(store, list(puts(count)), asynchronous=asynchronous)
    assert results == [None] * count
    assert [len(batch) for batch in api.batches] == [100] * (count // 100) + (
        [count % 100] if count % 100 else []
    )
    flattened = [item for batch in api.batches for item in batch]
    assert [item["reference"]["key"] for item in flattened] == [f"key-{i}" for i in range(count)]
    assert all(item["scope"] == {"user_id": "user-42"} for item in flattened)
    assert all(item["reference"]["namespace"] == ["user-42", "preferences"] for item in flattened)
    assert [item["metadata"]["langgraph"]["value"] for item in flattened] == [
        {"content": f"value-{i}"} for i in range(count)
    ]


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_accepts_any_mapping_value(asynchronous: bool) -> None:
    api = ContractAPI()
    value = MappingProxyType({"content": "read-only value", "tags": ["a"]})
    async with store_for(api) as store:
        operations: list[Op] = [PutOp(("user-42", "preferences"), "key-0", value)]
        results = await run_batch(store, operations, asynchronous=asynchronous)
    assert results == [None]
    [item] = api.batches[0]
    assert item["content"] == "read-only value"
    assert item["metadata"]["langgraph"]["value"] == {"content": "read-only value", "tags": ["a"]}


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_chunks_finish_before_following_get_search_and_put(
    asynchronous: bool,
) -> None:
    api = ContractAPI()
    first = puts(205)
    second = puts(101, start=205)
    operations: list[Op] = [
        *first,
        GetOp(("user-42", "preferences"), "key-204"),
        SearchOp(("user-42",), query="value"),
        *second,
        GetOp(("user-42", "preferences"), "key-305"),
    ]
    async with store_for(api) as store:
        results = await run_batch(store, operations, asynchronous=asynchronous)
    assert len(results) == len(operations)
    assert all(result is None for result in results[:205])
    assert results[205].value == {"content": "value-204"}
    assert results[206][0].key == "key-0"
    assert all(result is None for result in results[207:-1])
    assert results[-1].value == {"content": "value-305"}
    assert [len(batch) for batch in api.batches] == [100, 100, 5, 100, 1]
    transitions = [event[0] for event in api.events if event[0] != "resolve"]
    assert transitions == ["batch", "wait"] * 3 + ["search"] + ["batch", "wait"] * 2


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("state", ["partial", "failed", "dead_letter"])
@pytest.mark.parametrize("failure_batch", [1, 2])
async def test_failed_chunk_stops_later_writes_and_reads_without_claiming_rollback(
    asynchronous: bool, state: str, failure_batch: int
) -> None:
    api = ContractAPI(failure_batch=failure_batch, failure_state=state)
    operations: list[Op] = [*puts(205), GetOp(("user-42", "preferences"), "key-204")]
    async with store_for(api) as store:
        with pytest.raises(RuntimeError, match=f"batch ended as {state}"):
            await run_batch(store, operations, asynchronous=asynchronous)
    assert len(api.batches) == failure_batch
    # Accepted writes may already exist; the adapter neither retries nor rolls them back.
    assert len(api.records) == 100 * failure_batch
    assert api.events[-1] == ("wait", failure_batch)


async def test_async_cancellation_does_not_submit_another_chunk() -> None:
    api = ContractAPI()
    waiting = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/v1/operations/"):
            waiting.set()
            await asyncio.Event().wait()
        return api(request)

    async with store_for(handler) as store:
        task = asyncio.create_task(store.abatch(puts(205)))
        await asyncio.wait_for(waiting.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(api.batches) == 1
    assert len(api.records) == 100


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("count", [10_000, 10_001, 10_101])
@pytest.mark.parametrize("prefix", [(), ("user-42", "preferences")])
async def test_namespace_listing_reports_capacity_instead_of_truncating(
    asynchronous: bool, count: int, prefix: tuple[str, ...]
) -> None:
    requests: list[dict[str, Any]] = []
    conditions = (MatchCondition("prefix", prefix),) if prefix else None
    op = ListNamespacesOp(conditions, max_depth=3, limit=2, offset=1)

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        validated = SearchRequest.model_validate(body)
        assert body["scope"] == ({"user_id": "user-42"} if prefix else None)
        assert validated.reference_namespace_prefix == (list(prefix) or None)
        assert validated.metadata_filter == {"langgraph": {"value": {}}}
        assert validated.include_inactive is True
        requests.append(body)
        results = [
            {"reference_namespace": ["user-42", "preferences", f"item-{index:05d}", "details"]}
            for index in range(body["offset"], min(count, body["offset"] + body["limit"]))
        ]
        return httpx.Response(200, json={"results": results})

    async with store_for(handler) as store:
        if count > 10_000:
            with pytest.raises(
                RuntimeError, match="namespace listing exceeds 10000 matching memories"
            ):
                await run_batch(store, [op], asynchronous=asynchronous)
        else:
            results = await run_batch(store, [op], asynchronous=asynchronous)
            assert results == [
                [
                    ("user-42", "preferences", "item-00001"),
                    ("user-42", "preferences", "item-00002"),
                ]
            ]
    assert len(requests) == 101
    assert requests[-1]["offset"] == 10_000
    assert requests[-1]["limit"] == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("action", ["delete", "restore"])
@pytest.mark.parametrize("state", ["succeeded", "partial", "failed", "dead_letter"])
async def test_lifecycle_operations_require_terminal_success(
    asynchronous: bool, action: str, state: str
) -> None:
    api = ContractAPI()
    original = puts(1)[0]
    existing = {
        "id": "memory-42",
        "state": "active" if action == "delete" else "suppressed",
        "reference_namespace": list(original.namespace),
        "reference_key": original.key,
        "metadata": {"langgraph": {"value": original.value}},
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    lifecycle_calls: list[str] = []
    resolved_keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/memories:resolve":
            resolved_keys.append(json.loads(request.content)["key"])
            return httpx.Response(200, json=existing)
        if path == "/v1/memories/memory-42/lifecycle":
            lifecycle_calls.append(json.loads(request.content)["action"])
            return httpx.Response(200, json={"operation_id": "lifecycle"})
        if path == "/v1/operations/lifecycle":
            return httpx.Response(
                200,
                json={
                    "state": state,
                    "accepted_items": 1,
                    "succeeded_items": int(state == "succeeded"),
                    "failed_items": int(state in {"failed", "dead_letter"}),
                    "warning_items": int(state == "partial"),
                    "items": [{"state": state, "error": None if state == "succeeded" else {}}],
                },
            )
        return api(request)

    operations: list[Op] = [
        PutOp(original.namespace, original.key, None) if action == "delete" else original,
        GetOp(original.namespace, "unreachable-after-failure"),
    ]
    async with store_for(handler) as store:
        if state == "succeeded":
            assert len(await run_batch(store, operations, asynchronous=asynchronous)) == 2
        else:
            with pytest.raises(RuntimeError, match=f"{action} ended as {state}"):
                await run_batch(store, operations, asynchronous=asynchronous)
    assert lifecycle_calls == ["suppress" if action == "delete" else "restore"]
    if state != "succeeded":
        assert "unreachable-after-failure" not in resolved_keys


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_delete_is_an_ordering_barrier_between_large_put_runs(asynchronous: bool) -> None:
    api = ContractAPI()
    operations: list[Op] = [
        *puts(100),
        PutOp(("user-42", "preferences"), "key-99", None),
        *puts(101, start=99),
        GetOp(("user-42", "preferences"), "key-99"),
    ]
    async with store_for(api) as store:
        results = await run_batch(store, operations, asynchronous=asynchronous)
    assert results[-1].key == "key-99"
    assert results[-1].value == {"content": "value-99"}
    assert [len(batch) for batch in api.batches] == [100, 100, 1]
    transitions = [event[0] for event in api.events if event[0] != "resolve"]
    assert transitions == ["batch", "wait", "lifecycle", "wait", "batch", "wait", "batch", "wait"]


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_small_namespace_listing_preserves_suffix_filter_and_depth(
    asynchronous: bool,
) -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(SearchRequest.model_validate_json(request.content))
        return httpx.Response(
            200,
            json={
                "results": [
                    {"reference_namespace": ["user-42", "preferences", "personal"]},
                    {"reference_namespace": ["user-42", "preferences", "personal"]},
                    {"reference_namespace": ["user-42", "preferences", "work"]},
                ]
            },
        )

    op = ListNamespacesOp(
        (MatchCondition("prefix", ("user-42",)), MatchCondition("suffix", ("personal",))),
        max_depth=2,
    )
    async with store_for(handler) as store:
        assert await run_batch(store, [op], asynchronous=asynchronous) == [
            [("user-42", "preferences")]
        ]
    assert len(requests) == 1
    assert requests[0].reference_namespace_prefix == ["user-42"]
    assert requests[0].scope is not None
    assert requests[0].scope.user_id == "user-42"

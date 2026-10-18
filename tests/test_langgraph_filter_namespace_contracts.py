"""Filter compatibility and scoped wildcard listing against the pinned store contract."""

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest
from langgraph.store.base import ListNamespacesOp, MatchCondition, PutOp, SearchOp
from langgraph.store.memory import InMemoryStore

from braincore_memory.auth import Principal
from braincore_memory.models import SearchRequest
from braincore_memory.service import _search_filter

from .test_langgraph_batch_contract import run_batch, store_for

UNSUPPORTED_FILTERS = [
    *[{"score": {operator: 1}} for operator in ("$eq", "$ne", "$gt", "$gte", "$lt", "$lte")],
    {"value": ["first", "second"]},
    {"value": None},
    {"value": True},
    {"value": 1},
    {"value": 1.5},
    {"nested": {"value": ["first"]}},
    {"nested": {}},
]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("leading_put", [False, True])
@pytest.mark.parametrize("value_filter", UNSUPPORTED_FILTERS)
async def test_unsupported_filter_rejects_before_every_batch_request(
    asynchronous: bool, leading_put: bool, value_filter: dict[str, Any]
) -> None:
    requests: list[httpx.Request] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError(
            "unsupported filters must reject before any HTTP, including earlier puts"
        )

    ops = [SearchOp(("alice",), filter=value_filter)]
    if leading_put:
        ops.insert(0, PutOp(("alice",), "unwritten", {"style": "concise"}))
    async with store_for(forbidden) as store:
        with pytest.raises(ValueError, match="filter"):
            await run_batch(store, ops, asynchronous=asynchronous)
    assert not requests


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "value_filter",
    [
        None,
        {},
        {"style": "concise"},
        {"nested": {"style": "concise"}},
        {"nested": {"deeper": {"style": ""}}},
    ],
)
async def test_supported_filter_shapes_keep_filtering_and_pagination_in_http(
    asynchronous: bool, value_filter: dict[str, Any] | None
) -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        SearchRequest.model_validate(body)
        requests.append(body)
        assert body["metadata_filter"] == {"langgraph": {"value": value_filter or {}}}
        assert body["limit"] == 2 and body["offset"] == 1
        assert body["scope"] == {"user_id": "alice"}
        return httpx.Response(200, json={"results": []})

    async with store_for(handler) as store:
        result = await run_batch(
            store,
            [SearchOp(("alice",), filter=value_filter, limit=2, offset=1)],
            asynchronous=asynchronous,
        )
    assert result == [[]]
    assert len(requests) == 1


NAMESPACES = [
    ("alice", "preferences", "dark"),
    ("alice", "preferences", "light"),
    ("alice", "preferences", "dark", "detail"),
    ("alice", "work", "dark"),
    ("bob", "preferences", "dark"),
]
LIST_CASES = [
    ((MatchCondition("prefix", ("*", "preferences")),), None, 0, 100),
    ((MatchCondition("prefix", ("alice", "*", "dark")),), None, 0, 100),
    ((MatchCondition("suffix", ("*",)),), None, 0, 100),
    ((MatchCondition("suffix", ()),), None, 0, 100),
    ((MatchCondition("prefix", ()),), None, 0, 100),
    (
        (MatchCondition("prefix", ("alice", "*")), MatchCondition("suffix", ("dark", "*"))),
        None,
        0,
        100,
    ),
    ((MatchCondition("prefix", ("*", "preferences")),), 3, 1, 1),
    ((MatchCondition("prefix", ("alice", "preferences")),), 2, 0, 100),
]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("conditions,max_depth,offset,limit", LIST_CASES)
async def test_namespace_wildcards_preserve_scope_and_match_pinned_memory_store(
    asynchronous: bool,
    conditions: tuple[MatchCondition, ...],
    max_depth: int | None,
    offset: int,
    limit: int,
) -> None:
    principal = Principal(uuid4(), uuid4(), frozenset({"read"}), scope={"user_id": "alice"})
    reference = InMemoryStore()
    for index, namespace in enumerate(NAMESPACES):
        if namespace[0] == "alice":
            reference.put(namespace, f"key-{index}", {"style": "concise"})
    op = ListNamespacesOp(conditions, max_depth=max_depth, offset=offset, limit=limit)
    expected = reference.batch([op])[0]
    first_prefix = next(
        (tuple(condition.path) for condition in conditions if condition.match_type == "prefix"), ()
    )
    literal_prefix = (
        first_prefix[: first_prefix.index("*")] if "*" in first_prefix else first_prefix
    )
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        _, values = _search_filter(principal, SearchRequest.model_validate(body))
        assert values[1] == "alice", "the API must retain the authenticated credential scope"
        assert body["scope"] == ({"user_id": literal_prefix[0]} if literal_prefix else None)
        assert body["reference_namespace_prefix"] == (list(literal_prefix) or None)
        requests.append(body)
        candidates = [
            namespace
            for namespace in NAMESPACES
            if namespace[0] == "alice" and namespace[: len(literal_prefix)] == literal_prefix
        ]
        return httpx.Response(
            200,
            json={
                "results": [
                    {"reference_namespace": namespace}
                    for namespace in candidates[body["offset"] : body["offset"] + body["limit"]]
                ]
            },
        )

    async with store_for(handler) as store:
        result = await run_batch(store, [op], asynchronous=asynchronous)
    assert result == [expected]
    assert len(requests) == 1

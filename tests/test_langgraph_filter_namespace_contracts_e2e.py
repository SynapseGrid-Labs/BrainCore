"""Scoped wildcard scans and filter preflight through real auth and PostgreSQL."""

from typing import Any
from uuid import uuid4

import httpx
import pytest
from langgraph.store.base import PutOp, SearchOp

from braincore_memory.auth import key_digest
from braincore_memory.client import AsyncBrainCore
from braincore_memory.db import connection
from braincore_memory.langgraph import BrainCoreStore

from .conftest import ADMIN_KEY, auth_headers
from .test_memory_input_contracts_e2e import write_counts


async def seed_reference(
    client: httpx.AsyncClient,
    key: str,
    namespace: tuple[str, ...],
    value: dict[str, Any],
    marker: str,
) -> None:
    response = await client.post(
        "/v1/memories",
        headers=auth_headers(key, idempotency=f"namespace-{marker}-{uuid4()}"),
        json={
            "content": f"Synthetic reference {marker} {uuid4()}.",
            "scope": {"user_id": namespace[0]},
            "reference": {"namespace": list(namespace), "key": marker},
            "metadata": {"langgraph": {"value": value}},
        },
    )
    assert response.status_code == 202, response.text


@pytest.mark.parametrize("pattern", ["first-wildcard", "interior-wildcard", "root-suffix"])
async def test_wildcard_namespace_scans_obey_real_key_scope_and_tenant(
    client: httpx.AsyncClient, pattern: str
) -> None:
    marker = uuid4().hex
    foreign_key = f"bck_{uuid4().hex}_synthetic_namespace"
    async with connection() as conn, conn.transaction():
        tenant = await (
            await conn.execute(
                "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id",
                (f"synthetic-{marker}",),
            )
        ).fetchone()
        assert tenant is not None
        await conn.execute(
            "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles) "
            "VALUES (%s,%s,%s,%s,ARRAY['admin'])",
            (tenant["id"], "synthetic namespace proof", foreign_key[:12], key_digest(foreign_key)),
        )
    own_preferences = ("alice", "preferences", marker, "own")
    own_work = ("alice", "work", marker, "own")
    await seed_reference(client, ADMIN_KEY, own_preferences, {"style": "concise"}, marker)
    await seed_reference(client, ADMIN_KEY, own_work, {"style": "concise"}, marker)
    await seed_reference(
        client,
        ADMIN_KEY,
        ("bob", "preferences", marker, "other-user"),
        {"style": "concise"},
        marker,
    )
    await seed_reference(
        client,
        foreign_key,
        ("alice", "preferences", marker, "foreign-tenant"),
        {"style": "concise"},
        marker,
    )
    created = await client.post(
        "/v1/admin/api-keys",
        headers=auth_headers(idempotency=f"scope-{marker}"),
        json={
            "name": "synthetic scoped namespace reader",
            "roles": ["read"],
            "scope": {"user_id": "alice"},
        },
    )
    assert created.status_code == 201, created.text
    store = BrainCoreStore(
        async_client=AsyncBrainCore(api_key=created.json()["api_key"], client=client)
    )
    if pattern == "first-wildcard":
        found = await store.alist_namespaces(prefix=("*", "preferences", marker), suffix=("*",))
        assert found == [own_preferences]
    elif pattern == "interior-wildcard":
        found = await store.alist_namespaces(prefix=("alice", "*", marker))
        assert found == sorted([own_preferences, own_work])
    else:
        found = await store.alist_namespaces(prefix=(), suffix=(marker, "*"))
        assert found == sorted([own_preferences, own_work])


@pytest.mark.parametrize(
    "value_filter",
    [
        {"score": {"$gt": 1}},
        {"value": [1]},
        {"value": None},
        {"value": True},
        {"value": 1},
        {"value": {}},
    ],
)
async def test_unsupported_search_filter_cannot_accept_preceding_put_in_postgresql(
    client: httpx.AsyncClient, value_filter: dict[str, Any]
) -> None:
    store = BrainCoreStore(async_client=AsyncBrainCore(api_key=ADMIN_KEY, client=client))
    before = await write_counts()
    with pytest.raises(ValueError, match="filter"):
        await store.abatch(
            [
                PutOp(("alice",), uuid4().hex, {"style": "must not be written"}),
                SearchOp(("alice",), filter=value_filter),
            ]
        )
    assert await write_counts() == before


async def test_supported_nested_string_filter_retains_api_pagination_and_type_matching(
    client: httpx.AsyncClient,
) -> None:
    marker = uuid4().hex
    namespace = ("alice", f"filters-{marker}")
    matching = {"nested": {"style": "concise", "additional": "kept"}}
    values = [
        matching,
        {"nested": {"style": "verbose"}},
        {"nested": {"style": ["concise"]}},
        {"nested": [{"style": "concise"}]},
        {"nested": {"style": {"value": "concise"}}},
    ]
    for index, value in enumerate(values):
        await seed_reference(client, ADMIN_KEY, namespace, value, f"{marker}-{index}")
    store = BrainCoreStore(async_client=AsyncBrainCore(api_key=ADMIN_KEY, client=client))
    found = await store.asearch(namespace, filter={"nested": {"style": "concise"}}, limit=1)
    assert len(found) == 1
    assert found[0].value == matching
    assert (
        await store.asearch(namespace, filter={"nested": {"style": "concise"}}, limit=1, offset=1)
        == []
    )

"""Operation polling authorizes outcomes without granting general read access."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException

from braincore_memory import api
from braincore_memory.auth import Principal, authenticate, require_write


@pytest.mark.parametrize("roles", [{"write"}, {"read"}, {"read", "write"}, {"admin"}])
@pytest.mark.parametrize("session", [False, True])
async def test_own_outcomes_are_readable_without_mutation_csrf(
    roles: set[str], session: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = Principal(
        uuid4(),
        uuid4(),
        frozenset(roles),
        {"user_id": "alice"},
        session_id=uuid4() if session else None,
        csrf_valid=False,
    )
    operation_id = uuid4()
    items = [{"item_index": 0, "state": "failed", "error": {"code": "validation_failed"}}]

    class Cursor:
        async def fetchone(self) -> dict[str, Any]:
            return {"id": operation_id, "actor_key_id": actor.key_id, "state": "partial"}

        async def fetchall(self) -> list[dict[str, Any]]:
            return items

    queries: list[tuple[str, tuple[Any, ...]]] = []

    class Connection:
        async def execute(self, query: str, values: tuple[Any, ...]) -> Cursor:
            queries.append((query, values))
            return Cursor()

    @asynccontextmanager
    async def connect() -> AsyncIterator[Connection]:
        yield Connection()

    overrides: dict[Callable[..., Any], Callable[..., Any]] = {authenticate: lambda: actor}
    monkeypatch.setattr(api.app, "dependency_overrides", overrides)
    monkeypatch.setattr(api, "connection", connect)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        response = await client.get(f"/v1/operations/{operation_id}")
    assert response.status_code == 200, response.text
    assert response.json()["items"] == items
    assert len(queries) == 2
    assert all(values == (operation_id, actor.tenant_id) for _, values in queries)
    assert all("UPDATE" not in query and "INSERT" not in query for query, _ in queries)
    if roles == {"write"} and session:
        with pytest.raises(HTTPException) as error:
            require_write(actor)
        assert error.value.status_code == 403 and error.value.detail == "csrf_failed"


@pytest.mark.parametrize("roles", [set(), {"unknown"}, {"reader"}, {"writer"}])
async def test_unrelated_roles_cannot_poll_or_access_database(
    roles: set[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset(roles))

    def forbidden_connection() -> Any:
        raise AssertionError("unauthorized polling must not reach storage")

    monkeypatch.setattr(api.app, "dependency_overrides", {authenticate: lambda: actor})
    monkeypatch.setattr(api, "connection", forbidden_connection)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        response = await client.get(f"/v1/operations/{uuid4()}")
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "permission_denied"


@pytest.mark.parametrize("roles", [{"write"}, {"read"}, {"read", "write"}, {"admin"}])
@pytest.mark.parametrize("missing", [False, True])
async def test_other_actor_outcomes_stay_hidden_except_for_tenant_admin(
    roles: set[str], missing: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset(roles))
    operation_id = uuid4()
    item_queries: list[str] = []

    class Cursor:
        async def fetchone(self) -> dict[str, Any] | None:
            return None if missing else {"id": operation_id, "actor_key_id": uuid4()}

        async def fetchall(self) -> list[dict[str, Any]]:
            return []

    class Connection:
        async def execute(self, query: str, values: tuple[Any, ...]) -> Cursor:
            assert values == (operation_id, actor.tenant_id)
            if "operation_items" in query:
                item_queries.append(query)
            return Cursor()

    @asynccontextmanager
    async def connect() -> AsyncIterator[Connection]:
        yield Connection()

    monkeypatch.setattr(api.app, "dependency_overrides", {authenticate: lambda: actor})
    monkeypatch.setattr(api, "connection", connect)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        response = await client.get(f"/v1/operations/{operation_id}")
    allowed = not missing and "admin" in roles
    assert response.status_code == (200 if allowed else 404), response.text
    assert len(item_queries) == int(allowed)


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/v1/health", None),
        ("GET", "/openapi.json", None),
        ("GET", "/docs", None),
        ("GET", "/v1/memories/{id}", None),
        ("GET", "/v1/memories/{id}/explain", None),
        ("POST", "/v1/memories:resolve", {"namespace": ["demo"], "key": "one"}),
        ("POST", "/v1/search", {"query": "synthetic"}),
    ],
)
async def test_write_only_does_not_gain_general_read_access(
    method: str, path: str, body: dict[str, Any] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = Principal(uuid4(), uuid4(), frozenset({"write"}), {"user_id": "alice"})
    monkeypatch.setattr(api.app, "dependency_overrides", {authenticate: lambda: actor})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://test"
    ) as client:
        response = await client.request(method, path.format(id=uuid4()), json=body)
    assert response.status_code == 403, response.text
    assert response.json()["error"]["code"] == "permission_denied"

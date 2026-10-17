import time
import uuid
from typing import Any

import httpx


class BrainCoreError(RuntimeError):
    def __init__(self, status_code: int, code: str, message: str, details: dict[str, Any]):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = details


def _raise_for_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    try:
        error = response.json().get("error", {})
    except ValueError:
        error = {}
    raise BrainCoreError(
        response.status_code,
        str(error.get("code", "http_error")),
        str(error.get("message", response.reason_phrase)),
        error.get("details", {}) if isinstance(error.get("details"), dict) else {},
    )


def _idempotency(value: str | None) -> str:
    return value or str(uuid.uuid4())


class BrainCore:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8787",
        api_key: str | None = None,
        *,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=base_url.rstrip("/"), headers=headers, timeout=timeout
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "BrainCore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        request_headers = dict(kwargs.pop("headers", {}))
        if self._api_key:
            request_headers.setdefault("Authorization", f"Bearer {self._api_key}")
        if request_headers:
            kwargs["headers"] = request_headers
        response = self._client.request(method, path, **kwargs)
        _raise_for_error(response)
        return response.json() if response.content else {}

    def remember(
        self,
        content: str,
        scope: dict[str, str],
        *,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/memories",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "scope": scope, **extra},
        )

    def ingest(
        self,
        content: str,
        scope: dict[str, str],
        *,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/ingestions",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "scope": scope, **extra},
        )

    def remember_batch(
        self, items: list[dict[str, Any]], *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/memories:batch",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"items": items},
        )

    def ingest_batch(
        self, items: list[dict[str, Any]], *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/ingestions:batch",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"items": items},
        )

    def search(
        self,
        query: str | None,
        scope: dict[str, str] | None = None,
        *,
        limit: int = 10,
        offset: int = 0,
        include_inactive: bool = False,
        reference_namespace_prefix: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/search",
            json={
                "query": query,
                "scope": scope,
                "limit": limit,
                "offset": offset,
                "include_inactive": include_inactive,
                "reference_namespace_prefix": reference_namespace_prefix,
                "metadata_filter": metadata_filter,
            },
        )

    def get(self, memory_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/memories/{memory_id}")

    def resolve(
        self, namespace: list[str], key: str, scope: dict[str, str] | None = None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/memories:resolve",
            json={"namespace": namespace, "key": key, "scope": scope},
        )

    def explain(self, memory_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/memories/{memory_id}/explain")

    def revise(
        self, memory_id: str, content: str, reason: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/memories/{memory_id}/revisions",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "reason": reason},
        )

    def feedback(
        self,
        memory_id: str,
        rating: int,
        comment: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/memories/{memory_id}/feedback",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"rating": rating, "comment": comment},
        )

    def lifecycle(
        self, memory_id: str, action: str, reason: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/v1/memories/{memory_id}/lifecycle",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"action": action, "reason": reason},
        )

    def suppress(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return self.lifecycle(memory_id, "suppress", reason, **kwargs)

    def retire(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return self.lifecycle(memory_id, "retire", reason, **kwargs)

    def restore(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return self.lifecycle(memory_id, "restore", reason, **kwargs)

    def promote(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return self.lifecycle(memory_id, "promote", reason, **kwargs)

    def operation(self, operation_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/operations/{operation_id}")

    def retry_operation(
        self, operation_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/operations/{operation_id}/retry",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    def create_api_key(
        self,
        name: str,
        roles: list[str],
        scope: dict[str, str] | None = None,
        *,
        expires_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/admin/api-keys",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"name": name, "roles": roles, "scope": scope, "expires_at": expires_at},
        )

    def list_api_keys(self) -> dict[str, Any]:
        return self._request("GET", "/v1/admin/api-keys")

    def revoke_api_key(self, key_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/admin/api-keys/{key_id}/revoke",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    def rotate_api_key(self, key_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/admin/api-keys/{key_id}/rotate",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    def wait_for_operation(
        self, operation_id: str, *, timeout: float = 60.0, interval: float = 0.2
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            operation = self.operation(operation_id)
            if operation["state"] in {"succeeded", "partial", "failed", "dead_letter"}:
                return operation
            if time.monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} did not finish within {timeout}s")
            time.sleep(interval)


class AsyncBrainCore:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8787",
        api_key: str | None = None,
        *,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"), headers=headers, timeout=timeout
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncBrainCore":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        request_headers = dict(kwargs.pop("headers", {}))
        if self._api_key:
            request_headers.setdefault("Authorization", f"Bearer {self._api_key}")
        if request_headers:
            kwargs["headers"] = request_headers
        response = await self._client.request(method, path, **kwargs)
        _raise_for_error(response)
        return response.json() if response.content else {}

    async def remember(
        self,
        content: str,
        scope: dict[str, str],
        *,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/memories",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "scope": scope, **extra},
        )

    async def ingest(
        self,
        content: str,
        scope: dict[str, str],
        *,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/ingestions",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "scope": scope, **extra},
        )

    async def remember_batch(
        self, items: list[dict[str, Any]], *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/memories:batch",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"items": items},
        )

    async def ingest_batch(
        self, items: list[dict[str, Any]], *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/ingestions:batch",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"items": items},
        )

    async def search(
        self,
        query: str | None,
        scope: dict[str, str] | None = None,
        *,
        limit: int = 10,
        offset: int = 0,
        include_inactive: bool = False,
        reference_namespace_prefix: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/search",
            json={
                "query": query,
                "scope": scope,
                "limit": limit,
                "offset": offset,
                "include_inactive": include_inactive,
                "reference_namespace_prefix": reference_namespace_prefix,
                "metadata_filter": metadata_filter,
            },
        )

    async def get(self, memory_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v1/memories/{memory_id}")

    async def resolve(
        self, namespace: list[str], key: str, scope: dict[str, str] | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/memories:resolve",
            json={"namespace": namespace, "key": key, "scope": scope},
        )

    async def explain(self, memory_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v1/memories/{memory_id}/explain")

    async def revise(
        self, memory_id: str, content: str, reason: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/memories/{memory_id}/revisions",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"content": content, "reason": reason},
        )

    async def feedback(
        self,
        memory_id: str,
        rating: int,
        comment: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/memories/{memory_id}/feedback",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"rating": rating, "comment": comment},
        )

    async def lifecycle(
        self, memory_id: str, action: str, reason: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "PATCH",
            f"/v1/memories/{memory_id}/lifecycle",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"action": action, "reason": reason},
        )

    async def suppress(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return await self.lifecycle(memory_id, "suppress", reason, **kwargs)

    async def retire(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return await self.lifecycle(memory_id, "retire", reason, **kwargs)

    async def restore(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return await self.lifecycle(memory_id, "restore", reason, **kwargs)

    async def promote(self, memory_id: str, reason: str, **kwargs: Any) -> dict[str, Any]:
        return await self.lifecycle(memory_id, "promote", reason, **kwargs)

    async def operation(self, operation_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v1/operations/{operation_id}")

    async def retry_operation(
        self, operation_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/operations/{operation_id}/retry",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    async def create_api_key(
        self,
        name: str,
        roles: list[str],
        scope: dict[str, str] | None = None,
        *,
        expires_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/admin/api-keys",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
            json={"name": name, "roles": roles, "scope": scope, "expires_at": expires_at},
        )

    async def list_api_keys(self) -> dict[str, Any]:
        return await self._request("GET", "/v1/admin/api-keys")

    async def revoke_api_key(
        self, key_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/admin/api-keys/{key_id}/revoke",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    async def rotate_api_key(
        self, key_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/admin/api-keys/{key_id}/rotate",
            headers={"Idempotency-Key": _idempotency(idempotency_key)},
        )

    async def wait_for_operation(
        self, operation_id: str, *, timeout: float = 60.0, interval: float = 0.2
    ) -> dict[str, Any]:
        import asyncio

        try:
            async with asyncio.timeout(timeout):
                while True:
                    operation = await self.operation(operation_id)
                    if operation["state"] in {
                        "succeeded",
                        "partial",
                        "failed",
                        "dead_letter",
                    }:
                        return operation
                    await asyncio.sleep(interval)
        except TimeoutError as error:
            raise TimeoutError(
                f"operation {operation_id} did not finish within {timeout}s"
            ) from error

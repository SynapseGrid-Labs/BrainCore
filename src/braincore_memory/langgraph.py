"""LangGraph BaseStore adapter backed exclusively by the BrainCore HTTP API."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Literal, cast, overload

from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)

from .client import AsyncBrainCore, BrainCore, BrainCoreError

MAX_BATCH_ITEMS = 100
MAX_NAMESPACE_SCAN_ITEMS = 10_000


class BrainCoreStore(BaseStore):
    """Use BrainCore as a durable LangGraph store without bypassing its API."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8787",
        api_key: str | None = None,
        *,
        scope_field: str = "user_id",
        client: BrainCore | None = None,
        async_client: AsyncBrainCore | None = None,
    ) -> None:
        if scope_field not in {"project_id", "user_id", "agent_id", "run_id"}:
            raise ValueError("scope_field must be project_id, user_id, agent_id, or run_id")
        self.scope_field = scope_field
        self.client = client or BrainCore(base_url, api_key)
        self.async_client = async_client or AsyncBrainCore(base_url, api_key)
        self._owns_sync = client is None
        self._owns_async = async_client is None

    def close(self) -> None:
        if self._owns_sync:
            self.client.close()

    async def aclose(self) -> None:
        if self._owns_async:
            await self.async_client.close()

    def _scope(self, namespace: tuple[str, ...]) -> dict[str, str] | None:
        return {self.scope_field: namespace[0]} if namespace else None

    @staticmethod
    def _content(value: dict[str, Any]) -> str:
        for key in ("memory", "content", "text"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _remember_item(self, op: PutOp) -> dict[str, Any]:
        if op.value is None:
            raise ValueError("delete operations cannot be converted to remember items")
        return {
            "content": self._content(op.value),
            "scope": self._scope(op.namespace),
            "reference": {"namespace": list(op.namespace), "key": op.key},
            "evidence": {"source_type": "langgraph"},
            "metadata": {
                "langgraph": {
                    "namespace": list(op.namespace),
                    "key": op.key,
                    "value": op.value,
                }
            },
        }

    @staticmethod
    def _datetime(value: object) -> datetime:
        if not isinstance(value, str):
            raise RuntimeError("BrainCore returned an invalid timestamp")
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @overload
    @classmethod
    def _item(cls, memory: dict[str, Any], *, search: Literal[False] = False) -> Item: ...

    @overload
    @classmethod
    def _item(cls, memory: dict[str, Any], *, search: Literal[True]) -> SearchItem: ...

    @classmethod
    def _item(cls, memory: dict[str, Any], *, search: bool = False) -> Item | SearchItem:
        metadata = memory.get("metadata")
        langgraph = metadata.get("langgraph") if isinstance(metadata, dict) else None
        namespace = memory.get("reference_namespace")
        key = memory.get("reference_key")
        if not isinstance(langgraph, dict) or not isinstance(langgraph.get("value"), dict):
            raise RuntimeError("BrainCore memory is missing LangGraph value metadata")
        if not isinstance(namespace, list) or not all(isinstance(part, str) for part in namespace):
            raise RuntimeError("BrainCore memory is missing a valid reference namespace")
        if not isinstance(key, str):
            raise RuntimeError("BrainCore memory is missing a valid reference key")
        value = cast(dict[str, Any], langgraph["value"])
        created_at = cls._datetime(memory.get("created_at"))
        updated_at = cls._datetime(memory.get("updated_at"))
        if search:
            raw_score = memory.get("score")
            score = float(raw_score) if isinstance(raw_score, int | float) else None
            return SearchItem(tuple(namespace), key, value, created_at, updated_at, score)
        return Item(
            namespace=tuple(namespace),
            key=key,
            value=value,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _get(self, op: GetOp) -> Item | None:
        try:
            memory = self.client.resolve(list(op.namespace), op.key, self._scope(op.namespace))
        except BrainCoreError as error:
            if error.status_code == 404:
                return None
            raise
        if memory.get("state") != "active":
            return None
        return self._item(memory)

    async def _aget(self, op: GetOp) -> Item | None:
        try:
            memory = await self.async_client.resolve(
                list(op.namespace), op.key, self._scope(op.namespace)
            )
        except BrainCoreError as error:
            if error.status_code == 404:
                return None
            raise
        if memory.get("state") != "active":
            return None
        return self._item(memory)

    def _search(self, op: SearchOp) -> list[SearchItem]:
        metadata_filter = {"langgraph": {"value": op.filter or {}}}
        response = self.client.search(
            op.query,
            self._scope(op.namespace_prefix),
            limit=op.limit,
            offset=op.offset,
            reference_namespace_prefix=list(op.namespace_prefix) or None,
            metadata_filter=metadata_filter,
        )
        return [self._item(item, search=True) for item in response["results"]]

    async def _asearch(self, op: SearchOp) -> list[SearchItem]:
        metadata_filter = {"langgraph": {"value": op.filter or {}}}
        response = await self.async_client.search(
            op.query,
            self._scope(op.namespace_prefix),
            limit=op.limit,
            offset=op.offset,
            reference_namespace_prefix=list(op.namespace_prefix) or None,
            metadata_filter=metadata_filter,
        )
        return [self._item(item, search=True) for item in response["results"]]

    @staticmethod
    def _matches_namespace(namespace: tuple[str, ...], op: ListNamespacesOp) -> bool:
        for condition in op.match_conditions or ():
            path = tuple(condition.path)
            if condition.match_type == "prefix" and namespace[: len(path)] != path:
                return False
            if condition.match_type == "suffix" and namespace[-len(path) :] != path:
                return False
        return True

    def _list_namespaces(self, op: ListNamespacesOp) -> list[tuple[str, ...]]:
        prefix = next(
            (
                tuple(condition.path)
                for condition in op.match_conditions or ()
                if condition.match_type == "prefix"
            ),
            (),
        )
        namespaces: set[tuple[str, ...]] = set()
        offset = 0
        while offset <= MAX_NAMESPACE_SCAN_ITEMS:
            response = self.client.search(
                None,
                self._scope(prefix),
                limit=1 if offset == MAX_NAMESPACE_SCAN_ITEMS else 100,
                offset=offset,
                include_inactive=True,
                reference_namespace_prefix=list(prefix) or None,
                metadata_filter={"langgraph": {"value": {}}},
            )
            results = response["results"]
            if offset == MAX_NAMESPACE_SCAN_ITEMS:
                if results:
                    raise RuntimeError(
                        "BrainCore LangGraph namespace listing exceeds 10000 matching memories; "
                        "narrow the namespace prefix or authenticated key scope."
                    )
                break
            namespaces.update(
                tuple(value)
                for item in results
                if isinstance((value := item.get("reference_namespace")), list)
            )
            if len(results) < 100:
                break
            offset += 100
        selected = sorted(
            namespace for namespace in namespaces if self._matches_namespace(namespace, op)
        )
        if op.max_depth is not None:
            selected = sorted({namespace[: op.max_depth] for namespace in selected})
        return selected[op.offset : op.offset + op.limit]

    async def _alist_namespaces(self, op: ListNamespacesOp) -> list[tuple[str, ...]]:
        prefix = next(
            (
                tuple(condition.path)
                for condition in op.match_conditions or ()
                if condition.match_type == "prefix"
            ),
            (),
        )
        namespaces: set[tuple[str, ...]] = set()
        offset = 0
        while offset <= MAX_NAMESPACE_SCAN_ITEMS:
            response = await self.async_client.search(
                None,
                self._scope(prefix),
                limit=1 if offset == MAX_NAMESPACE_SCAN_ITEMS else 100,
                offset=offset,
                include_inactive=True,
                reference_namespace_prefix=list(prefix) or None,
                metadata_filter={"langgraph": {"value": {}}},
            )
            results = response["results"]
            if offset == MAX_NAMESPACE_SCAN_ITEMS:
                if results:
                    raise RuntimeError(
                        "BrainCore LangGraph namespace listing exceeds 10000 matching memories; "
                        "narrow the namespace prefix or authenticated key scope."
                    )
                break
            namespaces.update(
                tuple(value)
                for item in results
                if isinstance((value := item.get("reference_namespace")), list)
            )
            if len(results) < 100:
                break
            offset += 100
        selected = sorted(
            namespace for namespace in namespaces if self._matches_namespace(namespace, op)
        )
        if op.max_depth is not None:
            selected = sorted({namespace[: op.max_depth] for namespace in selected})
        return selected[op.offset : op.offset + op.limit]

    @staticmethod
    def _require_success(completed: dict[str, Any], action: str) -> None:
        if completed["state"] != "succeeded":
            raise RuntimeError(f"BrainCore LangGraph {action} ended as {completed['state']}")

    def _delete(self, op: PutOp) -> None:
        existing = self._get(GetOp(op.namespace, op.key))
        if existing is None:
            return
        memory = self.client.resolve(list(op.namespace), op.key, self._scope(op.namespace))
        accepted = self.client.suppress(
            str(memory["id"]), "LangGraph delete mapped to audited suppression."
        )
        completed = self.client.wait_for_operation(str(accepted["operation_id"]))
        self._require_success(completed, "delete")

    async def _adelete(self, op: PutOp) -> None:
        existing = await self._aget(GetOp(op.namespace, op.key))
        if existing is None:
            return
        memory = await self.async_client.resolve(
            list(op.namespace), op.key, self._scope(op.namespace)
        )
        accepted = await self.async_client.suppress(
            str(memory["id"]), "LangGraph delete mapped to audited suppression."
        )
        completed = await self.async_client.wait_for_operation(str(accepted["operation_id"]))
        self._require_success(completed, "delete")

    def _put_run(self, operations: list[PutOp]) -> None:
        accepted = self.client.remember_batch([self._remember_item(op) for op in operations])
        completed = self.client.wait_for_operation(str(accepted["operation_id"]))
        self._require_success(completed, "batch")
        for op in operations:
            memory = self.client.resolve(list(op.namespace), op.key, self._scope(op.namespace))
            if memory.get("state") != "active":
                restored = self.client.restore(
                    str(memory["id"]), "LangGraph put recreated a suppressed key."
                )
                completed = self.client.wait_for_operation(str(restored["operation_id"]))
                self._require_success(completed, "restore")

    async def _aput_run(self, operations: list[PutOp]) -> None:
        accepted = await self.async_client.remember_batch(
            [self._remember_item(op) for op in operations]
        )
        completed = await self.async_client.wait_for_operation(str(accepted["operation_id"]))
        self._require_success(completed, "batch")
        for op in operations:
            memory = await self.async_client.resolve(
                list(op.namespace), op.key, self._scope(op.namespace)
            )
            if memory.get("state") != "active":
                restored = await self.async_client.restore(
                    str(memory["id"]), "LangGraph put recreated a suppressed key."
                )
                completed = await self.async_client.wait_for_operation(
                    str(restored["operation_id"])
                )
                self._require_success(completed, "restore")

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        operations = list(ops)
        results: list[Result] = [None] * len(operations)
        index = 0
        while index < len(operations):
            operation = operations[index]
            if isinstance(operation, PutOp) and operation.value is not None:
                end = index + 1
                while end < min(len(operations), index + MAX_BATCH_ITEMS):
                    candidate = operations[end]
                    if not isinstance(candidate, PutOp) or candidate.value is None:
                        break
                    end += 1
                self._put_run(cast(list[PutOp], operations[index:end]))
                index = end
                continue
            if isinstance(operation, PutOp):
                self._delete(operation)
            elif isinstance(operation, GetOp):
                results[index] = self._get(operation)
            elif isinstance(operation, SearchOp):
                results[index] = self._search(operation)
            elif isinstance(operation, ListNamespacesOp):
                results[index] = self._list_namespaces(operation)
            else:
                raise TypeError(f"unsupported LangGraph operation: {type(operation).__name__}")
            index += 1
        return results

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        operations = list(ops)
        results: list[Result] = [None] * len(operations)
        index = 0
        while index < len(operations):
            operation = operations[index]
            if isinstance(operation, PutOp) and operation.value is not None:
                end = index + 1
                while end < min(len(operations), index + MAX_BATCH_ITEMS):
                    candidate = operations[end]
                    if not isinstance(candidate, PutOp) or candidate.value is None:
                        break
                    end += 1
                await self._aput_run(cast(list[PutOp], operations[index:end]))
                index = end
                continue
            if isinstance(operation, PutOp):
                await self._adelete(operation)
            elif isinstance(operation, GetOp):
                results[index] = await self._aget(operation)
            elif isinstance(operation, SearchOp):
                results[index] = await self._asearch(operation)
            elif isinstance(operation, ListNamespacesOp):
                results[index] = await self._alist_namespaces(operation)
            else:
                raise TypeError(f"unsupported LangGraph operation: {type(operation).__name__}")
            index += 1
        return results

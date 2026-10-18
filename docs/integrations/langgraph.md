# LangGraph integration

Install the Python SDK with the optional LangGraph adapter from the source
checkout (Python 3.11+). This source candidate does not include a published
2.0.0 PyPI distribution:

```bash
uv sync --frozen --extra langgraph
```

Run Python examples using `uv run python` in the checkout.

```python
from braincore_memory.langgraph import BrainCoreStore

store = BrainCoreStore(api_key="your-write-key", scope_field="user_id")

store.put(("user-42", "preferences"), "response-style", {"style": "concise"})
item = store.get(("user-42", "preferences"), "response-style")
matches = store.search(("user-42", "preferences"), query="concise")
```

With no injected clients, the store creates synchronous and asynchronous SDK
clients with the same `base_url` and `api_key`. Call `store.close()` and
`await store.aclose()` when finished to close both owned clients.

For custom HTTP transports or SDK clients, inject the modes you use explicitly:

```python
from braincore_memory import AsyncBrainCore, BrainCore

client = BrainCore(base_url="http://127.0.0.1:8787", api_key="your-write-key")
async_client = AsyncBrainCore(base_url="http://127.0.0.1:8787", api_key="your-write-key")
store = BrainCoreStore(client=client, async_client=async_client)
```

A single `client=` injection supports synchronous operations; a single
`async_client=` injection supports asynchronous operations. Calling the other
mode raises an actionable `ValueError` before making a request. Supply both
clients for mixed use. Injected clients retain their own URL, authentication,
timeouts, cookies and transports; the store does not infer or clone a missing
counterpart from them. Constructor `base_url` and `api_key` configure only
store-created clients. Injected clients remain caller-owned, so close them
through their SDK context managers or close methods.

Namespaces and keys are stable references layered over normal BrainCore scope.
The first namespace component supplies the selected `scope_field`; the complete
namespace remains the stable LangGraph reference. In the example, every
operation is therefore scoped to `user_id="user-42"`.
`put` creates direct evidence-backed memory; replacing a key creates a revision;
`get` and `search` use the supported HTTP API. LangGraph delete maps to audited
suppression, and a later put restores the reference rather than erasing history.

## Batches and operation outcomes

Consecutive non-delete puts use native batches of at most 100 items. The adapter
waits for each batch to succeed before submitting the next chunk or performing
a following get, search, or delete. Puts, suppression, and restoration require
successful terminal operation outcomes; a partial or failed outcome raises
`RuntimeError` and stops the remaining adapter operations. Already accepted
writes remain stored. A LangGraph batch spanning several API operations is not
one database transaction and does not roll back earlier successful chunks.
Synchronous and asynchronous methods follow the same ordering and bounds.

## Search filter compatibility

This adapter supports a subset of LangGraph `BaseStore` filters: string values
and nonempty nested objects whose leaves are strings. Nested objects match the
specified fields and can contain additional stored fields. Filter keys must be
strings and cannot begin with `$`. For example,
`filter={"style": "concise"}` and
`filter={"preferences": {"style": "concise"}}` are supported. Omitting the filter
or passing an empty top-level `{}` applies no value restriction.

Comparison operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`), arrays, `None`,
booleans, numbers and empty nested objects are unsupported. The adapter raises
an actionable `ValueError` for these forms before any HTTP request in that
`batch` or `abatch`, including puts that precede the invalid search. This limit
applies to filters; stored LangGraph values can still contain those JSON values.
Filtering and pagination for the supported subset both happen in the API.

## Namespace listing limit

Namespace listing examines at most 10,000 matching memory records, including
inactive records, then checks for another record with an API request at
`offset=10000`, `limit=1`. An empty final probe permits a complete list at that
limit; a nonempty probe raises `RuntimeError` instead of returning a silently
truncated list. The limit counts source memories, not distinct namespaces.

Namespace prefix and suffix patterns support `*` as one complete component,
matching any component at that position. Empty prefix/suffix patterns match all
namespaces. The API receives only the literal prefix before the first wildcard,
together with its first component's scope field and the LangGraph metadata
filter. When a prefix starts with `*`, the API scans the credential's authorized
scope; the wildcard is never sent as a scope identifier. All prefix/suffix
conditions are then matched component by component within the same scan bound.
A narrower prefix or authenticated key scope can therefore reduce the scan. A root listing
uses the credential's authorized scope. Suffix matching, `max_depth`, and the
listing's output `offset` and `limit` are applied after the scan and do not
reduce the number of source memories examined. There is no unbounded namespace
enumeration contract for larger matching sets.

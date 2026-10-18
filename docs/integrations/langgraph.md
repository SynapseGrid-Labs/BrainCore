# LangGraph integration

Install the Python SDK with the optional LangGraph adapter from the source
checkout (Python 3.11+). This source candidate does not include a published
2.0.0 PyPI distribution:

```bash
uv sync --frozen --extra langgraph
```

Run Python examples using `uv run python` in the checkout.

```python
from braincore_memory import BrainCore
from braincore_memory.langgraph import BrainCoreStore

client = BrainCore(api_key="your-write-key")
store = BrainCoreStore(client=client, scope_field="user_id")

store.put(("user-42", "preferences"), "response-style", {"style": "concise"})
item = store.get(("user-42", "preferences"), "response-style")
matches = store.search(("user-42", "preferences"), query="concise")
```

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

## Namespace listing limit

Namespace listing examines at most 10,000 matching memory records, including
inactive records, then checks for another record with an API request at
`offset=10000`, `limit=1`. An empty final probe permits a complete list at that
limit; a nonempty probe raises `RuntimeError` instead of returning a silently
truncated list. The limit counts source memories, not distinct namespaces.

The namespace prefix is passed to the API, together with the selected scope
field from its first component and the LangGraph metadata filter. A narrower
prefix or authenticated key scope can therefore reduce the scan. A root listing
uses the credential's authorized scope. Suffix matching, `max_depth`, and the
listing's output `offset` and `limit` are applied after the scan and do not
reduce the number of source memories examined. There is no unbounded namespace
enumeration contract for larger matching sets.

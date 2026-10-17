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
consecutive puts use the native batch API. `get` and `search` use the supported
HTTP API. LangGraph delete maps to audited suppression, and a later put restores
the reference rather than erasing history.

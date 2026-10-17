# Examples

Build and start the source checkout using the README, then export the API key
shown by `bun dist/cli.js up`:

```bash
export BRAINCORE_URL=http://127.0.0.1:8787
export BRAINCORE_API_KEY='your-key'
```

## curl

```bash
memory=$(curl --fail-with-body -sS "$BRAINCORE_URL/v1/memories" \
  -H "Authorization: Bearer $BRAINCORE_API_KEY" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d '{"content":"Alice prefers concise answers","scope":{"project_id":"demo","user_id":"alice"}}')

curl --fail-with-body -sS "$BRAINCORE_URL/v1/search" \
  -H "Authorization: Bearer $BRAINCORE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"query":"answer style","scope":{"project_id":"demo","user_id":"alice"}}'
```

## TypeScript

Build the SDK in the source checkout using `bun run build`. For a separate
consumer project, use `npm pack` in the checkout and install the resulting local
`.tgz` file with `npm install /path/to/synapsegrid-labs-braincore-2.0.0.tgz`.
This executes the package preparation lifecycle, which requires uv and Bun.
Version 2.0.0 is not assumed to exist in the npm registry.

```typescript
import { BrainCoreClient } from "@synapsegrid-labs/braincore";

const client = new BrainCoreClient({
  baseUrl: process.env.BRAINCORE_URL,
  apiKey: process.env.BRAINCORE_API_KEY,
});
const accepted = await client.remember(
  "Alice prefers concise answers",
  { project_id: "demo", user_id: "alice" },
  { evidence: { source_type: "example" } },
);
const operation = await client.waitForOperation(String(accepted.operation_id));
const memoryId = String(operation.items[0].memory_id);
console.log(await client.explain(memoryId));
```

## Python

In the source checkout, run `uv sync --frozen --extra langgraph` and execute
this example with `uv run python`. Alternatively build a wheel using `uv build`
and install the local wheel in an isolated Python environment; no 2.0.0 PyPI
artifact is required.

```python
import os
from braincore_memory import BrainCore

with BrainCore(
    base_url=os.environ["BRAINCORE_URL"],
    api_key=os.environ["BRAINCORE_API_KEY"],
) as client:
    accepted = client.remember(
        "Alice prefers concise answers",
        {"project_id": "demo", "user_id": "alice"},
        evidence={"source_type": "example"},
    )
    operation = client.wait_for_operation(accepted["operation_id"])
    memory_id = operation["items"][0]["memory_id"]
    print(client.explain(memory_id))
```

All three examples use the same HTTP contract and return evidence with the
memory. Revisions use `revise`; removals use audited `suppress`.

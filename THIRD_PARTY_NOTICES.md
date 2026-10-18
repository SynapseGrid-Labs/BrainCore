# Third-party components and data

BrainCore source is licensed under the [MIT License](LICENSE). Its dependencies,
container base images and benchmark inputs retain their own licenses; the
BrainCore license does not replace those terms.

## Dependency and image inventory

The exact source dependency inventories are `uv.lock` and `bun.lock`. Python
application dependencies include FastAPI, HTTPX, Psycopg, Pydantic,
pydantic-settings and Uvicorn. The optional LangGraph integration adds LangGraph,
LangGraph Checkpoint and LangGraph SDK. TypeScript runtime dependencies include
the Model Context Protocol SDK, Commander, Postgres.js and Zod. Dashboard builds
include React, React DOM and Vite and their transitive dependencies.

Image inputs are pinned in `Dockerfile` and `compose.yaml`: Python, Bun and
PostgreSQL/pgvector, plus operating-system packages. Retain the upstream license
files and notices supplied in those distributions. Review the licenses of the
actual resolved packages and image SBOM when building a distribution; a source
lockfile does not establish the contents or licensing of an independently
resolved image. Build tools and optional dependencies are separate inventory
surfaces even when absent from the runtime package.

## Benchmark inputs

The adapters reference these upstream projects and their manifests:

| Suite | Upstream | Declared license | Manifest |
| --- | --- | --- | --- |
| LoCoMo | [snap-research/locomo](https://github.com/snap-research/locomo) | CC BY-NC 4.0 | [locomo.json](benchmarks/manifests/locomo.json) |
| LongMemEval | [xiaowu0162/LongMemEval](https://github.com/xiaowu0162/LongMemEval) | MIT | [longmemeval.json](benchmarks/manifests/longmemeval.json) |
| BEAM | [mohammadtavakoli78/BEAM](https://github.com/mohammadtavakoli78/BEAM) | MIT | [beam.json](benchmarks/manifests/beam.json) |

The manifests record source revisions and, where available, dataset identity.
Datasets and upstream evaluation code are not redistributed here. Obtain them
separately from their official sources and verify the terms for the exact input
before use or redistribution. In particular, LoCoMo's non-commercial terms are
separate from BrainCore's MIT license. The small checked-in smoke fixture is
synthetic and does not establish benchmark performance.

## Assets and attribution

This source candidate contains no bundled screenshot, font, photograph, or
third-party dataset asset. Generated dashboard bundles include dependency code
and must retain applicable upstream notices. Public contributor attribution and
the copyright holder in `LICENSE` are intentional.

# Benchmark compatibility

BrainCore ships independent adapters for LoCoMo, LongMemEval, and BEAM. It
does not redistribute their datasets or evaluation code. Every run goes
through the supported `/v1` HTTP API and writes a dataset hash, configuration,
provider/model identity, platform, per-question stream health, latency, raw
retrievals, token usage (when returned), and artifact hashes.

1. Obtain a dataset from its official source and review the pinned manifest in
   `benchmarks/manifests/`. LoCoMo is CC BY-NC 4.0 and is not bundled.
2. Normalize it:

   ```bash
   uv run python benchmarks/adapter.py --suite longmemeval \
     --input /path/to/longmemeval_s_cleaned.json \
     --output artifacts/longmemeval-normalized.json
   ```

3. Run retrieval evaluation against a clean BrainCore scope:

   ```bash
   uv run python benchmarks/run.py --suite longmemeval \
     --input artifacts/longmemeval-normalized.json \
     --output-dir artifacts/longmemeval-run \
     --api-key "$BRAINCORE_API_KEY"
   ```

Add `--answer-endpoint`, `--answer-model`, and optionally `--answer-api-key` to
produce hypotheses for the benchmark's official scorer. BrainCore does not
present retrieval recall as answer correctness and does not publish a score
unless all declared cases completed. BEAM questions without ground-truth
evidence locations export contexts and hypotheses but report retrieval recall
as `null`.

For disposable PostgreSQL load, exact/HNSW comparison, and HTTP latency
experiments, see [Performance experiments](PERFORMANCE.md). These synthetic
measurements are separate from the quality adapters above.

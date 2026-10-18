-- Include applicable memories awaiting or missing a usable vector in coverage
-- checks without scanning the healthy corpus. Non-applicable empty source text
-- remains exempt. Apply in the migration window: this index can block writers.
CREATE INDEX memories_embedding_missing_idx
  ON braincore.memories (tenant_id)
  WHERE embedding_state IN ('pending','failed')
     OR (embedding_state = 'ready' AND embedding IS NULL);

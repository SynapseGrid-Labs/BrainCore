-- Explicit identities prevent equal-width embeddings from different models being mixed.
-- Legacy vectors are deliberately not relabeled: use the bounded, audited admin
-- refresh operation before enabling the new vector read path on an existing corpus.
ALTER TABLE braincore.memories
  ADD COLUMN embedding_model text,
  ADD COLUMN embedding_preprocessing_revision text;

-- Bounded identity-range probes disclose incompatible vector coverage without
-- counting/scanning every healthy memory on each request. Build in the planned
-- migration window; this transactional index build can block writers.
CREATE INDEX memories_embedding_identity_idx
  ON braincore.memories (tenant_id, embedding_model, embedding_preprocessing_revision)
  WHERE embedding IS NOT NULL AND embedding_state = 'ready';

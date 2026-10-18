-- Keep administrative repair pages bounded even for mostly healthy tenants.
-- Transactional builds require a planned maintenance window and disk/WAL headroom.
CREATE INDEX memories_tenant_scan_idx ON braincore.memories (tenant_id, id);
CREATE INDEX jobs_active_embedding_memory_idx
  ON braincore.jobs (tenant_id, (payload->>'memory_id'))
  WHERE kind = 'embed_memory' AND state IN ('queued','running');

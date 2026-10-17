-- Compatibility path filters use canonical project_id, including native writes.
-- The legacy_scope_path index serves a different retained source field.
CREATE INDEX memories_compat_project_scope_idx ON braincore.memories
  (tenant_id, object_type, project_id text_pattern_ops, state);

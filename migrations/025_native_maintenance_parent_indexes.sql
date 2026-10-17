-- Native nightly support checks must start with one exact memory parent.
-- Existing (control_id, field) keys remain authoritative for each control.
CREATE INDEX legacy_client_control_refs_memory_parent_idx
ON braincore.legacy_client_control_refs (memory_id, control_id)
WHERE field = 'memory_id' AND memory_id IS NOT NULL;

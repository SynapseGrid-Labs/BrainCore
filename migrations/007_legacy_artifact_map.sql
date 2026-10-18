CREATE TABLE braincore.legacy_artifact_map (
  source_id uuid NOT NULL,
  source_table text NOT NULL DEFAULT 'artifact' CHECK (source_table='artifact'),
  record_key jsonb NOT NULL,
  tenant_id uuid NOT NULL,
  native_artifact_id uuid NOT NULL,
  imported_payload_hash bytea NOT NULL,
  PRIMARY KEY (source_id,record_key),
  UNIQUE (native_artifact_id),
  FOREIGN KEY (native_artifact_id,tenant_id) REFERENCES braincore.artifacts(id,tenant_id),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash)
);
CREATE UNIQUE INDEX legacy_object_map_native_idx ON braincore.legacy_object_map(native_memory_id);

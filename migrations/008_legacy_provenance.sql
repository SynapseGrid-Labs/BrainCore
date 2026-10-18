-- Retain source evidence/support/revision identities independently of the
-- artifact-level evidence summary. The complete original rows stay in the
-- immutable ledger; native composite foreign keys enforce endpoint ownership.
CREATE TABLE braincore.legacy_fact_evidence (
  source_id uuid NOT NULL,
  source_table text NOT NULL DEFAULT 'fact_evidence' CHECK (source_table='fact_evidence'),
  record_key jsonb NOT NULL,
  native_id uuid NOT NULL UNIQUE,
  tenant_id uuid REFERENCES braincore.tenants(id),
  native_fact_id uuid,
  native_segment_id uuid,
  imported_payload_hash bytea NOT NULL,
  deleted boolean NOT NULL DEFAULT false,
  PRIMARY KEY (source_id,record_key),
  FOREIGN KEY (native_fact_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (native_segment_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash),
  CHECK ((native_fact_id IS NULL AND native_segment_id IS NULL) OR tenant_id IS NOT NULL)
);
CREATE INDEX legacy_fact_evidence_parent_idx ON braincore.legacy_fact_evidence
  (tenant_id,native_fact_id) WHERE NOT deleted;
CREATE TABLE braincore.legacy_memory_support (
  source_id uuid NOT NULL,
  source_table text NOT NULL DEFAULT 'memory_support' CHECK (source_table='memory_support'),
  record_key jsonb NOT NULL,
  native_id uuid NOT NULL UNIQUE,
  tenant_id uuid REFERENCES braincore.tenants(id),
  native_memory_id uuid,
  native_fact_id uuid,
  native_episode_id uuid,
  imported_payload_hash bytea NOT NULL,
  deleted boolean NOT NULL DEFAULT false,
  PRIMARY KEY (source_id,record_key),
  FOREIGN KEY (native_memory_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (native_fact_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (native_episode_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash),
  CHECK ((native_memory_id IS NULL AND native_fact_id IS NULL AND native_episode_id IS NULL)
         OR tenant_id IS NOT NULL)
);
CREATE INDEX legacy_memory_support_parent_idx ON braincore.legacy_memory_support
  (tenant_id,native_memory_id) WHERE NOT deleted;
CREATE TABLE braincore.legacy_memory_revisions (
  source_id uuid NOT NULL,
  source_table text NOT NULL DEFAULT 'memory_revision' CHECK (source_table='memory_revision'),
  record_key jsonb NOT NULL,
  native_id uuid NOT NULL UNIQUE,
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  native_memory_id uuid NOT NULL,
  imported_payload_hash bytea NOT NULL,
  deleted boolean NOT NULL DEFAULT false,
  PRIMARY KEY (source_id,record_key),
  FOREIGN KEY (native_memory_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash)
);
CREATE INDEX legacy_memory_revisions_parent_idx ON braincore.legacy_memory_revisions
  (tenant_id,native_memory_id);

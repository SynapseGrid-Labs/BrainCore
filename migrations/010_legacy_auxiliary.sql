-- Canonical auxiliary payloads retain original SQL NULL markers and precise JSON.
-- Referenced native endpoints are constrained independently from source ownership.
CREATE TABLE braincore.legacy_auxiliary_objects (
  source_id uuid NOT NULL REFERENCES braincore.legacy_sources(id),
  source_table text NOT NULL,
  record_key jsonb NOT NULL CHECK (jsonb_typeof(record_key)='object'),
  native_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES braincore.tenants(id),
  scope jsonb,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
  imported_payload_hash bytea,
  deleted boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id,source_table,record_key),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash),
  CHECK ((tenant_id IS NULL AND scope IS NULL) OR
    (tenant_id IS NOT NULL AND jsonb_typeof(scope)='object'
     AND scope ?| ARRAY['project_id','user_id','agent_id','run_id']
     AND scope - 'project_id' - 'user_id' - 'agent_id' - 'run_id'='{}'::jsonb))
);
CREATE INDEX legacy_auxiliary_owner_idx ON braincore.legacy_auxiliary_objects
  (tenant_id,source_table) WHERE NOT deleted;
CREATE TABLE braincore.legacy_auxiliary_refs (
  source_id uuid NOT NULL,
  source_table text NOT NULL,
  record_key jsonb NOT NULL,
  source_column text NOT NULL,
  target_source_table text NOT NULL,
  target_tenant_id uuid REFERENCES braincore.tenants(id),
  native_memory_id uuid,
  native_artifact_id uuid,
  native_auxiliary_id uuid,
  PRIMARY KEY (source_id,source_table,record_key,source_column),
  FOREIGN KEY (source_id,source_table,record_key)
    REFERENCES braincore.legacy_auxiliary_objects(source_id,source_table,record_key),
  FOREIGN KEY (native_memory_id,target_tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (native_artifact_id,target_tenant_id) REFERENCES braincore.artifacts(id,tenant_id),
  FOREIGN KEY (native_auxiliary_id) REFERENCES braincore.legacy_auxiliary_objects(native_id),
  CHECK (num_nonnulls(native_memory_id,native_artifact_id,native_auxiliary_id)=1),
  CHECK ((native_memory_id IS NULL AND native_artifact_id IS NULL) OR target_tenant_id IS NOT NULL)
);
CREATE FUNCTION braincore.keep_auxiliary_identity() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF (NEW.source_id,NEW.source_table,NEW.record_key,NEW.native_id,NEW.tenant_id)
     IS DISTINCT FROM (OLD.source_id,OLD.source_table,OLD.record_key,OLD.native_id,OLD.tenant_id) THEN
    RAISE EXCEPTION 'auxiliary identity and ownership are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER legacy_auxiliary_identity BEFORE UPDATE ON braincore.legacy_auxiliary_objects
  FOR EACH ROW EXECUTE FUNCTION braincore.keep_auxiliary_identity();
-- Current graph attributes are separate from original immutable ledger versions.
ALTER TABLE braincore.legacy_fact_evidence ADD COLUMN attributes jsonb;
ALTER TABLE braincore.legacy_memory_support ADD COLUMN attributes jsonb;
ALTER TABLE braincore.legacy_memory_revisions ADD COLUMN attributes jsonb;
CREATE INDEX legacy_fact_evidence_attributes_pending ON braincore.legacy_fact_evidence
  (source_id,record_key) WHERE attributes IS NULL;
CREATE INDEX legacy_memory_support_attributes_pending ON braincore.legacy_memory_support
  (source_id,record_key) WHERE attributes IS NULL;
CREATE INDEX legacy_memory_revisions_attributes_pending ON braincore.legacy_memory_revisions
  (source_id,record_key) WHERE attributes IS NULL;
CREATE FUNCTION braincore.check_auxiliary_endpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE actual_owner uuid; actual_type text;
BEGIN
  IF NEW.native_memory_id IS NOT NULL THEN
    SELECT tenant_id,object_type INTO actual_owner,actual_type FROM braincore.memories
      WHERE id=NEW.native_memory_id;
  ELSIF NEW.native_artifact_id IS NOT NULL THEN
    SELECT tenant_id,'artifact' INTO actual_owner,actual_type FROM braincore.artifacts
      WHERE id=NEW.native_artifact_id;
  ELSE
    SELECT tenant_id,source_table INTO actual_owner,actual_type
      FROM braincore.legacy_auxiliary_objects WHERE native_id=NEW.native_auxiliary_id;
  END IF;
  IF actual_type IS DISTINCT FROM NEW.target_source_table
    OR actual_owner IS DISTINCT FROM NEW.target_tenant_id THEN
    RAISE EXCEPTION 'auxiliary endpoint type or owner does not match';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER legacy_auxiliary_endpoint BEFORE INSERT OR UPDATE ON braincore.legacy_auxiliary_refs
  FOR EACH ROW EXECUTE FUNCTION braincore.check_auxiliary_endpoint();

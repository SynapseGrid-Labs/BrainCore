-- Explicit object types and external IDs prevent a source segment or fact
-- from being presented as an ordinary synthesized memory.
ALTER TABLE braincore.memories
  ADD COLUMN object_type text NOT NULL DEFAULT 'memory',
  ADD COLUMN legacy_source_id uuid REFERENCES braincore.legacy_sources(id),
  ADD COLUMN legacy_record_key jsonb,
  ADD COLUMN legacy_external_id uuid,
  ADD COLUMN legacy_scope_path text,
  ADD COLUMN legacy_fts_eligible boolean NOT NULL DEFAULT true,
  ADD CHECK (object_type IN ('entity','fact','memory','segment','episode')),
  ADD CHECK ((legacy_source_id IS NULL AND legacy_record_key IS NULL AND legacy_external_id IS NULL)
             OR (legacy_source_id IS NOT NULL AND legacy_record_key IS NOT NULL
                 AND legacy_external_id IS NOT NULL));
-- Unknown source validity is preserved as unknown, rather than invented dates.
ALTER TABLE braincore.memories ALTER COLUMN valid_from DROP NOT NULL;
CREATE UNIQUE INDEX memories_legacy_key_idx ON braincore.memories
  (legacy_source_id,object_type,legacy_record_key) WHERE legacy_source_id IS NOT NULL;
CREATE UNIQUE INDEX memories_legacy_external_idx ON braincore.memories
  (legacy_source_id,object_type,legacy_external_id) WHERE legacy_source_id IS NOT NULL;
CREATE INDEX memories_legacy_scope_idx ON braincore.memories
  (tenant_id,object_type,legacy_scope_path text_pattern_ops,state);
CREATE TABLE braincore.legacy_tenant_map (
  source_id uuid NOT NULL REFERENCES braincore.legacy_sources(id),
  original_tenant text NOT NULL CHECK (length(btrim(original_tenant))>0),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  PRIMARY KEY (source_id,original_tenant),
  UNIQUE (source_id,tenant_id)
);
CREATE TABLE braincore.legacy_object_map (
  source_id uuid NOT NULL,
  source_table text NOT NULL,
  record_key jsonb NOT NULL,
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  native_memory_id uuid NOT NULL,
  imported_payload_hash bytea NOT NULL,
  mapped_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id,source_table,record_key),
  FOREIGN KEY (native_memory_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
  FOREIGN KEY (source_id,source_table,record_key,imported_payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id,source_table,record_key,payload_hash)
);
CREATE FUNCTION braincore.legacy_object_id(source_id uuid,source_table text,record_key jsonb)
RETURNS uuid LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
  WITH hashed AS (
    SELECT public.digest(decode(replace(source_id::text,'-',''),'hex') ||
                         convert_to(source_table || ':' || record_key::text,'UTF8'),'sha1') AS bytes
  ), versioned AS (
    SELECT set_byte(set_byte(bytes,6,(get_byte(bytes,6)&15)|80),8,(get_byte(bytes,8)&63)|128) AS bytes
    FROM hashed
  ) SELECT encode(substring(bytes FROM 1 FOR 16),'hex')::uuid FROM versioned;
$$;

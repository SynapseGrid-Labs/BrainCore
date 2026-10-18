-- Canonical, lossless source history. These privileged tables have no public read route.
CREATE TABLE braincore.legacy_sources (
  id uuid PRIMARY KEY,
  source_schema text NOT NULL,
  key_mapping jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE braincore.legacy_record_versions (
  source_id uuid NOT NULL REFERENCES braincore.legacy_sources(id),
  source_table text NOT NULL,
  record_key jsonb NOT NULL,
  payload_hash bytea NOT NULL,
  payload jsonb NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, source_table, record_key, payload_hash),
  CHECK (octet_length(payload_hash)=32),
  CHECK (payload_hash=public.digest(payload::text,'sha256'))
);
CREATE TABLE braincore.legacy_record_heads (
  source_id uuid NOT NULL,
  source_table text NOT NULL,
  record_key jsonb NOT NULL,
  payload_hash bytea NOT NULL,
  deleted boolean NOT NULL DEFAULT false,
  last_change_id bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (source_id, source_table, record_key),
  FOREIGN KEY (source_id, source_table, record_key, payload_hash)
    REFERENCES braincore.legacy_record_versions(source_id, source_table, record_key, payload_hash)
);
CREATE TABLE braincore.legacy_copy_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id uuid NOT NULL REFERENCES braincore.legacy_sources(id),
  snapshot_id text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  copied_rows bigint NOT NULL DEFAULT 0
);
CREATE TABLE braincore.legacy_copy_batches (
  run_id uuid NOT NULL REFERENCES braincore.legacy_copy_runs(id),
  source_table text NOT NULL,
  batch_index integer NOT NULL,
  row_count integer NOT NULL,
  batch_hash bytea NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, source_table, batch_index)
);
CREATE TABLE braincore.legacy_applied_changes (
  source_id uuid NOT NULL REFERENCES braincore.legacy_sources(id),
  change_id bigint NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, change_id)
);
CREATE FUNCTION braincore.keep_legacy_version() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'original source history is immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER legacy_version_immutable BEFORE UPDATE OR DELETE ON braincore.legacy_record_versions
  FOR EACH ROW EXECUTE FUNCTION braincore.keep_legacy_version();

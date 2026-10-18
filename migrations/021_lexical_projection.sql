-- A transactional, derived lexical read surface. Canonical memories and audit
-- records remain authoritative. Existing rows are backfilled separately in
-- bounded transactions; never expose a partly populated projection as complete.
CREATE TABLE braincore.memories_lexical (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  project_id text,
  user_id text,
  agent_id text,
  run_id text,
  object_type text NOT NULL,
  legacy_scope_path text,
  reference_namespace text[],
  metadata jsonb NOT NULL,
  state text NOT NULL,
  valid_from timestamptz,
  valid_until timestamptz,
  legacy_fts_eligible boolean NOT NULL,
  updated_at timestamptz NOT NULL,
  search_vector tsvector NOT NULL,
  FOREIGN KEY (id,tenant_id) REFERENCES braincore.memories(id,tenant_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX memories_lexical_fts_idx ON braincore.memories_lexical USING gin(search_vector);
CREATE INDEX memories_lexical_tenant_scope_idx ON braincore.memories_lexical
  (tenant_id,project_id,user_id,agent_id,run_id,state);
CREATE INDEX memories_lexical_scope_idx ON braincore.memories_lexical
  (tenant_id,object_type,legacy_scope_path text_pattern_ops,state);
CREATE INDEX memories_lexical_metadata_idx ON braincore.memories_lexical USING gin(metadata);

CREATE TABLE braincore.lexical_projection_state (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  ready boolean NOT NULL DEFAULT false,
  backfill_after_id uuid,
  rows_processed bigint NOT NULL DEFAULT 0 CHECK(rows_processed >= 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO braincore.lexical_projection_state(singleton) VALUES(true);

-- AFTER triggers see the STORED generated search_vector. A fixed search_path
-- and qualified objects allow restricted canonical writers to maintain the
-- projection without gaining direct write privileges on the derived tables.
CREATE FUNCTION braincore.sync_memory_lexical_projection() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF TG_RELID <> 'braincore.memories'::regclass OR TG_WHEN <> 'AFTER'
     OR TG_OP NOT IN ('INSERT','UPDATE','DELETE') THEN
    RAISE EXCEPTION 'lexical projection trigger requires canonical memories' USING ERRCODE='42501';
  END IF;
  IF TG_OP='DELETE' THEN
    DELETE FROM braincore.memories_lexical WHERE id=OLD.id AND tenant_id=OLD.tenant_id;
    RETURN OLD;
  END IF;
  IF TG_OP='UPDATE' THEN
    IF ROW(NEW.id,NEW.tenant_id,NEW.project_id,NEW.user_id,NEW.agent_id,NEW.run_id,
           NEW.object_type,NEW.legacy_scope_path,NEW.reference_namespace,NEW.metadata,
           NEW.state,NEW.valid_from,NEW.valid_until,NEW.legacy_fts_eligible,
           NEW.updated_at,NEW.search_vector)
       IS NOT DISTINCT FROM
       ROW(OLD.id,OLD.tenant_id,OLD.project_id,OLD.user_id,OLD.agent_id,OLD.run_id,
           OLD.object_type,OLD.legacy_scope_path,OLD.reference_namespace,OLD.metadata,
           OLD.state,OLD.valid_from,OLD.valid_until,OLD.legacy_fts_eligible,
           OLD.updated_at,OLD.search_vector) THEN
      RETURN NEW;
    END IF;
  END IF;
  INSERT INTO braincore.memories_lexical
    (id,tenant_id,project_id,user_id,agent_id,run_id,object_type,legacy_scope_path,
     reference_namespace,metadata,state,valid_from,valid_until,legacy_fts_eligible,
     updated_at,search_vector)
  VALUES
    (NEW.id,NEW.tenant_id,NEW.project_id,NEW.user_id,NEW.agent_id,NEW.run_id,
     NEW.object_type,NEW.legacy_scope_path,NEW.reference_namespace,NEW.metadata,
     NEW.state,NEW.valid_from,NEW.valid_until,NEW.legacy_fts_eligible,
     NEW.updated_at,NEW.search_vector)
  ON CONFLICT(id) DO UPDATE SET
    tenant_id=EXCLUDED.tenant_id,project_id=EXCLUDED.project_id,user_id=EXCLUDED.user_id,
    agent_id=EXCLUDED.agent_id,run_id=EXCLUDED.run_id,object_type=EXCLUDED.object_type,
    legacy_scope_path=EXCLUDED.legacy_scope_path,
    reference_namespace=EXCLUDED.reference_namespace,metadata=EXCLUDED.metadata,
    state=EXCLUDED.state,valid_from=EXCLUDED.valid_from,valid_until=EXCLUDED.valid_until,
    legacy_fts_eligible=EXCLUDED.legacy_fts_eligible,updated_at=EXCLUDED.updated_at,
    search_vector=EXCLUDED.search_vector;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_memory_lexical_projection() FROM PUBLIC;
CREATE TRIGGER memories_lexical_sync AFTER INSERT OR UPDATE OR DELETE ON braincore.memories
  FOR EACH ROW EXECUTE FUNCTION braincore.sync_memory_lexical_projection();

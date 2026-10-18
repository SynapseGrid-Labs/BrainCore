-- Preserve the exact compatibility-view lexical representation without ranking
-- wide canonical rows. NULL scope marks an existing row awaiting this backfill.
ALTER TABLE braincore.memories_lexical
 ADD COLUMN scope jsonb,
 ADD COLUMN compat_fts tsvector,
 ADD COLUMN compat_ready boolean NOT NULL DEFAULT false;
CREATE INDEX memories_lexical_compat_fts_idx ON braincore.memories_lexical USING gin(compat_fts);
CREATE INDEX memories_lexical_compat_scope_idx ON braincore.memories_lexical USING gin(scope);

CREATE TABLE braincore.legacy_lexical_projection_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 ready boolean NOT NULL DEFAULT false,
 backfill_after_id uuid,
 rows_processed bigint NOT NULL DEFAULT 0 CHECK(rows_processed>=0),
 updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO braincore.legacy_lexical_projection_state(singleton) VALUES(true);

-- Called by guarded triggers and privileged backfill only. Lock canonical rows
-- before reading extension attributes so a delayed extension writer refreshes
-- after a backfill transaction, rather than losing its accepted correction.
CREATE FUNCTION braincore.refresh_legacy_lexical_projection(memory_ids uuid[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF memory_ids IS NULL OR cardinality(memory_ids)>10000 THEN
  RAISE EXCEPTION 'legacy lexical refresh requires at most 10000 IDs' USING ERRCODE='22023';
 END IF;
 PERFORM m.id FROM braincore.memories m WHERE m.id=ANY(memory_ids)
  ORDER BY m.id FOR NO KEY UPDATE;
 INSERT INTO braincore.memories_lexical
  (id,tenant_id,project_id,user_id,agent_id,run_id,object_type,legacy_scope_path,
   reference_namespace,metadata,state,valid_from,valid_until,legacy_fts_eligible,
   updated_at,search_vector,scope,compat_fts,compat_ready)
 SELECT m.id,m.tenant_id,m.project_id,m.user_id,m.agent_id,m.run_id,m.object_type,
  m.legacy_scope_path,m.reference_namespace,m.metadata,m.state,m.valid_from,
  m.valid_until,m.legacy_fts_eligible,m.updated_at,m.search_vector,m.scope,
  CASE WHEN NOT m.legacy_fts_eligible THEN NULL
       WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL
        THEN c.search_vector ELSE m.search_vector END,
  (m.legacy_source_id IS NULL OR m.legacy_attributes_ready OR x.memory_id IS NOT NULL)
 FROM braincore.memories m
 LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
 CROSS JOIN LATERAL (SELECT
  CASE WHEN x.current_override THEN x.content_hash
       WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
  CASE WHEN x.current_override THEN x.search_vector
       WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector) c
 WHERE m.id=ANY(memory_ids)
 ON CONFLICT(id) DO UPDATE SET
  tenant_id=EXCLUDED.tenant_id,project_id=EXCLUDED.project_id,user_id=EXCLUDED.user_id,
  agent_id=EXCLUDED.agent_id,run_id=EXCLUDED.run_id,object_type=EXCLUDED.object_type,
  legacy_scope_path=EXCLUDED.legacy_scope_path,reference_namespace=EXCLUDED.reference_namespace,
  metadata=EXCLUDED.metadata,state=EXCLUDED.state,valid_from=EXCLUDED.valid_from,
  valid_until=EXCLUDED.valid_until,legacy_fts_eligible=EXCLUDED.legacy_fts_eligible,
  updated_at=EXCLUDED.updated_at,search_vector=EXCLUDED.search_vector,
  scope=EXCLUDED.scope,compat_fts=EXCLUDED.compat_fts,compat_ready=EXCLUDED.compat_ready;
END; $$;
REVOKE ALL ON FUNCTION braincore.refresh_legacy_lexical_projection(uuid[]) FROM PUBLIC;

-- Retain migration 021's native projection fields, lifecycle, identity cascades
-- and embedding-only no-op policy; add fields affecting compatibility FTS.
CREATE OR REPLACE FUNCTION braincore.sync_memory_lexical_projection() RETURNS trigger
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
         NEW.updated_at,NEW.search_vector,NEW.scope,NEW.content,NEW.legacy_source_id,
         NEW.legacy_attributes_ready,NEW.legacy_content_hash,NEW.legacy_search_vector)
     IS NOT DISTINCT FROM
     ROW(OLD.id,OLD.tenant_id,OLD.project_id,OLD.user_id,OLD.agent_id,OLD.run_id,
         OLD.object_type,OLD.legacy_scope_path,OLD.reference_namespace,OLD.metadata,
         OLD.state,OLD.valid_from,OLD.valid_until,OLD.legacy_fts_eligible,
         OLD.updated_at,OLD.search_vector,OLD.scope,OLD.content,OLD.legacy_source_id,
         OLD.legacy_attributes_ready,OLD.legacy_content_hash,OLD.legacy_search_vector) THEN
   RETURN NEW;
  END IF;
 END IF;
 PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[NEW.id]);
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_memory_lexical_projection() FROM PUBLIC;

CREATE FUNCTION braincore.sync_legacy_attribute_lexical_projection() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_RELID <> 'braincore.legacy_client_attributes'::regclass OR TG_WHEN <> 'AFTER'
    OR TG_OP NOT IN ('INSERT','UPDATE','DELETE') THEN
  RAISE EXCEPTION 'legacy lexical trigger requires canonical client attributes' USING ERRCODE='42501';
 END IF;
 IF TG_OP='DELETE' THEN
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[OLD.memory_id]);
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.memory_id,NEW.tenant_id,NEW.content_hash,NEW.search_vector,NEW.current_override)
      IS NOT DISTINCT FROM
      ROW(OLD.memory_id,OLD.tenant_id,OLD.content_hash,OLD.search_vector,OLD.current_override) THEN
   RETURN NEW;
  END IF;
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[OLD.memory_id,NEW.memory_id]);
 ELSE
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[NEW.memory_id]);
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_legacy_attribute_lexical_projection() FROM PUBLIC;
CREATE TRIGGER legacy_attributes_lexical_sync
 AFTER INSERT OR UPDATE OR DELETE ON braincore.legacy_client_attributes
 FOR EACH ROW EXECUTE FUNCTION braincore.sync_legacy_attribute_lexical_projection();

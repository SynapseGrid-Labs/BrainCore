-- Compact canonical compatibility relations for structured and temporal reads.
-- The existing compatibility backfill copies these fields through its trusted
-- refresh helper; reset its independent gate before any reader may use them.
ALTER TABLE braincore.memories_lexical
 ADD COLUMN compat_external_id uuid,
 ADD COLUMN compat_subject_entity_id uuid,
 ADD COLUMN compat_object_entity_id uuid,
 ADD COLUMN compat_episode_id uuid,
 ADD COLUMN compat_confidence numeric,
 ADD COLUMN compat_last_seen_at timestamptz,
 ADD COLUMN compat_entity_name text,
 ADD COLUMN compat_aliases jsonb,
 ADD COLUMN compat_current_status text;
CREATE INDEX memories_lexical_compat_external_idx ON braincore.memories_lexical
 (tenant_id,object_type,compat_external_id);
CREATE INDEX memories_lexical_compat_subject_idx ON braincore.memories_lexical
 (tenant_id,compat_subject_entity_id) WHERE object_type='fact';
CREATE INDEX memories_lexical_compat_episode_idx ON braincore.memories_lexical
 (tenant_id,compat_episode_id) WHERE object_type='fact';
CREATE INDEX memories_lexical_compat_entities_idx ON braincore.memories_lexical
 (tenant_id,state,compat_external_id) WHERE object_type='entity';
UPDATE braincore.legacy_lexical_projection_state
 SET ready=false,backfill_after_id=NULL,rows_processed=0,updated_at=now()
 WHERE singleton=true;

CREATE OR REPLACE FUNCTION braincore.refresh_legacy_lexical_projection(memory_ids uuid[]) RETURNS void
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
   updated_at,search_vector,scope,compat_fts,compat_ready,compat_external_id,compat_subject_entity_id,
   compat_object_entity_id,compat_episode_id,compat_confidence,compat_last_seen_at,
   compat_entity_name,compat_aliases,compat_current_status)
 SELECT m.id,m.tenant_id,m.project_id,m.user_id,m.agent_id,m.run_id,m.object_type,
  m.legacy_scope_path,m.reference_namespace,m.metadata,m.state,m.valid_from,
  m.valid_until,m.legacy_fts_eligible,m.updated_at,m.search_vector,m.scope,
  CASE WHEN NOT m.legacy_fts_eligible THEN NULL
       WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL
        THEN c.search_vector ELSE m.search_vector END,
  (m.legacy_source_id IS NULL OR m.legacy_attributes_ready OR x.memory_id IS NOT NULL),
  COALESCE(m.legacy_external_id,client_ids.external_id,m.id),
  CASE WHEN m.object_type='fact' THEN (c.attributes->>'subject_entity_id')::uuid END,
  CASE WHEN m.object_type='fact' AND c.content_hash=public.digest(m.content,'sha256')
       THEN (c.attributes->>'object_entity_id')::uuid END,
  CASE WHEN m.object_type='fact' THEN (c.attributes->>'episode_id')::uuid END,
  CASE WHEN m.object_type='fact' THEN COALESCE((c.attributes->>'confidence')::numeric,1::numeric) END,
  CASE WHEN m.object_type='fact' THEN (c.attributes->>'last_seen_at')::timestamptz END,
  CASE WHEN m.object_type='entity' THEN m.content END,
  CASE WHEN m.object_type='entity' THEN CASE
   WHEN (c.attributes->'__braincore_sql_null_columns') ? 'aliases' THEN NULL
   ELSE c.attributes->'aliases' END END,
  CASE WHEN m.object_type='fact' THEN CASE WHEN m.state='active' THEN 'active'
   ELSE COALESCE(NULLIF(c.attributes->>'current_status','active'),
    CASE WHEN m.state='suppressed' THEN 'uncertain' ELSE 'superseded' END) END END
 FROM braincore.memories m
 LEFT JOIN braincore.legacy_client_ids client_ids ON client_ids.memory_id=m.id
  AND client_ids.tenant_id=m.tenant_id
 LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
 CROSS JOIN LATERAL (SELECT
  CASE WHEN x.current_override THEN x.attributes
       WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
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
  scope=EXCLUDED.scope,compat_fts=EXCLUDED.compat_fts,compat_ready=EXCLUDED.compat_ready,
  compat_external_id=EXCLUDED.compat_external_id,
  compat_subject_entity_id=EXCLUDED.compat_subject_entity_id,
  compat_object_entity_id=EXCLUDED.compat_object_entity_id,
  compat_episode_id=EXCLUDED.compat_episode_id,compat_confidence=EXCLUDED.compat_confidence,
  compat_last_seen_at=EXCLUDED.compat_last_seen_at,compat_entity_name=EXCLUDED.compat_entity_name,
  compat_aliases=EXCLUDED.compat_aliases,compat_current_status=EXCLUDED.compat_current_status;
END; $$;
REVOKE ALL ON FUNCTION braincore.refresh_legacy_lexical_projection(uuid[]) FROM PUBLIC;

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
         NEW.legacy_attributes_ready,NEW.legacy_content_hash,NEW.legacy_search_vector,
         NEW.legacy_attributes,NEW.legacy_external_id)
     IS NOT DISTINCT FROM
     ROW(OLD.id,OLD.tenant_id,OLD.project_id,OLD.user_id,OLD.agent_id,OLD.run_id,
         OLD.object_type,OLD.legacy_scope_path,OLD.reference_namespace,OLD.metadata,
         OLD.state,OLD.valid_from,OLD.valid_until,OLD.legacy_fts_eligible,
         OLD.updated_at,OLD.search_vector,OLD.scope,OLD.content,OLD.legacy_source_id,
         OLD.legacy_attributes_ready,OLD.legacy_content_hash,OLD.legacy_search_vector,
         OLD.legacy_attributes,OLD.legacy_external_id) THEN
   RETURN NEW;
  END IF;
 END IF;
 PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[NEW.id]);
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_memory_lexical_projection() FROM PUBLIC;

CREATE OR REPLACE FUNCTION braincore.sync_legacy_attribute_lexical_projection() RETURNS trigger
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
  IF ROW(NEW.memory_id,NEW.tenant_id,NEW.content_hash,NEW.search_vector,NEW.current_override,NEW.attributes)
      IS NOT DISTINCT FROM
      ROW(OLD.memory_id,OLD.tenant_id,OLD.content_hash,OLD.search_vector,OLD.current_override,OLD.attributes) THEN
   RETURN NEW;
  END IF;
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[OLD.memory_id,NEW.memory_id]);
 ELSE
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[NEW.memory_id]);
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_legacy_attribute_lexical_projection() FROM PUBLIC;
CREATE FUNCTION braincore.sync_legacy_id_search_projection() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_RELID <> 'braincore.legacy_client_ids'::regclass OR TG_WHEN <> 'AFTER'
    OR TG_OP NOT IN ('INSERT','UPDATE','DELETE') THEN
  RAISE EXCEPTION 'search projection trigger requires canonical client IDs' USING ERRCODE='42501';
 END IF;
 IF TG_OP='DELETE' THEN
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[OLD.memory_id]);
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.memory_id,NEW.tenant_id,NEW.external_id)
      IS NOT DISTINCT FROM ROW(OLD.memory_id,OLD.tenant_id,OLD.external_id) THEN
   RETURN NEW;
  END IF;
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[OLD.memory_id,NEW.memory_id]);
 ELSE
  PERFORM braincore.refresh_legacy_lexical_projection(ARRAY[NEW.memory_id]);
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION braincore.sync_legacy_id_search_projection() FROM PUBLIC;
CREATE TRIGGER legacy_ids_search_projection_sync
 AFTER INSERT OR UPDATE OR DELETE ON braincore.legacy_client_ids
 FOR EACH ROW EXECUTE FUNCTION braincore.sync_legacy_id_search_projection();

-- An uninitialized relation projection must fail, not look like an empty corpus.
CREATE FUNCTION braincore.require_legacy_search_projection() RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF NOT COALESCE((SELECT ready FROM braincore.legacy_lexical_projection_state
                  WHERE singleton=true),false) THEN
  RAISE EXCEPTION 'compatibility search projection is not ready' USING ERRCODE='55000';
 END IF;
 RETURN true;
END; $$;

CREATE VIEW braincore.compat_search_entities WITH(security_barrier=true) AS
SELECT l.id AS native_id,l.tenant_id,l.compat_external_id AS entity_id,l.compat_entity_name AS canonical_name,
 l.compat_aliases AS aliases
FROM braincore.memories_lexical l
WHERE l.object_type='entity' AND l.tenant_id=ANY(braincore.compat_tenants())
 AND braincore.compat_permits(l.scope)
 AND (l.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND l.compat_ready
 AND braincore.require_legacy_search_projection();

CREATE VIEW braincore.compat_search_facts WITH(security_barrier=true) AS
SELECT l.id AS native_id,l.tenant_id,l.compat_external_id AS fact_id,l.compat_subject_entity_id AS subject_entity_id,
 l.compat_object_entity_id AS object_entity_id,l.compat_episode_id AS episode_id,
 l.compat_confidence AS confidence,l.compat_last_seen_at AS last_seen_at,
 l.valid_from,l.valid_until AS valid_to,l.project_id AS scope_path,
 l.compat_current_status AS current_status
FROM braincore.memories_lexical l
WHERE l.object_type='fact' AND l.tenant_id=ANY(braincore.compat_tenants())
 AND braincore.compat_permits(l.scope)
 AND (l.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND l.compat_ready
 AND braincore.require_legacy_search_projection();

-- Derive the current evidence contract, preserving both imported evidence and
-- native control branches. Narrow canonical parent IDs before wide rendering.
DO $$
DECLARE definition text; bounded text; branch_guard text;
BEGIN
 PERFORM set_config('search_path','pg_catalog',true);
 definition := pg_get_viewdef('preserve.fact_evidence'::regclass,true);
 bounded := replace(definition,'JOIN braincore.memories m',
  'JOIN (SELECT * FROM braincore.memories WHERE id=ANY($1)) m');
 IF bounded=definition THEN RAISE EXCEPTION 'canonical evidence parent guard missing'; END IF;
 branch_guard := 'n.object_type = ''fact_evidence''::text AND (n.fields->>''fact_id'')::uuid IN (
  SELECT COALESCE(p.legacy_external_id,client_ids.external_id,p.id)
  FROM braincore.memories p LEFT JOIN braincore.legacy_client_ids client_ids
   ON client_ids.memory_id=p.id AND client_ids.tenant_id=p.tenant_id
  WHERE p.id=ANY($1) AND p.object_type=''fact''
   AND p.tenant_id=ANY(braincore.compat_tenants()) AND braincore.compat_permits(p.scope)
   AND (p.state=''active'' OR current_setting(''braincore.compat_include_inactive'',true)=''true''))';
 IF position('n.object_type = ''fact_evidence''::text' IN bounded)=0 THEN
  RAISE EXCEPTION 'canonical evidence control guard missing';
 END IF;
 bounded := replace(bounded,'n.object_type = ''fact_evidence''::text',branch_guard);
 EXECUTE format('CREATE FUNCTION braincore.compat_hydrate_fact_evidence(uuid[])
  RETURNS SETOF preserve.fact_evidence LANGUAGE sql STABLE SECURITY INVOKER
  SET search_path=pg_catalog AS %L',bounded);
END; $$;

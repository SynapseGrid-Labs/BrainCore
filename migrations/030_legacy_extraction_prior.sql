-- Compact, authenticated prior IDs for atomic extraction replacement.
-- Rendering content, embeddings and evidence remains outside this selection.
CREATE FUNCTION braincore.compat_extraction_prior(requested_tenant text, requested_artifact uuid)
RETURNS TABLE(object_type text,object_id uuid)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE primary_id uuid; primary_name text; visible_count bigint; primary_only boolean;
 run_ids uuid[]; runs_primary_only boolean;
BEGIN
 primary_id := braincore.compat_tenant();
 SELECT t.name INTO primary_name FROM braincore.tenants t WHERE t.id=primary_id;
 IF primary_name IS NULL OR requested_tenant IS DISTINCT FROM primary_name THEN
  RAISE EXCEPTION 'extraction tenant does not match authenticated primary reader'
   USING ERRCODE='42501';
 END IF;
 SELECT count(*),bool_and(a.tenant=requested_tenant) INTO visible_count,primary_only
 FROM preserve.artifact a WHERE a.artifact_id=requested_artifact;
 IF visible_count<>1 OR primary_only IS DISTINCT FROM true THEN
  RAISE EXCEPTION 'extraction artifact is absent, ambiguous or not primary-owned'
   USING ERRCODE='42501';
 END IF;
 -- Match the extraction-run view's current scope/history and operator guards.
 -- A primary extraction cannot retire another owner's or unowned operator run.
 WITH runs AS MATERIALIZED (
  SELECT (a.payload->>'run_id')::uuid AS id,a.tenant_id AS owner_id
  FROM braincore.legacy_auxiliary_objects a
  WHERE a.source_table='extraction_run'
   AND (a.payload->>'artifact_id')::uuid=requested_artifact
   AND (NOT a.deleted OR current_setting('braincore.compat_include_inactive',true)='true')
   AND braincore.compat_auxiliary_owner(a.tenant_id)
   AND (a.scope IS NOT NULL OR (a.tenant_id IS NULL AND
    braincore.compat_auxiliary_owner(a.tenant_id)))
   AND (braincore.compat_permits(a.scope) OR (a.tenant_id IS NULL AND
    braincore.compat_auxiliary_owner(a.tenant_id)))
  UNION ALL
  SELECT n.external_id,n.tenant_id FROM braincore.legacy_client_controls n
  WHERE n.object_type='extraction_run'
   AND (n.fields->>'artifact_id')::uuid=requested_artifact
   AND (NOT n.deleted OR current_setting('braincore.compat_include_inactive',true)='true')
   AND n.tenant_id=ANY(braincore.compat_tenants()) AND braincore.compat_permits(n.scope)
 )
 SELECT COALESCE(array_agg(r.id ORDER BY r.id),ARRAY[]::uuid[]),
  COALESCE(bool_and(r.owner_id IS NOT DISTINCT FROM primary_id),true)
 INTO run_ids,runs_primary_only FROM runs r;
 IF NOT runs_primary_only THEN
  RAISE EXCEPTION 'extraction prior run is not primary-owned' USING ERRCODE='42501';
 END IF;
 RETURN QUERY
 WITH prior AS MATERIALIZED (
  SELECT m.object_type AS kind,COALESCE(m.legacy_external_id,i.external_id,m.id) AS id,
   CASE WHEN m.object_type='fact' THEN (c.attributes->>'created_run_id')::uuid END AS run_id,
   CASE WHEN m.object_type='segment' THEN (c.attributes->>'artifact_id')::uuid
    WHEN m.object_type='episode' THEN (c.attributes->>'primary_artifact_id')::uuid END
    AS artifact_id,
   CASE WHEN m.state='active' THEN 'active' ELSE
    COALESCE(NULLIF(c.attributes->>'current_status','active'),
     CASE WHEN m.state='suppressed' THEN 'uncertain' ELSE 'superseded' END) END AS status
  FROM braincore.memories m
  LEFT JOIN braincore.legacy_client_ids i ON i.memory_id=m.id AND i.tenant_id=m.tenant_id
  LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
  CROSS JOIN LATERAL (SELECT CASE WHEN x.current_override THEN x.attributes
   WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
   (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
  WHERE m.tenant_id=primary_id
   AND (m.object_type IN ('segment','episode') OR
    (m.object_type='fact' AND cardinality(run_ids)>0))
   AND braincore.compat_permits(m.scope)
   AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
   AND (m.legacy_source_id IS NULL OR c.ready)
 )
 SELECT 'fact'::text,p.id FROM prior p JOIN unnest(run_ids) r(id) ON r.id=p.run_id
  WHERE p.kind='fact' AND p.status='active'
 UNION ALL SELECT p.kind,p.id FROM prior p
  WHERE p.kind IN ('segment','episode') AND p.artifact_id=requested_artifact
 UNION ALL SELECT 'extraction_run'::text,r.id FROM unnest(run_ids) r(id);
END; $$;

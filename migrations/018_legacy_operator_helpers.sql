-- NULL ownership denotes operator metadata, never tenant-default sharing.
CREATE FUNCTION braincore.compat_auxiliary_owner(owner_id uuid) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT braincore.compat_tenant() IS NOT NULL AND
  (owner_id=braincore.compat_tenant() OR (owner_id IS NULL AND
   current_setting('braincore.compat_is_admin',true)='true'))
$$;
DO $$
DECLARE helper text; definition text;
BEGIN
 FOREACH helper IN ARRAY ARRAY['archive_object','eval_case','eval_run','extraction_run',
  'memory_edge_evidence','project_service_map','publish_note','reflection_class','review_queue',
  'schema_migration','event_frame','media_artifact','memory_context_audit','memory_edge',
  'procedure','procedure_step','task_session','visual_region','working_memory','event'] LOOP
  definition := pg_get_viewdef(format('preserve.%I',helper)::regclass,true);
  IF position('a.tenant_id = braincore.compat_tenant()' IN definition)=0 THEN
   RAISE EXCEPTION 'operator helper ownership guard is missing';
  END IF;
  definition := replace(definition,'a.tenant_id = braincore.compat_tenant()',
   'braincore.compat_auxiliary_owner(a.tenant_id)');
  definition := replace(definition,'braincore.compat_permits(a.scope)',
   '(braincore.compat_permits(a.scope) OR (a.tenant_id IS NULL AND braincore.compat_auxiliary_owner(a.tenant_id)))');
  EXECUTE format('CREATE OR REPLACE VIEW preserve.%I WITH (security_barrier=true) AS %s',helper,definition);
 END LOOP;
END $$;

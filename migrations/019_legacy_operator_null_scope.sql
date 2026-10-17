-- Operator metadata has neither tenant nor scope; explicitadmin still required.
DO $$
DECLARE helper text; definition text;
BEGIN
 FOREACH helper IN ARRAY ARRAY['archive_object','eval_case','eval_run','extraction_run',
  'memory_edge_evidence','project_service_map','publish_note','reflection_class','review_queue',
  'schema_migration','event_frame','media_artifact','memory_context_audit','memory_edge',
  'procedure','procedure_step','task_session','visual_region','working_memory','event'] LOOP
  definition := pg_get_viewdef(format('preserve.%I',helper)::regclass,true);
  IF position('a.scope IS NOT NULL' IN definition)=0 THEN
   RAISE EXCEPTION 'operator helper scope guard is missing';
  END IF;
  definition := replace(definition,'a.scope IS NOT NULL',
   '(a.scope IS NOT NULL OR (a.tenant_id IS NULL AND braincore.compat_auxiliary_owner(a.tenant_id)))');
  EXECUTE format('CREATE OR REPLACE VIEW preserve.%I WITH (security_barrier=true) AS %s',helper,definition);
 END LOOP;
END $$;

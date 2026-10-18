-- Match the established canonical artifact grant scope policy.
CREATE OR REPLACE FUNCTION braincore.check_legacy_subtype_ref() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE endpoint_kind text; endpoint_owner uuid; source_scope jsonb; expected_kind text;
BEGIN
 expected_kind := CASE NEW.field
  WHEN 'subject_entity_id' THEN 'entity' WHEN 'object_entity_id' THEN 'entity'
  WHEN 'scope_entity_id' THEN 'entity' WHEN 'project_entity_id' THEN 'entity'
  WHEN 'episode_id' THEN 'episode' WHEN 'artifact_id' THEN 'artifact'
  WHEN 'primary_artifact_id' THEN 'artifact' WHEN 'created_run_id' THEN 'extraction_run' END;
 IF NEW.target_memory_id IS NOT NULL THEN
  SELECT object_type,tenant_id INTO endpoint_kind,endpoint_owner FROM braincore.memories
   WHERE id=NEW.target_memory_id;
 ELSIF NEW.target_artifact_id IS NOT NULL THEN
  SELECT 'artifact',tenant_id INTO endpoint_kind,endpoint_owner FROM braincore.artifacts
   WHERE id=NEW.target_artifact_id;
 ELSIF NEW.target_control_id IS NOT NULL THEN
  SELECT object_type,tenant_id INTO endpoint_kind,endpoint_owner FROM braincore.legacy_client_controls
   WHERE id=NEW.target_control_id AND NOT deleted;
 ELSE
  SELECT source_table,tenant_id INTO endpoint_kind,endpoint_owner FROM braincore.legacy_auxiliary_objects
   WHERE native_id=NEW.target_auxiliary_id AND NOT deleted;
 END IF;
 IF expected_kind IS NULL OR endpoint_kind IS DISTINCT FROM expected_kind
  OR endpoint_owner IS DISTINCT FROM NEW.target_tenant_id THEN
  RAISE EXCEPTION 'subtype endpoint type or owner does not match';
 END IF;
 IF NEW.tenant_id IS DISTINCT FROM endpoint_owner THEN
  SELECT scope INTO source_scope FROM braincore.memories
   WHERE id=NEW.memory_id AND tenant_id=NEW.tenant_id;
  IF NEW.target_artifact_id IS NULL OR NOT EXISTS(
   SELECT 1 FROM braincore.artifact_access_grants g WHERE g.id=NEW.access_grant_id
    AND g.artifact_id=NEW.target_artifact_id AND g.owner_tenant_id=endpoint_owner
    AND g.grantee_tenant_id=NEW.tenant_id AND source_scope @> g.scope
    AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())) THEN
   RAISE EXCEPTION 'subtype cross-owner endpoint requires an active scoped artifact grant';
  END IF;
 END IF;
 RETURN NEW;
END; $$;

-- Current subtype links constrain native endpoints; original ledger stays immutable.
ALTER TABLE braincore.legacy_client_controls ADD UNIQUE(id,tenant_id);
CREATE TABLE braincore.legacy_subtype_refs (
 memory_id uuid NOT NULL,tenant_id uuid NOT NULL,field text NOT NULL,
 target_tenant_id uuid NOT NULL,target_memory_id uuid,target_artifact_id uuid,
 target_control_id uuid,target_auxiliary_id uuid,access_grant_id uuid,
 PRIMARY KEY(memory_id,field),
 FOREIGN KEY(memory_id,tenant_id) REFERENCES braincore.memories(id,tenant_id),
 FOREIGN KEY(target_memory_id,target_tenant_id) REFERENCES braincore.memories(id,tenant_id),
 FOREIGN KEY(target_artifact_id,target_tenant_id) REFERENCES braincore.artifacts(id,tenant_id),
 FOREIGN KEY(target_control_id,target_tenant_id) REFERENCES braincore.legacy_client_controls(id,tenant_id),
 FOREIGN KEY(target_auxiliary_id) REFERENCES braincore.legacy_auxiliary_objects(native_id),
 FOREIGN KEY(access_grant_id) REFERENCES braincore.artifact_access_grants(id),
 CHECK(num_nonnulls(target_memory_id,target_artifact_id,target_control_id,target_auxiliary_id)=1)
);
CREATE FUNCTION braincore.check_legacy_subtype_ref() RETURNS trigger
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
    AND g.grantee_tenant_id=NEW.tenant_id AND g.allowed_scope @> source_scope
    AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())) THEN
   RAISE EXCEPTION 'subtype cross-owner endpoint requires an active scoped artifact grant';
  END IF;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER legacy_subtype_endpoint BEFORE INSERT OR UPDATE ON braincore.legacy_subtype_refs
 FOR EACH ROW EXECUTE FUNCTION braincore.check_legacy_subtype_ref();
CREATE OR REPLACE FUNCTION braincore.invalidate_legacy_client_extension() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.imported_payload_hash IS DISTINCT FROM OLD.imported_payload_hash THEN
  DELETE FROM braincore.legacy_client_attributes WHERE memory_id=NEW.native_memory_id;
  UPDATE braincore.memories SET legacy_attributes_ready=false,legacy_attributes='{}'::jsonb,
   legacy_content_hash=NULL,legacy_search_vector=NULL
   WHERE id=NEW.native_memory_id AND tenant_id=NEW.tenant_id;
  DELETE FROM braincore.legacy_subtype_refs WHERE memory_id=NEW.native_memory_id;
 END IF;
 RETURN NEW;
END; $$;

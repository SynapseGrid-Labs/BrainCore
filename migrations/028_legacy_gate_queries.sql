-- Reuse authorized projections once for the legacy stranded-artifact gate.
-- Invoker permissions and the existing views retain scope, grants and history.
CREATE FUNCTION braincore.compat_gate_stranded(requested_tenant text)
RETURNS TABLE(artifact_id uuid,source_key text,preservation_state preserve.preservation_state,
              original_path text,discovered_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE primary_name text;
BEGIN
 SELECT t.name INTO primary_name FROM braincore.tenants t
 WHERE t.id=braincore.compat_tenant();
 IF primary_name IS NULL OR requested_tenant IS DISTINCT FROM primary_name THEN
  RAISE EXCEPTION 'gate tenant must match the configured primary reader' USING ERRCODE='42501';
 END IF;
 RETURN QUERY
 WITH eligible_artifacts AS MATERIALIZED (
  SELECT a.artifact_id,a.source_key,a.preservation_state,a.original_path,a.discovered_at,
         a.tenant,a.source_type,
         regexp_replace(a.source_key,':[0-9]{4}-[0-9]{2}-[0-9]{2}$','') AS base_key
  FROM preserve.artifact a
  WHERE a.tenant=requested_tenant AND a.source_type IN ('codex_session','codex_shared')
 ), successful_ids AS MATERIALIZED (
  SELECT DISTINCT r.artifact_id FROM preserve.extraction_run r WHERE r.status='success'
 ), latest_success AS MATERIALIZED (
  SELECT a.tenant,a.source_type,a.base_key,max(a.discovered_at) AS discovered_at
  FROM eligible_artifacts a JOIN successful_ids r ON r.artifact_id=a.artifact_id
  GROUP BY a.tenant,a.source_type,a.base_key
 )
 SELECT a.artifact_id,a.source_key,a.preservation_state,a.original_path,a.discovered_at
 FROM eligible_artifacts a
 WHERE a.preservation_state NOT IN ('blocked','failed','pending_escalation')
  AND NOT EXISTS (SELECT 1 FROM successful_ids r WHERE r.artifact_id=a.artifact_id)
  AND NOT EXISTS (SELECT 1 FROM latest_success newer
    WHERE newer.tenant=a.tenant AND newer.source_type=a.source_type
      AND newer.base_key=a.base_key AND newer.discovered_at>a.discovered_at)
 ORDER BY a.discovered_at ASC,a.artifact_id ASC;
END; $$;

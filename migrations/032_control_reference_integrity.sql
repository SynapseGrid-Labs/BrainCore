-- Keep helper relationships constrained for both live and imported targets.
ALTER TABLE braincore.legacy_client_controls ADD UNIQUE(id,tenant_id);
ALTER TABLE braincore.legacy_auxiliary_objects ADD UNIQUE(native_id,tenant_id);
ALTER TABLE braincore.legacy_client_control_refs
 ADD COLUMN auxiliary_id uuid,
 ADD FOREIGN KEY(control_id,tenant_id) REFERENCES braincore.legacy_client_controls(id,tenant_id),
 ADD FOREIGN KEY(child_control_id,tenant_id) REFERENCES braincore.legacy_client_controls(id,tenant_id),
 ADD FOREIGN KEY(auxiliary_id,tenant_id) REFERENCES braincore.legacy_auxiliary_objects(native_id,tenant_id),
 DROP CONSTRAINT legacy_client_control_refs_check,
 ADD CHECK(num_nonnulls(memory_id,artifact_id,child_control_id,auxiliary_id)=1);

-- Source JSON arrays and live JSON arrays must hydrate as PostgreSQL UUID arrays.
-- Retain support for original PostgreSQL array literals in historical payloads.
CREATE FUNCTION braincore.compat_uuid_array(value jsonb) RETURNS uuid[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN jsonb_typeof(value)='array'
   THEN ARRAY(SELECT jsonb_array_elements_text(value))::uuid[]
   ELSE (value #>> '{}')::uuid[] END
$$;

CREATE OR REPLACE VIEW preserve.memory_context_audit WITH (security_barrier=true) AS
SELECT (a.payload->>'audit_id')::uuid AS audit_id,
 t.name AS tenant,
 (a.payload->>'query')::text AS query,
 (a.payload->>'trigger')::text AS trigger,
 braincore.compat_uuid_array(a.payload->'retrieved_memory_ids') AS retrieved_memory_ids,
 braincore.compat_uuid_array(a.payload->'injected_memory_ids') AS injected_memory_ids,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'omitted' THEN NULL ELSE a.payload->'omitted' END AS omitted,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'prompt_package' THEN NULL ELSE a.payload->'prompt_package' END AS prompt_package,
 (a.payload->>'total_tokens')::integer AS total_tokens,
 (a.payload->>'max_tokens')::integer AS max_tokens,
 (a.payload->>'relevance_reason')::text AS relevance_reason,
 (a.payload->>'actor')::text AS actor,
 (a.payload->>'route')::text AS route,
 (a.payload->>'request_id')::text AS request_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='memory_context_audit' AND (NOT a.deleted OR current_setting('braincore.compat_include_inactive',true)='true') AND a.tenant_id=ANY(braincore.compat_tenants())
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope)
UNION ALL SELECT n.external_id AS audit_id,
 t.name AS tenant,
 (n.fields->>'query')::text AS query,
 (n.fields->>'trigger')::text AS trigger,
 braincore.compat_uuid_array(n.fields->'retrieved_memory_ids') AS retrieved_memory_ids,
 braincore.compat_uuid_array(n.fields->'injected_memory_ids') AS injected_memory_ids,
 CASE WHEN (n.fields->'__braincore_sql_null_columns') ? 'omitted' THEN NULL ELSE n.fields->'omitted' END AS omitted,
 CASE WHEN (n.fields->'__braincore_sql_null_columns') ? 'prompt_package' THEN NULL ELSE n.fields->'prompt_package' END AS prompt_package,
 (n.fields->>'total_tokens')::integer AS total_tokens,
 (n.fields->>'max_tokens')::integer AS max_tokens,
 (n.fields->>'relevance_reason')::text AS relevance_reason,
 (n.fields->>'actor')::text AS actor,
 (n.fields->>'route')::text AS route,
 (n.fields->>'request_id')::text AS request_id,
 (n.fields->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_client_controls n JOIN braincore.tenants t ON t.id=n.tenant_id
WHERE n.object_type='memory_context_audit' AND (NOT n.deleted OR current_setting('braincore.compat_include_inactive',true)='true') AND n.tenant_id=ANY(braincore.compat_tenants())
 AND braincore.compat_permits(n.scope);

-- Canonical subtype extensions, never a second authoritative core store.
ALTER TABLE braincore.memories
 ADD COLUMN legacy_attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
 ADD COLUMN legacy_content_hash bytea,
 ADD COLUMN legacy_search_vector tsvector,
 ADD COLUMN legacy_attributes_ready boolean NOT NULL DEFAULT false;
CREATE SCHEMA IF NOT EXISTS preserve;
CREATE FUNCTION braincore.invalidate_legacy_client_attributes() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.imported_payload_hash IS DISTINCT FROM OLD.imported_payload_hash THEN
  UPDATE braincore.memories SET legacy_attributes_ready=false
   WHERE id=NEW.native_memory_id AND tenant_id=NEW.tenant_id;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER legacy_client_source_change AFTER UPDATE ON braincore.legacy_object_map
 FOR EACH ROW EXECUTE FUNCTION braincore.invalidate_legacy_client_attributes();
CREATE FUNCTION braincore.compat_tenant() RETURNS uuid LANGUAGE sql STABLE
 SET search_path=pg_catalog AS $$
 SELECT NULLIF(current_setting('braincore.compat_tenant_id',true),'')::uuid;
$$;
CREATE FUNCTION braincore.compat_permits(candidate jsonb) RETURNS boolean
 LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN current_setting('braincore.compat_scope',true)='null' THEN true
 ELSE candidate @> NULLIF(current_setting('braincore.compat_scope',true),'')::jsonb END;
$$;
CREATE FUNCTION braincore.block_legacy_sql_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION 'legacy SQL mutation requires canonical mutate_legacy_object contract; old ON CONFLICT writers are unsupported' USING ERRCODE='0A000';
END; $$;
CREATE VIEW preserve.entity WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS entity_id,
 (m.legacy_attributes->>'entity_type')::text AS entity_type,
 m.content AS canonical_name,
 CASE WHEN (m.legacy_attributes->'__braincore_sql_null_columns') ? 'aliases' THEN NULL ELSE m.legacy_attributes->'aliases' END AS aliases,
 CASE WHEN (m.legacy_attributes->'__braincore_sql_null_columns') ? 'attrs' THEN NULL ELSE m.legacy_attributes->'attrs' END AS attrs,
 (m.legacy_attributes->>'first_seen_at')::timestamp with time zone AS first_seen_at,
 (m.legacy_attributes->>'last_seen_at')::timestamp with time zone AS last_seen_at,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
WHERE m.object_type='entity' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND m.state='active'
 AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready);
CREATE TRIGGER entity_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.entity FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.fact WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS fact_id,
 (m.legacy_attributes->>'subject_entity_id')::uuid AS subject_entity_id,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN m.legacy_attributes->>'predicate' ELSE m.content END AS predicate,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN (m.legacy_attributes->>'object_entity_id')::uuid END AS object_entity_id,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN CASE WHEN (m.legacy_attributes->'__braincore_sql_null_columns') ? 'object_value' THEN NULL ELSE m.legacy_attributes->'object_value' END ELSE to_jsonb(m.content) END AS object_value,
 (m.legacy_attributes->>'fact_kind')::text AS fact_kind,
 (m.legacy_attributes->>'assertion_class')::text AS assertion_class,
 COALESCE((m.legacy_attributes->>'confidence')::numeric,1::numeric) AS confidence,
 m.valid_from AS valid_from,
 m.valid_until AS valid_to,
 (m.legacy_attributes->>'first_seen_at')::timestamp with time zone AS first_seen_at,
 (m.legacy_attributes->>'last_seen_at')::timestamp with time zone AS last_seen_at,
 CASE WHEN m.state='active' THEN 'active' ELSE 'retracted' END AS current_status,
 (m.legacy_attributes->>'canonical_fingerprint')::text AS canonical_fingerprint,
 (m.legacy_attributes->>'episode_id')::uuid AS episode_id,
 (m.legacy_attributes->>'created_run_id')::uuid AS created_run_id,
 COALESCE(m.legacy_scope_path,m.project_id) AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN m.legacy_content_hash=public.digest(m.content,'sha256') AND m.legacy_search_vector IS NOT NULL THEN m.legacy_search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (m.legacy_attributes->>'project_entity_id')::uuid AS project_entity_id,
 (m.legacy_attributes->>'is_milestone')::boolean AS is_milestone,
 (m.legacy_attributes->>'importance_score')::numeric(5,2) AS importance_score,
 COALESCE((m.legacy_attributes->>'priority')::integer,5) AS priority,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
WHERE m.object_type='fact' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND m.state='active'
 AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready);
CREATE TRIGGER fact_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.fact FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.memory WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS memory_id,
 (m.legacy_attributes->>'memory_type')::text AS memory_type,
 (m.legacy_attributes->>'scope_entity_id')::uuid AS scope_entity_id,
 (m.legacy_attributes->>'fingerprint')::text AS fingerprint,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN COALESCE(m.legacy_attributes->>'title',left(m.content,160)) ELSE left(m.content,160) END AS title,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN COALESCE(m.legacy_attributes->>'narrative',m.content) ELSE m.content END AS narrative,
 (m.legacy_attributes->>'support_count')::integer AS support_count,
 (m.legacy_attributes->>'contradiction_count')::integer AS contradiction_count,
 COALESCE((m.legacy_attributes->>'confidence')::numeric,1::numeric) AS confidence,
 m.valid_from AS valid_from,
 m.valid_until AS valid_to,
 CASE WHEN m.state='active' THEN 'published' ELSE 'draft' END AS lifecycle_state,
 (m.legacy_attributes->>'pipeline_version')::text AS pipeline_version,
 (m.legacy_attributes->>'model_name')::text AS model_name,
 (m.legacy_attributes->>'prompt_version')::text AS prompt_version,
 COALESCE(m.legacy_scope_path,m.project_id) AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN m.legacy_content_hash=public.digest(m.content,'sha256') AND m.legacy_search_vector IS NOT NULL THEN m.legacy_search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 m.updated_at AS updated_at,
 (m.legacy_attributes->>'project_entity_id')::uuid AS project_entity_id,
 (m.legacy_attributes->>'last_supported_at')::timestamp with time zone AS last_supported_at,
 COALESCE((m.legacy_attributes->>'priority')::integer,5) AS priority,
 t.name AS tenant,
 (m.legacy_attributes->>'namespace')::text AS namespace,
 CASE WHEN m.state='active' THEN 'active' ELSE 'suppressed' END AS governance_status,
 (m.legacy_attributes->>'source_class')::text AS source_class,
 (m.legacy_attributes->>'trust_class')::text AS trust_class,
 (m.legacy_attributes->>'salience')::numeric(4,3) AS salience,
 (m.legacy_attributes->>'strength')::numeric(4,3) AS strength,
 (m.legacy_attributes->>'stability')::numeric(4,3) AS stability,
 (m.legacy_attributes->>'quality_score')::numeric(4,3) AS quality_score,
 (m.legacy_attributes->>'summary_fidelity_score')::numeric(4,3) AS summary_fidelity_score,
 (m.legacy_attributes->>'token_count')::integer AS token_count,
 (m.legacy_attributes->>'schema_version')::integer AS schema_version,
 (m.legacy_attributes->>'config_version')::text AS config_version,
 CASE WHEN (m.legacy_attributes->'__braincore_sql_null_columns') ? 'redaction_metadata' THEN NULL ELSE m.legacy_attributes->'redaction_metadata' END AS redaction_metadata,
 CASE WHEN (m.legacy_attributes->'__braincore_sql_null_columns') ? 'governance_meta' THEN NULL ELSE m.legacy_attributes->'governance_meta' END AS governance_meta,
 (m.legacy_attributes->>'last_reinforced_at')::timestamp with time zone AS last_reinforced_at,
 (m.legacy_attributes->>'last_decayed_at')::timestamp with time zone AS last_decayed_at
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
WHERE m.object_type='memory' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND m.state='active'
 AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready);
CREATE TRIGGER memory_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.memory FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.segment WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS segment_id,
 (m.legacy_attributes->>'artifact_id')::uuid AS artifact_id,
 (m.legacy_attributes->>'ordinal')::integer AS ordinal,
 (m.legacy_attributes->>'line_start')::integer AS line_start,
 (m.legacy_attributes->>'line_end')::integer AS line_end,
 (m.legacy_attributes->>'ts_start')::timestamp with time zone AS ts_start,
 (m.legacy_attributes->>'ts_end')::timestamp with time zone AS ts_end,
 (m.legacy_attributes->>'section_label')::text AS section_label,
 m.content AS content,
 COALESCE(m.legacy_scope_path,m.project_id) AS scope_path,
 (m.legacy_attributes->>'source_sha256')::text AS source_sha256,
 (m.legacy_attributes->>'source_relpath')::text AS source_relpath,
 (m.legacy_attributes->>'archive_member_path')::text AS archive_member_path,
 (m.legacy_attributes->>'excerpt_hash')::text AS excerpt_hash,
 (m.legacy_attributes->>'byte_start')::bigint AS byte_start,
 (m.legacy_attributes->>'byte_end')::bigint AS byte_end,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN m.legacy_content_hash=public.digest(m.content,'sha256') AND m.legacy_search_vector IS NOT NULL THEN m.legacy_search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (m.legacy_attributes->>'project_entity_id')::uuid AS project_entity_id,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
WHERE m.object_type='segment' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND m.state='active'
 AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready);
CREATE TRIGGER segment_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.segment FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.episode WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS episode_id,
 (m.legacy_attributes->>'episode_type')::text AS episode_type,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN COALESCE(m.legacy_attributes->>'title',left(m.content,160)) ELSE left(m.content,160) END AS title,
 (m.legacy_attributes->>'start_at')::timestamp with time zone AS start_at,
 (m.legacy_attributes->>'end_at')::timestamp with time zone AS end_at,
 (m.legacy_attributes->>'severity')::text AS severity,
 (m.legacy_attributes->>'outcome')::text AS outcome,
 CASE WHEN m.legacy_content_hash=public.digest(m.content,'sha256') THEN COALESCE(m.legacy_attributes->>'summary',m.content) ELSE m.content END AS summary,
 (m.legacy_attributes->>'primary_artifact_id')::uuid AS primary_artifact_id,
 COALESCE(m.legacy_scope_path,m.project_id) AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN m.legacy_content_hash=public.digest(m.content,'sha256') AND m.legacy_search_vector IS NOT NULL THEN m.legacy_search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (m.legacy_attributes->>'project_entity_id')::uuid AS project_entity_id,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
WHERE m.object_type='episode' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND m.state='active'
 AND (m.legacy_source_id IS NULL OR m.legacy_attributes_ready);
CREATE TRIGGER episode_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.episode FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();

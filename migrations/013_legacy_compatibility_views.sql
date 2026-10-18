-- Canonical current auxiliary/evidence client contracts. No legacy-ledger read fallback.
ALTER TABLE braincore.artifacts ADD COLUMN legacy_attributes jsonb,
 ADD COLUMN legacy_attributes_ready boolean NOT NULL DEFAULT false;
CREATE TABLE braincore.legacy_client_attributes (
 memory_id uuid PRIMARY KEY, tenant_id uuid NOT NULL,
 attributes jsonb NOT NULL,content_hash bytea NOT NULL,search_vector tsvector,
 imported_payload_hash bytea NOT NULL,
 FOREIGN KEY(memory_id,tenant_id) REFERENCES braincore.memories(id,tenant_id)
);
CREATE FUNCTION braincore.invalidate_legacy_client_extension() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.imported_payload_hash IS DISTINCT FROM OLD.imported_payload_hash THEN
  DELETE FROM braincore.legacy_client_attributes WHERE memory_id=NEW.native_memory_id;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER legacy_client_extension_source_change AFTER UPDATE ON braincore.legacy_object_map
 FOR EACH ROW EXECUTE FUNCTION braincore.invalidate_legacy_client_extension();
CREATE FUNCTION braincore.invalidate_legacy_artifact_attributes() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.imported_payload_hash IS DISTINCT FROM OLD.imported_payload_hash THEN
  UPDATE braincore.artifacts SET legacy_attributes_ready=false
   WHERE id=NEW.native_artifact_id AND tenant_id=NEW.tenant_id;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER legacy_client_artifact_source_change AFTER UPDATE ON braincore.legacy_artifact_map
 FOR EACH ROW EXECUTE FUNCTION braincore.invalidate_legacy_artifact_attributes();
CREATE OR REPLACE VIEW preserve.entity WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS entity_id,
 (c.attributes->>'entity_type')::text AS entity_type,
 m.content AS canonical_name,
 CASE WHEN (c.attributes->'__braincore_sql_null_columns') ? 'aliases' THEN NULL ELSE c.attributes->'aliases' END AS aliases,
 CASE WHEN (c.attributes->'__braincore_sql_null_columns') ? 'attrs' THEN NULL ELSE c.attributes->'attrs' END AS attrs,
 (c.attributes->>'first_seen_at')::timestamp with time zone AS first_seen_at,
 (c.attributes->>'last_seen_at')::timestamp with time zone AS last_seen_at,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
CROSS JOIN LATERAL (SELECT
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector,
 (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
WHERE m.object_type='entity' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND (m.legacy_source_id IS NULL OR c.ready);
CREATE OR REPLACE VIEW preserve.fact WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS fact_id,
 (c.attributes->>'subject_entity_id')::uuid AS subject_entity_id,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN c.attributes->>'predicate' END AS predicate,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN (c.attributes->>'object_entity_id')::uuid END AS object_entity_id,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN CASE WHEN (c.attributes->'__braincore_sql_null_columns') ? 'object_value' THEN NULL ELSE c.attributes->'object_value' END ELSE NULL END AS object_value,
 (c.attributes->>'fact_kind')::text AS fact_kind,
 (c.attributes->>'assertion_class')::text AS assertion_class,
 COALESCE((c.attributes->>'confidence')::numeric,1::numeric) AS confidence,
 m.valid_from AS valid_from,
 m.valid_until AS valid_to,
 (c.attributes->>'first_seen_at')::timestamp with time zone AS first_seen_at,
 (c.attributes->>'last_seen_at')::timestamp with time zone AS last_seen_at,
 CASE WHEN m.state='active' THEN 'active' ELSE COALESCE(NULLIF(c.attributes->>'current_status','active'),CASE WHEN m.state='suppressed' THEN 'uncertain' ELSE 'superseded' END) END AS current_status,
 (c.attributes->>'canonical_fingerprint')::text AS canonical_fingerprint,
 (c.attributes->>'episode_id')::uuid AS episode_id,
 (c.attributes->>'created_run_id')::uuid AS created_run_id,
 m.project_id AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL THEN c.search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (c.attributes->>'project_entity_id')::uuid AS project_entity_id,
 (c.attributes->>'is_milestone')::boolean AS is_milestone,
 (c.attributes->>'importance_score')::numeric(5,2) AS importance_score,
 COALESCE((c.attributes->>'priority')::integer,5) AS priority,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
CROSS JOIN LATERAL (SELECT
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector,
 (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
WHERE m.object_type='fact' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND (m.legacy_source_id IS NULL OR c.ready);
CREATE OR REPLACE VIEW preserve.memory WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS memory_id,
 (c.attributes->>'memory_type')::text AS memory_type,
 (c.attributes->>'scope_entity_id')::uuid AS scope_entity_id,
 (c.attributes->>'fingerprint')::text AS fingerprint,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN COALESCE(c.attributes->>'title',left(m.content,160)) ELSE left(m.content,160) END AS title,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN COALESCE(c.attributes->>'narrative',m.content) ELSE m.content END AS narrative,
 (c.attributes->>'support_count')::integer AS support_count,
 (c.attributes->>'contradiction_count')::integer AS contradiction_count,
 COALESCE((c.attributes->>'confidence')::numeric,1::numeric) AS confidence,
 m.valid_from AS valid_from,
 m.valid_until AS valid_to,
 CASE WHEN m.state='active' THEN 'published' WHEN m.state='retired' THEN 'retired' ELSE COALESCE(c.attributes->>'lifecycle_state','published') END AS lifecycle_state,
 (c.attributes->>'pipeline_version')::text AS pipeline_version,
 (c.attributes->>'model_name')::text AS model_name,
 (c.attributes->>'prompt_version')::text AS prompt_version,
 m.project_id AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL THEN c.search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 m.updated_at AS updated_at,
 (c.attributes->>'project_entity_id')::uuid AS project_entity_id,
 (c.attributes->>'last_supported_at')::timestamp with time zone AS last_supported_at,
 COALESCE((c.attributes->>'priority')::integer,5) AS priority,
 t.name AS tenant,
 (c.attributes->>'namespace')::text AS namespace,
 CASE WHEN m.state='active' THEN CASE WHEN c.attributes->>'governance_status'='validated' THEN 'validated' ELSE 'active' END WHEN m.state='retired' THEN 'retired' ELSE COALESCE(NULLIF(NULLIF(c.attributes->>'governance_status','active'),'validated'),'suppressed') END AS governance_status,
 (c.attributes->>'source_class')::text AS source_class,
 (c.attributes->>'trust_class')::text AS trust_class,
 (c.attributes->>'salience')::numeric(4,3) AS salience,
 (c.attributes->>'strength')::numeric(4,3) AS strength,
 (c.attributes->>'stability')::numeric(4,3) AS stability,
 (c.attributes->>'quality_score')::numeric(4,3) AS quality_score,
 (c.attributes->>'summary_fidelity_score')::numeric(4,3) AS summary_fidelity_score,
 (c.attributes->>'token_count')::integer AS token_count,
 (c.attributes->>'schema_version')::integer AS schema_version,
 (c.attributes->>'config_version')::text AS config_version,
 CASE WHEN (c.attributes->'__braincore_sql_null_columns') ? 'redaction_metadata' THEN NULL ELSE c.attributes->'redaction_metadata' END AS redaction_metadata,
 CASE WHEN (c.attributes->'__braincore_sql_null_columns') ? 'governance_meta' THEN NULL ELSE c.attributes->'governance_meta' END AS governance_meta,
 (c.attributes->>'last_reinforced_at')::timestamp with time zone AS last_reinforced_at,
 (c.attributes->>'last_decayed_at')::timestamp with time zone AS last_decayed_at
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
CROSS JOIN LATERAL (SELECT
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector,
 (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
WHERE m.object_type='memory' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND (m.legacy_source_id IS NULL OR c.ready);
CREATE OR REPLACE VIEW preserve.segment WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS segment_id,
 (c.attributes->>'artifact_id')::uuid AS artifact_id,
 (c.attributes->>'ordinal')::integer AS ordinal,
 (c.attributes->>'line_start')::integer AS line_start,
 (c.attributes->>'line_end')::integer AS line_end,
 (c.attributes->>'ts_start')::timestamp with time zone AS ts_start,
 (c.attributes->>'ts_end')::timestamp with time zone AS ts_end,
 (c.attributes->>'section_label')::text AS section_label,
 m.content AS content,
 m.project_id AS scope_path,
 (c.attributes->>'source_sha256')::text AS source_sha256,
 (c.attributes->>'source_relpath')::text AS source_relpath,
 (c.attributes->>'archive_member_path')::text AS archive_member_path,
 (c.attributes->>'excerpt_hash')::text AS excerpt_hash,
 (c.attributes->>'byte_start')::bigint AS byte_start,
 (c.attributes->>'byte_end')::bigint AS byte_end,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL THEN c.search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (c.attributes->>'project_entity_id')::uuid AS project_entity_id,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
CROSS JOIN LATERAL (SELECT
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector,
 (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
WHERE m.object_type='segment' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND (m.legacy_source_id IS NULL OR c.ready);
CREATE OR REPLACE VIEW preserve.episode WITH (security_barrier=true) AS
SELECT COALESCE(m.legacy_external_id,m.id) AS episode_id,
 (c.attributes->>'episode_type')::text AS episode_type,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN COALESCE(c.attributes->>'title',left(m.content,160)) ELSE left(m.content,160) END AS title,
 (c.attributes->>'start_at')::timestamp with time zone AS start_at,
 (c.attributes->>'end_at')::timestamp with time zone AS end_at,
 (c.attributes->>'severity')::text AS severity,
 (c.attributes->>'outcome')::text AS outcome,
 CASE WHEN c.content_hash=public.digest(m.content,'sha256') THEN COALESCE(c.attributes->>'summary',m.content) ELSE m.content END AS summary,
 (c.attributes->>'primary_artifact_id')::uuid AS primary_artifact_id,
 m.project_id AS scope_path,
 CASE WHEN m.embedding_state='ready' THEN m.embedding END AS embedding,
 CASE WHEN NOT m.legacy_fts_eligible THEN NULL WHEN c.content_hash=public.digest(m.content,'sha256') AND c.search_vector IS NOT NULL THEN c.search_vector ELSE m.search_vector END AS fts,
 m.created_at AS created_at,
 (c.attributes->>'project_entity_id')::uuid AS project_entity_id,
 t.name AS tenant
FROM braincore.memories m JOIN braincore.tenants t ON t.id=m.tenant_id
LEFT JOIN braincore.legacy_client_attributes x ON x.memory_id=m.id AND x.tenant_id=m.tenant_id
CROSS JOIN LATERAL (SELECT
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_attributes ELSE x.attributes END AS attributes,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_content_hash ELSE x.content_hash END AS content_hash,
 CASE WHEN m.legacy_attributes_ready THEN m.legacy_search_vector ELSE x.search_vector END AS search_vector,
 (m.legacy_attributes_ready OR x.memory_id IS NOT NULL) AS ready) c
WHERE m.object_type='episode' AND m.tenant_id=braincore.compat_tenant()
 AND braincore.compat_permits(m.scope) AND (m.state='active' OR current_setting('braincore.compat_include_inactive',true)='true')
 AND (m.legacy_source_id IS NULL OR c.ready);
CREATE TYPE preserve.assertion_class AS ENUM ('deterministic','human_curated','corroborated_llm','single_source_llm','retired');
CREATE TYPE preserve.entity_type AS ENUM ('device','service','project','file','config_item','pattern_scope','incident','session');
CREATE TYPE preserve.extraction_method AS ENUM ('rule','llm','human_curated');
CREATE TYPE preserve.extraction_status AS ENUM ('pending','running','success','partial','failed');
CREATE TYPE preserve.fact_kind AS ENUM ('state','cause','impact','decision','remediation','lesson','constraint','config_change','event');
CREATE TYPE preserve.fact_status AS ENUM ('active','superseded','contradicted','uncertain');
CREATE TYPE preserve.lifecycle_state AS ENUM ('draft','published','retired');
CREATE TYPE preserve.memory_governance_status AS ENUM ('candidate','archived','active','review_required','validated','disputed','quarantined','suppressed','retired');
CREATE TYPE preserve.memory_namespace AS ENUM ('working','episodic','semantic','procedural','policy');
CREATE TYPE preserve.memory_outbox_status AS ENUM ('pending','processing','completed','failed','dead_letter');
CREATE TYPE preserve.memory_source_class AS ENUM ('observed','user_stated','system_inferred','summary_derived','replay_derived','imported_knowledge','corrected_by_user');
CREATE TYPE preserve.memory_trust_class AS ENUM ('deterministic','human_curated','corroborated_llm','single_source_llm','retired_superseded');
CREATE TYPE preserve.memory_type AS ENUM ('entity_summary','pattern','playbook','heuristic');
CREATE TYPE preserve.preservation_state AS ENUM ('discovered','archived','extracted','published','blocked','failed','pending_escalation');
CREATE TYPE preserve.review_status AS ENUM ('pending','approved','rejected','deferred');
CREATE TYPE preserve.source_type AS ENUM ('opsvault_incident','claude_session','claude_plan','device_log','config_diff','project_doc','pai_memory','codex_session','codex_shared','discord_conversation','telegram_chat','monitoring_alert','asana_task','git_commit','research_paper','vestige_memory','pai_auto_memory');
CREATE VIEW preserve.archive_object WITH (security_barrier=true) AS
SELECT (a.payload->>'archive_id')::uuid AS archive_id,
 (a.payload->>'source_key')::text AS source_key,
 (a.payload->>'source_type')::text AS source_type,
 (a.payload->>'original_path')::text AS original_path,
 (a.payload->>'archive_path')::text AS archive_path,
 (a.payload->>'manifest_path')::text AS manifest_path,
 (a.payload->>'format')::text AS format,
 (a.payload->>'original_bytes')::bigint AS original_bytes,
 (a.payload->>'compressed_bytes')::bigint AS compressed_bytes,
 (a.payload->>'checksum_sha256')::text AS checksum_sha256,
 (a.payload->>'redundancy_count')::integer AS redundancy_count,
 (a.payload->>'restore_tested_at')::timestamp with time zone AS restore_tested_at,
 (a.payload->>'verified_at')::timestamp with time zone AS verified_at,
 (a.payload->>'host')::text AS host,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='archive_object' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER archive_object_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.archive_object FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.eval_case WITH (security_barrier=true) AS
SELECT (a.payload->>'eval_case_id')::uuid AS eval_case_id,
 (a.payload->>'artifact_id')::uuid AS artifact_id,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'gold_labels' THEN NULL ELSE a.payload->'gold_labels' END AS gold_labels,
 (a.payload->>'notes')::text AS notes,
 (a.payload->>'source_type')::text AS source_type,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='eval_case' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER eval_case_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.eval_case FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.eval_run WITH (security_barrier=true) AS
SELECT (a.payload->>'eval_run_id')::uuid AS eval_run_id,
 (a.payload->>'pipeline_version')::text AS pipeline_version,
 (a.payload->>'model_name')::text AS model_name,
 (a.payload->>'prompt_version')::text AS prompt_version,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'results' THEN NULL ELSE a.payload->'results' END AS results,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'metrics' THEN NULL ELSE a.payload->'metrics' END AS metrics,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='eval_run' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER eval_run_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.eval_run FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.extraction_run WITH (security_barrier=true) AS
SELECT (a.payload->>'run_id')::uuid AS run_id,
 (a.payload->>'artifact_id')::uuid AS artifact_id,
 (a.payload->>'pipeline_version')::text AS pipeline_version,
 (a.payload->>'model_name')::text AS model_name,
 (a.payload->>'prompt_version')::text AS prompt_version,
 (a.payload->>'schema_version')::integer AS schema_version,
 (a.payload->>'idempotency_key')::text AS idempotency_key,
 (a.payload->>'status')::preserve.extraction_status AS status,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'raw_output' THEN NULL ELSE a.payload->'raw_output' END AS raw_output,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'validation_errors' THEN NULL ELSE a.payload->'validation_errors' END AS validation_errors,
 (a.payload->>'duration_ms')::integer AS duration_ms,
 (a.payload->>'started_at')::timestamp with time zone AS started_at,
 (a.payload->>'finished_at')::timestamp with time zone AS finished_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='extraction_run' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER extraction_run_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.extraction_run FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.memory_edge_evidence WITH (security_barrier=true) AS
SELECT (a.payload->>'edge_evidence_id')::uuid AS edge_evidence_id,
 (a.payload->>'edge_id')::uuid AS edge_id,
 (a.payload->>'fact_id')::uuid AS fact_id,
 (a.payload->>'episode_id')::uuid AS episode_id,
 (a.payload->>'segment_id')::uuid AS segment_id,
 (a.payload->>'notes')::text AS notes,
 (a.payload->>'weight')::numeric(3,2) AS weight,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='memory_edge_evidence' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER memory_edge_evidence_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.memory_edge_evidence FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.project_service_map WITH (security_barrier=true) AS
SELECT (a.payload->>'project_entity_id')::uuid AS project_entity_id,
 (a.payload->>'service_name')::text AS service_name
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='project_service_map' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER project_service_map_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.project_service_map FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.publish_note WITH (security_barrier=true) AS
SELECT (a.payload->>'publish_id')::uuid AS publish_id,
 (a.payload->>'memory_id')::uuid AS memory_id,
 (a.payload->>'file_path')::text AS file_path,
 (a.payload->>'content_hash')::text AS content_hash,
 (a.payload->>'published_at')::timestamp with time zone AS published_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='publish_note' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER publish_note_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.publish_note FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.reflection_class WITH (security_barrier=true) AS
SELECT (a.payload->>'class_key')::text AS class_key,
 (a.payload->>'parent_class_key')::text AS parent_class_key,
 (a.payload->>'display_name')::text AS display_name,
 (a.payload->>'target_kind')::text AS target_kind,
 (a.payload->>'description')::text AS description,
 (a.payload->>'requires_evidence')::boolean AS requires_evidence,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'derivation_policy' THEN NULL ELSE a.payload->'derivation_policy' END AS derivation_policy,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='reflection_class' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER reflection_class_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.reflection_class FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.review_queue WITH (security_barrier=true) AS
SELECT (a.payload->>'review_id')::uuid AS review_id,
 (a.payload->>'target_type')::text AS target_type,
 (a.payload->>'target_id')::uuid AS target_id,
 (a.payload->>'reason')::text AS reason,
 (a.payload->>'status')::preserve.review_status AS status,
 (a.payload->>'reviewer_notes')::text AS reviewer_notes,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'resolved_at')::timestamp with time zone AS resolved_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='review_queue' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER review_queue_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.review_queue FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.schema_migration WITH (security_barrier=true) AS
SELECT (a.payload->>'migration_name')::text AS migration_name,
 (a.payload->>'checksum')::text AS checksum,
 (a.payload->>'applied_at')::timestamp with time zone AS applied_at,
 (a.payload->>'applied_by')::text AS applied_by,
 (a.payload->>'execution_ms')::integer AS execution_ms,
 (a.payload->>'baseline')::boolean AS baseline
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='schema_migration' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER schema_migration_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.schema_migration FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.event_frame WITH (security_barrier=true) AS
SELECT (a.payload->>'event_frame_id')::uuid AS event_frame_id,
 t.name AS tenant,
 (a.payload->>'frame_fingerprint')::text AS frame_fingerprint,
 (a.payload->>'episode_id')::uuid AS episode_id,
 (a.payload->>'event_id')::uuid AS event_id,
 (a.payload->>'source_fact_id')::uuid AS source_fact_id,
 (a.payload->>'event_type')::text AS event_type,
 (a.payload->>'actor_entity_id')::uuid AS actor_entity_id,
 (a.payload->>'action')::text AS action,
 (a.payload->>'target_entity_id')::uuid AS target_entity_id,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'object_value' THEN NULL ELSE a.payload->'object_value' END AS object_value,
 (a.payload->>'time_start')::timestamp with time zone AS time_start,
 (a.payload->>'time_end')::timestamp with time zone AS time_end,
 (a.payload->>'location_entity_id')::uuid AS location_entity_id,
 (a.payload->>'cause_fact_id')::uuid AS cause_fact_id,
 (a.payload->>'effect_fact_id')::uuid AS effect_fact_id,
 (a.payload->>'outcome')::text AS outcome,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'assertion_class')::preserve.assertion_class AS assertion_class,
 (a.payload->>'evidence_segment_id')::uuid AS evidence_segment_id,
 (a.payload->>'scope_path')::text AS scope_path,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'frame_json' THEN NULL ELSE a.payload->'frame_json' END AS frame_json,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='event_frame' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER event_frame_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.event_frame FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.media_artifact WITH (security_barrier=true) AS
SELECT (a.payload->>'media_artifact_id')::uuid AS media_artifact_id,
 t.name AS tenant,
 (a.payload->>'artifact_id')::uuid AS artifact_id,
 (a.payload->>'source_segment_id')::uuid AS source_segment_id,
 (a.payload->>'project_entity_id')::uuid AS project_entity_id,
 (a.payload->>'media_type')::text AS media_type,
 (a.payload->>'mime_type')::text AS mime_type,
 (a.payload->>'sha256')::text AS sha256,
 (a.payload->>'width_px')::integer AS width_px,
 (a.payload->>'height_px')::integer AS height_px,
 (a.payload->>'duration_ms')::integer AS duration_ms,
 (a.payload->>'page_count')::integer AS page_count,
 (a.payload->>'scope_path')::text AS scope_path,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'media_meta' THEN NULL ELSE a.payload->'media_meta' END AS media_meta,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at,
 (a.payload->>'ingest_run_id')::uuid AS ingest_run_id,
 (a.payload->>'ingest_batch_key')::text AS ingest_batch_key
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='media_artifact' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER media_artifact_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.media_artifact FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.memory_context_audit WITH (security_barrier=true) AS
SELECT (a.payload->>'audit_id')::uuid AS audit_id,
 t.name AS tenant,
 (a.payload->>'query')::text AS query,
 (a.payload->>'trigger')::text AS trigger,
 (a.payload->>'retrieved_memory_ids')::uuid[] AS retrieved_memory_ids,
 (a.payload->>'injected_memory_ids')::uuid[] AS injected_memory_ids,
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
WHERE a.source_table='memory_context_audit' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER memory_context_audit_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.memory_context_audit FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.memory_edge WITH (security_barrier=true) AS
SELECT (a.payload->>'edge_id')::uuid AS edge_id,
 t.name AS tenant,
 (a.payload->>'source_type')::text AS source_type,
 (a.payload->>'source_id')::uuid AS source_id,
 (a.payload->>'target_type')::text AS target_type,
 (a.payload->>'target_id')::uuid AS target_id,
 (a.payload->>'edge_type')::text AS edge_type,
 (a.payload->>'edge_fingerprint')::text AS edge_fingerprint,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'assertion_class')::preserve.assertion_class AS assertion_class,
 (a.payload->>'evidence_segment_id')::uuid AS evidence_segment_id,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'valid_from')::timestamp with time zone AS valid_from,
 (a.payload->>'valid_to')::timestamp with time zone AS valid_to,
 (a.payload->>'scope_path')::text AS scope_path,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='memory_edge' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER memory_edge_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.memory_edge FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.procedure WITH (security_barrier=true) AS
SELECT (a.payload->>'procedure_id')::uuid AS procedure_id,
 t.name AS tenant,
 (a.payload->>'procedure_fingerprint')::text AS procedure_fingerprint,
 (a.payload->>'title')::text AS title,
 (a.payload->>'summary')::text AS summary,
 (a.payload->>'source_fact_id')::uuid AS source_fact_id,
 (a.payload->>'source_memory_id')::uuid AS source_memory_id,
 (a.payload->>'source_episode_id')::uuid AS source_episode_id,
 (a.payload->>'scope_entity_id')::uuid AS scope_entity_id,
 (a.payload->>'project_entity_id')::uuid AS project_entity_id,
 (a.payload->>'evidence_segment_id')::uuid AS evidence_segment_id,
 (a.payload->>'assertion_class')::preserve.assertion_class AS assertion_class,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'lifecycle_state')::preserve.lifecycle_state AS lifecycle_state,
 (a.payload->>'scope_path')::text AS scope_path,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'procedure_json' THEN NULL ELSE a.payload->'procedure_json' END AS procedure_json,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at,
 (a.payload->>'fts')::tsvector AS fts
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='procedure' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER procedure_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.procedure FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.procedure_step WITH (security_barrier=true) AS
SELECT (a.payload->>'procedure_step_id')::uuid AS procedure_step_id,
 (a.payload->>'procedure_id')::uuid AS procedure_id,
 t.name AS tenant,
 (a.payload->>'step_index')::integer AS step_index,
 (a.payload->>'action')::text AS action,
 (a.payload->>'expected_result')::text AS expected_result,
 (a.payload->>'source_fact_id')::uuid AS source_fact_id,
 (a.payload->>'evidence_segment_id')::uuid AS evidence_segment_id,
 (a.payload->>'assertion_class')::preserve.assertion_class AS assertion_class,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'scope_path')::text AS scope_path,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'step_json' THEN NULL ELSE a.payload->'step_json' END AS step_json,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='procedure_step' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER procedure_step_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.procedure_step FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.task_session WITH (security_barrier=true) AS
SELECT (a.payload->>'session_id')::uuid AS session_id,
 t.name AS tenant,
 (a.payload->>'session_key')::text AS session_key,
 (a.payload->>'agent_name')::text AS agent_name,
 (a.payload->>'task_title')::text AS task_title,
 (a.payload->>'status')::text AS status,
 (a.payload->>'source_artifact_id')::uuid AS source_artifact_id,
 (a.payload->>'parent_session_id')::uuid AS parent_session_id,
 (a.payload->>'scope_entity_id')::uuid AS scope_entity_id,
 (a.payload->>'project_entity_id')::uuid AS project_entity_id,
 (a.payload->>'scope_path')::text AS scope_path,
 (a.payload->>'started_at')::timestamp with time zone AS started_at,
 (a.payload->>'last_seen_at')::timestamp with time zone AS last_seen_at,
 (a.payload->>'ended_at')::timestamp with time zone AS ended_at,
 (a.payload->>'expires_at')::timestamp with time zone AS expires_at,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'session_json' THEN NULL ELSE a.payload->'session_json' END AS session_json,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='task_session' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER task_session_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.task_session FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.visual_region WITH (security_barrier=true) AS
SELECT (a.payload->>'visual_region_id')::uuid AS visual_region_id,
 t.name AS tenant,
 (a.payload->>'media_artifact_id')::uuid AS media_artifact_id,
 (a.payload->>'region_fingerprint')::text AS region_fingerprint,
 (a.payload->>'region_type')::text AS region_type,
 (a.payload->>'page_number')::integer AS page_number,
 (a.payload->>'x_min')::numeric(10,6) AS x_min,
 (a.payload->>'y_min')::numeric(10,6) AS y_min,
 (a.payload->>'x_max')::numeric(10,6) AS x_max,
 (a.payload->>'y_max')::numeric(10,6) AS y_max,
 (a.payload->>'coordinate_space')::text AS coordinate_space,
 (a.payload->>'label')::text AS label,
 (a.payload->>'source_segment_id')::uuid AS source_segment_id,
 (a.payload->>'linked_entity_id')::uuid AS linked_entity_id,
 (a.payload->>'linked_fact_id')::uuid AS linked_fact_id,
 (a.payload->>'linked_memory_id')::uuid AS linked_memory_id,
 (a.payload->>'linked_event_frame_id')::uuid AS linked_event_frame_id,
 (a.payload->>'linked_procedure_id')::uuid AS linked_procedure_id,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'assertion_class')::preserve.assertion_class AS assertion_class,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'region_meta' THEN NULL ELSE a.payload->'region_meta' END AS region_meta,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at,
 (a.payload->>'ingest_run_id')::uuid AS ingest_run_id,
 (a.payload->>'ingest_batch_key')::text AS ingest_batch_key
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='visual_region' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER visual_region_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.visual_region FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.working_memory WITH (security_barrier=true) AS
SELECT (a.payload->>'working_memory_id')::uuid AS working_memory_id,
 t.name AS tenant,
 (a.payload->>'session_id')::uuid AS session_id,
 (a.payload->>'working_memory_fingerprint')::text AS working_memory_fingerprint,
 (a.payload->>'memory_kind')::text AS memory_kind,
 (a.payload->>'content')::text AS content,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'content_json' THEN NULL ELSE a.payload->'content_json' END AS content_json,
 (a.payload->>'source_segment_id')::uuid AS source_segment_id,
 (a.payload->>'source_fact_id')::uuid AS source_fact_id,
 (a.payload->>'evidence_segment_id')::uuid AS evidence_segment_id,
 (a.payload->>'confidence')::numeric(3,2) AS confidence,
 (a.payload->>'promotion_status')::text AS promotion_status,
 (a.payload->>'promotion_reason')::text AS promotion_reason,
 (a.payload->>'promotion_block_reason')::text AS promotion_block_reason,
 (a.payload->>'promoted_memory_id')::uuid AS promoted_memory_id,
 (a.payload->>'expires_at')::timestamp with time zone AS expires_at,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at,
 (a.payload->>'updated_at')::timestamp with time zone AS updated_at,
 (a.payload->>'promotion_target_kind')::text AS promotion_target_kind,
 (a.payload->>'promotion_target_id')::uuid AS promotion_target_id,
 (a.payload->>'promotion_marked_at')::timestamp with time zone AS promotion_marked_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='working_memory' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER working_memory_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.working_memory FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.event WITH (security_barrier=true) AS
SELECT (a.payload->>'event_id')::uuid AS event_id,
 (a.payload->>'episode_id')::uuid AS episode_id,
 (a.payload->>'event_type')::text AS event_type,
 (a.payload->>'occurred_at')::timestamp with time zone AS occurred_at,
 (a.payload->>'description')::text AS description,
 (a.payload->>'entity_id')::uuid AS entity_id,
 (a.payload->>'segment_id')::uuid AS segment_id,
 CASE WHEN (a.payload->'__braincore_sql_null_columns') ? 'meta' THEN NULL ELSE a.payload->'meta' END AS meta,
 (a.payload->>'created_run_id')::uuid AS created_run_id,
 (a.payload->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE a.source_table='event' AND NOT a.deleted AND a.tenant_id=braincore.compat_tenant()
 AND a.scope IS NOT NULL AND braincore.compat_permits(a.scope);
CREATE TRIGGER event_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.event FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.artifact WITH (security_barrier=true) AS
SELECT COALESCE((a.legacy_attributes->>'artifact_id')::uuid,a.id) AS artifact_id,
 a.source_type AS source_type,
 COALESCE(a.legacy_attributes->>'source_key',a.source_uri) AS source_key,
 COALESCE(a.legacy_attributes->>'original_path',a.source_uri) AS original_path,
 (a.legacy_attributes->>'host')::text AS host,
 (a.legacy_attributes->>'ttl_at')::timestamp with time zone AS ttl_at,
 (a.legacy_attributes->>'sha256')::text AS sha256,
 (a.legacy_attributes->>'size_bytes')::bigint AS size_bytes,
 (a.legacy_attributes->>'value_score')::numeric(5,2) AS value_score,
 (a.legacy_attributes->>'preservation_state')::preserve.preservation_state AS preservation_state,
 (a.legacy_attributes->>'can_evict_hot')::boolean AS can_evict_hot,
 (a.legacy_attributes->>'can_query_raw')::boolean AS can_query_raw,
 (a.legacy_attributes->>'can_promote_memory')::boolean AS can_promote_memory,
 a.scope->>'project_id' AS scope_path,
 CASE WHEN (a.legacy_attributes->'__braincore_sql_null_columns') ? 'meta' THEN NULL ELSE a.legacy_attributes->'meta' END AS meta,
 (a.legacy_attributes->>'opsvault_file_id')::uuid AS opsvault_file_id,
 (a.legacy_attributes->>'opsvault_incident_id')::uuid AS opsvault_incident_id,
 (a.legacy_attributes->>'discovered_at')::timestamp with time zone AS discovered_at,
 COALESCE((a.legacy_attributes->>'updated_at')::timestamptz,a.created_at) AS updated_at,
 (a.legacy_attributes->>'project_entity_id')::uuid AS project_entity_id,
 (a.legacy_attributes->>'priority')::integer AS priority,
 t.name AS tenant
FROM braincore.artifacts a JOIN braincore.tenants t ON t.id=a.tenant_id
WHERE (a.tenant_id=braincore.compat_tenant() AND braincore.compat_permits(a.scope))
 OR EXISTS (SELECT 1 FROM braincore.artifact_access_grants g
   WHERE g.artifact_id=a.id AND g.owner_tenant_id=a.tenant_id
     AND g.grantee_tenant_id=braincore.compat_tenant() AND g.revoked_at IS NULL
     AND (g.expires_at IS NULL OR g.expires_at>statement_timestamp())
     AND braincore.compat_permits(g.scope));
CREATE TRIGGER artifact_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.artifact FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.fact_evidence WITH (security_barrier=true) AS
SELECT (g.attributes->>'fact_evidence_id')::uuid AS fact_evidence_id,
 COALESCE(m.legacy_external_id,m.id) AS fact_id,
 COALESCE(s.legacy_external_id,s.id) AS segment_id,
 (g.attributes->>'excerpt')::text AS excerpt,
 (g.attributes->>'extraction_method')::preserve.extraction_method AS extraction_method,
 (g.attributes->>'weight')::numeric(3,2) AS weight,
 (g.attributes->>'source_sha256')::text AS source_sha256,
 (g.attributes->>'source_relpath')::text AS source_relpath,
 (g.attributes->>'line_start')::integer AS line_start,
 (g.attributes->>'line_end')::integer AS line_end,
 (g.attributes->>'excerpt_hash')::text AS excerpt_hash,
 (g.attributes->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_fact_evidence g
JOIN braincore.memories m ON m.id=g.native_fact_id AND m.tenant_id=g.tenant_id
LEFT JOIN braincore.memories s ON s.id=g.native_segment_id AND s.tenant_id=g.tenant_id
WHERE NOT g.deleted AND g.attributes IS NOT NULL AND m.state='active'
 AND m.tenant_id=braincore.compat_tenant() AND braincore.compat_permits(m.scope);
CREATE TRIGGER fact_evidence_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.fact_evidence FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();
CREATE VIEW preserve.memory_support WITH (security_barrier=true) AS
SELECT (g.attributes->>'support_id')::uuid AS support_id,
 COALESCE(m.legacy_external_id,m.id) AS memory_id,
 COALESCE(f.legacy_external_id,f.id) AS fact_id,
 COALESCE(e.legacy_external_id,e.id) AS episode_id,
 (g.attributes->>'support_type')::text AS support_type,
 (g.attributes->>'notes')::text AS notes,
 (g.attributes->>'created_at')::timestamp with time zone AS created_at
FROM braincore.legacy_memory_support g
JOIN braincore.memories m ON m.id=g.native_memory_id AND m.tenant_id=g.tenant_id
LEFT JOIN braincore.memories f ON f.id=g.native_fact_id AND f.tenant_id=g.tenant_id
LEFT JOIN braincore.memories e ON e.id=g.native_episode_id AND e.tenant_id=g.tenant_id
WHERE NOT g.deleted AND g.attributes IS NOT NULL AND m.state='active'
 AND m.tenant_id=braincore.compat_tenant() AND braincore.compat_permits(m.scope);
CREATE TRIGGER memory_support_canonical_mutation_required INSTEAD OF INSERT OR UPDATE OR DELETE ON preserve.memory_support FOR EACH ROW EXECUTE FUNCTION braincore.block_legacy_sql_mutation();

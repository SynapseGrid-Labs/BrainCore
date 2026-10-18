-- Accepted native semantic writes permanently retain source-import authority.
-- Audit rows remain compatible with existing administration/tenant cleanup.
-- Runtime roles must not receive write privileges on this maintenance-owned table.
CREATE TABLE braincore.legacy_native_authority (
  relation_name text NOT NULL CHECK (relation_name IN
    ('memories','artifacts','legacy_fact_evidence','legacy_memory_support',
     'legacy_auxiliary_objects')),
  native_id uuid NOT NULL,
  tenant_id uuid REFERENCES braincore.tenants(id) ON DELETE CASCADE,
  first_audit_id uuid NOT NULL,
  imported_payload_hash bytea,
  PRIMARY KEY (relation_name,native_id)
);
COMMENT ON COLUMN braincore.legacy_native_authority.first_audit_id IS
  'First retained authority selector; deterministic UUID order is not audit chronology.';
REVOKE ALL ON braincore.legacy_native_authority FROM PUBLIC;

CREATE FUNCTION braincore.record_native_import_authority() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE relation_name text; owner_id uuid; original_hash bytea;
BEGIN
  IF TG_OP <> 'INSERT' OR TG_RELID <> 'braincore.audit_events'::regclass THEN
    RAISE EXCEPTION 'invalid authority trigger origin' USING ERRCODE='42501';
  END IF;
  IF NEW.target_type='memory' AND NEW.action IN
      ('memory.remember','memory.revise','memory.lifecycle','legacy_client.upsert',
       'legacy_client.suppress','legacy_client.retire','legacy_client.restore') THEN
    relation_name := 'memories';
    SELECT m.tenant_id,o.imported_payload_hash INTO owner_id,original_hash
      FROM braincore.memories m LEFT JOIN braincore.legacy_object_map o
        ON o.native_memory_id=m.id AND o.tenant_id=m.tenant_id
      WHERE m.id=NEW.target_id AND m.tenant_id=NEW.tenant_id;
  ELSIF NEW.target_type='artifact' AND NEW.action IN
      ('ingestion.accept','legacy_client.artifact','evidence.grant','evidence.revoke') THEN
    relation_name := 'artifacts';
    SELECT a.tenant_id,o.imported_payload_hash INTO owner_id,original_hash
      FROM braincore.artifacts a LEFT JOIN braincore.legacy_artifact_map o
        ON o.native_artifact_id=a.id AND o.tenant_id=a.tenant_id
      WHERE a.id=NEW.target_id AND a.tenant_id=NEW.tenant_id;
  ELSIF NEW.target_type='fact_evidence' AND NEW.action='legacy_client.fact_evidence' THEN
    relation_name := 'legacy_fact_evidence';
    SELECT tenant_id,imported_payload_hash INTO owner_id,original_hash
      FROM braincore.legacy_fact_evidence
      WHERE native_id=NEW.target_id AND tenant_id=NEW.tenant_id;
  ELSIF NEW.target_type='memory_support' AND NEW.action='legacy_client.memory_support' THEN
    relation_name := 'legacy_memory_support';
    SELECT tenant_id,imported_payload_hash INTO owner_id,original_hash
      FROM braincore.legacy_memory_support
      WHERE native_id=NEW.target_id AND tenant_id=NEW.tenant_id;
  ELSE
    -- Only accepted corrections of actual mapped auxiliary objects count.
    -- Source imports and embedding workers do not insert these audit actions.
    relation_name := 'legacy_auxiliary_objects';
    SELECT a.tenant_id,a.imported_payload_hash INTO owner_id,original_hash
      FROM braincore.legacy_auxiliary_objects a WHERE a.native_id=NEW.target_id
       AND (a.tenant_id=NEW.tenant_id OR a.tenant_id IS NULL)
       AND ((NEW.target_type='legacy_auxiliary' AND NEW.action IN
              ('legacy.auxiliary.created','legacy.auxiliary.corrected'))
         OR (NEW.target_type=a.source_table AND NEW.action='legacy_client.'||a.source_table));
  END IF;
  IF FOUND THEN
    INSERT INTO braincore.legacy_native_authority
      (relation_name,native_id,tenant_id,first_audit_id,imported_payload_hash)
      VALUES (relation_name,NEW.target_id,owner_id,NEW.id,original_hash)
      ON CONFLICT DO NOTHING;
  END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION braincore.record_native_import_authority() FROM PUBLIC;
CREATE TRIGGER audit_native_import_authority AFTER INSERT ON braincore.audit_events
FOR EACH ROW EXECUTE FUNCTION braincore.record_native_import_authority();

-- Conservative backfill: retained accepted audits establish current authority.
-- Deleted historical audits cannot be reconstructed or claimed complete.
INSERT INTO braincore.legacy_native_authority
  (relation_name,native_id,tenant_id,first_audit_id,imported_payload_hash)
SELECT DISTINCT ON (relation_name,native_id) * FROM (
  SELECT 'memories'::text AS relation_name,m.id AS native_id,m.tenant_id,a.id AS first_audit_id,
    o.imported_payload_hash
  FROM braincore.audit_events a JOIN braincore.memories m
    ON m.id=a.target_id AND m.tenant_id=a.tenant_id
  LEFT JOIN braincore.legacy_object_map o ON o.native_memory_id=m.id AND o.tenant_id=m.tenant_id
  WHERE a.target_type='memory' AND a.action IN
    ('memory.remember','memory.revise','memory.lifecycle','legacy_client.upsert',
     'legacy_client.suppress','legacy_client.retire','legacy_client.restore')
  UNION ALL
  SELECT 'artifacts',m.id,m.tenant_id,a.id,o.imported_payload_hash
  FROM braincore.audit_events a JOIN braincore.artifacts m
    ON m.id=a.target_id AND m.tenant_id=a.tenant_id
  LEFT JOIN braincore.legacy_artifact_map o ON o.native_artifact_id=m.id AND o.tenant_id=m.tenant_id
  WHERE a.target_type='artifact' AND a.action IN
    ('ingestion.accept','legacy_client.artifact','evidence.grant','evidence.revoke')
  UNION ALL
  SELECT 'legacy_fact_evidence',m.native_id,m.tenant_id,a.id,m.imported_payload_hash
  FROM braincore.audit_events a JOIN braincore.legacy_fact_evidence m
    ON m.native_id=a.target_id AND m.tenant_id=a.tenant_id
  WHERE a.target_type='fact_evidence' AND a.action='legacy_client.fact_evidence'
  UNION ALL
  SELECT 'legacy_memory_support',m.native_id,m.tenant_id,a.id,m.imported_payload_hash
  FROM braincore.audit_events a JOIN braincore.legacy_memory_support m
    ON m.native_id=a.target_id AND m.tenant_id=a.tenant_id
  WHERE a.target_type='memory_support' AND a.action='legacy_client.memory_support'
  UNION ALL
  SELECT 'legacy_auxiliary_objects',m.native_id,m.tenant_id,a.id,m.imported_payload_hash
  FROM braincore.audit_events a JOIN braincore.legacy_auxiliary_objects m
    ON m.native_id=a.target_id AND (m.tenant_id=a.tenant_id OR m.tenant_id IS NULL)
  WHERE (a.target_type='legacy_auxiliary' AND a.action IN
           ('legacy.auxiliary.created','legacy.auxiliary.corrected'))
     OR (a.target_type=m.source_table AND a.action='legacy_client.'||m.source_table)
) retained ORDER BY relation_name,native_id,first_audit_id
ON CONFLICT DO NOTHING;

CREATE FUNCTION braincore.source_import_allowed(relation_name text,native_id uuid,
                                                incoming_hash bytea)
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE prior_hash bytea;
BEGIN
  SELECT a.imported_payload_hash INTO prior_hash
    FROM braincore.legacy_native_authority a
    WHERE a.relation_name=source_import_allowed.relation_name
      AND a.native_id=source_import_allowed.native_id;
  IF NOT FOUND THEN RETURN true; END IF;
  IF prior_hash IS NOT NULL AND prior_hash=incoming_hash THEN RETURN false; END IF;
  RAISE EXCEPTION 'source_native_authority_conflict' USING ERRCODE='23514';
END;
$$;

-- Ownership and access are separate. A grant never changes an artifact owner.
CREATE TABLE braincore.artifact_access_grants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  artifact_id uuid NOT NULL,
  owner_tenant_id uuid NOT NULL,
  grantee_tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  scope jsonb NOT NULL,
  reason text NOT NULL CHECK (length(btrim(reason)) > 0),
  actor_key_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  revoked_at timestamptz,
  UNIQUE (id, artifact_id, owner_tenant_id, grantee_tenant_id),
  FOREIGN KEY (artifact_id, owner_tenant_id) REFERENCES braincore.artifacts(id, tenant_id),
  FOREIGN KEY (actor_key_id, owner_tenant_id) REFERENCES braincore.api_keys(id, tenant_id),
  CHECK (owner_tenant_id <> grantee_tenant_id),
  CHECK (scope ?| ARRAY['project_id','user_id','agent_id','run_id']),
  CHECK (scope - 'project_id' - 'user_id' - 'agent_id' - 'run_id' = '{}'::jsonb),
  CHECK (jsonb_typeof(scope) = 'object'),
  CHECK (NOT jsonb_path_exists(scope, '$.* ? (@.type() != "string" || @ == "")')),
  CHECK (expires_at IS NULL OR expires_at > created_at)
);
CREATE INDEX artifact_access_grantee_idx ON braincore.artifact_access_grants
  (grantee_tenant_id, artifact_id) WHERE revoked_at IS NULL;

ALTER TABLE braincore.memory_evidence ADD COLUMN artifact_tenant_id uuid;
UPDATE braincore.memory_evidence SET artifact_tenant_id = tenant_id;
ALTER TABLE braincore.memory_evidence ALTER COLUMN artifact_tenant_id SET NOT NULL;
ALTER TABLE braincore.memory_evidence ADD COLUMN access_grant_id uuid;
ALTER TABLE braincore.memory_evidence
  DROP CONSTRAINT memory_evidence_artifact_id_tenant_id_fkey,
  ADD FOREIGN KEY (artifact_id, artifact_tenant_id)
    REFERENCES braincore.artifacts(id, tenant_id),
  ADD FOREIGN KEY (access_grant_id, artifact_id, artifact_tenant_id, tenant_id)
    REFERENCES braincore.artifact_access_grants(id, artifact_id, owner_tenant_id, grantee_tenant_id),
  ADD CHECK ((artifact_tenant_id = tenant_id AND access_grant_id IS NULL)
             OR (artifact_tenant_id <> tenant_id AND access_grant_id IS NOT NULL));

CREATE FUNCTION braincore.check_evidence_access() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  -- Existing native writers retain their same-owner contract.
  NEW.artifact_tenant_id := COALESCE(NEW.artifact_tenant_id, NEW.tenant_id);
  IF NEW.artifact_tenant_id <> NEW.tenant_id THEN
    PERFORM 1 FROM braincore.artifact_access_grants g
      JOIN braincore.memories m ON m.id = NEW.memory_id AND m.tenant_id = NEW.tenant_id
      WHERE g.id = NEW.access_grant_id AND g.artifact_id = NEW.artifact_id
        AND g.owner_tenant_id = NEW.artifact_tenant_id AND g.grantee_tenant_id = NEW.tenant_id
        AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at > statement_timestamp())
        AND m.scope @> g.scope FOR SHARE OF g, m;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'shared evidence requires a current scoped grant' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_evidence_access BEFORE INSERT OR UPDATE ON braincore.memory_evidence
  FOR EACH ROW EXECUTE FUNCTION braincore.check_evidence_access();

CREATE FUNCTION braincore.keep_grant_identity() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF (to_jsonb(NEW) - 'revoked_at') IS DISTINCT FROM (to_jsonb(OLD) - 'revoked_at')
     OR OLD.revoked_at IS NOT NULL THEN
    RAISE EXCEPTION 'evidence grants are immutable except for first revocation' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER artifact_access_grant_identity BEFORE UPDATE ON braincore.artifact_access_grants
  FOR EACH ROW EXECUTE FUNCTION braincore.keep_grant_identity();

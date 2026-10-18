CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS braincore;

CREATE TABLE IF NOT EXISTS braincore.tenants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS braincore.api_keys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  name text NOT NULL,
  prefix text NOT NULL UNIQUE,
  digest bytea NOT NULL UNIQUE,
  roles text[] NOT NULL,
  scope jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  last_used_at timestamptz,
  revoked_at timestamptz,
  UNIQUE (id, tenant_id),
  CHECK (cardinality(roles) > 0),
  CHECK (roles <@ ARRAY['read','write','admin']::text[]),
  CHECK (
    ('admin' = ANY(roles) AND scope IS NULL)
    OR
    (
      'admin' <> ALL(roles) AND scope IS NOT NULL AND jsonb_typeof(scope) = 'object'
      AND scope ?| ARRAY['project_id','user_id','agent_id','run_id']
      AND scope - 'project_id' - 'user_id' - 'agent_id' - 'run_id' = '{}'::jsonb
    )
  )
);

CREATE TABLE IF NOT EXISTS braincore.dashboard_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  api_key_id uuid NOT NULL,
  digest bytea NOT NULL UNIQUE,
  csrf_digest bytea NOT NULL,
  roles text[] NOT NULL,
  scope jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  last_used_at timestamptz,
  revoked_at timestamptz,
  FOREIGN KEY (api_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.operations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  actor_key_id uuid NOT NULL,
  kind text NOT NULL,
  state text NOT NULL DEFAULT 'queued',
  accepted_items integer NOT NULL DEFAULT 0,
  succeeded_items integer NOT NULL DEFAULT 0,
  failed_items integer NOT NULL DEFAULT 0,
  warning_items integer NOT NULL DEFAULT 0,
  error jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  UNIQUE (id, tenant_id),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id),
  CHECK (state IN ('queued','running','succeeded','partial','failed','dead_letter'))
);

CREATE TABLE IF NOT EXISTS braincore.idempotency_records (
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  actor_key_id uuid NOT NULL REFERENCES braincore.api_keys(id),
  key text NOT NULL,
  request_hash bytea NOT NULL,
  operation_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL DEFAULT now() + interval '7 days',
  PRIMARY KEY (tenant_id, actor_key_id, key),
  FOREIGN KEY (operation_id, tenant_id) REFERENCES braincore.operations(id, tenant_id),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.mutation_receipts (
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  actor_key_id uuid NOT NULL REFERENCES braincore.api_keys(id),
  key text NOT NULL,
  kind text NOT NULL,
  request_hash bytea NOT NULL,
  response jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL DEFAULT now() + interval '7 days',
  PRIMARY KEY (tenant_id, actor_key_id, key),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idempotency_records_expiry_idx
  ON braincore.idempotency_records (expires_at);
CREATE INDEX IF NOT EXISTS mutation_receipts_expiry_idx
  ON braincore.mutation_receipts (expires_at);

CREATE TABLE IF NOT EXISTS braincore.artifacts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  source_type text NOT NULL,
  source_uri text,
  content text NOT NULL,
  content_sha256 bytea NOT NULL,
  scope jsonb NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id, tenant_id),
  CHECK (scope ?| ARRAY['project_id','user_id','agent_id','run_id']),
  CHECK (scope - 'project_id' - 'user_id' - 'agent_id' - 'run_id' = '{}'::jsonb)
);

CREATE INDEX IF NOT EXISTS artifacts_tenant_content_idx
  ON braincore.artifacts (tenant_id, content_sha256);
CREATE INDEX IF NOT EXISTS artifacts_scope_idx
  ON braincore.artifacts USING gin(scope);

CREATE TABLE IF NOT EXISTS braincore.memories (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  content text NOT NULL,
  dedupe_key bytea NOT NULL,
  project_id text,
  user_id text,
  agent_id text,
  run_id text,
  scope jsonb NOT NULL,
  reference_namespace text[],
  reference_key text,
  state text NOT NULL DEFAULT 'active',
  trust text NOT NULL DEFAULT 'stated',
  valid_from timestamptz NOT NULL DEFAULT now(),
  valid_until timestamptz,
  embedding vector(384),
  embedding_state text NOT NULL DEFAULT 'pending',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
  UNIQUE (id, tenant_id),
  CHECK (project_id IS NOT NULL OR user_id IS NOT NULL OR agent_id IS NOT NULL OR run_id IS NOT NULL),
  CHECK (scope ?| ARRAY['project_id','user_id','agent_id','run_id']),
  CHECK (scope - 'project_id' - 'user_id' - 'agent_id' - 'run_id' = '{}'::jsonb),
  CHECK ((scope->>'project_id') IS NOT DISTINCT FROM project_id),
  CHECK ((scope->>'user_id') IS NOT DISTINCT FROM user_id),
  CHECK ((scope->>'agent_id') IS NOT DISTINCT FROM agent_id),
  CHECK ((scope->>'run_id') IS NOT DISTINCT FROM run_id),
  CHECK ((reference_namespace IS NULL) = (reference_key IS NULL)),
  CHECK (state IN ('active','suppressed','retired')),
  CHECK (trust IN ('stated','extracted','corroborated','disputed')),
  CHECK (embedding_state IN ('pending','ready','failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS memories_tenant_dedupe_idx
  ON braincore.memories (tenant_id, dedupe_key);
CREATE UNIQUE INDEX IF NOT EXISTS memories_tenant_reference_idx
  ON braincore.memories (
    tenant_id,
    COALESCE(project_id,''),
    COALESCE(user_id,''),
    COALESCE(agent_id,''),
    COALESCE(run_id,''),
    reference_namespace,
    reference_key
  )
  WHERE reference_namespace IS NOT NULL;

CREATE INDEX IF NOT EXISTS memories_tenant_scope_idx ON braincore.memories
  (tenant_id, project_id, user_id, agent_id, run_id, state);
CREATE INDEX IF NOT EXISTS memories_scope_json_idx ON braincore.memories USING gin(scope);
CREATE INDEX IF NOT EXISTS memories_fts_idx ON braincore.memories USING gin(search_vector);
CREATE INDEX IF NOT EXISTS memories_embedding_idx ON braincore.memories
  USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS braincore.memory_revisions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  memory_id uuid NOT NULL,
  prior_revision_id uuid,
  content text NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id, tenant_id),
  FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id),
  FOREIGN KEY (prior_revision_id, tenant_id)
    REFERENCES braincore.memory_revisions(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.memory_evidence (
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  memory_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  quote text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (memory_id, artifact_id),
  FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id),
  FOREIGN KEY (artifact_id, tenant_id) REFERENCES braincore.artifacts(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.lifecycle_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  memory_id uuid NOT NULL,
  action text NOT NULL,
  prior_state text NOT NULL,
  next_state text NOT NULL,
  reason text NOT NULL,
  actor_key_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (action IN ('suppress','retire','restore','promote')),
  FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.feedback (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  memory_id uuid NOT NULL,
  rating smallint NOT NULL,
  comment text,
  actor_key_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (rating BETWEEN -1 AND 1),
  FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.operation_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  operation_id uuid NOT NULL,
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  item_index integer NOT NULL,
  state text NOT NULL DEFAULT 'queued',
  memory_id uuid,
  result jsonb,
  error jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (operation_id, item_index),
  UNIQUE (id, tenant_id),
  CHECK (state IN ('queued','running','succeeded','failed','partial')),
  FOREIGN KEY (operation_id, tenant_id) REFERENCES braincore.operations(id, tenant_id),
  FOREIGN KEY (memory_id, tenant_id) REFERENCES braincore.memories(id, tenant_id)
);

CREATE TABLE IF NOT EXISTS braincore.jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  operation_id uuid NOT NULL,
  operation_item_id uuid NOT NULL,
  kind text NOT NULL,
  payload jsonb NOT NULL,
  state text NOT NULL DEFAULT 'queued',
  attempts integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 5,
  available_at timestamptz NOT NULL DEFAULT now(),
  leased_until timestamptz,
  leased_by text,
  last_error jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK (state IN ('queued','running','succeeded','failed','dead_letter')),
  CHECK (kind IN ('embed_memory','extract_ingestion')),
  CHECK (attempts >= 0 AND max_attempts > 0),
  FOREIGN KEY (operation_id, tenant_id) REFERENCES braincore.operations(id, tenant_id),
  FOREIGN KEY (operation_item_id, tenant_id)
    REFERENCES braincore.operation_items(id, tenant_id)
);
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON braincore.jobs (state, available_at, created_at);

CREATE TABLE IF NOT EXISTS braincore.audit_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES braincore.tenants(id),
  actor_key_id uuid,
  action text NOT NULL,
  target_type text NOT NULL,
  target_id uuid,
  request_id text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (actor_key_id, tenant_id) REFERENCES braincore.api_keys(id, tenant_id)
);
CREATE INDEX IF NOT EXISTS audit_tenant_created_idx ON braincore.audit_events (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS braincore.schema_migrations (
  version text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);

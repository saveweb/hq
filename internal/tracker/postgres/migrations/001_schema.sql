CREATE TABLE IF NOT EXISTS tracker_schema_migrations (
    version bigint PRIMARY KEY,
    applied_at bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS tracker_users (
    id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('pending', 'active', 'suspended')),
    roles text[] NOT NULL DEFAULT '{}',
    github_user_id bigint,
    github_login text,
    github_avatar_url text,
    last_login_at bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tracker_users_github_id_idx
    ON tracker_users(github_user_id) WHERE github_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tracker_machine_tokens (
    user_id text PRIMARY KEY REFERENCES tracker_users(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    token text,
    created_at bigint NOT NULL,
    revoked_at bigint
);
ALTER TABLE tracker_machine_tokens ADD COLUMN IF NOT EXISTS token text;

CREATE TABLE IF NOT EXISTS tracker_web_sessions (
    token_hash bytea PRIMARY KEY,
    user_id text NOT NULL REFERENCES tracker_users(id) ON DELETE CASCADE,
    created_at bigint NOT NULL,
    expires_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS tracker_web_sessions_expiry_idx
    ON tracker_web_sessions(expires_at);

CREATE TABLE IF NOT EXISTS tracker_projects (
    id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('active', 'draining', 'archived')),
    identity_mode text NOT NULL DEFAULT 'external_id'
        CHECK (identity_mode IN ('none', 'external_id', 'unique_value')),
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS tracker_jobs (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    external_id text,
    value text NOT NULL,
    unique_value_digest bytea,
    spec jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL CHECK (status IN ('todo', 'wip', 'done', 'failed', 'reset_exhausted')),
    attempt_id text,
    worker_id text,
    lease_expires_at bigint,
    reset_count integer NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
    outcome jsonb,
    warc_receipts jsonb,
    execution_error jsonb,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    completed_at bigint,
    CHECK (external_id IS NULL OR unique_value_digest IS NULL),
    CHECK ((status = 'wip' AND attempt_id IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'wip' AND attempt_id IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS tracker_jobs_external_id_uidx
    ON tracker_jobs(project_id, external_id) WHERE external_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS tracker_jobs_value_digest_uidx
    ON tracker_jobs(project_id, unique_value_digest) WHERE unique_value_digest IS NOT NULL;
CREATE INDEX IF NOT EXISTS tracker_jobs_claim_idx
    ON tracker_jobs(project_id, created_at, job_id) WHERE status = 'todo';
CREATE INDEX IF NOT EXISTS tracker_jobs_expired_idx
    ON tracker_jobs(project_id, lease_expires_at) WHERE status = 'wip';

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (1, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

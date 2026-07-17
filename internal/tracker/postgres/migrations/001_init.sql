CREATE TABLE IF NOT EXISTS tracker_schema_migrations (
    version bigint PRIMARY KEY,
    applied_at bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS tracker_users (
    id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('pending', 'active', 'suspended')),
    roles text[] NOT NULL DEFAULT '{}',
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS tracker_machine_tokens (
    user_id text PRIMARY KEY REFERENCES tracker_users(id) ON DELETE CASCADE,
    token_value text NOT NULL UNIQUE,
    token_hash bytea NOT NULL UNIQUE,
    created_at bigint NOT NULL,
    revoked_at bigint
);

CREATE TABLE IF NOT EXISTS tracker_agents (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES tracker_users(id),
    kind text NOT NULL CHECK (kind IN ('shard', 'worker')),
    name text NOT NULL,
    version text NOT NULL,
    status text NOT NULL CHECK (status IN ('registered', 'online', 'offline', 'revoked')),
    endpoint text,
    endpoint_version bigint,
    tls_spki_sha256 text,
    endpoint_status text NOT NULL,
    attrs jsonb NOT NULL DEFAULT '{}',
    last_heartbeat_at bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    CHECK ((kind = 'worker' AND endpoint IS NULL AND endpoint_version IS NULL AND tls_spki_sha256 IS NULL)
        OR (kind = 'shard' AND endpoint IS NOT NULL AND endpoint_version IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS tracker_agents_user_idx ON tracker_agents(user_id);

CREATE TABLE IF NOT EXISTS tracker_projects (
    id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('active', 'draining', 'archived')),
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS tracker_shards (
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    id text NOT NULL,
    status text NOT NULL CHECK (status IN ('loading', 'active', 'draining', 'paused', 'offline', 'recovering')),
    owner_agent_id text NOT NULL REFERENCES tracker_agents(id),
    generation bigint NOT NULL CHECK (generation >= 1),
    owner_lease_expires_at bigint NOT NULL DEFAULT 0,
    source_uri text,
    source_format text,
    source_etag text,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    PRIMARY KEY (project_id, id)
);
CREATE INDEX IF NOT EXISTS tracker_shards_assignment_idx
    ON tracker_shards(project_id, status, owner_lease_expires_at, id);
CREATE INDEX IF NOT EXISTS tracker_shards_owner_idx ON tracker_shards(owner_agent_id);

CREATE TABLE IF NOT EXISTS tracker_worker_sessions (
    id text PRIMARY KEY,
    project_id text NOT NULL REFERENCES tracker_projects(id),
    agent_id text NOT NULL REFERENCES tracker_agents(id),
    user_id text NOT NULL REFERENCES tracker_users(id),
    attrs jsonb NOT NULL DEFAULT '{}',
    created_at bigint NOT NULL,
    lease_expires_at bigint NOT NULL,
    last_heartbeat_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS tracker_worker_sessions_agent_idx
    ON tracker_worker_sessions(user_id, agent_id, lease_expires_at);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (1, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

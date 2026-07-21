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
    token_hash bytea NOT NULL UNIQUE,
    created_at bigint NOT NULL,
    revoked_at bigint
);

CREATE TABLE IF NOT EXISTS tracker_projects (
    id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('active', 'draining', 'archived')),
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (1, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

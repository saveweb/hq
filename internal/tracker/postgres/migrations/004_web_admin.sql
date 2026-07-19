ALTER TABLE tracker_users ADD COLUMN IF NOT EXISTS github_user_id bigint;
ALTER TABLE tracker_users ADD COLUMN IF NOT EXISTS github_login text;
ALTER TABLE tracker_users ADD COLUMN IF NOT EXISTS github_avatar_url text;
ALTER TABLE tracker_users ADD COLUMN IF NOT EXISTS last_login_at bigint;

CREATE UNIQUE INDEX IF NOT EXISTS tracker_users_github_id_idx
    ON tracker_users(github_user_id) WHERE github_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tracker_web_sessions (
    token_hash bytea PRIMARY KEY,
    user_id text NOT NULL REFERENCES tracker_users(id) ON DELETE CASCADE,
    created_at bigint NOT NULL,
    expires_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS tracker_web_sessions_expiry_idx
    ON tracker_web_sessions(expires_at);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (4, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

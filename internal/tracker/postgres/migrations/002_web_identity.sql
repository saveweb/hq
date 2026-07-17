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

CREATE TABLE IF NOT EXISTS tracker_audit_log (
    id bigserial PRIMARY KEY,
    actor_user_id text NOT NULL REFERENCES tracker_users(id),
    action text NOT NULL,
    target_id text NOT NULL,
    reason text NOT NULL DEFAULT '',
    created_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS tracker_audit_log_created_idx
    ON tracker_audit_log(created_at DESC, id DESC);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (2, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

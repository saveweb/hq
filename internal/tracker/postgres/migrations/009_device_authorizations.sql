CREATE TABLE IF NOT EXISTS tracker_device_authorizations (
    device_hash bytea PRIMARY KEY,
    user_code text NOT NULL UNIQUE,
    user_id text REFERENCES tracker_users(id) ON DELETE CASCADE,
    status text NOT NULL DEFAULT 'authorization_pending'
        CHECK (status IN ('authorization_pending', 'authorized', 'access_denied')),
    created_at bigint NOT NULL,
    expires_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS tracker_device_authorizations_expiry_idx
    ON tracker_device_authorizations(expires_at);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (9, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

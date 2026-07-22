CREATE TABLE IF NOT EXISTS tracker_workers (
    worker_id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES tracker_users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS tracker_workers_user_idx ON tracker_workers(user_id);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (6, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

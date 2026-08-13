ALTER TABLE tracker_workers
    ADD COLUMN IF NOT EXISTS last_seen_at bigint;

UPDATE tracker_workers
SET last_seen_at = EXTRACT(EPOCH FROM clock_timestamp())::bigint
WHERE last_seen_at IS NULL;

ALTER TABLE tracker_workers
    ALTER COLUMN last_seen_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS tracker_workers_last_seen_idx
    ON tracker_workers(last_seen_at DESC, worker_id);

CREATE INDEX IF NOT EXISTS tracker_workers_user_last_seen_idx
    ON tracker_workers(user_id, last_seen_at DESC, worker_id);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (8, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

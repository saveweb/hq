ALTER TABLE tracker_shards DROP CONSTRAINT IF EXISTS tracker_shards_status_check;
ALTER TABLE tracker_shards ADD CONSTRAINT tracker_shards_status_check CHECK (
    status IN ('loading', 'active', 'draining', 'paused', 'offline', 'recovering', 'load_failed')
);

ALTER TABLE tracker_shards ADD COLUMN IF NOT EXISTS load_error_code text;

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (3, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

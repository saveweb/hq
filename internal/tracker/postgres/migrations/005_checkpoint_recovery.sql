ALTER TABLE tracker_shards DROP CONSTRAINT IF EXISTS tracker_shards_status_check;
ALTER TABLE tracker_shards ADD CONSTRAINT tracker_shards_status_check CHECK (
    status IN ('loading', 'active', 'draining', 'paused', 'offline', 'recovering',
        'load_failed', 'recovery_failed')
);

ALTER TABLE tracker_shards ADD COLUMN IF NOT EXISTS recovery_error_code text;

ALTER TABLE tracker_shards DROP CONSTRAINT IF EXISTS tracker_shards_recovery_source_check;
ALTER TABLE tracker_shards ADD CONSTRAINT tracker_shards_recovery_source_check CHECK (
    status <> 'recovering' OR (
        checkpoint_uri IS NOT NULL AND checkpoint_format = 'sqlite-zstd-v1'
        AND source_uri IS NULL AND source_format IS NULL AND source_etag IS NULL
    )
);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (5, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

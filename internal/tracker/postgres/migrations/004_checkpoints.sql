ALTER TABLE tracker_shards
    ADD COLUMN IF NOT EXISTS checkpoint_uri text,
    ADD COLUMN IF NOT EXISTS checkpoint_format text,
    ADD COLUMN IF NOT EXISTS checkpoint_seq bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS checkpoint_generation bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_checksum text,
    ADD COLUMN IF NOT EXISTS checkpoint_size bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_at bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_id text,
    ADD COLUMN IF NOT EXISTS checkpoint_s3_upload_id text,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_uri text,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_seq bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_generation bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_checksum text,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_size bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_upload_started_at bigint;

ALTER TABLE tracker_shards DROP CONSTRAINT IF EXISTS tracker_shards_checkpoint_pointer_check;
ALTER TABLE tracker_shards ADD CONSTRAINT tracker_shards_checkpoint_pointer_check CHECK (
    (checkpoint_uri IS NULL AND checkpoint_format IS NULL AND checkpoint_generation IS NULL
        AND checkpoint_checksum IS NULL AND checkpoint_size IS NULL AND checkpoint_at IS NULL)
    OR
    (checkpoint_uri IS NOT NULL AND checkpoint_format = 'sqlite-zstd-v1'
        AND checkpoint_seq > 0 AND checkpoint_generation > 0
        AND checkpoint_checksum IS NOT NULL AND checkpoint_size > 0 AND checkpoint_at >= 0)
);

ALTER TABLE tracker_shards DROP CONSTRAINT IF EXISTS tracker_shards_checkpoint_upload_check;
ALTER TABLE tracker_shards ADD CONSTRAINT tracker_shards_checkpoint_upload_check CHECK (
    (checkpoint_upload_id IS NULL AND checkpoint_s3_upload_id IS NULL
        AND checkpoint_upload_uri IS NULL AND checkpoint_upload_seq IS NULL
        AND checkpoint_upload_generation IS NULL AND checkpoint_upload_checksum IS NULL
        AND checkpoint_upload_size IS NULL AND checkpoint_upload_started_at IS NULL)
    OR
    (checkpoint_upload_id IS NOT NULL AND checkpoint_s3_upload_id IS NOT NULL
        AND checkpoint_upload_uri IS NOT NULL AND checkpoint_upload_seq > checkpoint_seq
        AND checkpoint_upload_generation = generation
        AND checkpoint_upload_checksum IS NOT NULL AND checkpoint_upload_size > 0
        AND checkpoint_upload_started_at >= 0)
);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (4, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

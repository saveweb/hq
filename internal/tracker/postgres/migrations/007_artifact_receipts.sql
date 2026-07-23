DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'tracker_jobs'
          AND column_name = 'warc_receipts'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'tracker_jobs'
          AND column_name = 'artifact_receipts'
    ) THEN
        ALTER TABLE tracker_jobs RENAME COLUMN warc_receipts TO artifact_receipts;
    END IF;
END $$;

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (7, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

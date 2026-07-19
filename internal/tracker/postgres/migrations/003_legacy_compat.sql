DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'tracker_machine_tokens'
          AND column_name = 'token_value'
    ) THEN
        ALTER TABLE tracker_machine_tokens ALTER COLUMN token_value DROP NOT NULL;
    END IF;
END $$;

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (3, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

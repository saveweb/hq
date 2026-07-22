ALTER TABLE tracker_projects
    ADD COLUMN IF NOT EXISTS client_versions text[] NOT NULL DEFAULT '{}';

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (3, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

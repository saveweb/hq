ALTER TABLE tracker_projects
    ADD COLUMN IF NOT EXISTS max_resets integer NOT NULL DEFAULT 3;

ALTER TABLE tracker_projects
    DROP CONSTRAINT IF EXISTS tracker_projects_max_resets_check,
    ADD CONSTRAINT tracker_projects_max_resets_check
        CHECK (max_resets BETWEEN 0 AND 1000);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (4, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

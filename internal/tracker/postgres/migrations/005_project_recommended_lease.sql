ALTER TABLE tracker_projects
    ADD COLUMN IF NOT EXISTS recommended_lease_seconds bigint NOT NULL DEFAULT 300;

ALTER TABLE tracker_projects
    DROP CONSTRAINT IF EXISTS tracker_projects_recommended_lease_seconds_check,
    ADD CONSTRAINT tracker_projects_recommended_lease_seconds_check
        CHECK (recommended_lease_seconds BETWEEN 1 AND 3600);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (5, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

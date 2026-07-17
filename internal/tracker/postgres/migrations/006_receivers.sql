CREATE TABLE IF NOT EXISTS tracker_receivers (
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    id text NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'removed')),
    sink_uri text NOT NULL,
    format text NOT NULL CHECK (format = 'jobs-jsonl-zstd-v1'),
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    PRIMARY KEY (project_id, id)
);
CREATE INDEX IF NOT EXISTS tracker_receivers_active_idx
    ON tracker_receivers(project_id, status, id);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (6, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

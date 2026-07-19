CREATE TABLE IF NOT EXISTS tracker_jobs (
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    id text NOT NULL,
    spec jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('todo', 'wip', 'done', 'failed', 'reset_exhausted')),
    attempt_id text,
    worker_id text,
    lease_expires_at bigint,
    reset_count integer NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
    outcome jsonb,
    warc_receipts jsonb,
    execution_error jsonb,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    completed_at bigint,
    PRIMARY KEY (project_id, id),
    CHECK ((status = 'wip' AND attempt_id IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'wip' AND attempt_id IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL))
);

CREATE INDEX IF NOT EXISTS tracker_jobs_claim_idx
    ON tracker_jobs(project_id, created_at, id) WHERE status = 'todo';
CREATE INDEX IF NOT EXISTS tracker_jobs_expired_idx
    ON tracker_jobs(project_id, lease_expires_at) WHERE status = 'wip';

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (2, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

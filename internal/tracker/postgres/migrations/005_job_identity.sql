ALTER TABLE tracker_projects
    ADD COLUMN IF NOT EXISTS identity_mode text NOT NULL DEFAULT 'external_id'
    CHECK (identity_mode IN ('none', 'external_id', 'unique_value'));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'tracker_jobs'
          AND column_name = 'id'
    ) THEN
        RAISE NOTICE 'dropping pre-release tracker_jobs schema and all queued jobs';
        DROP TABLE tracker_jobs;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS tracker_jobs (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    external_id text,
    value text NOT NULL,
    unique_value_digest bytea,
    spec jsonb NOT NULL DEFAULT '{}',
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
    CHECK (external_id IS NULL OR unique_value_digest IS NULL),
    CHECK ((status = 'wip' AND attempt_id IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'wip' AND attempt_id IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS tracker_jobs_external_id_uidx
    ON tracker_jobs(project_id, external_id) WHERE external_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS tracker_jobs_value_digest_uidx
    ON tracker_jobs(project_id, unique_value_digest) WHERE unique_value_digest IS NOT NULL;
CREATE INDEX IF NOT EXISTS tracker_jobs_claim_idx
    ON tracker_jobs(project_id, created_at, job_id) WHERE status = 'todo';
CREATE INDEX IF NOT EXISTS tracker_jobs_expired_idx
    ON tracker_jobs(project_id, lease_expires_at) WHERE status = 'wip';

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (5, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

ALTER TABLE tracker_projects
    ADD COLUMN IF NOT EXISTS dispatch_qps double precision,
    ADD COLUMN IF NOT EXISTS worker_claim_qps double precision,
    ADD COLUMN IF NOT EXISTS max_jobs_per_claim integer NOT NULL DEFAULT 256,
    ADD COLUMN IF NOT EXISTS policy_version bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS dispatch_tokens double precision,
    ADD COLUMN IF NOT EXISTS dispatch_refilled_at_ns bigint;

ALTER TABLE tracker_projects
    DROP CONSTRAINT IF EXISTS tracker_projects_dispatch_qps_check,
    ADD CONSTRAINT tracker_projects_dispatch_qps_check
        CHECK (dispatch_qps IS NULL OR (dispatch_qps > 0 AND dispatch_qps < 'Infinity'::double precision)),
    DROP CONSTRAINT IF EXISTS tracker_projects_worker_claim_qps_check,
    ADD CONSTRAINT tracker_projects_worker_claim_qps_check
        CHECK (worker_claim_qps IS NULL OR (worker_claim_qps > 0 AND worker_claim_qps < 'Infinity'::double precision)),
    DROP CONSTRAINT IF EXISTS tracker_projects_max_jobs_per_claim_check,
    ADD CONSTRAINT tracker_projects_max_jobs_per_claim_check
        CHECK (max_jobs_per_claim BETWEEN 1 AND 256),
    DROP CONSTRAINT IF EXISTS tracker_projects_policy_version_check,
    ADD CONSTRAINT tracker_projects_policy_version_check
        CHECK (policy_version > 0);

CREATE TABLE IF NOT EXISTS tracker_worker_claim_buckets (
    project_id text NOT NULL REFERENCES tracker_projects(id) ON DELETE CASCADE,
    worker_id text NOT NULL,
    bucket_started_at_ms bigint NOT NULL,
    claim_requests bigint NOT NULL DEFAULT 0 CHECK (claim_requests >= 0),
    jobs_dispatched bigint NOT NULL DEFAULT 0 CHECK (jobs_dispatched >= 0),
    policy_version bigint NOT NULL,
    PRIMARY KEY (project_id, worker_id, bucket_started_at_ms)
);

CREATE INDEX IF NOT EXISTS tracker_worker_claim_buckets_project_time_idx
    ON tracker_worker_claim_buckets(project_id, bucket_started_at_ms DESC);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (2, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

INSERT INTO tracker_users(id,status,roles,created_at,updated_at)
VALUES (
    'gh_0',
    'active',
    ARRAY['worker']::text[],
    EXTRACT(EPOCH FROM clock_timestamp())::bigint,
    EXTRACT(EPOCH FROM clock_timestamp())::bigint
)
ON CONFLICT (id) DO UPDATE SET
    status='active',
    roles=ARRAY['worker']::text[],
    github_user_id=NULL,
    github_login=NULL,
    github_avatar_url=NULL,
    updated_at=EXCLUDED.updated_at
WHERE tracker_users.status IS DISTINCT FROM 'active'
   OR tracker_users.roles IS DISTINCT FROM ARRAY['worker']::text[]
   OR tracker_users.github_user_id IS NOT NULL
   OR tracker_users.github_login IS NOT NULL
   OR tracker_users.github_avatar_url IS NOT NULL;

CREATE TABLE IF NOT EXISTS tracker_project_anonymous_tokens (
    project_id text PRIMARY KEY REFERENCES tracker_projects(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    token text NOT NULL,
    created_at bigint NOT NULL
);

INSERT INTO tracker_schema_migrations(version, applied_at)
VALUES (10, EXTRACT(EPOCH FROM clock_timestamp())::bigint)
ON CONFLICT (version) DO NOTHING;

// Package postgres is the production PostgreSQL implementation of tracker.Store.
package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

//go:embed migrations/*.sql
var migrations embed.FS

var _ tracker.Store = (*Store)(nil)

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: parse database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("tracker postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("tracker postgres: list migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7532194861021)`); err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
				continue
			}
			script, err := migrations.ReadFile("migrations/" + entry.Name())
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(script)); err != nil {
				return fmt.Errorf("apply %s: %w", entry.Name(), err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("tracker postgres: migrate: %w", err)
	}
	return nil
}

func (s *Store) AuthenticateMachineToken(ctx context.Context, machineToken string) (tracker.User, error) {
	if machineToken == "" || len(machineToken) > 1024 {
		return tracker.User{}, invalidMachineToken()
	}
	var user tracker.User
	var roles []string
	err := s.pool.QueryRow(ctx, `
		SELECT u.id, u.status, u.roles
		FROM tracker_machine_tokens mt
		JOIN tracker_users u ON u.id = mt.user_id
		WHERE mt.token_hash = $1 AND mt.revoked_at IS NULL
	`, tokenDigest(machineToken)).Scan(&user.ID, &user.Status, &roles)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.User{}, invalidMachineToken()
	}
	if err != nil {
		return tracker.User{}, fmt.Errorf("tracker postgres: authenticate: %w", err)
	}
	user.Roles = roleMap(roles)
	return user, nil
}

// PutUserAndToken is an explicit bootstrap/admin primitive. The trusted
// control-plane database keeps the reusable value so the contributor portal
// can show it again; the digest provides a fixed-width authentication index.
func (s *Store) PutUserAndToken(ctx context.Context, user tracker.User, machineToken string, now int64) error {
	if !queue.ValidateIdentifier(user.ID) || machineToken == "" || len(machineToken) > 1024 ||
		(user.Status != tracker.UserStatusPending && user.Status != tracker.UserStatusActive && user.Status != tracker.UserStatusSuspended) {
		return fmt.Errorf("tracker postgres: invalid bootstrap user")
	}
	roles := make([]string, 0, len(user.Roles))
	for role, enabled := range user.Roles {
		if !enabled || (role != tracker.RoleAdmin && role != tracker.RoleShardOwner && role != tracker.RoleWorker) {
			continue
		}
		roles = append(roles, role)
	}
	sort.Strings(roles)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO tracker_users(
				id, github_user_id, github_login, github_avatar_url,
				status, roles, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
			ON CONFLICT (id) DO UPDATE SET
				github_user_id = COALESCE(EXCLUDED.github_user_id, tracker_users.github_user_id),
				github_login = COALESCE(NULLIF(EXCLUDED.github_login, ''), tracker_users.github_login),
				github_avatar_url = COALESCE(EXCLUDED.github_avatar_url, tracker_users.github_avatar_url),
				status = EXCLUDED.status, roles = EXCLUDED.roles, updated_at = EXCLUDED.updated_at
		`, user.ID, user.GitHubUserID, user.GitHubLogin, user.GitHubAvatarURL,
			user.Status, roles, now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO tracker_machine_tokens(user_id, token_value, token_hash, created_at, revoked_at)
			VALUES ($1, $2, $3, $4, NULL)
			ON CONFLICT (user_id) DO UPDATE
			SET token_value = EXCLUDED.token_value, token_hash = EXCLUDED.token_hash,
				created_at = EXCLUDED.created_at, revoked_at = NULL
		`, user.ID, machineToken, tokenDigest(machineToken), now)
		return err
	})
}

func (s *Store) PutProject(ctx context.Context, project tracker.Project, now int64) error {
	if !queue.ValidateIdentifier(project.ID) ||
		(project.Status != tracker.ProjectStatusActive && project.Status != tracker.ProjectStatusDraining && project.Status != tracker.ProjectStatusArchived) {
		return fmt.Errorf("tracker postgres: invalid project")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tracker_projects(id, status, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
	`, project.ID, project.Status, now)
	return err
}

func (s *Store) PutShard(ctx context.Context, shard tracker.Shard, now int64) error {
	if !queue.ValidateIdentifier(shard.ProjectID) || !queue.ValidateIdentifier(shard.ID) ||
		!queue.ValidateIdentifier(shard.OwnerAgentID) || shard.Generation < 1 {
		return fmt.Errorf("tracker postgres: invalid shard")
	}
	if (shard.SourceURI == nil) != (shard.SourceFormat == nil) ||
		(shard.SourceURI == nil) != (shard.SourceETag == nil) ||
		(shard.SourceFormat != nil && *shard.SourceFormat != "jobs-jsonl-zstd-v1") ||
		(shard.SourceURI != nil && (*shard.SourceURI == "" || *shard.SourceETag == "" || len(*shard.SourceETag) > 512)) ||
		(shard.SourceURI != nil && shard.Status != tracker.ShardStatusLoading) ||
		(shard.Status == tracker.ShardStatusLoading && shard.SourceURI == nil) {
		return fmt.Errorf("tracker postgres: invalid shard source metadata")
	}
	if shard.Status == tracker.ShardStatusRecovering {
		result, err := s.pool.Exec(ctx, `
			UPDATE tracker_shards SET
				status='recovering', owner_agent_id=$3, generation=$4,
				owner_lease_expires_at=$5,
				source_uri=NULL, source_format=NULL, source_etag=NULL,
				load_error_code=NULL, recovery_error_code=NULL,
				checkpoint_upload_id=NULL, checkpoint_s3_upload_id=NULL,
				checkpoint_upload_uri=NULL, checkpoint_upload_seq=NULL,
				checkpoint_upload_generation=NULL, checkpoint_upload_checksum=NULL,
				checkpoint_upload_size=NULL, checkpoint_upload_started_at=NULL,
				updated_at=$6
			WHERE project_id=$1 AND id=$2 AND $4 > generation
				AND checkpoint_uri IS NOT NULL AND checkpoint_format='sqlite-zstd-v1'
		`, shard.ProjectID, shard.ID, shard.OwnerAgentID, shard.Generation,
			shard.OwnerLeaseExpiresAt, now)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return &tracker.Error{
				Code:    protocol.ErrorStaleGeneration,
				Message: "checkpoint recovery requires an existing published checkpoint and newer generation",
			}
		}
		return nil
	}
	result, err := s.pool.Exec(ctx, `
		INSERT INTO tracker_shards(
			project_id, id, status, owner_agent_id, generation, owner_lease_expires_at,
			source_uri, source_format, source_etag, load_error_code, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10,$10)
		ON CONFLICT (project_id, id) DO UPDATE SET
			status = EXCLUDED.status, owner_agent_id = EXCLUDED.owner_agent_id,
			generation = EXCLUDED.generation, owner_lease_expires_at = EXCLUDED.owner_lease_expires_at,
			source_uri = EXCLUDED.source_uri, source_format = EXCLUDED.source_format,
			source_etag = EXCLUDED.source_etag, load_error_code = NULL, recovery_error_code = NULL,
			checkpoint_upload_id = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_id END,
			checkpoint_s3_upload_id = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_s3_upload_id END,
			checkpoint_upload_uri = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_uri END,
			checkpoint_upload_seq = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_seq END,
			checkpoint_upload_generation = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_generation END,
			checkpoint_upload_checksum = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_checksum END,
			checkpoint_upload_size = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_size END,
			checkpoint_upload_started_at = CASE WHEN EXCLUDED.generation > tracker_shards.generation THEN NULL ELSE tracker_shards.checkpoint_upload_started_at END,
			updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.generation > tracker_shards.generation OR (
			EXCLUDED.generation = tracker_shards.generation
			AND EXCLUDED.owner_agent_id = tracker_shards.owner_agent_id
			AND EXCLUDED.source_uri IS NOT DISTINCT FROM tracker_shards.source_uri
			AND EXCLUDED.source_format IS NOT DISTINCT FROM tracker_shards.source_format
			AND EXCLUDED.source_etag IS NOT DISTINCT FROM tracker_shards.source_etag
		)
	`, shard.ProjectID, shard.ID, shard.Status, shard.OwnerAgentID, shard.Generation,
		shard.OwnerLeaseExpiresAt, shard.SourceURI, shard.SourceFormat, shard.SourceETag, now)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return &tracker.Error{
			Code:    protocol.ErrorStaleGeneration,
			Message: "shard generation, owner, or immutable source identity is stale",
		}
	}
	return nil
}

func tokenDigest(token string) []byte {
	digest := sha256.Sum256([]byte("saveweb-machine-token-v1\x00" + token))
	return digest[:]
}

func roleMap(roles []string) map[string]bool {
	result := make(map[string]bool, len(roles))
	for _, role := range roles {
		result[role] = true
	}
	return result
}

func encodeAttrs(attrs map[string]any) ([]byte, error) {
	if attrs == nil {
		return nil, fmt.Errorf("attrs must not be nil")
	}
	return json.Marshal(attrs)
}

func decodeAttrs(encoded []byte) (map[string]any, error) {
	result := make(map[string]any)
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func newID(prefix string) (string, error) {
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func invalidMachineToken() *tracker.Error {
	return &tracker.Error{Code: protocol.ErrorInvalidMachineToken, Message: "invalid machine token"}
}

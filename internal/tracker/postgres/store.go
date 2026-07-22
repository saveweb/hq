// Package postgres implements the SavewebHQ coordinator store.
package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
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

type Store struct{ pool *pgxpool.Pool }

const (
	defaultProjectMaxResets               = 3
	maxProjectMaxResets                   = 1000
	defaultProjectRecommendedLeaseSeconds = int64(300)
	maxProjectLeaseSeconds                = int64(3600)
)

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: parse database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}
func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
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

func (s *Store) AuthenticateMachineToken(ctx context.Context, token string) (tracker.User, error) {
	if token == "" || len(token) > 1024 {
		return tracker.User{}, invalidMachineToken()
	}
	var user tracker.User
	var roles []string
	err := s.pool.QueryRow(ctx, `SELECT u.id,u.status,u.roles FROM tracker_machine_tokens mt JOIN tracker_users u ON u.id=mt.user_id WHERE mt.token_hash=$1 AND mt.revoked_at IS NULL`, tokenDigest(token)).Scan(&user.ID, &user.Status, &roles)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.User{}, invalidMachineToken()
	}
	if err != nil {
		return tracker.User{}, err
	}
	user.Roles = roleMap(roles)
	return user, nil
}

func (s *Store) PutUserAndToken(ctx context.Context, user tracker.User, token string, now int64) error {
	if !queue.ValidateIdentifier(user.ID) || token == "" || len(token) > 1024 {
		return fmt.Errorf("invalid bootstrap user")
	}
	roles := []string{}
	for role, enabled := range user.Roles {
		if enabled && (role == tracker.RoleAdmin || role == tracker.RoleWorker) {
			roles = append(roles, role)
		}
	}
	sort.Strings(roles)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO tracker_users(id,status,roles,created_at,updated_at) VALUES($1,$2,$3,$4,$4) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,roles=EXCLUDED.roles,updated_at=EXCLUDED.updated_at`, user.ID, user.Status, roles, now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO tracker_machine_tokens(user_id,token_hash,token,created_at,revoked_at) VALUES($1,$2,$3,$4,NULL) ON CONFLICT(user_id) DO UPDATE SET token_hash=EXCLUDED.token_hash,token=EXCLUDED.token,created_at=EXCLUDED.created_at,revoked_at=NULL`, user.ID, tokenDigest(token), token, now)
		return err
	})
}
func (s *Store) PutProject(ctx context.Context, project tracker.Project, now int64) error {
	if !queue.ValidateIdentifier(project.ID) || (project.Status != tracker.ProjectStatusActive && project.Status != tracker.ProjectStatusDraining && project.Status != tracker.ProjectStatusArchived) {
		return tracker.InvalidRequest("invalid project")
	}
	if project.IdentityMode != "" && project.IdentityMode != tracker.IdentityModeNone && project.IdentityMode != tracker.IdentityModeExternalID && project.IdentityMode != tracker.IdentityModeUniqueValue {
		return tracker.InvalidRequest("invalid project identity mode")
	}
	if project.ClaimOrder != "" && project.ClaimOrder != tracker.ClaimOrderFIFO && project.ClaimOrder != tracker.ClaimOrderRandom {
		return tracker.InvalidRequest("invalid project claim order")
	}
	if !validProjectQPS(project.DispatchQPS) || !validProjectQPS(project.WorkerClaimQPS) {
		return tracker.InvalidRequest("project QPS must be a positive finite number")
	}
	if project.MaxJobsPerClaim == 0 {
		project.MaxJobsPerClaim = maxProjectBatch
	}
	if project.MaxJobsPerClaim < 1 || project.MaxJobsPerClaim > maxProjectBatch {
		return tracker.InvalidRequest("max jobs per claim must be between 1 and 256")
	}
	maxResets := defaultProjectMaxResets
	if project.MaxResets != nil {
		maxResets = *project.MaxResets
	}
	if maxResets < 0 || maxResets > maxProjectMaxResets {
		return tracker.InvalidRequest("max resets must be between 0 and 1000")
	}
	recommendedLeaseSeconds := defaultProjectRecommendedLeaseSeconds
	if project.RecommendedLeaseSeconds != nil {
		recommendedLeaseSeconds = *project.RecommendedLeaseSeconds
	}
	if recommendedLeaseSeconds < 1 || recommendedLeaseSeconds > maxProjectLeaseSeconds {
		return tracker.InvalidRequest("recommended lease seconds must be between 1 and 3600")
	}
	if err := validateClientVersions(project.ClientVersions); err != nil {
		return err
	}
	project.ClientVersions = append([]string{}, project.ClientVersions...)
	sort.Strings(project.ClientVersions)
	return storeError("put project", pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO tracker_projects(
				id,status,identity_mode,claim_order,dispatch_qps,worker_claim_qps,max_jobs_per_claim,max_resets,recommended_lease_seconds,client_versions,policy_version,created_at,updated_at
			)
			VALUES($1,$2,COALESCE(NULLIF($3,''),'external_id'),COALESCE(NULLIF($4,''),'fifo'),$5,$6,$7,$8,$9,$10,1,$11,$11)
			ON CONFLICT(id) DO UPDATE SET
				status=EXCLUDED.status,
				claim_order=COALESCE(NULLIF($4,''),tracker_projects.claim_order),
				dispatch_qps=EXCLUDED.dispatch_qps,
				worker_claim_qps=EXCLUDED.worker_claim_qps,
				max_jobs_per_claim=EXCLUDED.max_jobs_per_claim,
				max_resets=EXCLUDED.max_resets,
				recommended_lease_seconds=EXCLUDED.recommended_lease_seconds,
				client_versions=EXCLUDED.client_versions,
				policy_version=tracker_projects.policy_version + CASE WHEN
					tracker_projects.dispatch_qps IS DISTINCT FROM EXCLUDED.dispatch_qps OR
					tracker_projects.worker_claim_qps IS DISTINCT FROM EXCLUDED.worker_claim_qps OR
					tracker_projects.max_jobs_per_claim IS DISTINCT FROM EXCLUDED.max_jobs_per_claim OR
					tracker_projects.max_resets IS DISTINCT FROM EXCLUDED.max_resets OR
					tracker_projects.recommended_lease_seconds IS DISTINCT FROM EXCLUDED.recommended_lease_seconds OR
					tracker_projects.client_versions IS DISTINCT FROM EXCLUDED.client_versions
				THEN 1 ELSE 0 END,
				dispatch_tokens=CASE WHEN tracker_projects.dispatch_qps IS DISTINCT FROM EXCLUDED.dispatch_qps THEN NULL ELSE tracker_projects.dispatch_tokens END,
				dispatch_refilled_at_ns=CASE WHEN tracker_projects.dispatch_qps IS DISTINCT FROM EXCLUDED.dispatch_qps THEN NULL ELSE tracker_projects.dispatch_refilled_at_ns END,
				updated_at=EXCLUDED.updated_at
			WHERE $3='' OR tracker_projects.identity_mode=$3
		`, project.ID, project.Status, project.IdentityMode, project.ClaimOrder, project.DispatchQPS, project.WorkerClaimQPS, project.MaxJobsPerClaim, maxResets, recommendedLeaseSeconds, project.ClientVersions, now)
		if err == nil && tag.RowsAffected() == 0 {
			return tracker.InvalidRequest("project identity mode cannot be changed")
		}
		return err
	}))
}

func validateClientVersions(versions []string) error {
	if len(versions) > 64 {
		return tracker.InvalidRequest("client_versions must contain at most 64 versions")
	}
	seen := make(map[string]struct{}, len(versions))
	for _, version := range versions {
		if version == "" || len(version) > 128 || strings.TrimSpace(version) != version {
			return tracker.InvalidRequest("client_versions entries must be non-empty, at most 128 bytes, and have no surrounding whitespace")
		}
		if _, exists := seen[version]; exists {
			return tracker.InvalidRequest("client_versions entries must be unique")
		}
		seen[version] = struct{}{}
	}
	return nil
}

func (s *Store) CheckProjectClientVersion(ctx context.Context, userID, projectID, clientVersion string) error {
	if !queue.ValidateIdentifier(userID) || !queue.ValidateIdentifier(projectID) {
		return tracker.InvalidRequest("invalid project ID")
	}
	var userStatus string
	var roles []string
	var allowed []string
	if err := s.pool.QueryRow(ctx, `
		SELECT u.status,u.roles,p.client_versions
		FROM tracker_users u CROSS JOIN tracker_projects p
		WHERE u.id=$1 AND p.id=$2
	`, userID, projectID).Scan(&userStatus, &roles, &allowed); errors.Is(err, pgx.ErrNoRows) {
		return &tracker.Error{Code: protocol.ErrorNotFound, Message: "user or project not found"}
	} else if err != nil {
		return storeError("check project client version", err)
	}
	if userStatus != tracker.UserStatusActive || !roleMap(roles)[tracker.RoleWorker] {
		return &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "active worker role required"}
	}
	for _, version := range allowed {
		if clientVersion == version {
			return nil
		}
	}
	return &tracker.Error{
		Code: protocol.ErrorClientUpgrade, Message: "client version is not supported; upgrade required",
		Details: map[string]any{"client_version": clientVersion, "client_versions": allowed},
	}
}

func validProjectQPS(value *float64) bool {
	return value == nil || (!math.IsNaN(*value) && !math.IsInf(*value, 0) && *value > 0)
}

func tokenDigest(token string) []byte {
	sum := sha256.Sum256([]byte("saveweb-machine-token-v1\x00" + token))
	return sum[:]
}
func roleMap(roles []string) map[string]bool {
	result := map[string]bool{}
	for _, role := range roles {
		result[role] = true
	}
	return result
}
func newID(prefix string) (string, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
func invalidMachineToken() *tracker.Error {
	return &tracker.Error{Code: protocol.ErrorInvalidMachineToken, Message: "invalid machine token"}
}
func storeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var domain *tracker.Error
	if errors.As(err, &domain) {
		return domain
	}
	return fmt.Errorf("tracker postgres: %s: %w", operation, err)
}

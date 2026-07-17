package postgres

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
)

const portalUserColumns = `
	id, github_user_id, COALESCE(github_login, ''), github_avatar_url,
	status, roles, last_login_at`

func (s *Store) UpsertGitHubUser(
	ctx context.Context,
	identity tracker.GitHubIdentity,
	autoWorker bool,
	now int64,
) (tracker.User, error) {
	if identity.UserID < 1 || identity.Login == "" || len(identity.Login) > 255 ||
		(identity.AvatarURL != nil && len(*identity.AvatarURL) > 2048) {
		return tracker.User{}, fmt.Errorf("tracker postgres: invalid GitHub identity")
	}
	var result tracker.User
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		result, err = scanPortalUser(tx.QueryRow(ctx, `
			SELECT `+portalUserColumns+`
			FROM tracker_users WHERE github_user_id=$1 FOR UPDATE
		`, identity.UserID))
		if err == nil {
			result, err = scanPortalUser(tx.QueryRow(ctx, `
				UPDATE tracker_users SET github_login=$2, github_avatar_url=$3,
					last_login_at=$4, updated_at=$4
				WHERE github_user_id=$1
				RETURNING `+portalUserColumns,
				identity.UserID, identity.Login, identity.AvatarURL, now))
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		status := tracker.UserStatusPending
		roles := []string{}
		if autoWorker {
			status = tracker.UserStatusActive
			roles = []string{tracker.RoleWorker}
		}
		userID := "gh_" + strconv.FormatInt(identity.UserID, 10)
		result, err = scanPortalUser(tx.QueryRow(ctx, `
			INSERT INTO tracker_users(
				id, github_user_id, github_login, github_avatar_url, status, roles,
				last_login_at, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$7)
			RETURNING `+portalUserColumns,
			userID, identity.UserID, identity.Login, identity.AvatarURL, status, roles, now))
		return err
	})
	if err != nil {
		return tracker.User{}, fmt.Errorf("tracker postgres: upsert GitHub user: %w", err)
	}
	return result, nil
}

func (s *Store) CreateWebSession(
	ctx context.Context,
	userID string,
	tokenHash []byte,
	now, expiresAt int64,
) error {
	if userID == "" || len(tokenHash) != 32 || now < 0 || expiresAt <= now {
		return fmt.Errorf("tracker postgres: invalid web session")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM tracker_web_sessions WHERE expires_at <= $1`, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO tracker_web_sessions(token_hash, user_id, created_at, expires_at)
			VALUES ($1,$2,$3,$4)
		`, tokenHash, userID, now, expiresAt)
		return err
	})
}

func (s *Store) AuthenticateWebSession(ctx context.Context, tokenHash []byte, now int64) (tracker.User, error) {
	if len(tokenHash) != 32 {
		return tracker.User{}, pgx.ErrNoRows
	}
	user, err := scanPortalUser(s.pool.QueryRow(ctx, `
		SELECT `+prefixedPortalUserColumns("u")+`
		FROM tracker_web_sessions ws
		JOIN tracker_users u ON u.id=ws.user_id
		WHERE ws.token_hash=$1 AND ws.expires_at>$2
	`, tokenHash, now))
	if err != nil {
		return tracker.User{}, err
	}
	return user, nil
}

func (s *Store) DeleteWebSession(ctx context.Context, tokenHash []byte) error {
	if len(tokenHash) != 32 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM tracker_web_sessions WHERE token_hash=$1`, tokenHash)
	return err
}

func (s *Store) MachineToken(ctx context.Context, userID string) (string, error) {
	var token string
	err := s.pool.QueryRow(ctx, `
		SELECT token_value FROM tracker_machine_tokens
		WHERE user_id=$1 AND revoked_at IS NULL
	`, userID).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return token, err
}

func (s *Store) ResetMachineToken(ctx context.Context, userID string, now int64) (string, error) {
	token, err := randomMachineToken()
	if err != nil {
		return "", err
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT true FROM tracker_users WHERE id=$1 FOR UPDATE`, userID).Scan(&exists); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tracker_machine_tokens(user_id, token_value, token_hash, created_at, revoked_at)
			VALUES ($1,$2,$3,$4,NULL)
			ON CONFLICT (user_id) DO UPDATE SET token_value=EXCLUDED.token_value,
				token_hash=EXCLUDED.token_hash, created_at=EXCLUDED.created_at, revoked_at=NULL
		`, userID, token, tokenDigest(token), now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO tracker_audit_log(actor_user_id, action, target_id, reason, created_at)
			VALUES ($1, 'machine_token.reset', $1, 'self-service reset', $2)
		`, userID, now)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("tracker postgres: reset machine token: %w", err)
	}
	return token, nil
}

func (s *Store) ListUserAgents(ctx context.Context, userID string) ([]tracker.Agent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+agentColumns+` FROM tracker_agents
		WHERE user_id=$1 ORDER BY id
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.Agent{}
	for rows.Next() {
		agent, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, agent)
	}
	return result, rows.Err()
}

func (s *Store) ListUsers(ctx context.Context) ([]tracker.User, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+portalUserColumns+` FROM tracker_users
		ORDER BY COALESCE(github_login, id), id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.User{}
	for rows.Next() {
		user, err := scanPortalUser(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, user)
	}
	return result, rows.Err()
}

func (s *Store) UpdateUserAccess(
	ctx context.Context,
	actorID, targetID, status string,
	roles map[string]bool,
	reason string,
	now int64,
) error {
	roleList, err := validRoleList(roles)
	if err != nil || (status != tracker.UserStatusPending && status != tracker.UserStatusActive && status != tracker.UserStatusSuspended) ||
		actorID == "" || targetID == "" || strings.TrimSpace(reason) == "" || len(reason) > 1000 {
		return fmt.Errorf("tracker postgres: invalid user access update")
	}
	if actorID == targetID && (status != tracker.UserStatusActive || !roles[tracker.RoleAdmin]) {
		return fmt.Errorf("tracker postgres: administrator cannot remove their own access")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var actorStatus string
		var actorRoles []string
		if err := tx.QueryRow(ctx, `SELECT status, roles FROM tracker_users WHERE id=$1 FOR UPDATE`, actorID).
			Scan(&actorStatus, &actorRoles); err != nil {
			return err
		}
		if actorStatus != tracker.UserStatusActive || !roleMap(actorRoles)[tracker.RoleAdmin] {
			return fmt.Errorf("administrator role required")
		}
		command, err := tx.Exec(ctx, `
			UPDATE tracker_users SET status=$2, roles=$3, updated_at=$4 WHERE id=$1
		`, targetID, status, roleList, now)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		if status == tracker.UserStatusSuspended {
			if _, err := tx.Exec(ctx, `
				UPDATE tracker_agents SET status='revoked', updated_at=$2
				WHERE user_id=$1 AND status <> 'revoked'
			`, targetID, now); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO tracker_audit_log(actor_user_id, action, target_id, reason, created_at)
			VALUES ($1, 'user.access.update', $2, $3, $4)
		`, actorID, targetID, reason, now)
		return err
	})
}

func (s *Store) ListProjects(ctx context.Context) ([]tracker.Project, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, status FROM tracker_projects ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.Project{}
	for rows.Next() {
		var project tracker.Project
		if err := rows.Scan(&project.ID, &project.Status); err != nil {
			return nil, err
		}
		result = append(result, project)
	}
	return result, rows.Err()
}

func (s *Store) ListAdminShards(ctx context.Context) ([]tracker.AdminShard, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, id, status, owner_agent_id, generation,
			owner_lease_expires_at, source_uri, source_format, source_etag,
			load_error_code, recovery_error_code,
			checkpoint_uri, checkpoint_seq, checkpoint_generation,
			checkpoint_size, checkpoint_at,
			checkpoint_upload_id, checkpoint_upload_started_at
		FROM tracker_shards ORDER BY project_id, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.AdminShard{}
	for rows.Next() {
		var shard tracker.AdminShard
		if err := rows.Scan(
			&shard.ProjectID, &shard.ID, &shard.Status, &shard.OwnerAgentID,
			&shard.Generation, &shard.OwnerLeaseExpiresAt,
			&shard.SourceURI, &shard.SourceFormat, &shard.SourceETag,
			&shard.LoadErrorCode, &shard.RecoveryErrorCode,
			&shard.CheckpointURI, &shard.CheckpointSequence,
			&shard.CheckpointGeneration, &shard.CheckpointSize, &shard.CheckpointAt,
			&shard.CheckpointUploadID, &shard.CheckpointUploadStartedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, shard)
	}
	return result, rows.Err()
}

func (s *Store) ListReceivers(ctx context.Context) ([]tracker.Receiver, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, id, status, sink_uri, format
		FROM tracker_receivers ORDER BY project_id, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.Receiver{}
	for rows.Next() {
		var receiver tracker.Receiver
		if err := rows.Scan(
			&receiver.ProjectID, &receiver.ID, &receiver.Status,
			&receiver.SinkURI, &receiver.Format,
		); err != nil {
			return nil, err
		}
		result = append(result, receiver)
	}
	return result, rows.Err()
}

func (s *Store) ListShardAgents(ctx context.Context) ([]tracker.Agent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+agentColumns+` FROM tracker_agents
		WHERE kind='shard' ORDER BY status, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.Agent{}
	for rows.Next() {
		agent, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, agent)
	}
	return result, rows.Err()
}

func (s *Store) ListAuditEvents(ctx context.Context, limit int) ([]tracker.AuditEvent, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("tracker postgres: invalid audit event limit")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, actor_user_id, action, target_id, reason, created_at
		FROM tracker_audit_log ORDER BY created_at DESC, id DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []tracker.AuditEvent{}
	for rows.Next() {
		var event tracker.AuditEvent
		if err := rows.Scan(
			&event.ID, &event.ActorID, &event.Action, &event.TargetID,
			&event.Reason, &event.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) AdminPutProject(
	ctx context.Context,
	actorID string,
	project tracker.Project,
	reason string,
	now int64,
) error {
	return s.adminCommand(ctx, actorID, "project.put", project.ID, reason, now, func(tx pgx.Tx) error {
		return putProject(ctx, tx, project, now)
	})
}

func (s *Store) AdminPutShard(
	ctx context.Context,
	actorID string,
	shard tracker.Shard,
	reason string,
	now int64,
) error {
	target := shard.ProjectID + "/" + shard.ID
	return s.adminCommand(ctx, actorID, "shard.put", target, reason, now, func(tx pgx.Tx) error {
		return putShard(ctx, tx, shard, now)
	})
}

func (s *Store) AdminPutReceiver(
	ctx context.Context,
	actorID string,
	receiver tracker.Receiver,
	reason string,
	now int64,
) error {
	target := receiver.ProjectID + "/" + receiver.ID
	return s.adminCommand(ctx, actorID, "receiver.put", target, reason, now, func(tx pgx.Tx) error {
		return putReceiver(ctx, tx, receiver, now)
	})
}

func (s *Store) adminCommand(
	ctx context.Context,
	actorID, action, target, reason string,
	now int64,
	command func(pgx.Tx) error,
) error {
	reason = strings.TrimSpace(reason)
	if actorID == "" || action == "" || target == "" || reason == "" || len(reason) > 1000 || now < 0 {
		return fmt.Errorf("tracker postgres: invalid admin command")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status string
		var roles []string
		if err := tx.QueryRow(ctx, `
			SELECT status, roles FROM tracker_users WHERE id=$1 FOR UPDATE
		`, actorID).Scan(&status, &roles); err != nil {
			return err
		}
		if status != tracker.UserStatusActive || !roleMap(roles)[tracker.RoleAdmin] {
			return fmt.Errorf("administrator role required")
		}
		if err := command(tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO tracker_audit_log(actor_user_id, action, target_id, reason, created_at)
			VALUES ($1,$2,$3,$4,$5)
		`, actorID, action, target, reason, now)
		return err
	})
}

func scanPortalUser(row rowScanner) (tracker.User, error) {
	var user tracker.User
	var roles []string
	err := row.Scan(
		&user.ID, &user.GitHubUserID, &user.GitHubLogin, &user.GitHubAvatarURL,
		&user.Status, &roles, &user.LastLoginAt,
	)
	if err != nil {
		return tracker.User{}, err
	}
	user.Roles = roleMap(roles)
	return user, nil
}

func prefixedPortalUserColumns(prefix string) string {
	return prefix + `.id, ` + prefix + `.github_user_id, COALESCE(` + prefix + `.github_login, ''), ` +
		prefix + `.github_avatar_url, ` + prefix + `.status, ` + prefix + `.roles, ` + prefix + `.last_login_at`
}

func validRoleList(roles map[string]bool) ([]string, error) {
	result := []string{}
	for role, enabled := range roles {
		if !enabled {
			continue
		}
		if role != tracker.RoleAdmin && role != tracker.RoleShardOwner && role != tracker.RoleWorker {
			return nil, fmt.Errorf("unknown role %q", role)
		}
		result = append(result, role)
	}
	sort.Strings(result)
	return result, nil
}

func randomMachineToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "mt_" + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

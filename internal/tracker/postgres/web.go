package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/tracker"
)

const webUserColumns = `id,github_user_id,COALESCE(github_login,''),github_avatar_url,status,roles,last_login_at`

func (s *Store) UpsertGitHubAdmin(ctx context.Context, identity tracker.GitHubIdentity, now int64) (tracker.User, error) {
	if err := validateGitHubIdentity(identity); err != nil {
		return tracker.User{}, err
	}
	roles := []string{tracker.RoleAdmin}
	var user tracker.User
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		user, err = scanWebUser(tx.QueryRow(ctx, `
			UPDATE tracker_users SET github_login=$2,github_avatar_url=$3,
				status='active',roles=$4,last_login_at=$5,updated_at=$5
			WHERE github_user_id=$1 RETURNING `+webUserColumns,
			identity.UserID, identity.Login, identity.AvatarURL, roles, now))
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		userID := "gh_" + strconv.FormatInt(identity.UserID, 10)
		user, err = scanWebUser(tx.QueryRow(ctx, `
			INSERT INTO tracker_users(id,github_user_id,github_login,github_avatar_url,status,roles,last_login_at,created_at,updated_at)
			VALUES($1,$2,$3,$4,'active',$5,$6,$6,$6)
			RETURNING `+webUserColumns,
			userID, identity.UserID, identity.Login, identity.AvatarURL, roles, now))
		return err
	})
	if err != nil {
		return tracker.User{}, storeError("upsert GitHub admin", err)
	}
	return user, nil
}

func (s *Store) UpsertGitHubPendingWorker(ctx context.Context, identity tracker.GitHubIdentity, now int64) (tracker.User, error) {
	if err := validateGitHubIdentity(identity); err != nil {
		return tracker.User{}, err
	}
	userID := "gh_" + strconv.FormatInt(identity.UserID, 10)
	roles := []string{tracker.RoleWorker}
	user, err := scanWebUser(s.pool.QueryRow(ctx, `
		INSERT INTO tracker_users(id,github_user_id,github_login,github_avatar_url,status,roles,last_login_at,created_at,updated_at)
		VALUES($1,$2,$3,$4,'pending',$5,$6,$6,$6)
		ON CONFLICT (github_user_id) WHERE github_user_id IS NOT NULL DO UPDATE
		SET github_login=EXCLUDED.github_login,github_avatar_url=EXCLUDED.github_avatar_url,
			last_login_at=EXCLUDED.last_login_at,updated_at=EXCLUDED.updated_at
		RETURNING `+webUserColumns,
		userID, identity.UserID, identity.Login, identity.AvatarURL, roles, now))
	if err != nil {
		return tracker.User{}, storeError("upsert GitHub pending worker", err)
	}
	return user, nil
}

func validateGitHubIdentity(identity tracker.GitHubIdentity) error {
	if identity.UserID < 1 || identity.Login == "" || len(identity.Login) > 255 || (identity.AvatarURL != nil && len(*identity.AvatarURL) > 2048) {
		return fmt.Errorf("tracker postgres: invalid GitHub identity")
	}
	return nil
}

func (s *Store) CreateWebSession(ctx context.Context, userID string, tokenHash []byte, now, expiresAt int64) error {
	if userID == "" || len(tokenHash) != 32 || now < 1 || expiresAt <= now {
		return fmt.Errorf("tracker postgres: invalid web session")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM tracker_web_sessions WHERE expires_at <= $1`, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO tracker_web_sessions(token_hash,user_id,created_at,expires_at) VALUES($1,$2,$3,$4)`, tokenHash, userID, now, expiresAt)
		return err
	})
}

func (s *Store) AuthenticateWebSession(ctx context.Context, tokenHash []byte, now int64) (tracker.User, error) {
	if len(tokenHash) != 32 {
		return tracker.User{}, tracker.ErrWebSessionNotFound
	}
	user, err := scanWebUser(s.pool.QueryRow(ctx, `
		SELECT `+prefixedWebUserColumns("u.")+`
		FROM tracker_web_sessions ws JOIN tracker_users u ON u.id=ws.user_id
		WHERE ws.token_hash=$1 AND ws.expires_at>$2 AND u.status='active'
	`, tokenHash, now))
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.User{}, tracker.ErrWebSessionNotFound
	}
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

type webUserScanner interface {
	Scan(...any) error
}

func scanWebUser(row webUserScanner) (tracker.User, error) {
	var user tracker.User
	var roles []string
	err := row.Scan(&user.ID, &user.GitHubUserID, &user.GitHubLogin, &user.GitHubAvatarURL, &user.Status, &roles, &user.LastLoginAt)
	if err != nil {
		return tracker.User{}, err
	}
	user.Roles = roleMap(roles)
	return user, nil
}

func prefixedWebUserColumns(prefix string) string {
	return prefix + "id," + prefix + "github_user_id,COALESCE(" + prefix + "github_login,'')," + prefix + "github_avatar_url," + prefix + "status," + prefix + "roles," + prefix + "last_login_at"
}

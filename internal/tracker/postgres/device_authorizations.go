package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/tracker"
)

func (s *Store) CreateDeviceAuthorization(ctx context.Context, deviceHash []byte, userCode string, now, expiresAt int64) error {
	if len(deviceHash) != 32 || len(userCode) != 43 || now < 1 || expiresAt <= now {
		return fmt.Errorf("tracker postgres: invalid device authorization")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM tracker_device_authorizations WHERE expires_at <= $1`, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO tracker_device_authorizations(device_hash,user_code,created_at,expires_at)
			VALUES($1,$2,$3,$4)
		`, deviceHash, userCode, now, expiresAt)
		return err
	})
}

func (s *Store) AuthorizeDevice(ctx context.Context, userCode, userID string, now int64) (bool, error) {
	if len(userCode) != 43 || userID == "" || now < 1 {
		return false, fmt.Errorf("tracker postgres: invalid device authorization decision")
	}
	var authorized bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var eligible bool
		err := tx.QueryRow(ctx, `
			SELECT u.status='active'
				AND 'worker'=ANY(u.roles)
				AND mt.revoked_at IS NULL
				AND COALESCE(mt.token,'')<>''
			FROM tracker_users u
			LEFT JOIN tracker_machine_tokens mt ON mt.user_id=u.id
			WHERE u.id=$1
		`, userID).Scan(&eligible)
		if err != nil {
			return err
		}
		status := tracker.DeviceAuthorizationDenied
		if eligible {
			status = tracker.DeviceAuthorizationAuthorized
		}
		tag, err := tx.Exec(ctx, `
			UPDATE tracker_device_authorizations
			SET user_id=$2,status=$3
			WHERE user_code=$1 AND expires_at>$4 AND status='authorization_pending'
		`, userCode, userID, status, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		authorized = eligible
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tracker.InvalidRequest("device authorization is missing, expired, or already decided")
	}
	return authorized, storeError("authorize device", err)
}

func (s *Store) RedeemDeviceAuthorization(ctx context.Context, deviceHash []byte, now int64) (tracker.DeviceAuthorization, error) {
	if len(deviceHash) != 32 || now < 1 {
		return tracker.DeviceAuthorization{}, fmt.Errorf("tracker postgres: invalid device authorization redemption")
	}
	var result tracker.DeviceAuthorization
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var userID *string
		err := tx.QueryRow(ctx, `
			SELECT status,user_id
			FROM tracker_device_authorizations
			WHERE device_hash=$1 AND expires_at>$2
			FOR UPDATE
		`, deviceHash, now).Scan(&result.Status, &userID)
		if err != nil {
			return err
		}
		if result.Status == tracker.DeviceAuthorizationPending {
			return nil
		}
		if result.Status == tracker.DeviceAuthorizationAuthorized && userID != nil {
			err = tx.QueryRow(ctx, `
				SELECT mt.token
				FROM tracker_users u
				JOIN tracker_machine_tokens mt ON mt.user_id=u.id
				WHERE u.id=$1 AND u.status='active' AND 'worker'=ANY(u.roles)
					AND mt.revoked_at IS NULL AND COALESCE(mt.token,'')<>''
			`, *userID).Scan(&result.MachineToken)
			if errors.Is(err, pgx.ErrNoRows) {
				result.Status = tracker.DeviceAuthorizationDenied
			} else if err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `DELETE FROM tracker_device_authorizations WHERE device_hash=$1`, deviceHash)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.DeviceAuthorization{}, tracker.InvalidRequest("device authorization is missing or expired")
	}
	return result, storeError("redeem device authorization", err)
}

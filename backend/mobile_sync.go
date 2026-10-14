package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

// Acknowledgements and cursor advancement commit with the actual log mutation.
// The client retains unacknowledged data; a lost HTTP response is safe to retry.
func acceptMobileEvent(ctx context.Context, tx pgx.Tx, driverID, id, kind string, at time.Time) (bool, error) {
	if len(id) > 100 {
		return false, errors.New("invalid client event id")
	}
	if id != "" {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mobile_event_receipts WHERE driver_id=$1 AND event_id=$2)`, driverID, id).Scan(&exists); err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	var latest *time.Time
	err := tx.QueryRow(ctx, `SELECT recorded_at FROM mobile_sync_cursor WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&latest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if latest != nil && at.Before(*latest) {
		return false, errors.New("Out-of-order offline record retained for review")
	}
	var edited bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM log_edit_batches b JOIN drivers d ON d.id=b.driver_id WHERE b.driver_id=$1 AND b.created_at>$2 AND b.undone_at IS NULL AND b.log_date>=($2 AT TIME ZONE d.home_terminal_timezone)::date)`, driverID, at).Scan(&edited); err != nil {
		return false, err
	}
	if edited {
		return false, errors.New("A newer administrative edit conflicts with this offline record; saved data requires review")
	}
	if id != "" {
		if _, err = tx.Exec(ctx, `INSERT INTO mobile_event_receipts(driver_id,event_id,kind,recorded_at) VALUES($1,$2,$3,$4)`, driverID, id, kind, at); err != nil {
			return false, err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO mobile_sync_cursor(driver_id,recorded_at) VALUES($1,$2) ON CONFLICT(driver_id) DO UPDATE SET recorded_at=GREATEST(mobile_sync_cursor.recorded_at,EXCLUDED.recorded_at)`, driverID, at)
	return false, err
}

func operatorName(ctx context.Context) string {
	if i, ok := ctx.Value(webIdentityContextKey).(*WebIdentity); ok && i != nil {
		if i.FullName != "" {
			return i.FullName
		}
		return i.Username
	}
	return "Admin"
}

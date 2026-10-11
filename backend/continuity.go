package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ensureDriverDayContinuity materializes a daily projection from the driver's continuous status history.
// It is deliberately server-side so days remain complete even when neither the website nor Android UI was opened.
func (s *Server) ensureDriverDayContinuity(ctx context.Context, driverID, date, timezone string) error {
	requested, err := time.Parse("2006-01-02", date)
	if err != nil {
		return err
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	localNow := time.Now().In(loc)
	today := localNow.Format("2006-01-02")
	if date > today {
		return nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*)::int FROM duty_segments WHERE driver_id=$1 AND log_date=$2`, driverID, date).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		if date < today {
			// A historical open fragment must visually and mathematically reach midnight.
			if _, err = tx.Exec(ctx, `UPDATE duty_segments SET end_minute=1440,end_second=86400,updated_at=now() WHERE driver_id=$1 AND log_date=$2 AND end_minute IS NULL`, driverID, date); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	}

	// Status immediately before this day. The last segment is authoritative for a missing day.
	priorStatus, priorSpecial := "OFF", ""
	err = tx.QueryRow(ctx, `
SELECT duty_status,special_status FROM duty_segments
WHERE driver_id=$1 AND log_date<$2::date
ORDER BY log_date DESC,COALESCE(start_second,start_minute*60) DESC,id DESC LIMIT 1`, driverID, date).Scan(&priorStatus, &priorSpecial)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// Live status may have begun on the requested day. If so, split the day at the exact status_since time.
	liveStatus, liveSpecial := priorStatus, ""
	var statusSince *time.Time
	var locationText string
	liveErr := tx.QueryRow(ctx, `SELECT duty_status,status_since,location_text FROM driver_live_state WHERE driver_id=$1`, driverID).Scan(&liveStatus, &statusSince, &locationText)
	if liveErr != nil && !errors.Is(liveErr, pgx.ErrNoRows) {
		return liveErr
	}
	if normalized, ok := normalizeStatus(liveStatus); ok {
		liveStatus = normalized
	} else {
		liveStatus = priorStatus
	}

	dayStart := time.Date(requested.Year(), requested.Month(), requested.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.AddDate(0, 0, 1)
	splitSecond := -1
	if statusSince != nil {
		localSince := statusSince.In(loc)
		if !localSince.Before(dayStart) && localSince.Before(dayEnd) {
			splitSecond = localSince.Hour()*3600 + localSince.Minute()*60 + localSince.Second()
		} else if localSince.Before(dayStart) || localSince.Equal(dayStart) {
			// If the current status has already been active since before this day, it owns the whole day.
			liveSpecial = ""
			priorStatus = liveStatus
			priorSpecial = liveSpecial
		}
	}

	endSecond := 86400
	openToday := date == today
	if openToday {
		endSecond = localNow.Hour()*3600 + localNow.Minute()*60 + localNow.Second()
	}
	if endSecond < 0 {
		endSecond = 0
	}
	if endSecond > 86400 {
		endSecond = 86400
	}

	if splitSecond > 0 && splitSecond < endSecond && priorStatus != liveStatus {
		if _, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,special_status,note,origin)
VALUES($1,$2,0,0,$3,$4,$5,$6,'','System')`, driverID, date, splitSecond/60, splitSecond, priorStatus, priorSpecial); err != nil {
			return err
		}
		if openToday {
			if _, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,special_status,note,origin,location_text)
VALUES($1,$2,$3,$4,NULL,NULL,$5,$6,'','System',$7)`, driverID, date, splitSecond/60, splitSecond, liveStatus, liveSpecial, locationText); err != nil {
				return err
			}
		} else {
			if _, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,special_status,note,origin,location_text)
VALUES($1,$2,$3,$4,1440,86400,$5,$6,'','System',$7)`, driverID, date, splitSecond/60, splitSecond, liveStatus, liveSpecial, locationText); err != nil {
				return err
			}
		}
	} else {
		status := priorStatus
		special := priorSpecial
		if statusSince != nil && !statusSince.In(loc).After(dayStart) {
			status = liveStatus
			special = liveSpecial
		}
		if openToday {
			if _, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,special_status,note,origin,location_text)
VALUES($1,$2,0,0,NULL,NULL,$3,$4,'','System',$5)`, driverID, date, status, special, locationText); err != nil {
				return err
			}
		} else {
			if _, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,special_status,note,origin,location_text)
VALUES($1,$2,0,0,1440,86400,$3,$4,'','System',$5)`, driverID, date, status, special, locationText); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

func lockDriver(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id)
	return err
}

// Every day is a projection of the latest known status, independent of app activity.
// Missing intervals carry the preceding status; they are never assumed to be OFF.
func continuousDay(in []SegmentResponse, prior SegmentResponse, limit int, open bool) []SegmentResponse {
	out := []SegmentResponse{}
	cursor := 0
	carry := copySegment(prior)
	appendCarry := func(end int) {
		if end <= cursor {
			return
		}
		x := copySegment(carry)
		x.ID = ""
		x.StartSecond = cursor
		x.StartMinute = cursor / 60
		x.EndSecond = intPointer(end)
		x.EndMinute = intPointer(end / 60)
		out = append(out, x)
		cursor = end
	}
	for _, original := range in {
		x := copySegment(original)
		start := effectiveStartSecond(x)
		if start >= limit {
			break
		}
		if start > cursor {
			appendCarry(start)
		}
		end := limit
		if x.EndSecond != nil {
			end = *x.EndSecond
		} else if x.EndMinute != nil {
			end = *x.EndMinute * 60
		}
		if end > limit {
			end = limit
		}
		if start < cursor {
			start = cursor
		}
		if end <= start {
			continue
		}
		x.StartSecond = start
		x.StartMinute = start / 60
		x.EndSecond = intPointer(end)
		x.EndMinute = intPointer(end / 60)
		out = append(out, x)
		carry = x
		cursor = end
	}
	// Extend the final fragment, retaining its identity for selections and edits.
	if cursor < limit && len(out) > 0 {
		out[len(out)-1].EndSecond = intPointer(limit)
		out[len(out)-1].EndMinute = intPointer(limit / 60)
	} else if cursor < limit {
		appendCarry(limit)
	}
	if open && len(out) > 0 {
		out[len(out)-1].EndSecond = nil
		out[len(out)-1].EndMinute = nil
	}
	return out
}

func (s *Server) continuousDayTx(ctx context.Context, tx pgx.Tx, id, date, tz string) error {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	today := now.Format("2006-01-02")
	if date > today {
		return nil
	}
	prior := SegmentResponse{DutyStatus: "OFF", Origin: "System"}
	err = tx.QueryRow(ctx, `SELECT duty_status,special_status,location_text FROM duty_segments WHERE driver_id=$1 AND log_date<$2::date ORDER BY log_date DESC,COALESCE(start_second,start_minute*60) DESC,id DESC LIMIT 1`, id, date).Scan(&prior.DutyStatus, &prior.SpecialStatus, &prior.LocationText)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var live string
	var since *time.Time
	err = tx.QueryRow(ctx, `SELECT duty_status,status_since FROM driver_live_state WHERE driver_id=$1`, id).Scan(&live, &since)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if since != nil && since.In(loc).Format("2006-01-02") < date {
		prior.DutyStatus = live
		prior.SpecialStatus = ""
	}
	before, err := s.scanSegmentsTx(ctx, tx, id, date)
	if err != nil {
		return err
	}
	limit := 86400
	if date == today {
		limit = now.Hour()*3600 + now.Minute()*60 + now.Second()
	}
	after := continuousDay(before, prior, limit, date == today)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) == string(b) {
		return nil
	}
	return s.replaceDaySegmentsTx(ctx, tx, id, date, after, date == today)
}

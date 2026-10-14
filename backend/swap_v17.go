package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type swapBlock struct {
	Segment  SegmentResponse
	Duration int
	Selected int
}

// Re-time the continuous interval without changing status order or the duration
// of any unselected event. Split at calendar boundaries only for daily display.
func retimeSwap(blocks []swapBlock, first, second, wanted int) ([]swapBlock, error) {
	if first < 0 || second < 0 || first >= len(blocks) || second >= len(blocks) || first == second {
		return nil, errors.New("select two different statuses")
	}
	total := blocks[first].Duration + blocks[second].Duration
	if wanted < 1 || wanted >= total {
		return nil, errors.New("both statuses must retain at least one second")
	}
	out := append([]swapBlock(nil), blocks...)
	out[first].Duration = wanted
	out[second].Duration = total - wanted
	return out, nil
}

func (s *Server) handleAdminSwap(w http.ResponseWriter, r *http.Request) {
	id, date := r.PathValue("id"), r.PathValue("date")
	var in struct {
		First          string `json:"first_segment_id"`
		Second         string `json:"second_segment_id"`
		SecondDate     string `json:"second_date"`
		Wanted         int    `json:"first_result_seconds"`
		FirstRevision  int64  `json:"first_revision"`
		SecondRevision int64  `json:"second_revision"`
	}
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	if in.SecondDate == "" {
		in.SecondDate = date
	}
	firstDate, e1 := time.Parse("2006-01-02", date)
	secondDate, e2 := time.Parse("2006-01-02", in.SecondDate)
	if e1 != nil || e2 != nil {
		writeError(w, 400, "invalid log date")
		return
	}
	lo, hi := firstDate, secondDate
	if lo.After(hi) {
		lo, hi = hi, lo
	}
	if hi.Sub(lo) > 30*24*time.Hour {
		writeError(w, 400, "select statuses within the 30-day log window")
		return
	}
	tz, _, _, err := s.driverDayLimit(r.Context(), id, hi.Format("2006-01-02"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if err = lockDriver(r.Context(), tx, id); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	today, _, nowSecond, _, err := localDriverMoment(r.Context(), tx, id, time.Now())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	dates := []string{}
	before := map[string][]SegmentResponse{}
	blocks := []swapBlock{}
	first, second := -1, -1
	for d := lo; !d.After(hi); d = d.AddDate(0, 0, 1) {
		key := d.Format("2006-01-02")
		dates = append(dates, key)
		if err = s.continuousDayTx(r.Context(), tx, id, key, tz); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		segs, e := s.scanSegmentsTx(r.Context(), tx, id, key)
		if e != nil {
			writeError(w, 500, e.Error())
			return
		}
		before[key] = segs
		for _, seg := range segs {
			end := 86400
			if key == today {
				end = nowSecond
			}
			if seg.EndSecond != nil {
				end = *seg.EndSecond
			} else if seg.EndMinute != nil {
				end = *seg.EndMinute * 60
			}
			if seg.ID == in.First && key == date {
				first = len(blocks)
				if in.FirstRevision > 0 && int64(seg.Revision) != in.FirstRevision {
					writeError(w, 409, "first status changed; select it again")
					return
				}
			}
			if seg.ID == in.Second && key == in.SecondDate {
				second = len(blocks)
				if in.SecondRevision > 0 && int64(seg.Revision) != in.SecondRevision {
					writeError(w, 409, "second status changed; select it again")
					return
				}
			}
			blocks = append(blocks, swapBlock{Segment: seg, Duration: end - effectiveStartSecond(seg)})
		}
	}
	changed, err := retimeSwap(blocks, first, second, in.Wanted)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	after := map[string][]SegmentResponse{}
	cursor := 0
	for _, block := range changed {
		remaining := block.Duration
		firstFragment := true
		for remaining > 0 {
			dayIndex := cursor / 86400
			if dayIndex >= len(dates) {
				writeError(w, 400, "invalid timeline")
				return
			}
			start := cursor % 86400
			length := remaining
			if length > 86400-start {
				length = 86400 - start
			}
			x := copySegment(block.Segment)
			if !firstFragment {
				x.ID = ""
			}
			firstFragment = false
			x.StartSecond = start
			x.StartMinute = start / 60
			x.EndSecond = intPointer(start + length)
			x.EndMinute = intPointer((start + length) / 60)
			x.Edited = true
			x.EditReason = "Cross-date duration correction"
			x.Revision++
			after[dates[dayIndex]] = append(after[dates[dayIndex]], x)
			cursor += length
			remaining -= length
		}
	}
	var operation string
	if err = tx.QueryRow(r.Context(), `SELECT gen_random_uuid()::text`).Scan(&operation); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	// Delete first: a status ID can move into another day in this same transaction.
	if _, err = tx.Exec(r.Context(), `DELETE FROM duty_segments WHERE driver_id=$1 AND log_date BETWEEN $2::date AND $3::date`, id, dates[0], dates[len(dates)-1]); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	for _, key := range dates {
		if err = s.replaceDaySegmentsTx(r.Context(), tx, id, key, after[key], key == today); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		saved, e := s.scanSegmentsTx(r.Context(), tx, id, key)
		if e != nil {
			writeError(w, 500, e.Error())
			return
		}
		a, _ := json.Marshal(before[key])
		b, _ := json.Marshal(saved)
		if _, err = tx.Exec(r.Context(), `INSERT INTO log_edit_batches(driver_id,log_date,action,reason,operator_name,start_minute,end_minute,before_segments,after_segments,operation_id) VALUES($1,$2,'swap','Cross-date duration correction',$3,0,1440,$4::jsonb,$5::jsonb,$6::uuid)`, id, key, operatorName(r.Context()), string(a), string(b), operation); err != nil {
			writeError(w, 500, err.Error())
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_ = s.recalcHOSFromSegments(r.Context(), id)
	writeJSON(w, 200, map[string]any{"ok": true, "operation_id": operation, "affected_dates": dates, "first_after_seconds": in.Wanted, "second_after_seconds": changed[second].Duration})
}

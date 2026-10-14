package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"reflect"
)

func (s *Server) undoSwapTx(ctx context.Context, tx pgx.Tx, id, op, operator string) error {
	rows, err := tx.Query(ctx, `SELECT to_char(log_date,'YYYY-MM-DD'),before_segments::text,after_segments::text FROM log_edit_batches WHERE driver_id=$1 AND operation_id=$2::uuid AND undone_at IS NULL ORDER BY log_date FOR UPDATE`, id, op)
	if err != nil {
		return err
	}
	type snapshot struct {
		date          string
		before, after []SegmentResponse
	}
	all := []snapshot{}
	for rows.Next() {
		var x snapshot
		var a, b string
		if err = rows.Scan(&x.date, &a, &b); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal([]byte(a), &x.before); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal([]byte(b), &x.after); err != nil {
			rows.Close()
			return err
		}
		all = append(all, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(all) == 0 {
		return errors.New("swap already undone")
	}
	for _, x := range all {
		current, e := s.scanSegmentsTx(ctx, tx, id, x.date)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(current, x.after) {
			return errors.New("one of the affected days changed after this swap; review later changes first")
		}
	}
	for _, x := range all {
		if _, err = tx.Exec(ctx, `DELETE FROM duty_segments WHERE driver_id=$1 AND log_date=$2`, id, x.date); err != nil {
			return err
		}
	}
	today := driverCurrentDateServer(ctx, tx, id)
	for _, x := range all {
		if err = s.replaceDaySegmentsTx(ctx, tx, id, x.date, x.before, x.date == today); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE log_edit_batches SET undone_at=now(),undone_by=$3 WHERE driver_id=$1 AND operation_id=$2::uuid`, id, op, operator)
	return err
}

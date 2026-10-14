package main

import (
	"context"
	"time"
)

func (s *Server) ensureDriverDayContinuity(ctx context.Context, id, date, tz string) error {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockDriver(ctx, tx, id); err != nil {
		return err
	}
	if err = s.continuousDayTx(ctx, tx, id, date, tz); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

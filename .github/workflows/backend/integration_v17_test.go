package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// Run against a disposable PostgreSQL database only.
func TestV17DatabaseFlow(t *testing.T) {
	url := os.Getenv("NEKSUS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set NEKSUS_TEST_DATABASE_URL to a disposable database")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Server{db: db}
	if err = runMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := db.Exec(ctx, sql, args...); e != nil {
			t.Fatal(e)
		}
	}
	id := fmt.Sprintf("test-v17-%d", time.Now().UnixNano())
	defer db.Exec(ctx, "DELETE FROM drivers WHERE id=$1", id)
	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -4)
	exec("INSERT INTO drivers(id,full_name,home_terminal_timezone) VALUES($1,'Regression Driver','UTC')", id)
	exec("INSERT INTO driver_live_state(driver_id,duty_status,status_since) VALUES($1,'SB',$2)", id, base.AddDate(0, 0, -3))
	call := func(kind, when string, payload map[string]any, code int) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(payload)
		r := httptest.NewRequest("POST", "/", bytes.NewReader(b))
		r = r.WithContext(context.WithValue(ctx, driverIDContextKey, id))
		r.SetPathValue("id", id)
		r.SetPathValue("date", when)
		w := httptest.NewRecorder()
		switch kind {
		case "telemetry":
			s.handleDriverTelemetry(w, r)
		case "status":
			s.handleDriverSelfStatus(w, r)
		case "swap":
			s.handleAdminSwap(w, r)
		case "undo":
			s.handleAdminLogUndo(w, r)
		}
		if w.Code != code {
			t.Fatalf("%s status %d: %s", kind, w.Code, w.Body.String())
		}
		return w
	}
	date := base.Format("2006-01-02")
	for i := 0; i < 4; i++ {
		if err = s.ensureDriverDayContinuity(ctx, id, base.AddDate(0, 0, i).Format("2006-01-02"), "UTC"); err != nil {
			t.Fatal(err)
		}
	}
	var duty string
	var end int
	if err = db.QueryRow(ctx, "SELECT duty_status,end_second FROM duty_segments WHERE driver_id=$1 AND log_date=$2", id, date).Scan(&duty, &end); err != nil || duty != "SB" || end != 86400 {
		t.Fatalf("carry %s %d %v", duty, end, err)
	}
	telemetry := func(at time.Time, speed float64, key string) map[string]any {
		return map[string]any{"recorded_at": at.Format(time.RFC3339Nano), "client_event_id": key, "latitude": 41, "longitude": -87, "accuracy_meters": 5, "speed_mps": speed, "speed_valid": true, "motion_version": 17, "auto_stop_seconds": 30, "auto_stop_status": "ON"}
	}
	start := base.Add(11 * time.Hour)
	for i := 0; i < 5; i++ {
		call("telemetry", "", telemetry(start.Add(time.Duration(i)*3*time.Second), 0, fmt.Sprint("zero", i)), 200)
	}
	if err = db.QueryRow(ctx, "SELECT duty_status FROM driver_live_state WHERE driver_id=$1", id).Scan(&duty); err != nil || duty != "SB" {
		t.Fatal("zero activated driving", duty, err)
	}
	for i := 0; i < 3; i++ {
		call("telemetry", "", telemetry(start.Add(time.Duration(30+i*3)*time.Second), 10, fmt.Sprint("move", i)), 200)
	}
	if err = db.QueryRow(ctx, "SELECT duty_status FROM driver_live_state WHERE driver_id=$1", id).Scan(&duty); err != nil || duty != "DR" {
		t.Fatal("movement missed", duty, err)
	}
	for i := 0; i <= 10; i++ {
		call("telemetry", "", telemetry(start.Add(time.Duration(45+i*3)*time.Second), 0, fmt.Sprint("stop", i)), 200)
	}
	call("telemetry", "", telemetry(start.Add(75*time.Second), 0, "stop10"), 200)
	if err = db.QueryRow(ctx, "SELECT duty_status FROM driver_live_state WHERE driver_id=$1", id).Scan(&duty); err != nil || duty != "ON" {
		t.Fatal("stop transition missed", duty, err)
	}
	call("status", "", map[string]any{"client_event_id": "manual-sb", "recorded_at": base.AddDate(0, 0, 1).Add(3 * time.Hour).Format(time.RFC3339), "status": "SB", "latitude": 41, "longitude": -87, "speed_mps": 0}, 200)
	for i := 1; i < 4; i++ {
		if err = s.ensureDriverDayContinuity(ctx, id, base.AddDate(0, 0, i).Format("2006-01-02"), "UTC"); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(ctx, "SELECT duty_status,end_second FROM duty_segments WHERE driver_id=$1 AND log_date=$2 ORDER BY start_second DESC LIMIT 1", id, base.AddDate(0, 0, 3).Format("2006-01-02")).Scan(&duty, &end); err != nil || duty != "SB" || end != 86400 {
		t.Fatalf("offline carry %s %d %v", duty, end, err)
	}
	call("telemetry", "", telemetry(start, 0, "out-of-order"), 409)
	exec("DELETE FROM duty_segments WHERE driver_id=$1", id)
	day2 := base.AddDate(0, 0, 2).Format("2006-01-02")
	for _, d := range []string{date, base.AddDate(0, 0, 1).Format("2006-01-02"), day2} {
		exec("INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status) VALUES($1,$2,0,0,600,36000,'SB'),($1,$2,600,36000,900,54000,'ON'),($1,$2,900,54000,1440,86400,'OFF')", id, d)
	}
	var first, second string
	_ = db.QueryRow(ctx, "SELECT id::text FROM duty_segments WHERE driver_id=$1 AND log_date=$2 AND start_second=0", id, date).Scan(&first)
	_ = db.QueryRow(ctx, "SELECT id::text FROM duty_segments WHERE driver_id=$1 AND log_date=$2 AND start_second=36000", id, day2).Scan(&second)
	call("swap", date, map[string]any{"first_segment_id": first, "second_segment_id": second, "second_date": day2, "first_result_seconds": 18000, "first_revision": 1, "second_revision": 1}, 200)
	var total int
	if err = db.QueryRow(ctx, "SELECT sum(end_second-start_second)::int FROM duty_segments WHERE driver_id=$1", id).Scan(&total); err != nil || total != 3*86400 {
		t.Fatal("swap lost duration", total, err)
	}
	call("undo", day2, map[string]any{}, 200)
	if err = db.QueryRow(ctx, "SELECT end_second FROM duty_segments WHERE id=$1::uuid", first).Scan(&end); err != nil || end != 36000 {
		t.Fatal("undo did not restore first day", end, err)
	}
	t.Log("PASS: migrations, multi-day carry, zero speed, sustained motion, stop delay, replay, offline status, cross-date swap and undo")
}

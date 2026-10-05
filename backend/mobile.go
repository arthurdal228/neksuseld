package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	autoDriveStartSpeedMPS = 2.2352  // 5 mph
	autoDriveStopSpeedMPS  = 0.89408 // 2 mph
	autoDriveStartSamples  = 2
	autoDriveStopHold      = 5 * time.Minute
)

type telemetryInput struct {
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	AccuracyMeters float64 `json:"accuracy_meters"`
	SpeedMPS       float64 `json:"speed_mps"`
	OdometerMiles  float64 `json:"odometer_miles"`
	LocationText   string  `json:"location_text"`
	RecordedAt     string  `json:"recorded_at"`
}

func validCoordinates(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)
}

func localDriverMoment(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, driverID string, at time.Time) (string, int, int, string, error) {
	var timezone string
	if err := q.QueryRow(ctx, `SELECT home_terminal_timezone FROM drivers WHERE id=$1 AND active=true`, driverID).Scan(&timezone); err != nil {
		return "", 0, 0, "", err
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	t := at.In(loc)
	second := t.Hour()*3600 + t.Minute()*60 + t.Second()
	return t.Format("2006-01-02"), second / 60, second, timezone, nil
}

func (s *Server) transitionDriverStatusTx(ctx context.Context, tx pgx.Tx, driverID, status, note, origin, locationText string, lat, lon, odometer, accuracy, speed float64, at time.Time, automatic bool) (string, int, int, bool, error) {
	logDate, minute, second, _, err := localDriverMoment(ctx, tx, driverID, at)
	if err != nil {
		return "", 0, 0, false, err
	}
	currentStatus := "OFF"
	var existingLocation string
	var existingLat, existingLon, existingOdo float64
	err = tx.QueryRow(ctx, `SELECT duty_status,location_text,latitude,longitude,odometer_miles FROM driver_live_state WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&currentStatus, &existingLocation, &existingLat, &existingLon, &existingOdo)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", 0, 0, false, err
	}
	if strings.TrimSpace(locationText) == "" {
		locationText = existingLocation
	}
	if !validCoordinates(lat, lon) {
		lat, lon = existingLat, existingLon
	}
	if odometer <= 0 {
		odometer = existingOdo
	}
	changed := currentStatus != status
	_, err = tx.Exec(ctx, `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,odometer_miles,gps_accuracy_meters,gps_speed_mps,gps_recorded_at,status_since,revision)
VALUES($1,$2,true,$3,$4,$5,$6,$7,$8,$9,$9,1)
ON CONFLICT(driver_id) DO UPDATE SET
 duty_status=EXCLUDED.duty_status, connected=true, location_text=EXCLUDED.location_text,
 latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude, odometer_miles=GREATEST(EXCLUDED.odometer_miles,0),
 gps_accuracy_meters=EXCLUDED.gps_accuracy_meters, gps_speed_mps=EXCLUDED.gps_speed_mps, gps_recorded_at=EXCLUDED.gps_recorded_at,
 status_since=CASE WHEN driver_live_state.duty_status<>EXCLUDED.duty_status THEN EXCLUDED.status_since ELSE driver_live_state.status_since END,
 revision=driver_live_state.revision+1`, driverID, status, locationText, lat, lon, odometer, accuracy, speed, at)
	if err != nil {
		return "", 0, 0, false, err
	}

	var openID, openStatus string
	var openStartMinute, openStartSecond int
	err = tx.QueryRow(ctx, `
SELECT id::text,duty_status,start_minute,COALESCE(start_second,start_minute*60)
FROM duty_segments
WHERE driver_id=$1 AND log_date=$2 AND end_minute IS NULL
ORDER BY start_minute DESC, created_at DESC
LIMIT 1 FOR UPDATE`, driverID, logDate).Scan(&openID, &openStatus, &openStartMinute, &openStartSecond)
	hadOpen := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", 0, 0, false, err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,note,origin,location_text,odometer_miles) VALUES($1,$2,$3,$4,NULL,NULL,$5,$6,$7,$8,$9)`, driverID, logDate, minute, second, status, note, origin, locationText, odometer)
	} else if openStatus != status {
		if second < openStartSecond {
			second = openStartSecond
			minute = second / 60
		}
		if _, err = tx.Exec(ctx, `UPDATE duty_segments SET end_minute=$2,end_second=$3 WHERE id=$1::uuid`, openID, minute, second); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,start_second,end_minute,end_second,duty_status,note,origin,location_text,odometer_miles) VALUES($1,$2,$3,$4,NULL,NULL,$5,$6,$7,$8,$9)`, driverID, logDate, minute, second, status, note, origin, locationText, odometer)
		}
	}
	if err != nil {
		return "", 0, 0, false, err
	}
	if changed || !hadOpen {
		eventType := "duty_status"
		if automatic && status == "DR" {
			eventType = "drive_start"
		} else if automatic && currentStatus == "DR" && status != "DR" {
			eventType = "drive_end"
		}
		_, err = tx.Exec(ctx, `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,latitude,longitude,gps_accuracy_meters,odometer_miles,origin)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, driverID, logDate, minute, at, eventType, status, note, locationText, lat, lon, accuracy, odometer, origin)
		if err != nil {
			return "", 0, 0, false, err
		}
	}
	return logDate, minute, second, changed, nil
}

func (s *Server) handleDriverOdometer(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	var in struct {
		OdometerMiles float64 `json:"odometer_miles"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil || in.OdometerMiles <= 0 {
		writeError(w, 400, "a positive odometer value is required")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if _, err = tx.Exec(r.Context(), `UPDATE driver_live_state SET odometer_miles=$2,revision=revision+1 WHERE driver_id=$1`, driverID, in.OdometerMiles); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	var pendingID string
	err = tx.QueryRow(r.Context(), `SELECT id::text FROM drive_sessions WHERE driver_id=$1 AND state='pending' ORDER BY ended_at DESC LIMIT 1 FOR UPDATE`, driverID).Scan(&pendingID)
	if err == nil {
		if _, err = tx.Exec(r.Context(), `UPDATE drive_sessions SET end_odometer_miles=$2 WHERE id=$1::uuid`, pendingID, in.OdometerMiles); err == nil {
			err = s.normalizeDriveTx(r.Context(), tx, driverID, pendingID, in.OdometerMiles)
		}
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 500, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	d, _ := s.queryDriver(r.Context(), driverID)
	writeJSON(w, 200, map[string]any{"ok": true, "driver": d})
}

func haversineMiles(lat1, lon1, lat2, lon2 float64) float64 {
	const earthMiles = 3958.7613
	p1, p2 := lat1*math.Pi/180, lat2*math.Pi/180
	dlat := (lat2 - lat1) * math.Pi / 180
	dlon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	return 2 * earthMiles * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func (s *Server) handleDriverTelemetry(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	var in telemetryInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	if !validCoordinates(in.Latitude, in.Longitude) {
		writeError(w, 400, "valid GPS coordinates are required")
		return
	}
	if in.SpeedMPS < 0 {
		in.SpeedMPS = 0
	}
	if in.AccuracyMeters < 0 {
		in.AccuracyMeters = 0
	}
	at := time.Now().UTC()
	if strings.TrimSpace(in.RecordedAt) != "" {
		if t, err := time.Parse(time.RFC3339Nano, in.RecordedAt); err == nil {
			at = t.UTC()
		}
	}
	if at.After(time.Now().UTC().Add(2 * time.Minute)) {
		at = time.Now().UTC()
	}
	if strings.TrimSpace(in.LocationText) == "" {
		in.LocationText = "Location unavailable"
	}
	var serverOdometer float64
	_ = s.db.QueryRow(r.Context(), `SELECT COALESCE(odometer_miles,0) FROM driver_live_state WHERE driver_id=$1`, driverID).Scan(&serverOdometer)
	in.OdometerMiles = serverOdometer

	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())

	// Some Android/mock providers move coordinates correctly but report speed as zero.
	// Derive a fallback speed from the previous GPS fix so sustained real movement can still enter DR.
	var previousLat, previousLon float64
	var previousRecordedAt *time.Time
	_ = tx.QueryRow(r.Context(), `SELECT latitude,longitude,gps_recorded_at FROM driver_live_state WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&previousLat, &previousLon, &previousRecordedAt)
	effectiveSpeedMPS := math.Max(0, in.SpeedMPS)
	if previousRecordedAt != nil && validCoordinates(previousLat, previousLon) && at.After(*previousRecordedAt) {
		dt := at.Sub(*previousRecordedAt).Seconds()
		if dt >= 1 && dt <= 120 {
			distanceMeters := haversineMiles(previousLat, previousLon, in.Latitude, in.Longitude) * 1609.344
			derived := distanceMeters / dt
			if derived >= 0 && derived <= 80 && derived > effectiveSpeedMPS {
				effectiveSpeedMPS = derived
			}
		}
	}
	in.SpeedMPS = effectiveSpeedMPS

	// Update live GPS first, preserving the current duty status.
	_, err = tx.Exec(r.Context(), `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,odometer_miles,gps_accuracy_meters,gps_speed_mps,gps_recorded_at,status_since,revision)
VALUES($1,'OFF',true,$2,$3,$4,$5,$6,$7,$8,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET connected=true,location_text=EXCLUDED.location_text,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,
 odometer_miles=CASE WHEN EXCLUDED.odometer_miles>0 THEN EXCLUDED.odometer_miles ELSE driver_live_state.odometer_miles END,
 gps_accuracy_meters=EXCLUDED.gps_accuracy_meters,gps_speed_mps=EXCLUDED.gps_speed_mps,gps_recorded_at=EXCLUDED.gps_recorded_at,revision=driver_live_state.revision+1`,
		driverID, in.LocationText, in.Latitude, in.Longitude, in.OdometerMiles, in.AccuracyMeters, in.SpeedMPS, at)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	var movingSamples int
	var stoppedSince, movingStartedAt *time.Time
	var activeDriveID *string
	var movingStartLat, movingStartLon, movingStartAccuracy, movingStartOdo, lastSpeed float64
	var movingStartLocation string
	err = tx.QueryRow(r.Context(), `
SELECT moving_samples,stopped_since,active_drive_id::text,moving_started_at,moving_start_latitude,moving_start_longitude,
 moving_start_accuracy,moving_start_odometer,moving_start_location_text,last_speed_mps
FROM driver_motion_state WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&movingSamples, &stoppedSince, &activeDriveID, &movingStartedAt, &movingStartLat, &movingStartLon, &movingStartAccuracy, &movingStartOdo, &movingStartLocation, &lastSpeed)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(r.Context(), `INSERT INTO driver_motion_state(driver_id,last_sample_at,last_speed_mps) VALUES($1,$2,$3)`, driverID, at, in.SpeedMPS)
		movingSamples = 0
		stoppedSince = nil
		activeDriveID = nil
		movingStartedAt = nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 500, err.Error())
		return
	}

	started, ended := false, false
	if activeDriveID == nil || *activeDriveID == "" {
		if in.SpeedMPS >= autoDriveStartSpeedMPS && (in.AccuracyMeters <= 200 || in.AccuracyMeters == 0) {
			if movingSamples == 0 || movingStartedAt == nil {
				t := at
				movingStartedAt = &t
				movingStartLat = in.Latitude
				movingStartLon = in.Longitude
				movingStartAccuracy = in.AccuracyMeters
				movingStartOdo = in.OdometerMiles
				movingStartLocation = in.LocationText
			}
			movingSamples++
		} else {
			movingSamples = 0
			movingStartedAt = nil
		}
		if movingSamples >= autoDriveStartSamples && movingStartedAt != nil {
			logDate, _, startSecond, _, e := localDriverMoment(r.Context(), tx, driverID, *movingStartedAt)
			if e != nil {
				writeError(w, 500, e.Error())
				return
			}
			var driveID string
			e = tx.QueryRow(r.Context(), `
INSERT INTO drive_sessions(driver_id,log_date,state,started_at,start_second,start_latitude,start_longitude,start_location_text,start_odometer_miles)
VALUES($1,$2,'active',$3,$4,$5,$6,$7,$8) RETURNING id::text`, driverID, logDate, *movingStartedAt, startSecond, movingStartLat, movingStartLon, movingStartLocation, movingStartOdo).Scan(&driveID)
			if e != nil {
				writeError(w, 500, e.Error())
				return
			}
			_, _ = tx.Exec(r.Context(), `INSERT INTO drive_points(session_id,driver_id,recorded_at,latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9)`, driveID, driverID, *movingStartedAt, movingStartLat, movingStartLon, movingStartAccuracy, autoDriveStartSpeedMPS, movingStartOdo, movingStartLocation)
			_, _ = tx.Exec(r.Context(), `INSERT INTO drive_points(session_id,driver_id,recorded_at,latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9)`, driveID, driverID, at, in.Latitude, in.Longitude, in.AccuracyMeters, in.SpeedMPS, in.OdometerMiles, in.LocationText)
			_, _, _, _, e = s.transitionDriverStatusTx(r.Context(), tx, driverID, "DR", "Automatic driving detected", "DriveAnalyzer", movingStartLocation, movingStartLat, movingStartLon, movingStartOdo, movingStartAccuracy, in.SpeedMPS, *movingStartedAt, true)
			if e != nil {
				writeError(w, 500, e.Error())
				return
			}
			activeDriveID = &driveID
			movingSamples = 0
			stoppedSince = nil
			started = true
		}
	} else {
		_, err = tx.Exec(r.Context(), `INSERT INTO drive_points(session_id,driver_id,recorded_at,latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9)`, *activeDriveID, driverID, at, in.Latitude, in.Longitude, in.AccuracyMeters, in.SpeedMPS, in.OdometerMiles, in.LocationText)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		if in.SpeedMPS <= autoDriveStopSpeedMPS {
			if stoppedSince == nil {
				t := at
				stoppedSince = &t
			}
			if at.Sub(*stoppedSince) >= autoDriveStopHold {
				// Close the drive at the first stationary sample, not five minutes later.
				// This keeps the endpoint tied to the real GPS point where movement stopped.
				endAt := *stoppedSince
				endLat, endLon, endAccuracy, endSpeed, endOdo := in.Latitude, in.Longitude, in.AccuracyMeters, in.SpeedMPS, in.OdometerMiles
				endLocation := in.LocationText
				_ = tx.QueryRow(r.Context(), `
SELECT latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text
FROM drive_points
WHERE session_id=$1::uuid AND recorded_at <= $2 + interval '1 second'
ORDER BY recorded_at DESC LIMIT 1`, *activeDriveID, endAt).Scan(&endLat, &endLon, &endAccuracy, &endSpeed, &endOdo, &endLocation)
				_, _, endSecond, _, e := localDriverMoment(r.Context(), tx, driverID, endAt)
				if e != nil {
					writeError(w, 500, e.Error())
					return
				}
				_, e = tx.Exec(r.Context(), `UPDATE drive_sessions SET state='pending',ended_at=$2,end_second=$3,end_latitude=$4,end_longitude=$5,end_location_text=$6,end_odometer_miles=$7 WHERE id=$1::uuid`, *activeDriveID, endAt, endSecond, endLat, endLon, endLocation, endOdo)
				if e != nil {
					writeError(w, 500, e.Error())
					return
				}
				if endOdo > 0 {
					_ = s.normalizeDriveTx(r.Context(), tx, driverID, *activeDriveID, endOdo)
				}
				_, _, _, _, e = s.transitionDriverStatusTx(r.Context(), tx, driverID, "ON", "Automatic stop after driving", "DriveAnalyzer", endLocation, endLat, endLon, endOdo, endAccuracy, endSpeed, endAt, true)
				if e != nil {
					writeError(w, 500, e.Error())
					return
				}
				activeDriveID = nil
				stoppedSince = nil
				ended = true
			}
		} else {
			stoppedSince = nil
		}
	}

	var activeArg any
	if activeDriveID != nil && *activeDriveID != "" {
		activeArg = *activeDriveID
	} else {
		activeArg = nil
	}
	_, err = tx.Exec(r.Context(), `
INSERT INTO driver_motion_state(driver_id,moving_samples,stopped_since,active_drive_id,last_sample_at,last_speed_mps,moving_started_at,moving_start_latitude,moving_start_longitude,moving_start_accuracy,moving_start_odometer,moving_start_location_text)
VALUES($1,$2,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT(driver_id) DO UPDATE SET moving_samples=EXCLUDED.moving_samples,stopped_since=EXCLUDED.stopped_since,active_drive_id=EXCLUDED.active_drive_id,
 last_sample_at=EXCLUDED.last_sample_at,last_speed_mps=EXCLUDED.last_speed_mps,moving_started_at=EXCLUDED.moving_started_at,
 moving_start_latitude=EXCLUDED.moving_start_latitude,moving_start_longitude=EXCLUDED.moving_start_longitude,moving_start_accuracy=EXCLUDED.moving_start_accuracy,
 moving_start_odometer=EXCLUDED.moving_start_odometer,moving_start_location_text=EXCLUDED.moving_start_location_text,updated_at=now()`,
		driverID, movingSamples, stoppedSince, activeArg, at, in.SpeedMPS, movingStartedAt, movingStartLat, movingStartLon, movingStartAccuracy, movingStartOdo, movingStartLocation)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if started || ended {
		_ = s.recalcHOSFromSegments(r.Context(), driverID)
	}
	writeJSON(w, 200, map[string]any{"ok": true, "started_drive": started, "ended_drive": ended, "active_drive_id": activeDriveID, "speed_mps": in.SpeedMPS})
}

func formatCoord(v float64) string {
	return strconv.FormatFloat(v, 'f', 5, 64)
}

func (s *Server) scanDriveSession(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, driverID, id string) (DriveSessionResponse, error) {
	var d DriveSessionResponse
	var started time.Time
	var ended *time.Time
	var logDate time.Time
	err := q.QueryRow(ctx, `SELECT id::text,driver_id,log_date,state,started_at,ended_at,start_second,end_second,start_latitude,start_longitude,end_latitude,end_longitude,start_location_text,end_location_text,start_odometer_miles,end_odometer_miles,gps_distance_miles,odometer_distance_miles,variance_miles,variance_percent,normalized,review_reason FROM drive_sessions WHERE driver_id=$1 AND id=$2::uuid`, driverID, id).Scan(&d.ID, &d.DriverID, &logDate, &d.State, &started, &ended, &d.StartSecond, &d.EndSecond, &d.StartLatitude, &d.StartLongitude, &d.EndLatitude, &d.EndLongitude, &d.StartLocationText, &d.EndLocationText, &d.StartOdometerMiles, &d.EndOdometerMiles, &d.GPSDistanceMiles, &d.OdometerDistanceMiles, &d.VarianceMiles, &d.VariancePercent, &d.Normalized, &d.ReviewReason)
	if err != nil {
		return d, err
	}
	d.LogDate = logDate.Format("2006-01-02")
	d.StartedAt = started.UTC().Format(time.RFC3339)
	d.EndedAt = timeString(ended)
	return d, nil
}

func (s *Server) handleDriverDriveState(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	var activeID, pendingID string
	_ = s.db.QueryRow(r.Context(), `SELECT id::text FROM drive_sessions WHERE driver_id=$1 AND state='active' ORDER BY started_at DESC LIMIT 1`, driverID).Scan(&activeID)
	_ = s.db.QueryRow(r.Context(), `SELECT id::text FROM drive_sessions WHERE driver_id=$1 AND state IN ('pending','review') ORDER BY ended_at DESC NULLS LAST LIMIT 1`, driverID).Scan(&pendingID)
	var active, pending any
	if activeID != "" {
		if d, e := s.scanDriveSession(r.Context(), s.db, driverID, activeID); e == nil {
			active = d
		}
	}
	if pendingID != "" {
		if d, e := s.scanDriveSession(r.Context(), s.db, driverID, pendingID); e == nil {
			pending = d
		}
	}
	writeJSON(w, 200, map[string]any{"active": active, "pending": pending, "analyzer": map[string]any{"start_speed_mph": 5, "start_samples": autoDriveStartSamples, "stop_speed_mph": 2, "stop_hold_seconds": int(autoDriveStopHold.Seconds())}})
}

func (s *Server) handleDriverNormalizeDrive(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	driveID := r.PathValue("id")
	var in struct {
		EndOdometerMiles float64 `json:"end_odometer_miles"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil || in.EndOdometerMiles <= 0 {
		writeError(w, 400, "ending odometer is required")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if err = s.normalizeDriveTx(r.Context(), tx, driverID, driveID, in.EndOdometerMiles); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_ = s.recalcHOSFromSegments(r.Context(), driverID)
	d, err := s.scanDriveSession(r.Context(), s.db, driverID, driveID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "drive": d})
}

func (s *Server) normalizeDriveTx(ctx context.Context, tx pgx.Tx, driverID, driveID string, endOdo float64) error {
	var startOdo float64
	var started time.Time
	var ended *time.Time
	var logDate time.Time
	if err := tx.QueryRow(ctx, `SELECT start_odometer_miles,started_at,ended_at,log_date FROM drive_sessions WHERE id=$1::uuid AND driver_id=$2 FOR UPDATE`, driveID, driverID).Scan(&startOdo, &started, &ended, &logDate); err != nil {
		return err
	}
	if ended == nil {
		return errors.New("drive is still active")
	}
	rows, err := tx.Query(ctx, `SELECT recorded_at,latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text FROM drive_points WHERE session_id=$1::uuid AND recorded_at BETWEEN $2 AND $3 ORDER BY recorded_at`, driveID, started, *ended)
	if err != nil {
		return err
	}
	defer rows.Close()
	type pt struct {
		t                              time.Time
		lat, lon, accuracy, speed, odo float64
		loc                            string
	}
	pts := []pt{}
	for rows.Next() {
		var p pt
		if err := rows.Scan(&p.t, &p.lat, &p.lon, &p.accuracy, &p.speed, &p.odo, &p.loc); err != nil {
			return err
		}
		pts = append(pts, p)
	}
	if len(pts) < 2 {
		_, _ = tx.Exec(ctx, `UPDATE drive_sessions SET state='review',review_reason='Not enough GPS points',end_odometer_miles=$2 WHERE id=$1::uuid`, driveID, endOdo)
		return nil
	}
	gps := 0.0
	for i := 1; i < len(pts); i++ {
		prev, cur := pts[i-1], pts[i]
		prevOK := prev.accuracy <= 150 || prev.accuracy == 0
		curOK := cur.accuracy <= 150 || cur.accuracy == 0
		dt := cur.t.Sub(prev.t).Seconds()
		if !prevOK || !curOK || dt <= 0 {
			continue
		}
		d := haversineMiles(prev.lat, prev.lon, cur.lat, cur.lon)
		// Reject obvious GPS jumps. Allow a small stationary jitter budget plus ~150 mph maximum implied movement.
		maxPlausible := math.Max(0.15, dt/3600*150+0.05)
		if d <= maxPlausible {
			gps += d
		}
	}
	odo := endOdo - startOdo
	if startOdo <= 0 || endOdo <= startOdo {
		_, _ = tx.Exec(ctx, `UPDATE drive_sessions SET state='pending',review_reason='Enter a valid ending odometer',end_odometer_miles=$2,gps_distance_miles=$3 WHERE id=$1::uuid`, driveID, endOdo, gps)
		return nil
	}
	variance := math.Abs(odo - gps)
	pct := 0.0
	if gps > 0.1 {
		pct = variance / gps * 100
	}
	tolerance := math.Max(5, gps*0.10)
	state := "normalized"
	reason := ""
	normalized := true
	if gps < 0.25 {
		state = "review"
		reason = "GPS route is too short to validate"
		normalized = false
	} else if variance > tolerance {
		state = "review"
		reason = "Odometer and GPS route distance do not agree"
		normalized = false
	}
	_, err = tx.Exec(ctx, `UPDATE drive_sessions SET state=$2,end_odometer_miles=$3,gps_distance_miles=$4,odometer_distance_miles=$5,variance_miles=$6,variance_percent=$7,normalized=$8,review_reason=$9 WHERE id=$1::uuid`, driveID, state, endOdo, gps, odo, variance, pct, normalized, reason)
	if err != nil {
		return err
	}
	if !normalized {
		return nil
	}
	_, _ = tx.Exec(ctx, `DELETE FROM eld_events WHERE drive_session_id=$1::uuid AND event_type='intermediate'`, driveID)
	// Create hourly intermediates from the recorded GPS track. Mileage is interpolated between user-entered odometers.
	for target := started.Add(time.Hour); target.Before(*ended); target = target.Add(time.Hour) {
		idx := sort.Search(len(pts), func(i int) bool { return !pts[i].t.Before(target) })
		if idx >= len(pts) {
			idx = len(pts) - 1
		} else if idx > 0 && target.Sub(pts[idx-1].t) < pts[idx].t.Sub(target) {
			idx--
		}
		p := pts[idx]
		frac := target.Sub(started).Seconds() / ended.Sub(started).Seconds()
		if frac < 0 {
			frac = 0
		}
		if frac > 1 {
			frac = 1
		}
		interOdo := startOdo + odo*frac
		_, minute, _, _, e := localDriverMoment(ctx, tx, driverID, target)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,latitude,longitude,gps_accuracy_meters,odometer_miles,origin,drive_session_id) VALUES($1,$2,$3,$4,'intermediate','DR','Normalized hourly intermediate',$5,$6,$7,$8,$9,'DriveNormalizer',$10::uuid)`, driverID, logDate.Format("2006-01-02"), minute, target, p.loc, p.lat, p.lon, p.accuracy, interOdo, driveID)
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *Server) handleAdminSwap(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	date := r.PathValue("date")
	var in struct {
		FirstSegmentID     string `json:"first_segment_id"`
		SecondSegmentID    string `json:"second_segment_id"`
		FirstResultSeconds int    `json:"first_result_seconds"`
		// Legacy v12 fields are kept for a short compatibility window.
		SegmentAID      string `json:"segment_a_id"`
		SegmentBID      string `json:"segment_b_id"`
		FromSegmentID   string `json:"from_segment_id"`
		TransferSeconds int    `json:"transfer_seconds"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	segs, err := s.scanSegmentsTx(r.Context(), tx, driverID, date)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	firstID, secondID := strings.TrimSpace(in.FirstSegmentID), strings.TrimSpace(in.SecondSegmentID)
	if firstID == "" || secondID == "" {
		// Translate the old v12 transfer request into the new "first result" model.
		firstID, secondID = strings.TrimSpace(in.SegmentAID), strings.TrimSpace(in.SegmentBID)
	}
	firstIndex, secondIndex := -1, -1
	for i, x := range segs {
		if x.ID == firstID {
			firstIndex = i
		}
		if x.ID == secondID {
			secondIndex = i
		}
	}
	if firstIndex < 0 || secondIndex < 0 {
		writeError(w, 404, "selected status was not found")
		return
	}
	if firstIndex == secondIndex || int(math.Abs(float64(firstIndex-secondIndex))) != 1 {
		writeError(w, 400, "swap requires two touching statuses")
		return
	}

	first := segs[firstIndex]
	second := segs[secondIndex]
	firstStart, firstEnd := effectiveStartSecond(first), 86400
	if first.EndSecond != nil {
		firstEnd = *first.EndSecond
	} else if first.EndMinute != nil {
		firstEnd = *first.EndMinute * 60
	}
	secondStart, secondEnd := effectiveStartSecond(second), 86400
	if second.EndSecond != nil {
		secondEnd = *second.EndSecond
	} else if second.EndMinute != nil {
		secondEnd = *second.EndMinute * 60
	}
	if date == driverCurrentDateServer(r.Context(), tx, driverID) {
		if _, _, nowSecond, _, e := localDriverMoment(r.Context(), tx, driverID, time.Now().UTC()); e == nil {
			if first.EndSecond == nil && first.EndMinute == nil {
				firstEnd = nowSecond
			}
			if second.EndSecond == nil && second.EndMinute == nil {
				secondEnd = nowSecond
			}
		}
	}
	firstDuration := firstEnd - firstStart
	secondDuration := secondEnd - secondStart
	if firstDuration <= 0 || secondDuration <= 0 {
		writeError(w, 400, "selected status duration is invalid")
		return
	}
	totalDuration := firstDuration + secondDuration
	desiredFirst := in.FirstResultSeconds

	if desiredFirst <= 0 && in.TransferSeconds > 0 {
		// Legacy behavior compatibility. The frontend no longer sends this form.
		desiredFirst = firstDuration
		if in.FromSegmentID == first.ID {
			desiredFirst -= in.TransferSeconds
		} else if in.FromSegmentID == second.ID {
			desiredFirst += in.TransferSeconds
		}
	}
	if desiredFirst <= 0 || desiredFirst >= totalDuration {
		writeError(w, 400, "result would remove an entire status")
		return
	}
	if desiredFirst == firstDuration {
		writeError(w, 400, "first selected status duration did not change")
		return
	}

	leftIndex, rightIndex := firstIndex, secondIndex
	if leftIndex > rightIndex {
		leftIndex, rightIndex = rightIndex, leftIndex
	}
	left, right := segs[leftIndex], segs[rightIndex]
	leftStart := effectiveStartSecond(left)
	rightEnd := 86400
	if right.EndSecond != nil {
		rightEnd = *right.EndSecond
	} else if right.EndMinute != nil {
		rightEnd = *right.EndMinute * 60
	} else if date == driverCurrentDateServer(r.Context(), tx, driverID) {
		if _, _, nowSecond, _, e := localDriverMoment(r.Context(), tx, driverID, time.Now().UTC()); e == nil {
			rightEnd = nowSecond
		}
	}
	leftEnd := rightEnd
	if left.EndSecond != nil {
		leftEnd = *left.EndSecond
	} else if left.EndMinute != nil {
		leftEnd = *left.EndMinute * 60
	}
	rightStart := effectiveStartSecond(right)
	if math.Abs(float64(leftEnd-rightStart)) > 1 {
		writeError(w, 400, "selected statuses are not contiguous")
		return
	}

	var boundary int
	if firstIndex == leftIndex {
		// First selected is the left status. Its result runs forward from its fixed start.
		boundary = leftStart + desiredFirst
	} else {
		// First selected is the right status. Its result runs backward from its fixed end.
		boundary = rightEnd - desiredFirst
	}
	if boundary <= leftStart || boundary >= rightEnd {
		writeError(w, 400, "result would remove an entire status")
		return
	}

	beforeJSON, _ := json.Marshal(segs)
	left.EndSecond = intPointer(boundary)
	left.EndMinute = intPointer(boundary / 60)
	right.StartSecond = boundary
	right.StartMinute = boundary / 60
	segs[leftIndex], segs[rightIndex] = left, right
	afterJSON, _ := json.Marshal(segs)
	if err = s.replaceDaySegmentsTx(r.Context(), tx, driverID, date, segs, date == driverCurrentDateServer(r.Context(), tx, driverID)); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, err = tx.Exec(r.Context(), `INSERT INTO log_edit_batches(driver_id,log_date,action,reason,operator_name,start_minute,end_minute,before_segments,after_segments) VALUES($1,$2,'swap','Admin swap','Admin',$3,$4,$5::jsonb,$6::jsonb)`, driverID, date, left.StartMinute, rightEnd/60, string(beforeJSON), string(afterJSON))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_ = s.recalcHOSFromSegments(r.Context(), driverID)
	writeJSON(w, 200, map[string]any{
		"ok":                    true,
		"first_segment_id":      first.ID,
		"second_segment_id":     second.ID,
		"first_before_seconds":  firstDuration,
		"first_after_seconds":   desiredFirst,
		"second_before_seconds": secondDuration,
		"second_after_seconds":  totalDuration - desiredFirst,
		"boundary_second":       boundary,
	})
}

func driverCurrentDateServer(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, driverID string) string {
	var tz string
	if q.QueryRow(ctx, `SELECT home_terminal_timezone FROM drivers WHERE id=$1`, driverID).Scan(&tz) != nil {
		return time.Now().UTC().Format("2006-01-02")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	return time.Now().In(loc).Format("2006-01-02")
}

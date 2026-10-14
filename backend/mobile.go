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
	autoDriveStartSpeedMPS = 1.34112 // 3 mph
	autoDriveStopSpeedMPS  = 0.44704 // treat sub-1 mph GPS jitter as stopped
	autoDriveStartSamples  = 3
	autoDriveStopHold      = 60 * time.Second
)

type telemetryInput struct {
	Latitude        float64 `json:"latitude"`
	Longitude       float64 `json:"longitude"`
	AccuracyMeters  float64 `json:"accuracy_meters"`
	SpeedMPS        float64 `json:"speed_mps"`
	OdometerMiles   float64 `json:"odometer_miles"`
	LocationText    string  `json:"location_text"`
	RecordedAt      string  `json:"recorded_at"`
	AutoStopStatus  string  `json:"auto_stop_status"`
	AutoStopSeconds *int    `json:"auto_stop_seconds"`
	ClientEventID   string  `json:"client_event_id"`
	MotionVersion   int     `json:"motion_version"`
	SpeedValid      *bool   `json:"speed_valid"`
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
	logDate, minute, second, tz, err := localDriverMoment(ctx, tx, driverID, at)
	if err != nil {
		return "", 0, 0, false, err
	}
	if err = lockDriver(ctx, tx, driverID); err != nil {
		return "", 0, 0, false, err
	}
	if err = s.continuousDayTx(ctx, tx, driverID, logDate, tz); err != nil {
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

	before, err := s.scanSegmentsTx(ctx, tx, driverID, logDate)
	if err != nil {
		return "", 0, 0, false, err
	}
	hadOpen := len(before) > 0
	if changed || !hadOpen {
		after := []SegmentResponse{}
		for _, x := range before {
			start := effectiveStartSecond(x)
			end := 86400
			if x.EndSecond != nil {
				end = *x.EndSecond
			} else if x.EndMinute != nil {
				end = *x.EndMinute * 60
			}
			if end <= second {
				after = append(after, x)
				continue
			}
			if start < second {
				x.EndSecond = intPointer(second)
				x.EndMinute = intPointer(second / 60)
				after = append(after, x)
			}
			break
		}
		after = append(after, SegmentResponse{StartMinute: minute, StartSecond: second, EndMinute: intPointer(1440), EndSecond: intPointer(86400), DutyStatus: status, Note: note, Origin: origin, LocationText: locationText, OdometerMiles: odometer})
		today := driverCurrentDateServer(ctx, tx, driverID)
		if err = s.replaceDaySegmentsTx(ctx, tx, driverID, logDate, after, logDate == today); err != nil {
			return "", 0, 0, false, err
		}
		// Remove only derived future projections so late offline events propagate accurately.
		if _, err = tx.Exec(ctx, `DELETE FROM duty_segments WHERE driver_id=$1 AND log_date>$2::date AND origin='System' AND edited=false`, driverID, logDate); err != nil {
			return "", 0, 0, false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE duty_segments SET end_minute=1440,end_second=86400 WHERE driver_id=$1 AND log_date<$2::date AND end_minute IS NULL`, driverID, logDate); err != nil {
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
	in.AutoStopStatus = strings.ToUpper(strings.TrimSpace(in.AutoStopStatus))
	if in.AutoStopStatus != "OFF" && in.AutoStopStatus != "ON" {
		in.AutoStopStatus = "ON"
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

	if err = lockDriver(r.Context(), tx, driverID); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	duplicate, err := acceptMobileEvent(r.Context(), tx, driverID, in.ClientEventID, "telemetry", at)
	if err != nil {
		writeError(w, 409, err.Error())
		return
	}
	if duplicate {
		writeJSON(w, 200, map[string]any{"ok": true, "duplicate": true})
		return
	}
	var previousAt, latestStatus *time.Time
	var previousLat, previousLon, previousAccuracy float64
	_ = tx.QueryRow(r.Context(), `SELECT latitude,longitude,gps_accuracy_meters,gps_recorded_at,status_since FROM driver_live_state WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&previousLat, &previousLon, &previousAccuracy, &previousAt, &latestStatus)
	if latestStatus != nil && at.Before(*latestStatus) {
		writeError(w, 409, "Offline sample predates a newer status change; review the saved queue before retrying")
		return
	}
	validSpeed := in.SpeedValid == nil || *in.SpeedValid
	if in.MotionVersion < 17 {
		// Old clients cannot distinguish measured zero from GPS drift. Confirm positive
		// speed with displacement beyond BOTH fixes' horizontal uncertainty.
		validSpeed = in.AccuracyMeters > 0 && in.AccuracyMeters <= 35
		if in.SpeedMPS > autoDriveStopSpeedMPS {
			if previousAt == nil || !at.After(*previousAt) || at.Sub(*previousAt) > 15*time.Second {
				validSpeed = false
			} else {
				dt := at.Sub(*previousAt).Seconds()
				distance := haversineMiles(previousLat, previousLon, in.Latitude, in.Longitude) * 1609.344
				conservative := math.Max(0, distance-in.AccuracyMeters-previousAccuracy) / dt
				in.SpeedMPS = math.Min(in.SpeedMPS, conservative)
			}
		}
	}
	if in.AccuracyMeters <= 0 || in.AccuracyMeters > 35 || in.SpeedMPS > 60 {
		validSpeed = false
	}
	if !validSpeed {
		in.SpeedMPS = 0
	}
	stopSeconds := 60
	if in.AutoStopSeconds != nil {
		stopSeconds = *in.AutoStopSeconds
	}
	if stopSeconds != 0 && (stopSeconds < 15 || stopSeconds > 900) {
		writeError(w, 400, "auto_stop_seconds must be 0 or between 15 and 900")
		return
	}

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

	if previousAt == nil || at.Sub(*previousAt) > 15*time.Second {
		movingSamples = 0
		movingStartedAt = nil
		stoppedSince = nil
	}
	started, ended := false, false
	if activeDriveID == nil || *activeDriveID == "" {
		if validSpeed && in.SpeedMPS > autoDriveStartSpeedMPS {
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
		if movingSamples >= autoDriveStartSamples && movingStartedAt != nil && at.Sub(*movingStartedAt) >= 6*time.Second {
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
		if validSpeed && in.SpeedMPS <= autoDriveStopSpeedMPS {
			if stoppedSince == nil {
				t := at
				stoppedSince = &t
			}
			if stopSeconds > 0 && at.Sub(*stoppedSince) >= time.Duration(stopSeconds)*time.Second {
				// The DR hold remains DR until the selected delay expires.
				endAt := at
				endLat, endLon, endAccuracy, endSpeed, endOdo := in.Latitude, in.Longitude, in.AccuracyMeters, in.SpeedMPS, in.OdometerMiles
				endLocation := in.LocationText
				pointErr := tx.QueryRow(r.Context(), `
SELECT latitude,longitude,accuracy_meters,speed_mps,odometer_miles,location_text
FROM drive_points
WHERE session_id=$1::uuid AND recorded_at <= $2::timestamptz + interval '1 second'
ORDER BY recorded_at DESC LIMIT 1`, *activeDriveID, endAt).Scan(&endLat, &endLon, &endAccuracy, &endSpeed, &endOdo, &endLocation)
				if pointErr != nil && !errors.Is(pointErr, pgx.ErrNoRows) {
					writeError(w, 500, pointErr.Error())
					return
				}
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
				_, _, _, _, e = s.transitionDriverStatusTx(r.Context(), tx, driverID, in.AutoStopStatus, "Automatic stop after driving", "DriveAnalyzer", endLocation, endLat, endLon, endOdo, endAccuracy, endSpeed, endAt, true)
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
	if _, err = tx.Exec(r.Context(), `UPDATE driver_live_state SET gps_recorded_at=$2,gps_speed_mps=$3 WHERE driver_id=$1`, driverID, at, in.SpeedMPS); err != nil {
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
	writeJSON(w, 200, map[string]any{"ok": true, "started_drive": started, "ended_drive": ended, "active_drive_id": activeDriveID, "speed_mps": in.SpeedMPS, "speed_valid": validSpeed})
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
	writeJSON(w, 200, map[string]any{"active": active, "pending": pending, "analyzer": map[string]any{"start_speed_mph": 3, "start_samples": autoDriveStartSamples, "stop_speed_mph": 0, "stop_hold_seconds": int(autoDriveStopHold.Seconds())}})
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

// finishActiveDriveSessionTx closes an automatic drive so a stopped driver can choose a manual status
// before the one-minute automatic fallback fires.
func (s *Server) finishActiveDriveSessionTx(ctx context.Context, tx pgx.Tx, driverID string, at time.Time, lat, lon, accuracy, speed float64, locationText string) (bool, error) {
	var driveID string
	var odo float64
	err := tx.QueryRow(ctx, `SELECT id::text FROM drive_sessions WHERE driver_id=$1 AND state='active' ORDER BY started_at DESC LIMIT 1 FOR UPDATE`, driverID).Scan(&driveID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = tx.QueryRow(ctx, `SELECT COALESCE(odometer_miles,0) FROM driver_live_state WHERE driver_id=$1`, driverID).Scan(&odo)
	_, _, endSecond, _, err := localDriverMoment(ctx, tx, driverID, at)
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE drive_sessions SET state='pending',ended_at=$2,end_second=$3,end_latitude=$4,end_longitude=$5,end_location_text=$6,end_odometer_miles=$7 WHERE id=$1::uuid`, driveID, at, endSecond, lat, lon, locationText, odo); err != nil {
		return false, err
	}
	if odo > 0 {
		_ = s.normalizeDriveTx(ctx, tx, driverID, driveID, odo)
	}
	if _, err = tx.Exec(ctx, `UPDATE driver_motion_state SET active_drive_id=NULL,stopped_since=NULL,moving_samples=0,moving_started_at=NULL,last_speed_mps=$2,updated_at=now() WHERE driver_id=$1`, driverID, speed); err != nil {
		return false, err
	}
	return true, nil
}

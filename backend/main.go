package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type Server struct {
	db             *pgxpool.Pool
	databaseURL    string
	hub            *Hub
	adminToken     string
	apiToken       string
	allowedOrigins []string
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*WSClient]struct{}
}

type WSClient struct {
	conn   *websocket.Conn
	mu     sync.Mutex
	authed bool
}

type notifyPayload struct {
	Table    string `json:"table"`
	Op       string `json:"op"`
	DriverID string `json:"driver_id"`
	ID       string `json:"id"`
}

type contextKey string

const driverIDContextKey contextKey = "driver_id"

var driverUsernamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{4,40}$`)

type DriverResponse struct {
	ID                   string  `json:"id"`
	Username             string  `json:"username"`
	FirstName            string  `json:"first_name"`
	LastName             string  `json:"last_name"`
	Name                 string  `json:"name"`
	Carrier              string  `json:"carrier"`
	Phone                string  `json:"phone"`
	Email                string  `json:"email"`
	LicenseIssueState    string  `json:"license_issue_state"`
	LicenseNumber        string  `json:"license_number"`
	HomeTerminalTimezone string  `json:"home_terminal_timezone"`
	Truck                string  `json:"truck"`
	Trailer              string  `json:"trailer"`
	BOL                  string  `json:"bol"`
	VehicleType          string  `json:"vehicle_type"`
	Certified            bool    `json:"certified"`
	CurrentStatus        string  `json:"current_status"`
	Connected            bool    `json:"connected"`
	LocationText         string  `json:"location_text"`
	Latitude             float64 `json:"latitude"`
	Longitude            float64 `json:"longitude"`
	StatusSince          *string `json:"status_since,omitempty"`
	Revision             int64   `json:"revision"`
}

type HOSResponse struct {
	DriverID              string  `json:"driver_id"`
	CurrentStatus         string  `json:"current_status"`
	BreakRemainingSeconds int     `json:"break_remaining_seconds"`
	DriveRemainingSeconds int     `json:"drive_remaining_seconds"`
	ShiftRemainingSeconds int     `json:"shift_remaining_seconds"`
	CycleRemainingSeconds int     `json:"cycle_remaining_seconds"`
	ShiftStartedAt        *string `json:"shift_started_at,omitempty"`
	Last10HResetAt        *string `json:"last_10h_reset_at,omitempty"`
	Last34HResetAt        *string `json:"last_34h_reset_at,omitempty"`
	CalculatedAt          string  `json:"calculated_at"`
	Revision              int64   `json:"revision"`
}

type LiveStateResponse struct {
	DriverID     string  `json:"driver_id"`
	Status       string  `json:"status"`
	Connected    bool    `json:"connected"`
	LocationText string  `json:"location_text"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	UnitNumber   string  `json:"unit_number"`
	StatusSince  *string `json:"status_since,omitempty"`
	Revision     int64   `json:"revision"`
}

type SegmentResponse struct {
	ID            string `json:"id"`
	StartMinute   int    `json:"start_minute"`
	EndMinute     *int   `json:"end_minute,omitempty"`
	DutyStatus    string `json:"duty_status"`
	SpecialStatus string `json:"special_status"`
	Note          string `json:"note"`
	Origin        string `json:"origin"`
	Edited        bool   `json:"edited"`
	Revision      int64  `json:"revision"`
}

type EventResponse struct {
	ID              string  `json:"id"`
	Minute          int     `json:"minute"`
	EventTime       *string `json:"event_time,omitempty"`
	EventType       string  `json:"event_type"`
	DutyStatus      *string `json:"duty_status,omitempty"`
	Note            string  `json:"note"`
	LocationText    string  `json:"location_text"`
	OdometerMiles   float64 `json:"odometer_miles"`
	EngineHours     float64 `json:"engine_hours"`
	DurationSeconds *int    `json:"duration_seconds,omitempty"`
	Origin          string  `json:"origin"`
	Diagnostic      bool    `json:"diagnostic"`
	Violation       bool    `json:"violation"`
	Edited          bool    `json:"edited"`
	EditReason      string  `json:"edit_reason"`
	CoDriver        string  `json:"co_driver"`
	Revision        int64   `json:"revision"`
}

type AlertResponse struct {
	ID         string `json:"id"`
	DriverID   string `json:"driver_id"`
	Kind       string `json:"kind"`
	Priority   string `json:"priority"`
	Title      string `json:"title"`
	Detail     string `json:"detail"`
	EventID    string `json:"event_id"`
	Status     string `json:"status"`
	Owner      string `json:"owner"`
	Resolution string `json:"resolution"`
}

func main() {
	ctx := context.Background()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("database pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("database ping: %v", err)
	}
	if err := runMigrations(ctx, pool); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	s := &Server{
		db:             pool,
		databaseURL:    databaseURL,
		hub:            &Hub{clients: map[*WSClient]struct{}{}},
		adminToken:     strings.TrimSpace(os.Getenv("ADMIN_TOKEN")),
		apiToken:       strings.TrimSpace(os.Getenv("API_TOKEN")),
		allowedOrigins: parseOrigins(os.Getenv("ALLOWED_ORIGINS")),
	}

	go s.listenForChanges(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v1/driver/login", s.handleDriverLogin)
	mux.Handle("GET /v1/driver/me", s.driverAuth(http.HandlerFunc(s.handleDriverMe)))
	mux.Handle("GET /v1/driver/hos", s.driverAuth(http.HandlerFunc(s.handleDriverSelfHOS)))
	mux.Handle("GET /v1/driver/logs/{date}", s.driverAuth(http.HandlerFunc(s.handleDriverSelfLog)))
	mux.Handle("POST /v1/driver/status", s.driverAuth(http.HandlerFunc(s.handleDriverSelfStatus)))
	mux.Handle("POST /v1/driver/heartbeat", s.driverAuth(http.HandlerFunc(s.handleDriverHeartbeat)))
	mux.Handle("POST /v1/driver/logout", s.driverAuth(http.HandlerFunc(s.handleDriverLogout)))
	mux.Handle("GET /v1/drivers", s.readAuth(http.HandlerFunc(s.handleDrivers)))
	mux.Handle("GET /v1/alerts", s.readAuth(http.HandlerFunc(s.handleAlerts)))
	mux.Handle("GET /v1/fleet/live", s.readAuth(http.HandlerFunc(s.handleFleetLive)))
	mux.Handle("GET /v1/hos/current", s.readAuth(http.HandlerFunc(s.handleHOSBulk)))
	mux.Handle("GET /v1/drivers/{id}/hos", s.readAuth(http.HandlerFunc(s.handleDriverHOS)))
	mux.Handle("GET /v1/drivers/{id}/logs/{date}", s.readAuth(http.HandlerFunc(s.handleDriverLog)))
	mux.HandleFunc("GET /v1/ws", s.handleWS)

	mux.Handle("GET /v1/admin/session", s.adminAuth(http.HandlerFunc(s.handleAdminSession)))
	mux.Handle("POST /v1/admin/drivers", s.adminAuth(http.HandlerFunc(s.handleAdminDriver)))
	mux.Handle("DELETE /v1/admin/drivers/{id}", s.adminAuth(http.HandlerFunc(s.handleAdminDriverDelete)))
	mux.Handle("PUT /v1/admin/drivers/{id}/live", s.adminAuth(http.HandlerFunc(s.handleAdminLive)))
	mux.Handle("PUT /v1/admin/drivers/{id}/hos", s.adminAuth(http.HandlerFunc(s.handleAdminHOS)))
	mux.Handle("POST /v1/admin/drivers/{id}/segments", s.adminAuth(http.HandlerFunc(s.handleAdminSegment)))
	mux.Handle("POST /v1/admin/drivers/{id}/events", s.adminAuth(http.HandlerFunc(s.handleAdminEvent)))
	mux.Handle("POST /v1/admin/alerts", s.adminAuth(http.HandlerFunc(s.handleAdminAlert)))

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "10000"
	}
	addr := ":" + port
	log.Printf("NEKSUS API listening on %s", addr)
	if err := http.ListenAndServe(addr, s.cors(mux)); err != nil {
		log.Fatal(err)
	}
}

func runMigrations(ctx context.Context, db *pgxpool.Pool) error {
	b, err := migrationFS.ReadFile("migrations/001_init.sql")
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, string(b))
	return err
}

func parseOrigins(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{"*"}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.TrimRight(p, "/"))
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}

func (s *Server) originAllowed(origin string) bool {
	if origin == "" {
		return true
	}
	origin = strings.TrimRight(origin, "/")
	for _, o := range s.allowedOrigins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && s.originAllowed(origin) {
			if len(s.allowedOrigins) == 1 && s.allowedOrigins[0] == "*" {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}
		}
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) string {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func (s *Server) readAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := s.apiToken
		if expected == "" {
			expected = s.adminToken
		}
		if expected != "" && bearer(r) != expected {
			writeError(w, http.StatusUnauthorized, "invalid access token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.adminToken == "" {
			writeError(w, http.StatusServiceUnavailable, "ADMIN_TOKEN is not configured")
			return
		}
		if bearer(r) != s.adminToken {
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) driverAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearer(r)
		if token == "" {
			writeError(w, http.StatusUnauthorized, "driver login required")
			return
		}
		hash := sha256.Sum256([]byte(token))
		var driverID string
		err := s.db.QueryRow(r.Context(), `
SELECT ds.driver_id
FROM driver_sessions ds
JOIN drivers d ON d.id=ds.driver_id
WHERE ds.token_hash=$1 AND ds.expires_at>now() AND d.active=true`, hash[:]).Scan(&driverID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "driver session expired or invalid")
			return
		}
		_, _ = s.db.Exec(r.Context(), `UPDATE driver_sessions SET last_seen_at=now() WHERE token_hash=$1`, hash[:])
		ctx := context.WithValue(r.Context(), driverIDContextKey, driverID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func generateDriverID() (string, error) {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "DRV-" + strings.ToUpper(hex.EncodeToString(b)), nil
}

func generateSessionToken() (string, []byte, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	hash := sha256.Sum256([]byte(token))
	return token, hash[:], nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg, "status": status})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nilWriter{}, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

type nilWriter struct{}

func (nilWriter) Header() http.Header       { return http.Header{} }
func (nilWriter) Write([]byte) (int, error) { return 0, nil }
func (nilWriter) WriteHeader(int)           {}

func timeString(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "database": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "neksus-api", "database": "up", "time": time.Now().UTC()})
}

func (s *Server) queryDriver(ctx context.Context, id string) (DriverResponse, error) {
	var d DriverResponse
	var statusSince *time.Time
	err := s.db.QueryRow(ctx, `
SELECT d.id, d.username, d.first_name, d.last_name, d.full_name, COALESCE(c.name,d.carrier), d.phone, d.email,
       d.license_issue_state, d.license_number, d.home_terminal_timezone, d.truck_unit, d.trailer_number, d.shipping_document,
       d.vehicle_type, d.certified,
       COALESCE(ls.duty_status,'OFF'), COALESCE(ls.connected,false), COALESCE(ls.location_text,''),
       COALESCE(ls.latitude,0), COALESCE(ls.longitude,0), ls.status_since, COALESCE(ls.revision,0)
FROM drivers d
LEFT JOIN companies c ON c.id=d.company_id
LEFT JOIN driver_live_state ls ON ls.driver_id=d.id
WHERE d.id=$1 AND d.active=true`, id).Scan(
		&d.ID, &d.Username, &d.FirstName, &d.LastName, &d.Name, &d.Carrier, &d.Phone, &d.Email,
		&d.LicenseIssueState, &d.LicenseNumber, &d.HomeTerminalTimezone, &d.Truck, &d.Trailer, &d.BOL,
		&d.VehicleType, &d.Certified, &d.CurrentStatus, &d.Connected, &d.LocationText, &d.Latitude,
		&d.Longitude, &statusSince, &d.Revision,
	)
	d.StatusSince = timeString(statusSince)
	return d, err
}

func (s *Server) handleDrivers(w http.ResponseWriter, r *http.Request) {
	limit := 500
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.Query(r.Context(), `
SELECT d.id, d.username, d.first_name, d.last_name, d.full_name, COALESCE(c.name,d.carrier), d.phone, d.email,
       d.license_issue_state, d.license_number, d.home_terminal_timezone, d.truck_unit, d.trailer_number, d.shipping_document,
       d.vehicle_type, d.certified,
       COALESCE(ls.duty_status,'OFF'), COALESCE(ls.connected,false), COALESCE(ls.location_text,''),
       COALESCE(ls.latitude,0), COALESCE(ls.longitude,0), ls.status_since, COALESCE(ls.revision,0)
FROM drivers d
LEFT JOIN companies c ON c.id=d.company_id
LEFT JOIN driver_live_state ls ON ls.driver_id=d.id
WHERE d.active=true
ORDER BY d.full_name
LIMIT $1`, limit)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []DriverResponse{}
	for rows.Next() {
		var d DriverResponse
		var statusSince *time.Time
		if err := rows.Scan(&d.ID, &d.Username, &d.FirstName, &d.LastName, &d.Name, &d.Carrier, &d.Phone, &d.Email,
			&d.LicenseIssueState, &d.LicenseNumber, &d.HomeTerminalTimezone, &d.Truck, &d.Trailer, &d.BOL,
			&d.VehicleType, &d.Certified, &d.CurrentStatus, &d.Connected, &d.LocationText, &d.Latitude,
			&d.Longitude, &statusSince, &d.Revision); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		d.StatusSince = timeString(statusSince)
		out = append(out, d)
	}
	writeJSON(w, 200, map[string]any{"drivers": out})
}

func (s *Server) queryHOS(ctx context.Context, id string) (HOSResponse, error) {
	var h HOSResponse
	var shift, r10, r34 *time.Time
	var calc time.Time
	err := s.db.QueryRow(ctx, `
SELECT h.driver_id, COALESCE(ls.duty_status,'OFF'), h.break_remaining_seconds,
       h.drive_remaining_seconds, h.shift_remaining_seconds, h.cycle_remaining_seconds,
       h.shift_started_at, h.last_10h_reset_at, h.last_34h_reset_at, h.calculated_at, h.revision
FROM driver_hos_current h
LEFT JOIN driver_live_state ls ON ls.driver_id=h.driver_id
WHERE h.driver_id=$1`, id).Scan(&h.DriverID, &h.CurrentStatus, &h.BreakRemainingSeconds,
		&h.DriveRemainingSeconds, &h.ShiftRemainingSeconds, &h.CycleRemainingSeconds,
		&shift, &r10, &r34, &calc, &h.Revision)
	if err != nil {
		return h, err
	}
	h.ShiftStartedAt = timeString(shift)
	h.Last10HResetAt = timeString(r10)
	h.Last34HResetAt = timeString(r34)
	h.CalculatedAt = calc.UTC().Format(time.RFC3339)
	return h, nil
}

func (s *Server) handleDriverHOS(w http.ResponseWriter, r *http.Request) {
	h, err := s.queryHOS(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "HOS not found")
		return
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, h)
}

func (s *Server) handleHOSBulk(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
SELECT h.driver_id, COALESCE(ls.duty_status,'OFF'), h.break_remaining_seconds,
       h.drive_remaining_seconds, h.shift_remaining_seconds, h.cycle_remaining_seconds,
       h.shift_started_at, h.last_10h_reset_at, h.last_34h_reset_at, h.calculated_at, h.revision
FROM driver_hos_current h
LEFT JOIN driver_live_state ls ON ls.driver_id=h.driver_id
JOIN drivers d ON d.id=h.driver_id AND d.active=true
ORDER BY h.driver_id`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []HOSResponse{}
	for rows.Next() {
		var h HOSResponse
		var shift, r10, r34 *time.Time
		var calc time.Time
		if err := rows.Scan(&h.DriverID, &h.CurrentStatus, &h.BreakRemainingSeconds,
			&h.DriveRemainingSeconds, &h.ShiftRemainingSeconds, &h.CycleRemainingSeconds,
			&shift, &r10, &r34, &calc, &h.Revision); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		h.ShiftStartedAt = timeString(shift)
		h.Last10HResetAt = timeString(r10)
		h.Last34HResetAt = timeString(r34)
		h.CalculatedAt = calc.UTC().Format(time.RFC3339)
		out = append(out, h)
	}
	writeJSON(w, 200, map[string]any{"hos": out})
}

func (s *Server) queryLiveState(ctx context.Context, id string) (LiveStateResponse, error) {
	var x LiveStateResponse
	var since *time.Time
	err := s.db.QueryRow(ctx, `
SELECT ls.driver_id, ls.duty_status, ls.connected, ls.location_text, ls.latitude, ls.longitude,
       d.truck_unit, ls.status_since, ls.revision
FROM driver_live_state ls JOIN drivers d ON d.id=ls.driver_id
WHERE ls.driver_id=$1 AND d.active=true`, id).Scan(&x.DriverID, &x.Status, &x.Connected,
		&x.LocationText, &x.Latitude, &x.Longitude, &x.UnitNumber, &since, &x.Revision)
	x.StatusSince = timeString(since)
	return x, err
}

func (s *Server) handleFleetLive(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
SELECT ls.driver_id, ls.duty_status, ls.connected, ls.location_text, ls.latitude, ls.longitude,
       d.truck_unit, ls.status_since, ls.revision
FROM driver_live_state ls JOIN drivers d ON d.id=ls.driver_id
WHERE d.active=true ORDER BY d.full_name`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []LiveStateResponse{}
	for rows.Next() {
		var x LiveStateResponse
		var since *time.Time
		if err := rows.Scan(&x.DriverID, &x.Status, &x.Connected, &x.LocationText, &x.Latitude,
			&x.Longitude, &x.UnitNumber, &since, &x.Revision); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		x.StatusSince = timeString(since)
		out = append(out, x)
	}
	writeJSON(w, 200, map[string]any{"drivers": out})
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "open"
	}
	rows, err := s.db.Query(r.Context(), `
SELECT id::text, COALESCE(driver_id,''), kind, priority, title, detail, event_id, status, owner, resolution
FROM alerts WHERE status=$1 ORDER BY created_at DESC LIMIT 1000`, status)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []AlertResponse{}
	for rows.Next() {
		var a AlertResponse
		if err := rows.Scan(&a.ID, &a.DriverID, &a.Kind, &a.Priority, &a.Title, &a.Detail,
			&a.EventID, &a.Status, &a.Owner, &a.Resolution); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		out = append(out, a)
	}
	writeJSON(w, 200, map[string]any{"alerts": out})
}

func (s *Server) handleDriverLog(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	date := r.PathValue("date")
	if _, err := time.Parse("2006-01-02", date); err != nil {
		writeError(w, 400, "date must be YYYY-MM-DD")
		return
	}
	var timezone string
	if err := s.db.QueryRow(r.Context(), `SELECT home_terminal_timezone FROM drivers WHERE id=$1 AND active=true`, id).Scan(&timezone); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, 404, "driver not found")
		} else {
			writeError(w, 500, err.Error())
		}
		return
	}
	segs := []SegmentResponse{}
	rows, err := s.db.Query(r.Context(), `
SELECT id::text, start_minute, end_minute, duty_status, special_status, note, origin, edited, revision
FROM duty_segments WHERE driver_id=$1 AND log_date=$2 ORDER BY start_minute,id`, id, date)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	for rows.Next() {
		var x SegmentResponse
		if err := rows.Scan(&x.ID, &x.StartMinute, &x.EndMinute, &x.DutyStatus, &x.SpecialStatus,
			&x.Note, &x.Origin, &x.Edited, &x.Revision); err != nil {
			rows.Close()
			writeError(w, 500, err.Error())
			return
		}
		segs = append(segs, x)
	}
	rows.Close()

	events := []EventResponse{}
	erows, err := s.db.Query(r.Context(), `
SELECT id::text, minute, event_time, event_type, duty_status, note, location_text, odometer_miles,
       engine_hours, duration_seconds, origin, diagnostic, violation, edited, edit_reason, co_driver, revision
FROM eld_events WHERE driver_id=$1 AND log_date=$2 ORDER BY minute,id`, id, date)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	for erows.Next() {
		var x EventResponse
		var et *time.Time
		if err := erows.Scan(&x.ID, &x.Minute, &et, &x.EventType, &x.DutyStatus, &x.Note,
			&x.LocationText, &x.OdometerMiles, &x.EngineHours, &x.DurationSeconds, &x.Origin,
			&x.Diagnostic, &x.Violation, &x.Edited, &x.EditReason, &x.CoDriver, &x.Revision); err != nil {
			erows.Close()
			writeError(w, 500, err.Error())
			return
		}
		x.EventTime = timeString(et)
		events = append(events, x)
	}
	erows.Close()

	payload := map[string]any{
		"driver_id": id,
		"date":      date,
		"timezone":  timezone,
		"segments":  segs,
		"events":    events,
	}
	if h, err := s.queryHOS(r.Context(), id); err == nil {
		payload["hos"] = h
	}
	writeJSON(w, 200, payload)
}

func normalizeStatus(v string) (string, bool) {
	v = strings.ToUpper(strings.TrimSpace(v))
	switch v {
	case "OFF", "OFFDUTY", "OFF DUTY":
		return "OFF", true
	case "SB", "SLEEPER", "SLEEPER BERTH":
		return "SB", true
	case "DR", "DRIVING", "D":
		return "DR", true
	case "ON", "ONDUTY", "ON DUTY":
		return "ON", true
	default:
		return "", false
	}
}

func (s *Server) handleDriverLogin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	in.Username = strings.TrimSpace(in.Username)
	if in.Username == "" || in.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}
	var driverID, passwordHash string
	err := s.db.QueryRow(r.Context(), `SELECT id,password_hash FROM drivers WHERE lower(username)=lower($1) AND active=true`, in.Username).Scan(&driverID, &passwordHash)
	if err != nil || passwordHash == "" || bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(in.Password)) != nil {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	token, tokenHash, err := generateSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create session")
		return
	}
	expiresAt := time.Now().UTC().Add(30 * 24 * time.Hour)
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	_, _ = tx.Exec(r.Context(), `DELETE FROM driver_sessions WHERE expires_at<=now()`)
	if _, err = tx.Exec(r.Context(), `INSERT INTO driver_sessions(token_hash,driver_id,expires_at) VALUES($1,$2,$3)`, tokenHash, driverID, expiresAt); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	d, err := s.queryDriver(r.Context(), driverID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "expires_at": expiresAt.Format(time.RFC3339), "driver": d})
}

func (s *Server) handleDriverMe(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	d, err := s.queryDriver(r.Context(), driverID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "driver session is not valid")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"driver": d})
}

func (s *Server) handleDriverSelfHOS(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	r.SetPathValue("id", driverID)
	s.handleDriverHOS(w, r)
}

func (s *Server) handleDriverSelfLog(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	r.SetPathValue("id", driverID)
	s.handleDriverLog(w, r)
}

func (s *Server) handleDriverSelfStatus(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	var in struct {
		Status        string   `json:"status"`
		LocationText  *string  `json:"location_text"`
		Latitude      *float64 `json:"latitude"`
		Longitude     *float64 `json:"longitude"`
		OdometerMiles float64  `json:"odometer_miles"`
		EngineHours   float64  `json:"engine_hours"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	status, ok := normalizeStatus(in.Status)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid duty status")
		return
	}
	var timezone string
	if err := s.db.QueryRow(r.Context(), `SELECT home_terminal_timezone FROM drivers WHERE id=$1 AND active=true`, driverID).Scan(&timezone); err != nil {
		writeError(w, http.StatusUnauthorized, "driver is not active")
		return
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().UTC()
	localNow := now.In(loc)
	logDate := localNow.Format("2006-01-02")
	minute := localNow.Hour()*60 + localNow.Minute()

	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())

	currentStatus := "OFF"
	currentLocation := ""
	currentLat, currentLon := 0.0, 0.0
	var statusSince *time.Time
	err = tx.QueryRow(r.Context(), `SELECT duty_status,location_text,latitude,longitude,status_since FROM driver_live_state WHERE driver_id=$1 FOR UPDATE`, driverID).Scan(&currentStatus, &currentLocation, &currentLat, &currentLon, &statusSince)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 500, err.Error())
		return
	}
	if in.LocationText != nil {
		currentLocation = strings.TrimSpace(*in.LocationText)
	}
	if in.Latitude != nil {
		currentLat = *in.Latitude
	}
	if in.Longitude != nil {
		currentLon = *in.Longitude
	}
	changed := currentStatus != status
	_, err = tx.Exec(r.Context(), `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,status_since,revision)
VALUES($1,$2,true,$3,$4,$5,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET
 duty_status=EXCLUDED.duty_status,
 connected=true,
 location_text=EXCLUDED.location_text,
 latitude=EXCLUDED.latitude,
 longitude=EXCLUDED.longitude,
 status_since=CASE WHEN driver_live_state.duty_status<>EXCLUDED.duty_status THEN now() ELSE driver_live_state.status_since END,
 revision=driver_live_state.revision+1`, driverID, status, currentLocation, currentLat, currentLon)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	var openID, openStatus string
	var openStart int
	err = tx.QueryRow(r.Context(), `
SELECT id::text,duty_status,start_minute
FROM duty_segments
WHERE driver_id=$1 AND log_date=$2 AND end_minute IS NULL
ORDER BY start_minute DESC, created_at DESC
LIMIT 1 FOR UPDATE`, driverID, logDate).Scan(&openID, &openStatus, &openStart)
	hadOpen := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 500, err.Error())
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(r.Context(), `INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin) VALUES($1,$2,$3,NULL,$4,$5,'DriverApp')`, driverID, logDate, minute, status, "Status from NEKSUS Driver")
	} else if openStatus != status {
		endMinute := minute
		if endMinute < openStart {
			endMinute = openStart
		}
		if _, err = tx.Exec(r.Context(), `UPDATE duty_segments SET end_minute=$2 WHERE id=$1::uuid`, openID, endMinute); err == nil {
			_, err = tx.Exec(r.Context(), `INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin) VALUES($1,$2,$3,NULL,$4,$5,'DriverApp')`, driverID, logDate, minute, status, "Status from NEKSUS Driver")
		}
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if changed || !hadOpen {
		_, err = tx.Exec(r.Context(), `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,odometer_miles,engine_hours,origin)
VALUES($1,$2,$3,$4,'duty_status',$5,$6,$7,$8,$9,'DriverApp')`, driverID, logDate, minute, now, status, "Duty status changed in NEKSUS Driver", currentLocation, in.OdometerMiles, in.EngineHours)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	d, err := s.queryDriver(r.Context(), driverID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "driver": d, "log_date": logDate, "minute": minute})
}

func (s *Server) handleDriverHeartbeat(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	var in struct {
		LocationText *string  `json:"location_text"`
		Latitude     *float64 `json:"latitude"`
		Longitude    *float64 `json:"longitude"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in)
	}
	current, err := s.queryLiveState(r.Context(), driverID)
	if errors.Is(err, pgx.ErrNoRows) {
		current = LiveStateResponse{DriverID: driverID, Status: "OFF"}
	} else if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if in.LocationText != nil {
		current.LocationText = strings.TrimSpace(*in.LocationText)
	}
	if in.Latitude != nil {
		current.Latitude = *in.Latitude
	}
	if in.Longitude != nil {
		current.Longitude = *in.Longitude
	}
	_, err = s.db.Exec(r.Context(), `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,status_since,revision)
VALUES($1,$2,true,$3,$4,$5,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET connected=true,location_text=EXCLUDED.location_text,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,revision=driver_live_state.revision+1`,
		driverID, current.Status, current.LocationText, current.Latitude, current.Longitude)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "connected": true})
}

func (s *Server) handleDriverLogout(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	token := bearer(r)
	if token != "" {
		hash := sha256.Sum256([]byte(token))
		_, _ = s.db.Exec(r.Context(), `DELETE FROM driver_sessions WHERE token_hash=$1`, hash[:])
	}
	if driverID != "" {
		_, _ = s.db.Exec(r.Context(), `UPDATE driver_live_state SET connected=false,revision=revision+1 WHERE driver_id=$1`, driverID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAdminSession(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"role":    "admin",
		"service": "neksus-api",
	})
}

func (s *Server) handleAdminDriver(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID                string `json:"id"`
		Username          string `json:"username"`
		FirstName         string `json:"first_name"`
		LastName          string `json:"last_name"`
		Email             string `json:"email"`
		Phone             string `json:"phone"`
		Password          string `json:"password"`
		LicenseIssueState string `json:"license_issue_state"`
		LicenseNumber     string `json:"license_number"`
		Vehicle           string `json:"vehicle"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	in.ID = strings.TrimSpace(in.ID)
	in.Username = strings.TrimSpace(in.Username)
	in.FirstName = strings.TrimSpace(in.FirstName)
	in.LastName = strings.TrimSpace(in.LastName)
	in.Email = strings.TrimSpace(in.Email)
	in.Phone = strings.TrimSpace(in.Phone)
	in.LicenseIssueState = strings.ToUpper(strings.TrimSpace(in.LicenseIssueState))
	in.LicenseNumber = strings.TrimSpace(in.LicenseNumber)
	in.Vehicle = strings.TrimSpace(in.Vehicle)
	creating := in.ID == ""
	if !driverUsernamePattern.MatchString(in.Username) {
		writeError(w, 400, "username must be 4-40 characters using letters, numbers, dot, underscore or dash")
		return
	}
	if in.FirstName == "" || in.LastName == "" {
		writeError(w, 400, "first name and last name are required")
		return
	}
	if len(in.LicenseIssueState) != 2 || in.LicenseNumber == "" {
		writeError(w, 400, "driver license issue state and license number are required")
		return
	}
	if len(in.Password) > 0 && (len(in.Password) < 6 || len(in.Password) > 24) {
		writeError(w, 400, "password must contain 6-24 characters")
		return
	}
	if creating && in.Password == "" {
		writeError(w, 400, "password is required for a new driver")
		return
	}
	if creating {
		var err error
		in.ID, err = generateDriverID()
		if err != nil {
			writeError(w, 500, "could not generate driver id")
			return
		}
	}
	var usernameTaken bool
	if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM drivers WHERE lower(username)=lower($1) AND id<>$2)`, in.Username, in.ID).Scan(&usernameTaken); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if usernameTaken {
		writeError(w, http.StatusConflict, "username is already in use")
		return
	}
	passwordHash := ""
	if in.Password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
		if err != nil {
			writeError(w, 500, "could not secure password")
			return
		}
		passwordHash = string(hash)
	}
	fullName := strings.TrimSpace(in.FirstName + " " + in.LastName)
	carrier := "NEKSUS ELD"
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var companyID string
	if err := tx.QueryRow(r.Context(), `
INSERT INTO companies(name) VALUES($1)
ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name
RETURNING id::text`, carrier).Scan(&companyID); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, err = tx.Exec(r.Context(), `
INSERT INTO drivers(id,username,first_name,last_name,full_name,password_hash,license_issue_state,license_number,company_id,carrier,phone,email,truck_unit,active)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::uuid,$10,$11,$12,$13,true)
ON CONFLICT(id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
 full_name=EXCLUDED.full_name, password_hash=CASE WHEN EXCLUDED.password_hash<>'' THEN EXCLUDED.password_hash ELSE drivers.password_hash END,
 license_issue_state=EXCLUDED.license_issue_state, license_number=EXCLUDED.license_number,
 company_id=EXCLUDED.company_id, carrier=EXCLUDED.carrier, phone=EXCLUDED.phone, email=EXCLUDED.email,
 truck_unit=EXCLUDED.truck_unit, active=true`, in.ID, in.Username, in.FirstName, in.LastName, fullName, passwordHash,
		in.LicenseIssueState, in.LicenseNumber, companyID, carrier, in.Phone, in.Email, in.Vehicle)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, err = tx.Exec(r.Context(), `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,status_since,revision)
VALUES($1,'OFF',false,'',0,0,now(),1)
ON CONFLICT(driver_id) DO NOTHING`, in.ID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, err = tx.Exec(r.Context(), `
INSERT INTO driver_hos_current(driver_id,break_remaining_seconds,drive_remaining_seconds,shift_remaining_seconds,cycle_remaining_seconds,revision)
VALUES($1,28800,39600,50400,252000,1)
ON CONFLICT(driver_id) DO NOTHING`, in.ID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	d, err := s.queryDriver(r.Context(), in.ID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	status := http.StatusOK
	if creating {
		status = http.StatusCreated
	}
	writeJSON(w, status, d)
}

func (s *Server) handleAdminDriverDelete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "driver id is required")
		return
	}
	ct, err := s.db.Exec(r.Context(), `DELETE FROM drivers WHERE id=$1`, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "driver not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAdminLive(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var in struct {
		Status       string   `json:"status"`
		Connected    *bool    `json:"connected"`
		LocationText *string  `json:"location_text"`
		Latitude     *float64 `json:"latitude"`
		Longitude    *float64 `json:"longitude"`
		Truck        *string  `json:"truck"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	var exists bool
	if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM drivers WHERE id=$1)`, id).Scan(&exists); err != nil || !exists {
		writeError(w, 404, "driver not found")
		return
	}
	if in.Truck != nil {
		if _, err := s.db.Exec(r.Context(), `UPDATE drivers SET truck_unit=$2 WHERE id=$1`, id, strings.TrimSpace(*in.Truck)); err != nil {
			writeError(w, 500, err.Error())
			return
		}
	}
	current, err := s.queryLiveState(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		current = LiveStateResponse{DriverID: id, Status: "OFF"}
	} else if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	status := current.Status
	if strings.TrimSpace(in.Status) != "" {
		var ok bool
		status, ok = normalizeStatus(in.Status)
		if !ok {
			writeError(w, 400, "invalid status")
			return
		}
	}
	connected := current.Connected
	if in.Connected != nil {
		connected = *in.Connected
	}
	location := current.LocationText
	if in.LocationText != nil {
		location = *in.LocationText
	}
	lat := current.Latitude
	if in.Latitude != nil {
		lat = *in.Latitude
	}
	lon := current.Longitude
	if in.Longitude != nil {
		lon = *in.Longitude
	}
	_, err = s.db.Exec(r.Context(), `
INSERT INTO driver_live_state(driver_id,duty_status,connected,location_text,latitude,longitude,status_since,revision)
VALUES($1,$2,$3,$4,$5,$6,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET duty_status=EXCLUDED.duty_status, connected=EXCLUDED.connected,
 location_text=EXCLUDED.location_text, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
 status_since=CASE WHEN driver_live_state.duty_status<>EXCLUDED.duty_status THEN now() ELSE driver_live_state.status_since END,
 revision=driver_live_state.revision+1`, id, status, connected, location, lat, lon)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	x, err := s.queryLiveState(r.Context(), id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, x)
}

func (s *Server) handleAdminHOS(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var in struct {
		BreakRemainingSeconds int     `json:"break_remaining_seconds"`
		DriveRemainingSeconds int     `json:"drive_remaining_seconds"`
		ShiftRemainingSeconds int     `json:"shift_remaining_seconds"`
		CycleRemainingSeconds int     `json:"cycle_remaining_seconds"`
		ShiftStartedAt        *string `json:"shift_started_at"`
		Last10HResetAt        *string `json:"last_10h_reset_at"`
		Last34HResetAt        *string `json:"last_34h_reset_at"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	for _, v := range []int{in.BreakRemainingSeconds, in.DriveRemainingSeconds, in.ShiftRemainingSeconds, in.CycleRemainingSeconds} {
		if v < 0 {
			writeError(w, 400, "HOS remaining values cannot be negative")
			return
		}
	}
	parseOptional := func(v *string) (*time.Time, error) {
		if v == nil || strings.TrimSpace(*v) == "" {
			return nil, nil
		}
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(*v))
		if err != nil {
			return nil, err
		}
		return &t, nil
	}
	shift, err := parseOptional(in.ShiftStartedAt)
	if err != nil {
		writeError(w, 400, "invalid shift_started_at")
		return
	}
	r10, err := parseOptional(in.Last10HResetAt)
	if err != nil {
		writeError(w, 400, "invalid last_10h_reset_at")
		return
	}
	r34, err := parseOptional(in.Last34HResetAt)
	if err != nil {
		writeError(w, 400, "invalid last_34h_reset_at")
		return
	}
	ct, err := s.db.Exec(r.Context(), `
INSERT INTO driver_hos_current(driver_id,break_remaining_seconds,drive_remaining_seconds,shift_remaining_seconds,cycle_remaining_seconds,shift_started_at,last_10h_reset_at,last_34h_reset_at,calculated_at,revision)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET break_remaining_seconds=EXCLUDED.break_remaining_seconds,
 drive_remaining_seconds=EXCLUDED.drive_remaining_seconds, shift_remaining_seconds=EXCLUDED.shift_remaining_seconds,
 cycle_remaining_seconds=EXCLUDED.cycle_remaining_seconds, shift_started_at=EXCLUDED.shift_started_at,
 last_10h_reset_at=EXCLUDED.last_10h_reset_at, last_34h_reset_at=EXCLUDED.last_34h_reset_at,
 calculated_at=now(), revision=driver_hos_current.revision+1`, id, in.BreakRemainingSeconds, in.DriveRemainingSeconds,
		in.ShiftRemainingSeconds, in.CycleRemainingSeconds, shift, r10, r34)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		writeError(w, 404, "driver not found")
		return
	}
	h, err := s.queryHOS(r.Context(), id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, h)
}

func (s *Server) handleAdminSegment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var in struct {
		Date          string `json:"date"`
		StartMinute   int    `json:"start_minute"`
		EndMinute     *int   `json:"end_minute"`
		Status        string `json:"status"`
		SpecialStatus string `json:"special_status"`
		Note          string `json:"note"`
		Origin        string `json:"origin"`
		Edited        bool   `json:"edited"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if _, err := time.Parse("2006-01-02", in.Date); err != nil {
		writeError(w, 400, "date must be YYYY-MM-DD")
		return
	}
	status, ok := normalizeStatus(in.Status)
	if !ok {
		writeError(w, 400, "invalid status")
		return
	}
	if in.StartMinute < 0 || in.StartMinute > 1440 || (in.EndMinute != nil && (*in.EndMinute < in.StartMinute || *in.EndMinute > 1440)) {
		writeError(w, 400, "invalid minute range")
		return
	}
	if in.Origin == "" {
		in.Origin = "ELD"
	}
	var x SegmentResponse
	err := s.db.QueryRow(r.Context(), `
INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,special_status,note,origin,edited)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
RETURNING id::text,start_minute,end_minute,duty_status,special_status,note,origin,edited,revision`,
		id, in.Date, in.StartMinute, in.EndMinute, status, in.SpecialStatus, in.Note, in.Origin, in.Edited).Scan(
		&x.ID, &x.StartMinute, &x.EndMinute, &x.DutyStatus, &x.SpecialStatus, &x.Note, &x.Origin, &x.Edited, &x.Revision)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, x)
}

func (s *Server) handleAdminEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var in struct {
		Date            string  `json:"date"`
		Minute          int     `json:"minute"`
		EventTime       *string `json:"event_time"`
		EventType       string  `json:"event_type"`
		DutyStatus      *string `json:"duty_status"`
		Note            string  `json:"note"`
		LocationText    string  `json:"location_text"`
		OdometerMiles   float64 `json:"odometer_miles"`
		EngineHours     float64 `json:"engine_hours"`
		DurationSeconds *int    `json:"duration_seconds"`
		Origin          string  `json:"origin"`
		Diagnostic      bool    `json:"diagnostic"`
		Violation       bool    `json:"violation"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if _, err := time.Parse("2006-01-02", in.Date); err != nil {
		writeError(w, 400, "date must be YYYY-MM-DD")
		return
	}
	if in.Minute < 0 || in.Minute > 1440 {
		writeError(w, 400, "minute must be 0..1440")
		return
	}
	if in.EventType == "" {
		in.EventType = "duty_status"
	}
	if in.Origin == "" {
		in.Origin = "ELD"
	}
	var duty *string
	if in.DutyStatus != nil && strings.TrimSpace(*in.DutyStatus) != "" {
		v, ok := normalizeStatus(*in.DutyStatus)
		if !ok {
			writeError(w, 400, "invalid duty_status")
			return
		}
		duty = &v
	}
	var eventTime *time.Time
	if in.EventTime != nil && strings.TrimSpace(*in.EventTime) != "" {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(*in.EventTime))
		if err != nil {
			writeError(w, 400, "event_time must be RFC3339")
			return
		}
		eventTime = &t
	}
	var x EventResponse
	var et *time.Time
	err := s.db.QueryRow(r.Context(), `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,odometer_miles,engine_hours,duration_seconds,origin,diagnostic,violation)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
RETURNING id::text,minute,event_time,event_type,duty_status,note,location_text,odometer_miles,engine_hours,duration_seconds,origin,diagnostic,violation,edited,edit_reason,co_driver,revision`,
		id, in.Date, in.Minute, eventTime, in.EventType, duty, in.Note, in.LocationText, in.OdometerMiles, in.EngineHours, in.DurationSeconds, in.Origin, in.Diagnostic, in.Violation).Scan(
		&x.ID, &x.Minute, &et, &x.EventType, &x.DutyStatus, &x.Note, &x.LocationText, &x.OdometerMiles, &x.EngineHours, &x.DurationSeconds,
		&x.Origin, &x.Diagnostic, &x.Violation, &x.Edited, &x.EditReason, &x.CoDriver, &x.Revision)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	x.EventTime = timeString(et)
	writeJSON(w, 201, x)
}

func (s *Server) handleAdminAlert(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DriverID string `json:"driver_id"`
		Kind     string `json:"kind"`
		Priority string `json:"priority"`
		Title    string `json:"title"`
		Detail   string `json:"detail"`
		EventID  string `json:"event_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(in.Title) == "" {
		writeError(w, 400, "title is required")
		return
	}
	if in.Kind == "" {
		in.Kind = "alert"
	}
	if in.Priority == "" {
		in.Priority = "attention"
	}
	var a AlertResponse
	err := s.db.QueryRow(r.Context(), `
INSERT INTO alerts(driver_id,kind,priority,title,detail,event_id)
VALUES(NULLIF($1,''),$2,$3,$4,$5,$6)
RETURNING id::text,COALESCE(driver_id,''),kind,priority,title,detail,event_id,status,owner,resolution`,
		in.DriverID, in.Kind, in.Priority, in.Title, in.Detail, in.EventID).Scan(&a.ID, &a.DriverID, &a.Kind, &a.Priority, &a.Title, &a.Detail, &a.EventID, &a.Status, &a.Owner, &a.Resolution)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, a)
}

func (h *Hub) add(c *WSClient) { h.mu.Lock(); h.clients[c] = struct{}{}; h.mu.Unlock() }
func (h *Hub) remove(c *WSClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	_ = c.conn.Close()
}
func (h *Hub) broadcast(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.RLock()
	clients := make([]*WSClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		if !c.authed {
			continue
		}
		c.mu.Lock()
		err := c.conn.WriteMessage(websocket.TextMessage, b)
		c.mu.Unlock()
		if err != nil {
			h.remove(c)
		}
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return s.originAllowed(r.Header.Get("Origin")) }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	expectedToken := s.apiToken
	if expectedToken == "" {
		expectedToken = s.adminToken
	}
	c := &WSClient{conn: conn, authed: expectedToken == ""}
	s.hub.add(c)
	defer s.hub.remove(c)
	for {
		_, b, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]any
		if json.Unmarshal(b, &msg) != nil {
			continue
		}
		t, _ := msg["type"].(string)
		switch t {
		case "authenticate":
			token, _ := msg["token"].(string)
			if expectedToken == "" || token == expectedToken {
				c.authed = true
				c.mu.Lock()
				_ = c.conn.WriteJSON(map[string]any{"type": "authenticated"})
				c.mu.Unlock()
			} else {
				c.mu.Lock()
				_ = c.conn.WriteJSON(map[string]any{"type": "error", "error": "invalid access token"})
				c.mu.Unlock()
			}
		case "ping":
			c.mu.Lock()
			_ = c.conn.WriteJSON(map[string]any{"type": "pong", "ts": time.Now().UnixMilli()})
			c.mu.Unlock()
		case "pong", "subscribe":
			// Topic subscriptions are accepted. This single-workspace starter broadcasts all authorized changes.
		}
	}
}

func (s *Server) listenForChanges(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := pgx.Connect(ctx, s.databaseURL)
		if err != nil {
			log.Printf("LISTEN connect: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if _, err = conn.Exec(ctx, "LISTEN neksus_changes"); err != nil {
			log.Printf("LISTEN start: %v", err)
			_ = conn.Close(ctx)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("PostgreSQL realtime listener active")
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				log.Printf("LISTEN wait: %v", err)
				_ = conn.Close(ctx)
				time.Sleep(2 * time.Second)
				break
			}
			var p notifyPayload
			if json.Unmarshal([]byte(n.Payload), &p) != nil {
				continue
			}
			s.handleDBNotification(ctx, p)
		}
	}
}

func (s *Server) handleDBNotification(ctx context.Context, p notifyPayload) {
	switch p.Table {
	case "drivers":
		s.hub.broadcast(map[string]any{"type": "driver.updated", "driver_id": p.DriverID})
	case "driver_live_state":
		if x, err := s.queryLiveState(ctx, p.DriverID); err == nil {
			s.hub.broadcast(map[string]any{"type": "fleet.driver.updated", "driver_id": p.DriverID, "data": x})
		} else {
			s.hub.broadcast(map[string]any{"type": "driver.updated", "driver_id": p.DriverID})
		}
	case "driver_hos_current":
		if h, err := s.queryHOS(ctx, p.DriverID); err == nil {
			s.hub.broadcast(map[string]any{"type": "driver.hos.updated", "driver_id": p.DriverID, "data": h})
		}
	case "duty_segments", "eld_events":
		s.hub.broadcast(map[string]any{"type": "driver.log.updated", "driver_id": p.DriverID})
	case "alerts":
		s.hub.broadcast(map[string]any{"type": "alerts.updated"})
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lmicroseconds)
}

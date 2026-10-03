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
var usdotPattern = regexp.MustCompile(`^[0-9]{1,8}$`)

type CompanyResponse struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	USDOT       string `json:"usdot"`
	DriverCount int    `json:"driver_count"`
}

type DriverResponse struct {
	ID                   string  `json:"id"`
	CompanyID            string  `json:"company_id"`
	USDOT                string  `json:"usdot"`
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
	ID               string  `json:"id"`
	StartMinute      int     `json:"start_minute"`
	EndMinute        *int    `json:"end_minute,omitempty"`
	DutyStatus       string  `json:"duty_status"`
	SpecialStatus    string  `json:"special_status"`
	Note             string  `json:"note"`
	Origin           string  `json:"origin"`
	Edited           bool    `json:"edited"`
	Revision         int64   `json:"revision"`
	LocationText     string  `json:"location_text"`
	OdometerMiles    float64 `json:"odometer_miles"`
	EngineHours      float64 `json:"engine_hours"`
	TrailerNumber    string  `json:"trailer_number"`
	ShippingDocument string  `json:"shipping_document"`
	EditReason       string  `json:"edit_reason"`
}

type LogEditHistoryResponse struct {
	ID          string  `json:"id"`
	Action      string  `json:"action"`
	Reason      string  `json:"reason"`
	Operator    string  `json:"operator"`
	StartMinute int     `json:"start_minute"`
	EndMinute   int     `json:"end_minute"`
	CreatedAt   string  `json:"created_at"`
	UndoneAt    *string `json:"undone_at,omitempty"`
	UndoneBy    string  `json:"undone_by,omitempty"`
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

type AlarmResponse struct {
	ID             string  `json:"id"`
	DriverID       string  `json:"driver_id"`
	Title          string  `json:"title"`
	Message        string  `json:"message"`
	Ringtone       string  `json:"ringtone"`
	CreatedAt      string  `json:"created_at"`
	DeliveredAt    *string `json:"delivered_at,omitempty"`
	AcknowledgedAt *string `json:"acknowledged_at,omitempty"`
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
	mux.Handle("GET /v1/driver/alarms", s.driverAuth(http.HandlerFunc(s.handleDriverAlarms)))
	mux.Handle("POST /v1/driver/alarms/{id}/delivered", s.driverAuth(http.HandlerFunc(s.handleDriverAlarmDelivered)))
	mux.Handle("POST /v1/driver/logout", s.driverAuth(http.HandlerFunc(s.handleDriverLogout)))
	mux.Handle("GET /v1/companies", s.readAuth(http.HandlerFunc(s.handleCompanies)))
	mux.Handle("GET /v1/drivers", s.readAuth(http.HandlerFunc(s.handleDrivers)))
	mux.Handle("GET /v1/alerts", s.readAuth(http.HandlerFunc(s.handleAlerts)))
	mux.Handle("GET /v1/fleet/live", s.readAuth(http.HandlerFunc(s.handleFleetLive)))
	mux.Handle("GET /v1/hos/current", s.readAuth(http.HandlerFunc(s.handleHOSBulk)))
	mux.Handle("GET /v1/drivers/{id}/hos", s.readAuth(http.HandlerFunc(s.handleDriverHOS)))
	mux.Handle("GET /v1/drivers/{id}/logs/{date}", s.readAuth(http.HandlerFunc(s.handleDriverLog)))
	mux.HandleFunc("GET /v1/ws", s.handleWS)

	mux.Handle("GET /v1/admin/session", s.adminAuth(http.HandlerFunc(s.handleAdminSession)))
	mux.Handle("POST /v1/admin/companies", s.adminAuth(http.HandlerFunc(s.handleAdminCompany)))
	mux.Handle("POST /v1/admin/drivers", s.adminAuth(http.HandlerFunc(s.handleAdminDriver)))
	mux.Handle("DELETE /v1/admin/drivers/{id}", s.adminAuth(http.HandlerFunc(s.handleAdminDriverDelete)))
	mux.Handle("PUT /v1/admin/drivers/{id}/live", s.adminAuth(http.HandlerFunc(s.handleAdminLive)))
	mux.Handle("PUT /v1/admin/drivers/{id}/hos", s.adminAuth(http.HandlerFunc(s.handleAdminHOS)))
	mux.Handle("POST /v1/admin/drivers/{id}/segments", s.adminAuth(http.HandlerFunc(s.handleAdminSegment)))
	mux.Handle("POST /v1/admin/drivers/{id}/logs/{date}/range-edit", s.adminAuth(http.HandlerFunc(s.handleAdminRangeEdit)))
	mux.Handle("POST /v1/admin/drivers/{id}/logs/{date}/undo", s.adminAuth(http.HandlerFunc(s.handleAdminLogUndo)))
	mux.Handle("POST /v1/admin/drivers/{id}/events", s.adminAuth(http.HandlerFunc(s.handleAdminEvent)))
	mux.Handle("POST /v1/admin/drivers/{id}/alarms", s.adminAuth(http.HandlerFunc(s.handleAdminDriverAlarm)))
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
SELECT d.id, COALESCE(d.company_id::text,''), COALESCE(c.usdot,''), d.username, d.first_name, d.last_name, d.full_name, COALESCE(c.name,d.carrier), d.phone, d.email,
       d.license_issue_state, d.license_number, d.home_terminal_timezone, d.truck_unit, d.trailer_number, d.shipping_document,
       d.vehicle_type, d.certified,
       COALESCE(ls.duty_status,'OFF'), COALESCE(ls.connected,false), COALESCE(ls.location_text,''),
       COALESCE(ls.latitude,0), COALESCE(ls.longitude,0), ls.status_since, COALESCE(ls.revision,0)
FROM drivers d
LEFT JOIN companies c ON c.id=d.company_id
LEFT JOIN driver_live_state ls ON ls.driver_id=d.id
WHERE d.id=$1 AND d.active=true`, id).Scan(
		&d.ID, &d.CompanyID, &d.USDOT, &d.Username, &d.FirstName, &d.LastName, &d.Name, &d.Carrier, &d.Phone, &d.Email,
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
SELECT d.id, COALESCE(d.company_id::text,''), COALESCE(c.usdot,''), d.username, d.first_name, d.last_name, d.full_name, COALESCE(c.name,d.carrier), d.phone, d.email,
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
		if err := rows.Scan(&d.ID, &d.CompanyID, &d.USDOT, &d.Username, &d.FirstName, &d.LastName, &d.Name, &d.Carrier, &d.Phone, &d.Email,
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

func (s *Server) handleCompanies(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
SELECT c.id::text,c.name,c.usdot,count(d.id)::int
FROM companies c
LEFT JOIN drivers d ON d.company_id=c.id AND d.active=true
GROUP BY c.id,c.name,c.usdot
ORDER BY c.name`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []CompanyResponse{}
	for rows.Next() {
		var c CompanyResponse
		if err := rows.Scan(&c.ID, &c.Name, &c.USDOT, &c.DriverCount); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		out = append(out, c)
	}
	writeJSON(w, 200, map[string]any{"companies": out})
}

func (s *Server) handleAdminCompany(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name  string `json:"name"`
		USDOT string `json:"usdot"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	in.USDOT = strings.TrimSpace(in.USDOT)
	if in.Name == "" || len(in.Name) > 120 {
		writeError(w, 400, "company name is required and must be 120 characters or less")
		return
	}
	if !usdotPattern.MatchString(in.USDOT) {
		writeError(w, 400, "USDOT must contain 1-8 digits")
		return
	}
	var c CompanyResponse
	err := s.db.QueryRow(r.Context(), `
INSERT INTO companies(name,usdot)
VALUES($1,$2)
ON CONFLICT(name) DO UPDATE SET usdot=EXCLUDED.usdot WHERE companies.usdot=''
RETURNING id::text,name,usdot`, in.Name, in.USDOT).Scan(&c.ID, &c.Name, &c.USDOT)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(strings.ToLower(err.Error()), "usdot") || strings.Contains(strings.ToLower(err.Error()), "duplicate key") {
			writeError(w, http.StatusConflict, "company name or USDOT already exists")
			return
		}
		writeError(w, 500, err.Error())
		return
	}
	_ = s.db.QueryRow(r.Context(), `SELECT count(*)::int FROM drivers WHERE company_id=$1::uuid AND active=true`, c.ID).Scan(&c.DriverCount)
	writeJSON(w, http.StatusCreated, c)
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

func scanSegments(ctx context.Context, db *pgxpool.Pool, id, date string) ([]SegmentResponse, error) {
	segs := []SegmentResponse{}
	rows, err := db.Query(ctx, `
SELECT id::text, start_minute, end_minute, duty_status, special_status, note, origin, edited, revision,
       location_text, odometer_miles, engine_hours, trailer_number, shipping_document, edit_reason
FROM duty_segments WHERE driver_id=$1 AND log_date=$2 ORDER BY start_minute,id`, id, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var x SegmentResponse
		if err := rows.Scan(&x.ID, &x.StartMinute, &x.EndMinute, &x.DutyStatus, &x.SpecialStatus,
			&x.Note, &x.Origin, &x.Edited, &x.Revision, &x.LocationText, &x.OdometerMiles,
			&x.EngineHours, &x.TrailerNumber, &x.ShippingDocument, &x.EditReason); err != nil {
			return nil, err
		}
		segs = append(segs, x)
	}
	return segs, rows.Err()
}

func (s *Server) repairCurrentLogIfEmpty(ctx context.Context, id, date, timezone string) error {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().UTC()
	localNow := now.In(loc)
	if localNow.Format("2006-01-02") != date {
		return nil
	}
	var status, locationText string
	var statusSince *time.Time
	err = s.db.QueryRow(ctx, `SELECT duty_status,location_text,status_since FROM driver_live_state WHERE driver_id=$1`, id).Scan(&status, &locationText, &statusSince)
	if errors.Is(err, pgx.ErrNoRows) {
		status = "OFF"
		statusSince = nil
	} else if err != nil {
		return err
	}
	if normalized, ok := normalizeStatus(status); ok {
		status = normalized
	} else {
		status = "OFF"
	}
	startMinute := 0
	if statusSince != nil {
		s := statusSince.In(loc)
		if s.Format("2006-01-02") == date {
			startMinute = s.Hour()*60 + s.Minute()
		}
	}
	if startMinute < 0 || startMinute > 1440 {
		startMinute = 0
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM duty_segments WHERE driver_id=$1 AND log_date=$2`, id, date).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return tx.Commit(ctx)
	}
	if status != "OFF" && startMinute > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin) VALUES($1,$2,0,$3,'OFF','Recovered online log continuity','System')`, id, date, startMinute); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin) VALUES($1,$2,$3,NULL,$4,'Recovered current online status','System')`, id, date, startMinute, status); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,origin) VALUES($1,$2,$3,$4,'duty_status',$5,'Recovered current online status',$6,'System')`, id, date, startMinute, now, status, locationText); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
	segs, err := scanSegments(r.Context(), s.db, id, date)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if len(segs) == 0 {
		if err := s.repairCurrentLogIfEmpty(r.Context(), id, date, timezone); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		segs, err = scanSegments(r.Context(), s.db, id, date)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
	}

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

	history := []LogEditHistoryResponse{}
	hrows, err := s.db.Query(r.Context(), `
SELECT id::text, action, reason, operator_name, start_minute, end_minute, created_at, undone_at, undone_by
FROM log_edit_batches WHERE driver_id=$1 AND log_date=$2 ORDER BY created_at DESC LIMIT 40`, id, date)
	if err == nil {
		for hrows.Next() {
			var h LogEditHistoryResponse
			var created time.Time
			var undone *time.Time
			if err := hrows.Scan(&h.ID, &h.Action, &h.Reason, &h.Operator, &h.StartMinute, &h.EndMinute, &created, &undone, &h.UndoneBy); err != nil {
				break
			}
			h.CreatedAt = created.UTC().Format(time.RFC3339)
			h.UndoneAt = timeString(undone)
			history = append(history, h)
		}
		hrows.Close()
	}

	payload := map[string]any{
		"driver_id":    id,
		"date":         date,
		"timezone":     timezone,
		"segments":     segs,
		"events":       events,
		"edit_history": history,
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

func ringtoneAllowed(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "alert", "urgent", "chime", "bell", "pulse":
		return true
	default:
		return false
	}
}

func (s *Server) handleDriverAlarms(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	rows, err := s.db.Query(r.Context(), `
SELECT id::text,driver_id,title,message,ringtone,created_at,delivered_at,acknowledged_at
FROM driver_alarms
WHERE driver_id=$1 AND delivered_at IS NULL
ORDER BY created_at ASC LIMIT 20`, driverID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []AlarmResponse{}
	for rows.Next() {
		var a AlarmResponse
		var created time.Time
		var delivered, ack *time.Time
		if err := rows.Scan(&a.ID, &a.DriverID, &a.Title, &a.Message, &a.Ringtone, &created, &delivered, &ack); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		a.CreatedAt = created.UTC().Format(time.RFC3339)
		a.DeliveredAt = timeString(delivered)
		a.AcknowledgedAt = timeString(ack)
		out = append(out, a)
	}
	writeJSON(w, 200, map[string]any{"alarms": out})
}

func (s *Server) handleDriverAlarmDelivered(w http.ResponseWriter, r *http.Request) {
	driverID, _ := r.Context().Value(driverIDContextKey).(string)
	id := strings.TrimSpace(r.PathValue("id"))
	ct, err := s.db.Exec(r.Context(), `UPDATE driver_alarms SET delivered_at=COALESCE(delivered_at,now()) WHERE id=$1::uuid AND driver_id=$2`, id, driverID)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		writeError(w, 404, "alarm not found")
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleAdminDriverAlarm(w http.ResponseWriter, r *http.Request) {
	driverID := strings.TrimSpace(r.PathValue("id"))
	var in struct {
		Title    string `json:"title"`
		Message  string `json:"message"`
		Ringtone string `json:"ringtone"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	in.Title = strings.TrimSpace(in.Title)
	in.Message = strings.TrimSpace(in.Message)
	in.Ringtone = strings.ToLower(strings.TrimSpace(in.Ringtone))
	if in.Title == "" {
		in.Title = "NEKSUS alert"
	}
	if in.Message == "" {
		writeError(w, 400, "alarm message is required")
		return
	}
	if len(in.Title) > 80 || len(in.Message) > 500 {
		writeError(w, 400, "alarm title or message is too long")
		return
	}
	if !ringtoneAllowed(in.Ringtone) {
		writeError(w, 400, "ringtone must be alert, urgent, chime, bell or pulse")
		return
	}
	var exists bool
	if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM drivers WHERE id=$1 AND active=true)`, driverID).Scan(&exists); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if !exists {
		writeError(w, 404, "driver not found")
		return
	}
	var a AlarmResponse
	var created time.Time
	err := s.db.QueryRow(r.Context(), `
INSERT INTO driver_alarms(driver_id,title,message,ringtone)
VALUES($1,$2,$3,$4)
RETURNING id::text,driver_id,title,message,ringtone,created_at`, driverID, in.Title, in.Message, in.Ringtone).Scan(&a.ID, &a.DriverID, &a.Title, &a.Message, &a.Ringtone, &created)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	a.CreatedAt = created.UTC().Format(time.RFC3339)
	writeJSON(w, http.StatusCreated, a)
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
		CompanyID         string `json:"company_id"`
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
	in.CompanyID = strings.TrimSpace(in.CompanyID)
	creating := in.ID == ""
	if !driverUsernamePattern.MatchString(in.Username) {
		writeError(w, 400, "username must be 4-40 characters using letters, numbers, dot, underscore or dash")
		return
	}
	if in.FirstName == "" || in.LastName == "" {
		writeError(w, 400, "first name and last name are required")
		return
	}
	if in.CompanyID == "" {
		writeError(w, 400, "select an existing company")
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
	var companyID, carrier string
	if err := s.db.QueryRow(r.Context(), `SELECT id::text,name FROM companies WHERE id=$1::uuid`, in.CompanyID).Scan(&companyID, &carrier); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, 400, "selected company does not exist")
		} else {
			writeError(w, 500, err.Error())
		}
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
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
	if creating {
		loc, _ := time.LoadLocation("America/Chicago")
		if loc == nil {
			loc = time.UTC
		}
		today := time.Now().In(loc)
		for daysAgo := 6; daysAgo >= 0; daysAgo-- {
			day := today.AddDate(0, 0, -daysAgo).Format("2006-01-02")
			if daysAgo == 0 {
				_, err = tx.Exec(r.Context(), `
INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin)
SELECT $1,$2,0,NULL,'OFF','Initial seven-day OFF duty history','System'
WHERE NOT EXISTS (SELECT 1 FROM duty_segments WHERE driver_id=$1 AND log_date=$2)`, in.ID, day)
			} else {
				_, err = tx.Exec(r.Context(), `
INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,note,origin)
SELECT $1,$2,0,1440,'OFF','Initial seven-day OFF duty history','System'
WHERE NOT EXISTS (SELECT 1 FROM duty_segments WHERE driver_id=$1 AND log_date=$2)`, in.ID, day)
			}
			if err != nil {
				writeError(w, 500, err.Error())
				return
			}
		}
		_, err = tx.Exec(r.Context(), `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,origin)
SELECT $1,$2,0,now(),'duty_status','OFF','Initial OFF duty status','System'
WHERE NOT EXISTS (SELECT 1 FROM eld_events WHERE driver_id=$1 AND log_date=$2 AND minute=0 AND duty_status='OFF')`, in.ID, today.Format("2006-01-02"))
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
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

type rangeEditInput struct {
	StartMinute      int     `json:"start_minute"`
	EndMinute        int     `json:"end_minute"`
	Status           string  `json:"status"`
	SpecialStatus    string  `json:"special_status"`
	Note             string  `json:"note"`
	Reason           string  `json:"reason"`
	Operator         string  `json:"operator"`
	LocationText     string  `json:"location_text"`
	OdometerMiles    float64 `json:"odometer_miles"`
	EngineHours      float64 `json:"engine_hours"`
	TrailerNumber    string  `json:"trailer_number"`
	ShippingDocument string  `json:"shipping_document"`
}

func intPointer(v int) *int { return &v }

func copySegment(x SegmentResponse) SegmentResponse {
	y := x
	if x.EndMinute != nil {
		v := *x.EndMinute
		y.EndMinute = &v
	}
	return y
}

func (s *Server) scanSegmentsTx(ctx context.Context, tx pgx.Tx, id, date string) ([]SegmentResponse, error) {
	rows, err := tx.Query(ctx, `
SELECT id::text, start_minute, end_minute, duty_status, special_status, note, origin, edited, revision,
       location_text, odometer_miles, engine_hours, trailer_number, shipping_document, edit_reason
FROM duty_segments WHERE driver_id=$1 AND log_date=$2 ORDER BY start_minute,id FOR UPDATE`, id, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SegmentResponse{}
	for rows.Next() {
		var x SegmentResponse
		if err := rows.Scan(&x.ID, &x.StartMinute, &x.EndMinute, &x.DutyStatus, &x.SpecialStatus,
			&x.Note, &x.Origin, &x.Edited, &x.Revision, &x.LocationText, &x.OdometerMiles,
			&x.EngineHours, &x.TrailerNumber, &x.ShippingDocument, &x.EditReason); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Server) driverDayLimit(ctx context.Context, id, date string) (string, int, bool, error) {
	var timezone string
	if err := s.db.QueryRow(ctx, `SELECT home_terminal_timezone FROM drivers WHERE id=$1 AND active=true`, id).Scan(&timezone); err != nil {
		return "", 0, false, err
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	requested, err := time.ParseInLocation("2006-01-02", date, loc)
	if err != nil {
		return timezone, 0, false, err
	}
	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	if requested.After(today) {
		return timezone, 0, false, errors.New("cannot edit a future log date")
	}
	current := requested.Equal(today)
	if current {
		minute := now.Hour()*60 + now.Minute()
		if minute < 1 {
			minute = 1
		}
		return timezone, minute, true, nil
	}
	return timezone, 1440, false, nil
}

func canonicalSegments(in []SegmentResponse, dayEnd int) []SegmentResponse {
	if dayEnd < 1 {
		dayEnd = 1
	}
	out := []SegmentResponse{}
	cursor := 0
	for _, original := range in {
		x := copySegment(original)
		start := x.StartMinute
		end := dayEnd
		if x.EndMinute != nil {
			end = *x.EndMinute
		}
		if start < 0 {
			start = 0
		}
		if start > dayEnd {
			start = dayEnd
		}
		if end < 0 {
			end = 0
		}
		if end > dayEnd {
			end = dayEnd
		}
		if start > cursor {
			out = append(out, SegmentResponse{StartMinute: cursor, EndMinute: intPointer(start), DutyStatus: "OFF", Note: "Continuity filler", Origin: "System"})
			cursor = start
		}
		if start < cursor {
			start = cursor
		}
		if end <= start {
			continue
		}
		x.StartMinute = start
		x.EndMinute = intPointer(end)
		out = append(out, x)
		cursor = end
		if cursor >= dayEnd {
			break
		}
	}
	if cursor < dayEnd {
		out = append(out, SegmentResponse{StartMinute: cursor, EndMinute: intPointer(dayEnd), DutyStatus: "OFF", Note: "Continuity filler", Origin: "System"})
	}
	if len(out) == 0 {
		out = append(out, SegmentResponse{StartMinute: 0, EndMinute: intPointer(dayEnd), DutyStatus: "OFF", Note: "Continuity filler", Origin: "System"})
	}
	return out
}

func sameEditableSegment(a, b SegmentResponse) bool {
	return a.DutyStatus == b.DutyStatus && a.SpecialStatus == b.SpecialStatus && a.Note == b.Note &&
		a.Origin == b.Origin && a.LocationText == b.LocationText && a.TrailerNumber == b.TrailerNumber &&
		a.ShippingDocument == b.ShippingDocument && a.EditReason == b.EditReason && a.Edited == b.Edited
}

func mergeAdjacentSegments(in []SegmentResponse) []SegmentResponse {
	out := []SegmentResponse{}
	for _, x := range in {
		if len(out) == 0 {
			out = append(out, x)
			continue
		}
		prev := &out[len(out)-1]
		if prev.EndMinute != nil && *prev.EndMinute == x.StartMinute && sameEditableSegment(*prev, x) {
			prev.EndMinute = x.EndMinute
			continue
		}
		out = append(out, x)
	}
	return out
}

func applyRangeOverlay(base []SegmentResponse, in rangeEditInput, duty, special string) []SegmentResponse {
	out := []SegmentResponse{}
	inserted := false
	makeReplacement := func() SegmentResponse {
		return SegmentResponse{
			StartMinute: in.StartMinute, EndMinute: intPointer(in.EndMinute), DutyStatus: duty, SpecialStatus: special,
			Note: in.Note, Origin: "Admin", Edited: true, Revision: 1, LocationText: in.LocationText,
			OdometerMiles: in.OdometerMiles, EngineHours: in.EngineHours, TrailerNumber: in.TrailerNumber,
			ShippingDocument: in.ShippingDocument, EditReason: in.Reason,
		}
	}
	for _, original := range base {
		x := copySegment(original)
		end := *x.EndMinute
		if end <= in.StartMinute {
			out = append(out, x)
			continue
		}
		if x.StartMinute >= in.EndMinute {
			if !inserted {
				out = append(out, makeReplacement())
				inserted = true
			}
			out = append(out, x)
			continue
		}
		if x.StartMinute < in.StartMinute {
			prefix := copySegment(x)
			prefix.EndMinute = intPointer(in.StartMinute)
			out = append(out, prefix)
		}
		if !inserted {
			out = append(out, makeReplacement())
			inserted = true
		}
		if end > in.EndMinute {
			suffix := copySegment(x)
			suffix.ID = ""
			suffix.StartMinute = in.EndMinute
			suffix.EndMinute = intPointer(end)
			out = append(out, suffix)
		}
	}
	if !inserted {
		out = append(out, makeReplacement())
	}
	return mergeAdjacentSegments(out)
}

func (s *Server) replaceDaySegmentsTx(ctx context.Context, tx pgx.Tx, driverID, date string, segs []SegmentResponse, keepLastOpen bool) error {
	if _, err := tx.Exec(ctx, `DELETE FROM duty_segments WHERE driver_id=$1 AND log_date=$2`, driverID, date); err != nil {
		return err
	}
	for i, x := range segs {
		var end *int = x.EndMinute
		if keepLastOpen && i == len(segs)-1 {
			end = nil
		}
		if x.Origin == "" {
			x.Origin = "Admin"
		}
		if x.ID != "" {
			_, err := tx.Exec(ctx, `
INSERT INTO duty_segments(id,driver_id,log_date,start_minute,end_minute,duty_status,special_status,note,origin,edited,revision,
 location_text,odometer_miles,engine_hours,trailer_number,shipping_document,edit_reason)
VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,GREATEST($11,1),$12,$13,$14,$15,$16,$17)`,
				x.ID, driverID, date, x.StartMinute, end, x.DutyStatus, x.SpecialStatus, x.Note, x.Origin, x.Edited,
				x.Revision, x.LocationText, x.OdometerMiles, x.EngineHours, x.TrailerNumber, x.ShippingDocument, x.EditReason)
			if err != nil {
				return err
			}
		} else {
			_, err := tx.Exec(ctx, `
INSERT INTO duty_segments(driver_id,log_date,start_minute,end_minute,duty_status,special_status,note,origin,edited,revision,
 location_text,odometer_miles,engine_hours,trailer_number,shipping_document,edit_reason)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,GREATEST($10,1),$11,$12,$13,$14,$15,$16)`,
				driverID, date, x.StartMinute, end, x.DutyStatus, x.SpecialStatus, x.Note, x.Origin, x.Edited,
				x.Revision, x.LocationText, x.OdometerMiles, x.EngineHours, x.TrailerNumber, x.ShippingDocument, x.EditReason)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type hosInterval struct {
	start  time.Time
	end    time.Time
	status string
}

func (s *Server) recalcHOSFromSegments(ctx context.Context, driverID string) error {
	var timezone string
	if err := s.db.QueryRow(ctx, `SELECT home_terminal_timezone FROM drivers WHERE id=$1`, driverID).Scan(&timezone); err != nil {
		return err
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	startDate := today.AddDate(0, 0, -7)
	rows, err := s.db.Query(ctx, `
SELECT log_date,start_minute,end_minute,duty_status
FROM duty_segments WHERE driver_id=$1 AND log_date BETWEEN $2::date AND $3::date
ORDER BY log_date,start_minute,id`, driverID, startDate.Format("2006-01-02"), today.Format("2006-01-02"))
	if err != nil {
		return err
	}
	defer rows.Close()
	intervals := []hosInterval{}
	for rows.Next() {
		var day time.Time
		var start int
		var end *int
		var status string
		if err := rows.Scan(&day, &start, &end, &status); err != nil {
			return err
		}
		base := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc)
		limit := 1440
		if base.Equal(today) {
			limit = now.Hour()*60 + now.Minute()
			if limit < 1 {
				limit = 1
			}
		}
		e := limit
		if end != nil {
			e = *end
		}
		if e > limit {
			e = limit
		}
		if e <= start {
			continue
		}
		intervals = append(intervals, hosInterval{start: base.Add(time.Duration(start) * time.Minute), end: base.Add(time.Duration(e) * time.Minute), status: status})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var restStart *time.Time
	var previousEnd *time.Time
	var last10, last34 *time.Time
	for _, iv := range intervals {
		rest := iv.status == "OFF" || iv.status == "SB"
		if rest {
			if restStart == nil || previousEnd == nil || !iv.start.Equal(*previousEnd) {
				t := iv.start
				restStart = &t
			}
			e := iv.end
			previousEnd = &e
			dur := iv.end.Sub(*restStart)
			if dur >= 10*time.Hour {
				t := iv.end
				last10 = &t
			}
			if dur >= 34*time.Hour {
				t := iv.end
				last34 = &t
			}
		} else {
			restStart = nil
			e := iv.end
			previousEnd = &e
		}
	}

	windowStart := startDate
	if last10 != nil && last10.After(windowStart) {
		windowStart = *last10
	}
	cycleStart := startDate
	if last34 != nil && last34.After(cycleStart) {
		cycleStart = *last34
	}
	var shiftStart *time.Time
	driveSinceReset := 0
	driveSinceBreak := 0
	cycleUsed := 0
	for _, iv := range intervals {
		cySt := iv.start
		if cySt.Before(cycleStart) {
			cySt = cycleStart
		}
		if iv.end.After(cycleStart) && (iv.status == "DR" || iv.status == "ON") {
			cycleUsed += int(iv.end.Sub(cySt).Seconds())
		}
		if iv.end.Before(windowStart) || iv.end.Equal(windowStart) {
			continue
		}
		st := iv.start
		if st.Before(windowStart) {
			st = windowStart
		}
		durSec := int(iv.end.Sub(st).Seconds())
		if durSec < 0 {
			durSec = 0
		}
		if (iv.status == "DR" || iv.status == "ON") && shiftStart == nil {
			t := st
			shiftStart = &t
		}
		if iv.status == "DR" {
			driveSinceReset += durSec
			driveSinceBreak += durSec
		} else if iv.end.Sub(st) >= 30*time.Minute {
			driveSinceBreak = 0
		}
	}
	driveRemaining := 11*3600 - driveSinceReset
	if driveRemaining < 0 {
		driveRemaining = 0
	}
	breakRemaining := 8*3600 - driveSinceBreak
	if breakRemaining < 0 {
		breakRemaining = 0
	}
	shiftRemaining := 14 * 3600
	if shiftStart != nil {
		shiftRemaining -= int(now.Sub(*shiftStart).Seconds())
		if shiftRemaining < 0 {
			shiftRemaining = 0
		}
	}
	cycleRemaining := 70*3600 - cycleUsed
	if cycleRemaining < 0 {
		cycleRemaining = 0
	}
	_, err = s.db.Exec(ctx, `
INSERT INTO driver_hos_current(driver_id,break_remaining_seconds,drive_remaining_seconds,shift_remaining_seconds,cycle_remaining_seconds,
 shift_started_at,last_10h_reset_at,last_34h_reset_at,calculated_at,revision)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,now(),1)
ON CONFLICT(driver_id) DO UPDATE SET break_remaining_seconds=EXCLUDED.break_remaining_seconds,
 drive_remaining_seconds=EXCLUDED.drive_remaining_seconds, shift_remaining_seconds=EXCLUDED.shift_remaining_seconds,
 cycle_remaining_seconds=EXCLUDED.cycle_remaining_seconds, shift_started_at=EXCLUDED.shift_started_at,
 last_10h_reset_at=EXCLUDED.last_10h_reset_at, last_34h_reset_at=EXCLUDED.last_34h_reset_at,
 calculated_at=now(), revision=driver_hos_current.revision+1`, driverID, breakRemaining, driveRemaining, shiftRemaining,
		cycleRemaining, shiftStart, last10, last34)
	return err
}

func (s *Server) handleAdminRangeEdit(w http.ResponseWriter, r *http.Request) {
	driverID := strings.TrimSpace(r.PathValue("id"))
	date := strings.TrimSpace(r.PathValue("date"))
	if driverID == "" {
		writeError(w, 400, "driver id is required")
		return
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		writeError(w, 400, "date must be YYYY-MM-DD")
		return
	}
	var in rangeEditInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	in.Status = strings.ToUpper(strings.TrimSpace(in.Status))
	in.SpecialStatus = strings.ToUpper(strings.TrimSpace(in.SpecialStatus))
	in.Reason = strings.TrimSpace(in.Reason)
	in.Operator = strings.TrimSpace(in.Operator)
	if in.Operator == "" {
		in.Operator = "Admin"
	}
	if len(in.Reason) < 3 {
		writeError(w, 400, "edit reason must be at least 3 characters")
		return
	}
	duty := ""
	special := ""
	switch in.Status {
	case "PC":
		duty, special = "OFF", "PC"
	case "YM":
		duty, special = "ON", "YM"
	default:
		var ok bool
		duty, ok = normalizeStatus(in.Status)
		if !ok {
			writeError(w, 400, "invalid status")
			return
		}
		if in.SpecialStatus == "PC" || in.SpecialStatus == "YM" {
			special = in.SpecialStatus
		}
	}
	_, dayEnd, current, err := s.driverDayLimit(r.Context(), driverID, date)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, 404, "driver not found")
		} else {
			writeError(w, 400, err.Error())
		}
		return
	}
	if in.StartMinute < 0 || in.EndMinute <= in.StartMinute || in.EndMinute > dayEnd {
		writeError(w, 400, "invalid edit range for this log date")
		return
	}
	if in.Note == "" {
		if special != "" {
			in.Note = special + " administrative edit"
		} else {
			in.Note = duty + " administrative edit"
		}
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	beforeRaw, err := s.scanSegmentsTx(r.Context(), tx, driverID, date)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	before := canonicalSegments(beforeRaw, dayEnd)
	after := applyRangeOverlay(before, in, duty, special)
	if current && len(after) > 0 {
		after[len(after)-1].EndMinute = intPointer(dayEnd)
	}
	beforeJSON, _ := json.Marshal(beforeRaw)
	afterJSON, _ := json.Marshal(after)
	if err := s.replaceDaySegmentsTx(r.Context(), tx, driverID, date, after, current); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(), `
INSERT INTO log_edit_batches(driver_id,log_date,action,reason,operator_name,start_minute,end_minute,before_segments,after_segments)
VALUES($1,$2,'range_edit',$3,$4,$5,$6,$7::jsonb,$8::jsonb)`, driverID, date, in.Reason, in.Operator, in.StartMinute, in.EndMinute, string(beforeJSON), string(afterJSON)); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(), `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,duty_status,note,location_text,odometer_miles,engine_hours,
 duration_seconds,origin,edited,edit_reason)
VALUES($1,$2,$3,now(),'administrative_edit',$4,$5,$6,$7,$8,$9,'Admin',true,$10)`,
		driverID, date, in.StartMinute, duty, in.Note, in.LocationText, in.OdometerMiles, in.EngineHours,
		(in.EndMinute-in.StartMinute)*60, in.Reason); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if current && in.EndMinute >= dayEnd {
		_, _ = tx.Exec(r.Context(), `UPDATE driver_live_state SET duty_status=$2,status_since=now(),revision=revision+1 WHERE driver_id=$1`, driverID, duty)
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_ = s.recalcHOSFromSegments(r.Context(), driverID)
	writeJSON(w, 200, map[string]any{"ok": true, "driver_id": driverID, "date": date, "start_minute": in.StartMinute, "end_minute": in.EndMinute})
}

func (s *Server) handleAdminLogUndo(w http.ResponseWriter, r *http.Request) {
	driverID := strings.TrimSpace(r.PathValue("id"))
	date := strings.TrimSpace(r.PathValue("date"))
	if _, err := time.Parse("2006-01-02", date); err != nil {
		writeError(w, 400, "date must be YYYY-MM-DD")
		return
	}
	var in struct {
		Operator string `json:"operator"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in)
	if strings.TrimSpace(in.Operator) == "" {
		in.Operator = "Admin"
	}
	_, _, current, err := s.driverDayLimit(r.Context(), driverID, date)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, 404, "driver not found")
		} else {
			writeError(w, 400, err.Error())
		}
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var batchID string
	var beforeJSON string
	var startMinute, endMinute int
	err = tx.QueryRow(r.Context(), `
SELECT id::text,before_segments::text,start_minute,end_minute
FROM log_edit_batches WHERE driver_id=$1 AND log_date=$2 AND undone_at IS NULL
ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, driverID, date).Scan(&batchID, &beforeJSON, &startMinute, &endMinute)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 409, "there is no edit to undo for this day")
		return
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	var before []SegmentResponse
	if err := json.Unmarshal([]byte(beforeJSON), &before); err != nil {
		writeError(w, 500, "stored edit snapshot is invalid")
		return
	}
	if err := s.replaceDaySegmentsTx(r.Context(), tx, driverID, date, before, current && len(before) > 0 && before[len(before)-1].EndMinute == nil); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(), `UPDATE log_edit_batches SET undone_at=now(),undone_by=$2 WHERE id=$1::uuid`, batchID, in.Operator); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(), `
INSERT INTO eld_events(driver_id,log_date,minute,event_time,event_type,note,origin,edited,edit_reason,duration_seconds)
VALUES($1,$2,$3,now(),'administrative_undo','Administrative log edit undone','Admin',true,'Undo previous edit',$4)`, driverID, date, startMinute, (endMinute-startMinute)*60); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_ = s.recalcHOSFromSegments(r.Context(), driverID)
	writeJSON(w, 200, map[string]any{"ok": true, "undone_edit_id": batchID})
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
RETURNING id::text,start_minute,end_minute,duty_status,special_status,note,origin,edited,revision,
 location_text,odometer_miles,engine_hours,trailer_number,shipping_document,edit_reason`,
		id, in.Date, in.StartMinute, in.EndMinute, status, in.SpecialStatus, in.Note, in.Origin, in.Edited).Scan(
		&x.ID, &x.StartMinute, &x.EndMinute, &x.DutyStatus, &x.SpecialStatus, &x.Note, &x.Origin, &x.Edited, &x.Revision,
		&x.LocationText, &x.OdometerMiles, &x.EngineHours, &x.TrailerNumber, &x.ShippingDocument, &x.EditReason)
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
	case "driver_alarms":
		s.hub.broadcast(map[string]any{"type": "driver.alarm.updated", "driver_id": p.DriverID, "alarm_id": p.ID})
	case "companies":
		s.hub.broadcast(map[string]any{"type": "companies.updated", "company_id": p.ID})
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lmicroseconds)
}

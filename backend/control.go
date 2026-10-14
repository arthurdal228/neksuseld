package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const webIdentityContextKey contextKey = "web_identity"

type WebIdentity struct {
	ID           string          `json:"id"`
	Username     string          `json:"username"`
	FullName     string          `json:"full_name"`
	Role         string          `json:"role"`
	AllCompanies bool            `json:"all_companies"`
	CompanyIDs   map[string]bool `json:"-"`
	Legacy       bool            `json:"-"`
}

type WebUserResponse struct {
	ID           string   `json:"id"`
	Username     string   `json:"username"`
	FullName     string   `json:"full_name"`
	Role         string   `json:"role"`
	Active       bool     `json:"active"`
	AllCompanies bool     `json:"all_companies"`
	CompanyIDs   []string `json:"company_ids"`
	LastLoginAt  *string  `json:"last_login_at,omitempty"`
	LastSeenAt   *string  `json:"last_seen_at,omitempty"`
	Online       bool     `json:"online"`
	CreatedAt    string   `json:"created_at"`
}

func (i *WebIdentity) canRead() bool { return i != nil }
func (i *WebIdentity) canEdit() bool {
	return i != nil && (i.Legacy || i.Role == "super_admin" || i.Role == "manager" || i.Role == "operator")
}
func (i *WebIdentity) canManageUsers() bool {
	return i != nil && (i.Legacy || i.Role == "super_admin")
}
func (i *WebIdentity) canManageCompanies() bool {
	return i != nil && (i.Legacy || i.Role == "super_admin" || i.Role == "manager")
}
func (i *WebIdentity) companyAllowed(id string) bool {
	if i == nil || i.Legacy || i.AllCompanies {
		return true
	}
	return i.CompanyIDs[id]
}

func identityFromContext(ctx context.Context) *WebIdentity {
	v, _ := ctx.Value(webIdentityContextKey).(*WebIdentity)
	return v
}

type statusCaptureWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCaptureWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusCaptureWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (s *Server) bootstrapControlAdmin(ctx context.Context) error {
	username := strings.TrimSpace(os.Getenv("CONTROL_ADMIN_USERNAME"))
	password := strings.TrimSpace(os.Getenv("CONTROL_ADMIN_PASSWORD"))
	if username == "" || password == "" {
		return nil
	}
	if len(username) < 4 || len(password) < 8 {
		return errors.New("CONTROL_ADMIN_USERNAME must be 4+ characters and CONTROL_ADMIN_PASSWORD must be 8+ characters")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	fullName := strings.TrimSpace(os.Getenv("CONTROL_ADMIN_NAME"))
	if fullName == "" {
		fullName = "NEKSUS Super Admin"
	}
	_, err = s.db.Exec(ctx, `
INSERT INTO web_users(username,full_name,password_hash,role,active,all_companies)
VALUES($1,$2,$3,'super_admin',true,true)
ON CONFLICT (lower(username)) DO UPDATE SET full_name=EXCLUDED.full_name,password_hash=EXCLUDED.password_hash,role='super_admin',active=true,all_companies=true`, username, fullName, string(hash))
	return err
}

func (s *Server) loadWebIdentity(ctx context.Context, token string) (*WebIdentity, error) {
	if strings.TrimSpace(token) == "" {
		return nil, pgx.ErrNoRows
	}
	h := sha256.Sum256([]byte(token))
	var i WebIdentity
	err := s.db.QueryRow(ctx, `
SELECT u.id::text,u.username,u.full_name,u.role,u.all_companies
FROM web_sessions ws
JOIN web_users u ON u.id=ws.user_id
WHERE ws.token_hash=$1 AND ws.expires_at>now() AND u.active=true`, h[:]).Scan(&i.ID, &i.Username, &i.FullName, &i.Role, &i.AllCompanies)
	if err != nil {
		return nil, err
	}
	i.CompanyIDs = map[string]bool{}
	rows, err := s.db.Query(ctx, `SELECT company_id::text FROM web_user_companies WHERE user_id=$1::uuid`, i.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		i.CompanyIDs[id] = true
	}
	_, _ = s.db.Exec(ctx, `UPDATE web_sessions SET last_seen_at=now() WHERE token_hash=$1`, h[:])
	return &i, rows.Err()
}

func (s *Server) legacyIdentity(token string, admin bool) *WebIdentity {
	if admin && s.adminToken != "" && token == s.adminToken {
		return &WebIdentity{Username: "legacy-admin", FullName: "Legacy Admin", Role: "super_admin", AllCompanies: true, Legacy: true, CompanyIDs: map[string]bool{}}
	}
	if !admin {
		if s.apiToken != "" && token == s.apiToken {
			return &WebIdentity{Username: "legacy-api", FullName: "Legacy API", Role: "viewer", AllCompanies: true, Legacy: true, CompanyIDs: map[string]bool{}}
		}
		if s.apiToken == "" && s.adminToken != "" && token == s.adminToken {
			return &WebIdentity{Username: "legacy-admin", FullName: "Legacy Admin", Role: "super_admin", AllCompanies: true, Legacy: true, CompanyIDs: map[string]bool{}}
		}
	}
	return nil
}

func (s *Server) authWebToken(r *http.Request, requireEdit bool) (*WebIdentity, error) {
	token := bearer(r)
	if token == "" {
		return nil, errors.New("login required")
	}
	if x := s.legacyIdentity(token, requireEdit); x != nil {
		return x, nil
	}
	i, err := s.loadWebIdentity(r.Context(), token)
	if err != nil {
		return nil, err
	}
	if requireEdit && !i.canEdit() {
		return nil, errors.New("write access required")
	}
	return i, nil
}

func (s *Server) webLogin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	in.Username = strings.TrimSpace(in.Username)
	var id, username, fullName, role, passwordHash string
	var all bool
	err := s.db.QueryRow(r.Context(), `SELECT id::text,username,full_name,role,password_hash,all_companies FROM web_users WHERE lower(username)=lower($1) AND active=true`, in.Username).Scan(&id, &username, &fullName, &role, &passwordHash, &all)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(in.Password)) != nil {
		writeError(w, 401, "invalid username or password")
		return
	}
	token, hash, err := generateSessionToken()
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, err = s.db.Exec(r.Context(), `INSERT INTO web_sessions(token_hash,user_id,expires_at) VALUES($1,$2::uuid,now()+interval '12 hours')`, hash, id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	_, _ = s.db.Exec(r.Context(), `UPDATE web_users SET last_login_at=now() WHERE id=$1::uuid`, id)
	i, err := s.loadWebIdentity(r.Context(), token)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	s.logWebActivity(r.Context(), i, "login", "", "", map[string]any{"source": "web"})
	writeJSON(w, 200, map[string]any{"token": token, "user": i, "expires_in_seconds": 43200})
}

func (s *Server) webLogout(w http.ResponseWriter, r *http.Request) {
	token := bearer(r)
	if token != "" {
		h := sha256.Sum256([]byte(token))
		_, _ = s.db.Exec(r.Context(), `DELETE FROM web_sessions WHERE token_hash=$1`, h[:])
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) webMe(w http.ResponseWriter, r *http.Request) {
	i := identityFromContext(r.Context())
	if i == nil {
		writeError(w, 401, "login required")
		return
	}
	companies := []CompanyResponse{}
	rows, err := s.db.Query(r.Context(), `SELECT c.id::text,c.name,c.usdot,count(d.id)::int FROM companies c LEFT JOIN drivers d ON d.company_id=c.id AND d.active=true GROUP BY c.id,c.name,c.usdot ORDER BY c.name`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var c CompanyResponse
			if rows.Scan(&c.ID, &c.Name, &c.USDOT, &c.DriverCount) == nil && i.companyAllowed(c.ID) {
				companies = append(companies, c)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"user": i, "companies": companies})
}

func (s *Server) webSessionAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i, err := s.authWebToken(r, false)
		if err != nil {
			writeError(w, 401, "session expired or invalid")
			return
		}
		ctx := context.WithValue(r.Context(), webIdentityContextKey, i)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (s *Server) controlAdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i, err := s.authWebToken(r, false)
		if err != nil {
			writeError(w, 401, "control login required")
			return
		}
		if !i.canManageUsers() {
			writeError(w, 403, "super admin access required")
			return
		}
		ctx := context.WithValue(r.Context(), webIdentityContextKey, i)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleControlUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT u.id::text,u.username,u.full_name,u.role,u.active,u.all_companies,u.last_login_at,u.created_at,(SELECT max(ws.last_seen_at) FROM web_sessions ws WHERE ws.user_id=u.id AND ws.expires_at>now()) FROM web_users u ORDER BY lower(u.username)`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []WebUserResponse{}
	for rows.Next() {
		var u WebUserResponse
		var last, lastSeen *time.Time
		var created time.Time
		if err := rows.Scan(&u.ID, &u.Username, &u.FullName, &u.Role, &u.Active, &u.AllCompanies, &last, &created, &lastSeen); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		u.LastLoginAt = timeString(last)
		u.LastSeenAt = timeString(lastSeen)
		u.Online = lastSeen != nil && time.Since(*lastSeen) < 2*time.Minute
		u.CreatedAt = created.UTC().Format(time.RFC3339)
		u.CompanyIDs = []string{}
		cr, err := s.db.Query(r.Context(), `SELECT company_id::text FROM web_user_companies WHERE user_id=$1::uuid ORDER BY company_id`, u.ID)
		if err == nil {
			for cr.Next() {
				var id string
				if cr.Scan(&id) == nil {
					u.CompanyIDs = append(u.CompanyIDs, id)
				}
			}
			cr.Close()
		}
		out = append(out, u)
	}
	writeJSON(w, 200, map[string]any{"users": out})
}

func validWebRole(v string) bool {
	switch v {
	case "super_admin", "manager", "operator", "viewer":
		return true
	}
	return false
}

func (s *Server) handleControlUserCreate(w http.ResponseWriter, r *http.Request) {
	actor := identityFromContext(r.Context())
	var in struct {
		Username     string   `json:"username"`
		FullName     string   `json:"full_name"`
		Password     string   `json:"password"`
		Role         string   `json:"role"`
		AllCompanies bool     `json:"all_companies"`
		CompanyIDs   []string `json:"company_ids"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	in.Username = strings.TrimSpace(in.Username)
	in.FullName = strings.TrimSpace(in.FullName)
	in.Role = strings.TrimSpace(in.Role)
	if len(in.Username) < 4 || len(in.Username) > 40 {
		writeError(w, 400, "username must be 4-40 characters")
		return
	}
	if len(in.Password) < 8 {
		writeError(w, 400, "password must be at least 8 characters")
		return
	}
	if !validWebRole(in.Role) {
		writeError(w, 400, "invalid role")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var id string
	err = tx.QueryRow(r.Context(), `INSERT INTO web_users(username,full_name,password_hash,role,active,all_companies) VALUES($1,$2,$3,$4,true,$5) RETURNING id::text`, in.Username, in.FullName, string(hash), in.Role, in.AllCompanies).Scan(&id)
	if err != nil {
		writeError(w, 409, "username already exists")
		return
	}
	if !in.AllCompanies {
		for _, cid := range uniqueStrings(in.CompanyIDs) {
			if _, err = tx.Exec(r.Context(), `INSERT INTO web_user_companies(user_id,company_id) VALUES($1::uuid,$2::uuid) ON CONFLICT DO NOTHING`, id, cid); err != nil {
				writeError(w, 400, "invalid company assignment")
				return
			}
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	s.logWebActivity(r.Context(), actor, "user.created", "", "", map[string]any{"user_id": id, "username": in.Username, "role": in.Role})
	writeJSON(w, 201, map[string]any{"ok": true, "id": id})
}

func (s *Server) handleControlUserUpdate(w http.ResponseWriter, r *http.Request) {
	actor := identityFromContext(r.Context())
	id := strings.TrimSpace(r.PathValue("id"))
	var in struct {
		FullName     *string   `json:"full_name"`
		Password     *string   `json:"password"`
		Role         *string   `json:"role"`
		Active       *bool     `json:"active"`
		AllCompanies *bool     `json:"all_companies"`
		CompanyIDs   *[]string `json:"company_ids"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if in.FullName != nil {
		_, err = tx.Exec(r.Context(), `UPDATE web_users SET full_name=$2 WHERE id=$1::uuid`, id, strings.TrimSpace(*in.FullName))
		if err != nil {
			writeError(w, 400, "invalid user")
			return
		}
	}
	if in.Role != nil {
		role := strings.TrimSpace(*in.Role)
		if !validWebRole(role) {
			writeError(w, 400, "invalid role")
			return
		}
		_, err = tx.Exec(r.Context(), `UPDATE web_users SET role=$2 WHERE id=$1::uuid`, id, role)
		if err != nil {
			writeError(w, 400, "invalid user")
			return
		}
	}
	if in.Active != nil {
		_, err = tx.Exec(r.Context(), `UPDATE web_users SET active=$2 WHERE id=$1::uuid`, id, *in.Active)
		if err != nil {
			writeError(w, 400, "invalid user")
			return
		}
	}
	if in.AllCompanies != nil {
		_, err = tx.Exec(r.Context(), `UPDATE web_users SET all_companies=$2 WHERE id=$1::uuid`, id, *in.AllCompanies)
		if err != nil {
			writeError(w, 400, "invalid user")
			return
		}
	}
	if in.Password != nil {
		if len(*in.Password) < 8 {
			writeError(w, 400, "password must be at least 8 characters")
			return
		}
		h, e := bcrypt.GenerateFromPassword([]byte(*in.Password), bcrypt.DefaultCost)
		if e != nil {
			writeError(w, 500, e.Error())
			return
		}
		_, err = tx.Exec(r.Context(), `UPDATE web_users SET password_hash=$2 WHERE id=$1::uuid`, id, string(h))
		if err != nil {
			writeError(w, 400, "invalid user")
			return
		}
		_, _ = tx.Exec(r.Context(), `DELETE FROM web_sessions WHERE user_id=$1::uuid`, id)
	}
	if in.CompanyIDs != nil {
		_, err = tx.Exec(r.Context(), `DELETE FROM web_user_companies WHERE user_id=$1::uuid`, id)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		for _, cid := range uniqueStrings(*in.CompanyIDs) {
			if _, err = tx.Exec(r.Context(), `INSERT INTO web_user_companies(user_id,company_id) VALUES($1::uuid,$2::uuid) ON CONFLICT DO NOTHING`, id, cid); err != nil {
				writeError(w, 400, "invalid company assignment")
				return
			}
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	s.logWebActivity(r.Context(), actor, "user.updated", "", "", map[string]any{"user_id": id})
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleControlCompanies(w http.ResponseWriter, r *http.Request) {
	s.handleCompanies(w, r)
}

func (s *Server) handleControlActivity(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT a.id,COALESCE(u.username,''),a.action,COALESCE(c.name,''),COALESCE(a.driver_id,''),a.detail,a.created_at FROM web_activity a LEFT JOIN web_users u ON u.id=a.user_id LEFT JOIN companies c ON c.id=a.company_id ORDER BY a.created_at DESC LIMIT 300`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var username, action, company, driver string
		var detail []byte
		var t time.Time
		if rows.Scan(&id, &username, &action, &company, &driver, &detail, &t) == nil {
			var d any
			_ = json.Unmarshal(detail, &d)
			out = append(out, map[string]any{"id": id, "username": username, "action": action, "company": company, "driver_id": driver, "detail": d, "created_at": t.UTC().Format(time.RFC3339)})
		}
	}
	writeJSON(w, 200, map[string]any{"activity": out})
}

func uniqueStrings(in []string) []string {
	m := map[string]bool{}
	out := []string{}
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" && !m[v] {
			m[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Server) logWebActivity(ctx context.Context, i *WebIdentity, action, companyID, driverID string, detail any) {
	if i == nil || i.Legacy {
		return
	}
	b, _ := json.Marshal(detail)
	_, _ = s.db.Exec(ctx, `INSERT INTO web_activity(user_id,action,company_id,driver_id,detail) VALUES($1::uuid,$2,NULLIF($3,'')::uuid,NULLIF($4,''),$5::jsonb)`, i.ID, action, companyID, driverID, string(b))
}

func (s *Server) driverCompanyID(ctx context.Context, driverID string) (string, error) {
	var id string
	err := s.db.QueryRow(ctx, `SELECT COALESCE(company_id::text,'') FROM drivers WHERE id=$1 AND active=true`, driverID).Scan(&id)
	return id, err
}
func (s *Server) ensureDriverAllowed(ctx context.Context, i *WebIdentity, driverID string) bool {
	if i == nil || i.Legacy || i.AllCompanies {
		return true
	}
	cid, err := s.driverCompanyID(ctx, driverID)
	return err == nil && i.companyAllowed(cid)
}
func (s *Server) ensureCompanyAllowed(i *WebIdentity, companyID string) bool {
	return i == nil || i.Legacy || i.companyAllowed(companyID)
}

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

type VehicleResponse struct {
	ID          string `json:"id"`
	CompanyID   string `json:"company_id"`
	CompanyName string `json:"company_name"`
	USDOT       string `json:"usdot"`
	UnitNumber  string `json:"unit_number"`
	VIN         string `json:"vin"`
	VehicleType string `json:"vehicle_type"`
	Active      bool   `json:"active"`
	DriverID    string `json:"driver_id,omitempty"`
	DriverName  string `json:"driver_name,omitempty"`
}

func (s *Server) handleVehicles(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
SELECT v.id::text,v.company_id::text,c.name,c.usdot,v.unit_number,v.vin,v.vehicle_type,v.active,
       COALESCE(d.id,''),COALESCE(d.full_name,'')
FROM vehicles v
JOIN companies c ON c.id=v.company_id
LEFT JOIN drivers d ON d.vehicle_id=v.id AND d.active=true
WHERE v.active=true
ORDER BY lower(c.name),lower(v.unit_number)`)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	out := []VehicleResponse{}
	identity := identityFromContext(r.Context())
	for rows.Next() {
		var v VehicleResponse
		if err := rows.Scan(&v.ID, &v.CompanyID, &v.CompanyName, &v.USDOT, &v.UnitNumber, &v.VIN, &v.VehicleType, &v.Active, &v.DriverID, &v.DriverName); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		if identity != nil && !identity.companyAllowed(v.CompanyID) {
			continue
		}
		out = append(out, v)
	}
	writeJSON(w, 200, map[string]any{"vehicles": out})
}

func normalizeVehicleType(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	switch v {
	case "BOX", "BOX TRUCK", "BOX_TRUCK":
		return "BOX_TRUCK"
	case "TRACTOR", "SLEEPER", "SLEEPER TRACTOR", "SLEEPER_TRACTOR":
		return "TRACTOR"
	default:
		if v == "" {
			return "TRACTOR"
		}
		return v
	}
}

func (s *Server) handleAdminVehicle(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID          string `json:"id"`
		CompanyID   string `json:"company_id"`
		UnitNumber  string `json:"unit_number"`
		VIN         string `json:"vin"`
		VehicleType string `json:"vehicle_type"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&in); err != nil {
		writeError(w, 400, "invalid JSON")
		return
	}
	in.ID = strings.TrimSpace(in.ID)
	in.CompanyID = strings.TrimSpace(in.CompanyID)
	in.UnitNumber = strings.TrimSpace(in.UnitNumber)
	in.VIN = strings.ToUpper(strings.TrimSpace(in.VIN))
	in.VehicleType = normalizeVehicleType(in.VehicleType)
	if in.CompanyID == "" || in.UnitNumber == "" {
		writeError(w, 400, "company and vehicle unit are required")
		return
	}
	if len(in.UnitNumber) > 32 || len(in.VIN) > 40 || len(in.VehicleType) > 40 {
		writeError(w, 400, "vehicle data is too long")
		return
	}
	i := identityFromContext(r.Context())
	if i != nil && !i.companyAllowed(in.CompanyID) {
		writeError(w, 403, "company is outside your access")
		return
	}
	var exists bool
	if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM companies WHERE id=$1::uuid)`, in.CompanyID).Scan(&exists); err != nil || !exists {
		writeError(w, 400, "selected company does not exist")
		return
	}
	creating := in.ID == ""
	var id string
	if creating {
		err := s.db.QueryRow(r.Context(), `INSERT INTO vehicles(company_id,unit_number,vin,vehicle_type,active) VALUES($1::uuid,$2,$3,$4,true) RETURNING id::text`, in.CompanyID, in.UnitNumber, in.VIN, in.VehicleType).Scan(&id)
		if err != nil {
			writeError(w, 409, "vehicle unit already exists in this company")
			return
		}
	} else {
		id = in.ID
		tx, err := s.db.Begin(r.Context())
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		defer tx.Rollback(r.Context())
		var oldCompany string
		if err := tx.QueryRow(r.Context(), `SELECT company_id::text FROM vehicles WHERE id=$1::uuid AND active=true FOR UPDATE`, id).Scan(&oldCompany); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeError(w, 404, "vehicle not found")
			} else {
				writeError(w, 500, err.Error())
			}
			return
		}
		if i != nil && !i.companyAllowed(oldCompany) {
			writeError(w, 403, "vehicle is outside your access")
			return
		}
		var assigned int
		_ = tx.QueryRow(r.Context(), `SELECT count(*)::int FROM drivers WHERE vehicle_id=$1::uuid AND active=true`, id).Scan(&assigned)
		if assigned > 0 && oldCompany != in.CompanyID {
			writeError(w, 409, "unassign the vehicle before moving it to another company")
			return
		}
		if _, err = tx.Exec(r.Context(), `UPDATE vehicles SET company_id=$2::uuid,unit_number=$3,vin=$4,vehicle_type=$5,active=true,updated_at=now() WHERE id=$1::uuid`, id, in.CompanyID, in.UnitNumber, in.VIN, in.VehicleType); err != nil {
			writeError(w, 409, "vehicle unit already exists in this company")
			return
		}
		if _, err = tx.Exec(r.Context(), `UPDATE drivers SET truck_unit=$2,vehicle_type=$3,updated_at=now() WHERE vehicle_id=$1::uuid AND active=true`, id, in.UnitNumber, in.VehicleType); err != nil {
			writeError(w, 500, err.Error())
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			writeError(w, 500, err.Error())
			return
		}
	}
	var v VehicleResponse
	err := s.db.QueryRow(r.Context(), `
SELECT v.id::text,v.company_id::text,c.name,c.usdot,v.unit_number,v.vin,v.vehicle_type,v.active,COALESCE(d.id,''),COALESCE(d.full_name,'')
FROM vehicles v JOIN companies c ON c.id=v.company_id LEFT JOIN drivers d ON d.vehicle_id=v.id AND d.active=true WHERE v.id=$1::uuid`, id).Scan(&v.ID, &v.CompanyID, &v.CompanyName, &v.USDOT, &v.UnitNumber, &v.VIN, &v.VehicleType, &v.Active, &v.DriverID, &v.DriverName)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	status := http.StatusOK
	if creating {
		status = http.StatusCreated
	}
	writeJSON(w, status, v)
}

func (s *Server) handleAdminVehicleDelete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, 400, "vehicle id is required")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var companyID string
	if err := tx.QueryRow(r.Context(), `SELECT company_id::text FROM vehicles WHERE id=$1::uuid AND active=true FOR UPDATE`, id).Scan(&companyID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, 404, "vehicle not found")
		} else {
			writeError(w, 500, err.Error())
		}
		return
	}
	if i := identityFromContext(r.Context()); i != nil && !i.companyAllowed(companyID) {
		writeError(w, 403, "vehicle is outside your access")
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE drivers SET vehicle_id=NULL,truck_unit='',updated_at=now() WHERE vehicle_id=$1::uuid AND active=true`, id); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE vehicles SET active=false,updated_at=now() WHERE id=$1::uuid`, id); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

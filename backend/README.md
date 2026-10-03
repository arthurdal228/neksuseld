# NEKSUS ELD backend — Render ready

This folder is the online API for the NEKSUS frontend. It uses Go + Render PostgreSQL and exposes the REST/WebSocket routes already expected by the current `index.html`.

## Render service settings

Create or update a **Render Web Service** connected to the same GitHub repository.

- Root Directory: `backend`
- Build Command: `go mod tidy && go build -o neksus-api .`
- Start Command: `./neksus-api`
- Health Check Path: `/health`
- Region: use the same region as your PostgreSQL database

## Environment variables

Add these in Render → Web Service → Environment:

```text
DATABASE_URL=<paste the Internal Database URL from your Render PostgreSQL>
ADMIN_TOKEN=<make a long random secret>
ALLOWED_ORIGINS=*
```

Optional:

```text
API_TOKEN=<leave empty for now, or set a read token later>
```

Do **not** put `DATABASE_URL` or `ADMIN_TOKEN` into GitHub or `index.html`.

When the service starts for the first time, it automatically creates the required PostgreSQL tables and realtime database triggers.

## REST endpoints

```text
GET  /health
POST /v1/driver/login    # username + password; returns driver session token
GET  /v1/driver/me       # driver session token required
POST /v1/driver/logout   # driver session token required
GET  /v1/admin/session   # verifies ADMIN_TOKEN
GET  /v1/drivers
GET  /v1/alerts?status=open
GET  /v1/fleet/live
GET  /v1/hos/current
GET  /v1/drivers/:id/hos
GET  /v1/drivers/:id/logs/:date
GET  /v1/ws
```

The current frontend uses `ADMIN_TOKEN` as the ELD login credential. The login screen verifies it with `GET /v1/admin/session`, keeps it only in browser `sessionStorage`, and sends it with protected REST/WebSocket data access. Closing the tab or choosing Sign out removes the session token.

Admin/testing endpoints require:

```text
Authorization: Bearer <ADMIN_TOKEN>
```

If `API_TOKEN` is not configured, normal `/v1/*` read endpoints also require `ADMIN_TOKEN`. If you later configure `API_TOKEN`, normal read/WebSocket access uses that token while admin writes continue to require `ADMIN_TOKEN`. `/health` remains public for Render health checks.

```text
POST   /v1/admin/drivers
DELETE /v1/admin/drivers/:id
PUT    /v1/admin/drivers/:id/live
PUT  /v1/admin/drivers/:id/hos
POST /v1/admin/drivers/:id/segments
POST /v1/admin/drivers/:id/events
POST /v1/admin/alerts
```

## Add a driver

The current NEKSUS admin frontend creates drivers through `POST /v1/admin/drivers`. The form contains only:

- Username
- First name
- Last name
- Email
- Phone number
- Password
- Driver license issue state
- Driver license number
- Vehicle

The backend generates the permanent internal driver ID automatically. Passwords are stored as bcrypt hashes; plaintext passwords are never stored in PostgreSQL.

Example admin request:

```bash
curl -X POST "https://YOUR-API.onrender.com/v1/admin/drivers" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username":"driver01",
    "first_name":"John",
    "last_name":"Smith",
    "email":"driver@example.com",
    "phone":"+1 555 0100",
    "password":"example-password",
    "license_issue_state":"IL",
    "license_number":"D1234567",
    "vehicle":"3101"
  }'
```

## Android driver login foundation

The backend is ready for a future Android driver app. The app should submit the driver's username and password to:

```text
POST /v1/driver/login
```

Request body:

```json
{
  "username": "driver01",
  "password": "example-password"
}
```

A successful response returns an opaque driver session token and the driver's profile. The Android app should keep the token in Android secure storage and send it as:

```text
Authorization: Bearer <DRIVER_SESSION_TOKEN>
```

Use `GET /v1/driver/me` to verify the session and `POST /v1/driver/logout` to revoke it. Driver sessions expire after 30 days.

## Set the driver's HOS snapshot

Values are in **seconds**.

```bash
curl -X PUT "https://YOUR-API.onrender.com/v1/admin/drivers/DRV001/hos" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "break_remaining_seconds":28800,
    "drive_remaining_seconds":39600,
    "shift_remaining_seconds":50400,
    "cycle_remaining_seconds":252000
  }'
```

Those numbers are only an example API format. Enter values from your actual source; the backend does not invent HOS values.

## Add a duty-status segment

```bash
curl -X POST "https://YOUR-API.onrender.com/v1/admin/drivers/DRV001/segments" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "date":"2026-10-02",
    "start_minute":480,
    "end_minute":720,
    "status":"DR",
    "note":"Imported ELD status",
    "origin":"ELD"
  }'
```

## Add an intermediate ELD event

```bash
curl -X POST "https://YOUR-API.onrender.com/v1/admin/drivers/DRV001/events" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "date":"2026-10-02",
    "minute":540,
    "event_type":"intermediate",
    "duty_status":"DR",
    "location_text":"Example location",
    "odometer_miles":100000,
    "engine_hours":4000.5,
    "origin":"ELD"
  }'
```

## Connect the NEKSUS frontend

After Render reports the service as Live, open:

**NEKSUS → Settings → Backend connection**

Use:

```text
REST API:
https://YOUR-API.onrender.com

WebSocket:
wss://YOUR-API.onrender.com/v1/ws
```

If you later set `API_TOKEN`, enter that token in the frontend's optional Bearer token field. Do not enter `ADMIN_TOKEN` there.

## Realtime behavior

PostgreSQL changes to drivers, live state, HOS, logs, events, and alerts trigger `LISTEN/NOTIFY`. The backend converts them to WebSocket updates for NEKSUS.

This starter is an operational prototype backend. It does not independently certify FMCSA compliance or calculate authoritative HOS from raw ELD telemetry.

## Android driver app endpoints (v5)

The matching NEKSUS Android Driver MVP uses driver-session authentication, not the admin token:

- `POST /v1/driver/login` - username/password login
- `GET /v1/driver/me` - current driver profile
- `GET /v1/driver/hos` - current driver's HOS projection
- `GET /v1/driver/logs/{date}` - current driver's daily segments/events
- `POST /v1/driver/status` - driver OFF/SB/DR/ON change; updates live state and records a DriverApp segment/event
- `POST /v1/driver/heartbeat` - marks the signed-in driver/device connected
- `POST /v1/driver/logout` - expires session and marks connection offline

Deploy backend v5 before testing the Android app.

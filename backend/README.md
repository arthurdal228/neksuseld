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
GET  /v1/companies
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
POST   /v1/admin/companies
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
- Existing company (selected by company ID)
- Vehicle

The backend generates the permanent internal driver ID automatically. A company must already exist and must have a USDOT number; the driver endpoint does not create companies. New drivers are seeded with the last seven log dates in OFF duty, with today left open until the driver changes status. Passwords are stored as bcrypt hashes; plaintext passwords are never stored in PostgreSQL.

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
    "company_id":"COMPANY_UUID_FROM_/v1/companies",
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

## v6: log continuity + driver alarms

v6 makes the current driver log use the driver's home-terminal date/timezone and repairs a missing current-day segment for older online drivers when their log is first requested.

Admin alarm endpoint:

```text
POST /v1/admin/drivers/{id}/alarms
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json

{
  "title": "Safety",
  "message": "Please call Safety when safely parked.",
  "ringtone": "urgent"
}
```

Supported ringtones: `alert`, `urgent`, `chime`, `bell`, `pulse`.

Driver-app delivery endpoints:

- `GET /v1/driver/alarms`
- `POST /v1/driver/alarms/{id}/delivered`

The Android v0.2 MVP uses a signed-in foreground connection service for near-real-time alarms. A production release should eventually use a push service such as FCM for more battery-efficient background delivery.

## Companies

Create carriers separately through `POST /v1/admin/companies` with `name` and `usdot`. `GET /v1/companies` returns the online company roster. The admin driver form selects one of these existing companies; it never creates a carrier from free text.

## Driver alarms

`POST /v1/admin/drivers/:id/alarms` accepts `alert`, `urgent`, `chime`, `bell`, or `pulse`. Signed-in Android drivers receive pending alarms through the driver alarm service.

## v8: server-backed multi-day log editing

v8 replaces the browser-only administrative edit layer with PostgreSQL-backed range editing.

New admin endpoints:

```text
POST /v1/admin/drivers/{id}/logs/{date}/range-edit
POST /v1/admin/drivers/{id}/logs/{date}/undo
```

`range-edit` accepts a continuous time range plus OFF/SB/DR/ON/PC/YM, annotation, location, odometer, engine hours, trailer, shipping document, operator, and edit reason. The backend transactionally rebuilds the affected duty-segment projection so the day remains continuous, appends an immutable administrative event to `eld_events`, and stores before/after snapshots in `log_edit_batches`.

Example:

```json
{
  "start_minute": 480,
  "end_minute": 615,
  "status": "DR",
  "note": "Corrected driving period",
  "reason": "Dispatch reviewed source records",
  "operator": "Safety Admin",
  "location_text": "Chicago, IL",
  "odometer_miles": 134220.5,
  "engine_hours": 8510.4,
  "trailer_number": "TR-12",
  "shipping_document": "BOL-4412"
}
```

The log GET response now also includes `edit_history`, and segment objects include the editing metadata fields. `undo` restores the previous server snapshot and marks the audit batch undone rather than deleting the audit record.

After edit/undo, the backend recalculates its operational HOS projection from recent duty segments and PostgreSQL notifications drive the existing `driver.log.updated` and `driver.hos.updated` WebSocket refreshes.

This HOS recalculation is an operational projection for this starter, not an independent regulatory certification engine.

## v10 Android drive analyzer / normalizer

The matching v10 Android app adds these driver-session endpoints:

```text
POST /v1/driver/telemetry
PUT  /v1/driver/odometer
GET  /v1/driver/drive-state
POST /v1/driver/drives/{id}/normalize
POST /v1/driver/logs/{date}/swap
```

Behavior:

- `/v1/driver/status` rejects manual `DR`.
- Manual `ON` requires a note.
- Manual OFF/SB/ON changes require valid device coordinates.
- GPS telemetry is stored in `drive_points` while an automatic drive is active.
- The analyzer starts DR after 3 sustained samples at 5+ mph and closes it after 5 minutes at <=2 mph, using the first stationary sample as the real endpoint.
- The normalizer compares GPS-track miles with the user-entered odometer delta. It creates hourly intermediate events only when the drive is normalized, and those intermediates use actual recorded GPS points.
- A distance mismatch is stored as `review`; it is not silently normalized.
- Swap transfers exact seconds across the boundary between two adjacent duty segments and preserves the combined duration.

This analyzer is prototype operational logic. It is not a substitute for validated ECM/ELD vehicle-motion data in a certified production ELD.

## v13 continuous episode metadata
`GET /v1/driver/logs/{date}` and the admin log endpoint now enrich daily `duty_segments` with `episode_start_date`, `episode_start_second`, `episode_end_date`, `episode_end_second`, `episode_duration_seconds`, and `episode_open`. Adjacent same-status fragments that touch across midnight are treated as one display episode while the stored per-day fragments remain intact for daily logs and edits.

New-driver seeding now starts exactly 168 hours before creation and no longer adds the user-visible "Initial seven-day OFF duty history" note. The migration clears that legacy note from existing seeded rows.

## v14 NEKSUS Control and web user authentication

Normal NEKSUS web users now authenticate with username/password through `/v1/web/login`; they no longer need the Render `ADMIN_TOKEN`.

Render bootstrap variables:

```text
CONTROL_ADMIN_USERNAME=neksusadmin
CONTROL_ADMIN_PASSWORD=use-a-strong-password
CONTROL_ADMIN_NAME=NEKSUS Super Admin
```

On startup, the backend creates/refreshes that account as `super_admin` with access to all companies. Use the separate `control/` frontend to create operator/manager/viewer accounts and assign companies.

Company scoping is enforced in the backend for company/driver lists, bulk HOS, live fleet data, alerts, driver-specific log/HOS routes, and driver-specific admin write routes. The original `ADMIN_TOKEN` remains available only as a maintenance/backward-compatibility path during migration.

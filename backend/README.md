# NEKSUS ELD backend — Render ready

This folder is the online API for the NEKSUS frontend. It uses Go + Render PostgreSQL and exposes the REST/WebSocket routes already expected by the current `index.html`.

## Render service settings

Create or update a **Render Web Service** connected to the same GitHub repository.

- Root Directory: `backend`
- Build Command: `go mod download && go build -o neksus-api .`
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
GET  /v1/drivers
GET  /v1/alerts?status=open
GET  /v1/fleet/live
GET  /v1/hos/current
GET  /v1/drivers/:id/hos
GET  /v1/drivers/:id/logs/:date
GET  /v1/ws
```

Admin/testing endpoints require:

```text
Authorization: Bearer <ADMIN_TOKEN>
```

```text
POST /v1/admin/drivers
PUT  /v1/admin/drivers/:id/live
PUT  /v1/admin/drivers/:id/hos
POST /v1/admin/drivers/:id/segments
POST /v1/admin/drivers/:id/events
POST /v1/admin/alerts
```

## Add your first real driver

Replace the URL and token below with your own Render service and `ADMIN_TOKEN`.

```bash
curl -X POST "https://YOUR-API.onrender.com/v1/admin/drivers" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id":"DRV001",
    "name":"REAL DRIVER NAME",
    "carrier":"YOUR COMPANY LLC",
    "truck":"3101",
    "timezone":"America/Chicago",
    "status":"OFF",
    "connected":true,
    "location_text":"Chicago, IL"
  }'
```

The frontend will then receive this driver from `GET /v1/drivers`.

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

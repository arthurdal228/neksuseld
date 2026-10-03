# NEKSUS ELD Driver Android v0.3.0

Driver-side companion for the NEKSUS admin site. Backend is preconfigured to:

`https://neksuseldv1.onrender.com`

Drivers are created in the NEKSUS admin site and sign in with the same username/password.

## v10 mobile behavior

- Location permission is requested once on first launch before login. NEKSUS records that it already asked and will not repeatedly show the Android permission dialog. If access is denied or later revoked, the app shows a persistent red **GPS OFF** indicator and an **Open Settings** action.
- Manual duty controls contain OFF, SB, and ON only. The driver cannot manually select DR.
- ON requires a note. Presets: PTI, Pickup, Delivery, DOT Inspection, Drop, Hook, Fuel, Loading, Unloading, Maintenance, and Other.
- Every manual status change requires a fresh real device GPS fix and sends latitude, longitude, accuracy, timestamp/location text, and the current driver-entered odometer.
- Odometer is entered by the driver from Dashboard/Vehicle.
- A foreground GPS analyzer monitors signed-in drivers with location permission.
- Automatic DR starts after 3 sustained GPS samples at 5+ mph.
- Automatic DR ends after the vehicle remains at or below 2 mph for 5 minutes. The recorded endpoint is the first stationary GPS point.
- GPS samples are stored as a drive track. When the drive ends, the normalizer compares GPS route distance with the user-entered start/end odometer values.
- If the distances reasonably agree, the drive becomes normalized and hourly intermediate events are created from actual recorded GPS points. If they do not agree, the drive is marked **Needs review** instead of inventing locations.
- Logs support exact-duration display and a two-status **Swap / Transfer Event** workflow using HH:MM:SS.
- Bottom navigation: Dashboard, Logs, Vehicle, Alerts, Profile. Sync/backend information remains inside Profile settings rather than as a separate tab.
- Dispatch alarms and the five existing ringtones remain supported.

## Required backend routes

Driver-session authentication is used for all private routes:

- `POST /v1/driver/login`
- `GET /v1/driver/me`
- `GET /v1/driver/hos`
- `GET /v1/driver/logs/{date}`
- `POST /v1/driver/status`
- `POST /v1/driver/telemetry`
- `PUT /v1/driver/odometer`
- `GET /v1/driver/drive-state`
- `POST /v1/driver/drives/{id}/normalize`
- `POST /v1/driver/logs/{date}/swap`
- `POST /v1/driver/heartbeat`
- `GET /v1/driver/alarms`
- `POST /v1/driver/logout`

## Build APK

Push the repository to GitHub, open **Actions > NEKSUS Android APK**, run the workflow, and download the `NEKSUS-Driver-debug` artifact.

## Important scope

This is an integration prototype, not a certified ELD. Phone GPS is used for movement analysis; it is not being represented as ECM/vehicle odometer data. The odometer in this build is driver-entered. Vehicle/ECM integration, FMCSA output-file transfer, diagnostics/malfunctions, offline/retry hardening, production signing, and compliance validation remain separate production work.

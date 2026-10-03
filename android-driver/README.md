# NEKSUS ELD Driver Android (v0.3 MVP)

This Android app is the driver-side companion for the NEKSUS admin site.

Backend base URL is preconfigured to:

`https://neksuseldv1.onrender.com`

Driver accounts are created from the NEKSUS admin site's **Add Driver** form. Drivers sign in with that username/password.

## Included flows

- Driver username/password login
- Driver profile and assigned vehicle
- Current OFF / SB / DR / ON status
- HOS clock display from the NEKSUS backend
- Driver status changes saved to PostgreSQL
- Daily log graph and event list using the driver's home-terminal date/timezone
- Automatic current-log continuity repair when an older online driver has no duty segment yet
- Background driver-alarm connection while the driver is signed in
- Five alarm ringtones: Alert, Urgent, Chime, Bell, Pulse
- Sync/backend details moved from Dashboard to **Profile > Settings & Sync**
- Driver logout
- NEKSUS triangular site mark used for launcher/app branding

## Backend version required

Deploy the matching backend v7 first. It includes the mobile alarm endpoints:

- `GET /v1/driver/alarms`
- `POST /v1/driver/alarms/{id}/delivered`
- `POST /v1/admin/drivers/{id}/alarms`

The Android app runs a foreground connection service while the driver is signed in and checks for pending alarms approximately every 8 seconds. Android notification permission must be allowed for audible alerts.

## GitHub build

The repository includes `.github/workflows/android-apk.yml`, using the GitHub runner's preinstalled Android SDK. Push to GitHub, open **Actions > NEKSUS Android APK**, run the workflow, then download `NEKSUS-Driver-debug`.

## Scope

This remains an integration MVP, not a certified ELD application. Bluetooth/ECM pairing, automatic engine-event ingestion, validated HOS calculations, FMCSA output-file transfer, diagnostics/malfunctions, production app signing, and production-grade push delivery still require dedicated implementation and testing.

## v7 integration notes

- No separate Sync tab is shown in the bottom navigation. Sync status/configuration lives under **Profile > Settings & Sync**.
- New drivers receive a seven-day OFF-duty history from the backend.
- Driver log rendering treats the current open segment as ending at the current home-terminal time instead of midnight.
- Company and USDOT are returned with the driver profile.

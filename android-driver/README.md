# NEKSUS ELD Driver Android (MVP)

This Android app is the driver-side companion for the NEKSUS admin site.

Backend base URL is preconfigured to:

`https://neksuseldv1.onrender.com`

Driver accounts are created from the NEKSUS admin site's **Add Driver** form. Drivers sign in with that username/password.

## Included MVP flows

- Driver username/password login
- Driver profile and assigned vehicle
- Current OFF / SB / DR / ON status
- HOS clock display from the NEKSUS backend
- Driver status changes saved to PostgreSQL
- Today/date log graph and event list
- 15-second REST refresh and 60-second heartbeat
- Driver logout
- Exact NEKSUS triangular site mark used for launcher/app branding

## Backend version required

Deploy the matching backend v5 first. It adds:

- `GET /v1/driver/hos`
- `GET /v1/driver/logs/{date}`
- `POST /v1/driver/status`
- `POST /v1/driver/heartbeat`

## Build locally

Open `android-driver/` in Android Studio and build the app.

## Build in GitHub automatically

The full NEKSUS v5 repository includes `.github/workflows/android-apk.yml`. Push the repository to GitHub, open **Actions > NEKSUS Android APK**, then download the `NEKSUS-Driver-debug` artifact after the workflow finishes.

## Scope

This is an integration MVP, not a certified ELD application. Bluetooth/ECM pairing, automatic engine-event ingestion, validated HOS calculations, FMCSA output-file transfer, diagnostics/malfunctions, and production signing still require dedicated implementation and testing.

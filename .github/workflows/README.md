# NEKSUS ELD v17 + NEKSUS Control

This release updates the web workspace, Go backend and Android driver source.
Android version: 0.6.0, versionCode 6. Backend health version: 17.0.0.

## Changes

- Main dashboard with Companies, Drivers and Vehicles tabs; company dashboard with only its drivers and vehicles.
- Searchable company selector keeps the selected company across navigation and reloads, scoped to the signed-in user.
- Duty graph colors are independent of custom accents. The trace is light on dark themes and dark on the light theme.
- Popups and draft inputs survive pointer movement, map focus, scrolling and background updates. Close/Cancel/Escape remain available.
- Removed the lower-left sign-out button; sign-out remains in Settings.
- Swap accepts non-adjacent statuses on different dates in the 30-day log window. It exchanges their durations by default and preserves the total timeline. Intervening statuses keep their duration but move in time. The dialog previews the resulting durations. Audit snapshots and Undo cover every affected date.
- Missing daily logs carry the last known status through midnight. Today's open status extends to the current home-terminal time.
- Android distinguishes a measured zero speed from missing speed. Stale, duplicate, low-accuracy and provider-switch samples cannot trigger driving by themselves. Sustained valid movement above 3 mph is confirmed before starting DR.
- Android stores telemetry and manual status changes in a driver-scoped SQLite outbox. It restores cached logs offline, keeps the last status until changed, then uploads records in order with duplicate protection.
- Stop delay: 30 seconds, 1, 2, 3 or 5 minutes, or Manual only. Existing ON/OFF fallback preference remains. Unknown/stale GPS does not count as confirmed stationary time.
- Removed mobile alarm previews. Incoming Phone Ring, Rooster, Siren and Alarm notifications remain.

## Deploy this release

1. Upload the release folders into the existing GitHub repository: backend/, frontend/, android-driver/, control/ and .github/.
2. Deploy Render first: Root Directory `backend`; Build `go mod tidy && go build -o neksus-api .`; Start `./neksus-api`.
3. Verify `/health` contains `"version":"17.0.0"` and `"database":"up"`.
4. Deploy the ELD Vercel project with Root Directory `frontend`. Include BOTH index.html and v17.js.
5. Rebuild the Android APK using the existing GitHub Actions workflow. Install version 0.6.0 as an update using the same application/signing identity to preserve local records.
6. NEKSUS Control is included unchanged. It does not require a separate update for these fixes.

Keep existing Render environment variables. No new variables are required.
The SQL migration runs automatically at backend startup and adds receipt/cursor tables and a swap-operation identifier. It does not reset your companies, drivers or logs.

This is a full-stack update: replacing only frontend/index.html is insufficient.
Do not use the older v13 file-deletion instructions; see DELETE_OLD_FILES_FIRST.txt in this release.

## Validation and remaining checks

- Go unit tests and full dependency build completed locally.
- Database integration test covers migrations, missing-day carry, stationary telemetry, sustained driving, configurable stop delay, duplicate replay, offline status changes, cross-date swaps and multi-day Undo. It runs against disposable PGlite/PostgreSQL-compatible storage, not your production database.
- Web and mobile JavaScript parsing and Android XML checks completed.
- Android Java compiles against API 35 using temporary generated resource identifiers for the compile check; the APK itself must still be built in GitHub Actions.
- Browser checks use simulated API responses and a simulated Android bridge. See VALIDATION.txt for the final browser results.
- Physical-device GPS/background behavior and production integration still need verification after deployment.

The Android location service needs location permission and must remain allowed to run. Loss of internet keeps records queued; force-stopping the app or turning the phone off prevents collection of new location samples. The last known duty status continues until another recorded change.
Conflicting offline records are retained and flagged for review instead of silently overwriting a newer administrative edit.

## Project layout

- frontend/ — operator website
- control/ — account creation, roles and company assignments
- backend/ — Go API and PostgreSQL migrations
- android-driver/ — native services and driver WebView UI
- .github/workflows/android-apk.yml — APK build workflow

Normal website users sign in using their Control-created username and password. Backend company authorization remains enforced.
Historical release notes describe earlier versions only.

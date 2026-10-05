# NEKSUS Android Driver

Driver-facing Android client for the NEKSUS backend.

Driver access includes login, OFF/SB/ON status changes, required ON Duty notes, GPS-backed status locations, HOS, logs, vehicle information, alarms, and automatic drive analysis.

Administrative log editing, Swap, and odometer editing are intentionally not available in the driver app.

Build with the repository GitHub Actions workflow: `.github/workflows/android-apk.yml`.

## v13
- DR detection uses both provider-reported speed and coordinate-derived movement speed.
- GPS, Network and Passive location providers are observed while signed in; reverse geocoding is moved off the movement callback path.
- The Logs tab shows seven stacked website-style daily duty graphs.
- Cross-midnight status taps display the full continuous episode duration.
- Break/Drive/Shift/Cycle use compact round ring indicators.
- The driver app does not expose location Map/Copy controls.

## v17 / Android 0.6.0

SQLite offline queue, reliable movement filtering, cached daily logs, configurable stationary delay and no alarm preview controls. Build using the included GitHub Actions workflow. Preserve the installed app data when updating. Requires backend v17.

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


## v15 / Android 0.5.0

- Automatic DR starts when effective GPS speed exceeds 3 mph. Effective speed uses both provider speed and coordinate-derived movement.
- While DR, a stationary stop is kept as DR initially. After about 12 seconds the UI asks the driver to choose ON or OFF. If ignored for 60 seconds, the configured fallback is applied automatically.
- Settings can choose ON Duty or Off Duty as the one-minute fallback.
- Mobile UI uses softer rounded surfaces plus density and accent customization.
- Seven daily log graphs keep website-style lanes and continuous cross-midnight episode durations.
- Break/Drive/Shift/Cycle are circular HOS rings.
- Alarm previews are Phone Ring, Rooster, Siren, and Alarm. The old short alarm set is removed.

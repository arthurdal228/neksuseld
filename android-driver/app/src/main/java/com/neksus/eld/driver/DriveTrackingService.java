package com.neksus.eld.driver;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Bundle;
import android.os.IBinder;

import org.json.JSONObject;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DriveTrackingService extends Service implements LocationListener {
    private static final String API = "https://neksuseldv1.onrender.com";
    private static final String CHANNEL = "neksus_drive_analyzer";
    private static final int NOTIFICATION_ID = 4200;
    private final ExecutorService network = Executors.newSingleThreadExecutor();
    private LocationManager locationManager;
    private long lastSentAt = 0L;

    @Override
    public void onCreate() {
        super.onCreate();
        createChannel();
        startForeground(NOTIFICATION_ID, notification("Waiting for GPS"));
        beginLocationUpdates();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (token().isEmpty() || !hasLocationPermission()) {
            stopSelf();
            return START_NOT_STICKY;
        }
        beginLocationUpdates();
        return START_STICKY;
    }

    private boolean hasLocationPermission() {
        return checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
                || checkSelfPermission(Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED;
    }

    private void beginLocationUpdates() {
        if (!hasLocationPermission()) return;
        if (locationManager == null) locationManager = getSystemService(LocationManager.class);
        if (locationManager == null) return;
        try {
            locationManager.removeUpdates(this);
            if (locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER)) {
                locationManager.requestLocationUpdates(LocationManager.GPS_PROVIDER, 8000L, 5f, this);
            }
            if (locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)) {
                locationManager.requestLocationUpdates(LocationManager.NETWORK_PROVIDER, 15000L, 15f, this);
            }
            Location last = locationManager.getLastKnownLocation(LocationManager.GPS_PROVIDER);
            if (last == null) last = locationManager.getLastKnownLocation(LocationManager.NETWORK_PROVIDER);
            if (last != null) onLocationChanged(last);
        } catch (SecurityException ignored) {
            stopSelf();
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        if (location == null) return;
        long now = System.currentTimeMillis();
        persistLocation(location);
        double mph = Math.max(0, location.getSpeed()) * 2.2369362921;
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm != null) nm.notify(NOTIFICATION_ID, notification(String.format(Locale.US, "GPS active · %.1f mph", mph)));
        if (now - lastSentAt < 7000L) return;
        lastSentAt = now;
        String auth = token();
        if (auth.isEmpty()) return;
        network.execute(() -> sendTelemetry(auth, location));
    }

    private void persistLocation(Location l) {
        SharedPreferences.Editor e = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).edit();
        e.putLong(MainActivity.PREF_LAST_LAT, Double.doubleToRawLongBits(l.getLatitude()));
        e.putLong(MainActivity.PREF_LAST_LON, Double.doubleToRawLongBits(l.getLongitude()));
        e.putLong(MainActivity.PREF_LAST_ACCURACY, Double.doubleToRawLongBits(l.hasAccuracy() ? l.getAccuracy() : 0));
        e.putLong(MainActivity.PREF_LAST_SPEED, Double.doubleToRawLongBits(l.hasSpeed() ? l.getSpeed() : 0));
        e.putLong(MainActivity.PREF_LAST_LOCATION_TIME, l.getTime() > 0 ? l.getTime() : System.currentTimeMillis());
        e.putString(MainActivity.PREF_LAST_LOCATION_TEXT, coordinateLabel(l.getLatitude(), l.getLongitude()));
        e.apply();
    }

    private String coordinateLabel(double lat, double lon) {
        return String.format(Locale.US, "GPS %.5f, %.5f", lat, lon);
    }

    private String token() {
        return getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).getString(MainActivity.PREF_TOKEN, "");
    }

    private double odometer() {
        SharedPreferences p = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE);
        return Double.longBitsToDouble(p.getLong(MainActivity.PREF_ODOMETER, Double.doubleToLongBits(0)));
    }

    private void sendTelemetry(String auth, Location l) {
        HttpURLConnection conn = null;
        try {
            JSONObject body = new JSONObject();
            body.put("latitude", l.getLatitude());
            body.put("longitude", l.getLongitude());
            body.put("accuracy_meters", l.hasAccuracy() ? l.getAccuracy() : 0);
            body.put("speed_mps", l.hasSpeed() ? Math.max(0, l.getSpeed()) : 0);
            body.put("odometer_miles", odometer());
            body.put("location_text", coordinateLabel(l.getLatitude(), l.getLongitude()));
            body.put("recorded_at", isoTime(l.getTime() > 0 ? l.getTime() : System.currentTimeMillis()));
            byte[] data = body.toString().getBytes(StandardCharsets.UTF_8);
            conn = (HttpURLConnection) new URL(API + "/v1/driver/telemetry").openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(8000);
            conn.setReadTimeout(8000);
            conn.setDoOutput(true);
            conn.setRequestProperty("Authorization", "Bearer " + auth);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setFixedLengthStreamingMode(data.length);
            try (OutputStream os = conn.getOutputStream()) { os.write(data); }
            int code = conn.getResponseCode();
            if (code == 401) stopSelf();
        } catch (Exception ignored) {
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private String isoTime(long millis) {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f.format(new Date(millis));
    }

    private void createChannel() {
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm == null) return;
        NotificationChannel c = new NotificationChannel(CHANNEL, "NEKSUS drive analyzer", NotificationManager.IMPORTANCE_LOW);
        c.setSound(null, null);
        c.setDescription("GPS movement analyzer used to create automatic driving records");
        nm.createNotificationChannel(c);
    }

    private Notification notification(String text) {
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(this, 20, open, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        return new Notification.Builder(this, CHANNEL)
                .setSmallIcon(R.drawable.ic_neksus_logo)
                .setContentTitle("NEKSUS Drive Analyzer")
                .setContentText(text)
                .setContentIntent(pi)
                .setOngoing(true)
                .setCategory(Notification.CATEGORY_SERVICE)
                .build();
    }

    @Override
    public void onProviderEnabled(String provider) {}
    @Override
    public void onProviderDisabled(String provider) {}
    @Override
    public void onStatusChanged(String provider, int status, Bundle extras) {}

    @Override
    public void onDestroy() {
        if (locationManager != null) {
            try { locationManager.removeUpdates(this); } catch (SecurityException ignored) {}
        }
        network.shutdownNow();
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) { return null; }
}

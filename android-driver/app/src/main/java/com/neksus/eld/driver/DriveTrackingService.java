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
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Bundle;
import android.os.IBinder;
import android.os.SystemClock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DriveTrackingService extends Service implements LocationListener {
    private static final String API = "https://neksuseldv1.onrender.com";
    private static final String CHANNEL = "neksus_drive_analyzer";
    private static final int NOTIFICATION_ID = 4200;
    private final ScheduledExecutorService network = Executors.newSingleThreadScheduledExecutor();
    private final MotionFilter motionFilter=new MotionFilter();
    private long lastFixElapsed=0, lastGpsElapsed=0;
    private final ExecutorService geocoderWorker = Executors.newSingleThreadExecutor();
    private LocationManager locationManager;
    private long lastSentAt = 0L;
    private Location lastMotionLocation;
    private long lastMotionWallMs = 0L;
    private String lastLocationLabel = "";
    private double lastLabelLat = 0;
    private double lastLabelLon = 0;
    private volatile boolean geocodePending = false;

    @Override
    public void onCreate() {
        super.onCreate();
        createChannel();
        startForeground(NOTIFICATION_ID, notification("Waiting for GPS"));
        beginLocationUpdates();
        network.scheduleWithFixedDelay(this::flushOutbox,0,5,TimeUnit.SECONDS);
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
            boolean gpsEnabled = locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER);
            boolean networkEnabled = locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER);
            if (gpsEnabled) {
                locationManager.requestLocationUpdates(LocationManager.GPS_PROVIDER, 3000L, 0f, this);
            }
            if (networkEnabled) {
                locationManager.requestLocationUpdates(LocationManager.NETWORK_PROVIDER, 5000L, 0f, this);
            }
            // PASSIVE also observes fixes injected through fused/mock providers used by test tools.
            try {
                locationManager.requestLocationUpdates(LocationManager.PASSIVE_PROVIDER, 2500L, 0f, this);
            } catch (Exception ignored) {}
            Location last = locationManager.getLastKnownLocation(LocationManager.GPS_PROVIDER);
            if (last == null) last = locationManager.getLastKnownLocation(LocationManager.NETWORK_PROVIDER);
            if (last == null) last = locationManager.getLastKnownLocation(LocationManager.PASSIVE_PROVIDER);
            if (last != null) onLocationChanged(last);
        } catch (SecurityException ignored) {
            stopSelf();
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        if (location == null) return;
        long now=System.currentTimeMillis(),elapsed=location.getElapsedRealtimeNanos()/1000000L;
        long age=SystemClock.elapsedRealtime()-elapsed;
        if(age<0||age>15000||elapsed<=lastFixElapsed)return;
        String provider=location.getProvider()==null?"unknown":location.getProvider();
        if(LocationManager.GPS_PROVIDER.equals(provider))lastGpsElapsed=elapsed;
        else if(lastGpsElapsed>0&&elapsed-lastGpsElapsed<15000)return;
        lastFixElapsed=elapsed;
        double speed=motionFilter.sample(provider,elapsed,location.getLatitude(),location.getLongitude(),location.hasAccuracy()?location.getAccuracy():0,location.hasSpeed(),location.getSpeed(),location.hasSpeedAccuracy()?location.getSpeedAccuracyMetersPerSecond():-1);
        String label=cachedLocationText(location.getLatitude(),location.getLongitude());
        persistLocation(location,label,Math.max(0,speed));
        getSharedPreferences(MainActivity.PREFS,MODE_PRIVATE).edit().putBoolean("speed_valid",speed>=0).apply();
        scheduleReverseGeocode(location.getLatitude(),location.getLongitude());
        NotificationManager nm=getSystemService(NotificationManager.class);
        if(nm!=null)nm.notify(NOTIFICATION_ID,notification(speed<0?"Waiting for an accurate movement fix":String.format(Locale.US,"GPS active · %.1f mph",speed*2.2369362921)));
        if(elapsed-lastSentAt<2500L)return;lastSentAt=elapsed;
        JSONObject session=driverSession();String owner=session.optString("driver_id");
        if(owner.isEmpty()||!session.optString("token").equals(token()))return;
        try{
            JSONObject body=new JSONObject();
            body.put("latitude",location.getLatitude());body.put("longitude",location.getLongitude());body.put("accuracy_meters",location.hasAccuracy()?location.getAccuracy():0);
            body.put("speed_mps",Math.max(0,speed));body.put("speed_valid",speed>=0);body.put("motion_version",17);
            body.put("auto_stop_status",autoStopStatus());body.put("auto_stop_seconds",getSharedPreferences(MainActivity.PREFS,MODE_PRIVATE).getInt("auto_stop_seconds",60));
            body.put("odometer_miles",odometer());body.put("location_text",label);body.put("recorded_at",isoTime(location.getTime()));
            OfflineStore.get(this).enqueue(owner,"/v1/driver/telemetry",body,location.getTime(),false);
        }catch(Exception e){try{OfflineStore.get(this).error(owner,"Unable to save GPS record: "+e.getMessage());}catch(Exception ignored){}}
    }

    private void persistLocation(Location l, String locationText, double effectiveSpeedMps) {
        SharedPreferences.Editor e = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).edit();
        e.putLong(MainActivity.PREF_LAST_LAT, Double.doubleToRawLongBits(l.getLatitude()));
        e.putLong(MainActivity.PREF_LAST_LON, Double.doubleToRawLongBits(l.getLongitude()));
        e.putLong(MainActivity.PREF_LAST_ACCURACY, Double.doubleToRawLongBits(l.hasAccuracy() ? l.getAccuracy() : 0));
        e.putLong(MainActivity.PREF_LAST_SPEED, Double.doubleToRawLongBits(effectiveSpeedMps));
        e.putLong(MainActivity.PREF_LAST_LOCATION_TIME, l.getTime());
        e.putString(MainActivity.PREF_LAST_LOCATION_TEXT, locationText);
        e.apply();
    }

    private String cachedLocationText(double lat, double lon) {
        if (!lastLocationLabel.isEmpty() && Math.abs(lat - lastLabelLat) < 0.02 && Math.abs(lon - lastLabelLon) < 0.02) {
            return lastLocationLabel;
        }
        SharedPreferences prefs = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE);
        String saved = prefs.getString(MainActivity.PREF_LAST_LOCATION_TEXT, "");
        double savedLat = Double.longBitsToDouble(prefs.getLong(MainActivity.PREF_LAST_LAT, Double.doubleToLongBits(0)));
        double savedLon = Double.longBitsToDouble(prefs.getLong(MainActivity.PREF_LAST_LON, Double.doubleToLongBits(0)));
        if (saved != null && !saved.trim().isEmpty() && Math.abs(lat - savedLat) < 0.02 && Math.abs(lon - savedLon) < 0.02) {
            return saved;
        }
        return "Location unavailable";
    }

    private void scheduleReverseGeocode(double lat, double lon) {
        if (geocodePending) return;
        if (!lastLocationLabel.isEmpty() && Math.abs(lat - lastLabelLat) < 0.003 && Math.abs(lon - lastLabelLon) < 0.003) return;
        geocodePending = true;
        geocoderWorker.execute(() -> {
            try {
                String label = reverseGeocode(lat, lon);
                if (!label.isEmpty()) {
                    lastLocationLabel = label;
                    lastLabelLat = lat;
                    lastLabelLon = lon;
                    getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).edit()
                            .putString(MainActivity.PREF_LAST_LOCATION_TEXT, label).apply();
                }
            } finally {
                geocodePending = false;
            }
        });
    }

    private String reverseGeocode(double lat, double lon) {
        try {
            if (!Geocoder.isPresent()) return "";
            Geocoder geocoder = new Geocoder(this, Locale.US);
            List<Address> list = geocoder.getFromLocation(lat, lon, 1);
            if (list == null || list.isEmpty()) return "";
            Address a = list.get(0);
            String city = a.getLocality();
            if (city == null || city.trim().isEmpty()) city = a.getSubAdminArea();
            if (city == null || city.trim().isEmpty()) city = a.getFeatureName();
            String state = a.getAdminArea();
            StringBuilder b = new StringBuilder();
            if (city != null && !city.trim().isEmpty()) b.append(city.trim());
            if (state != null && !state.trim().isEmpty()) {
                if (b.length() > 0) b.append(", ");
                b.append(state.trim());
            }
            return b.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    private String token() {
        return getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).getString(MainActivity.PREF_TOKEN, "");
    }

    private double odometer() {
        SharedPreferences p = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE);
        return Double.longBitsToDouble(p.getLong(MainActivity.PREF_ODOMETER, Double.doubleToLongBits(0)));
    }

    private String autoStopStatus() {
        String v = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE).getString(MainActivity.PREF_AUTO_STOP_STATUS, "ON");
        return "OFF".equalsIgnoreCase(v) ? "OFF" : "ON";
    }

    private JSONObject driverSession(){
        try{return new JSONObject(getSharedPreferences(MainActivity.PREFS,MODE_PRIVATE).getString("driver_session","{}"));}catch(Exception e){return new JSONObject();}
    }

    private void flushOutbox(){
        JSONObject session=driverSession();String auth=session.optString("token"),owner=session.optString("driver_id");
        if(auth.isEmpty()||owner.isEmpty()||!auth.equals(token()))return;
        OfflineStore store=OfflineStore.get(this);
        for(int i=0;i<200;i++){
            HttpURLConnection conn=null;
            try{
                if(!auth.equals(token())||!owner.equals(getSharedPreferences(MainActivity.PREFS,MODE_PRIVATE).getString("driver_id","")))return;
                JSONObject item=store.first(owner);if(item==null){store.error(owner,"");return;}
                byte[] data=item.getString("payload").getBytes(StandardCharsets.UTF_8);
                conn=(HttpURLConnection)new URL(API+item.getString("path")).openConnection();
                conn.setRequestMethod("POST");conn.setConnectTimeout(8000);conn.setReadTimeout(8000);conn.setDoOutput(true);
                conn.setRequestProperty("Authorization","Bearer "+auth);conn.setRequestProperty("Content-Type","application/json");conn.setFixedLengthStreamingMode(data.length);
                try(OutputStream os=conn.getOutputStream()){os.write(data);}
                int code=conn.getResponseCode();
                if(code>=200&&code<300){store.acknowledge(owner,item.getLong("seq"));store.error(owner,"");}
                else{store.error(owner,code==401?"Sign in again to sync saved records":code==409?"Saved records need review: a newer server change conflicts with offline data":"Records saved; sync will retry ("+code+")");return;}
            }catch(Exception e){try{store.error(owner,"Offline · records saved on this phone");}catch(Exception ignored){}return;}
            finally{if(conn!=null)conn.disconnect();}
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
        geocoderWorker.shutdownNow();
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) { return null; }
}

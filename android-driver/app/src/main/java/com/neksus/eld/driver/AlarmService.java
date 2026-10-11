package com.neksus.eld.driver;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.ContentResolver;
import android.content.Intent;
import android.content.SharedPreferences;
import android.media.AudioAttributes;
import android.net.Uri;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlarmService extends Service {
    private static final String API = "https://neksuseldv1.onrender.com";
    private static final String SYNC_CHANNEL = "neksus_driver_connection";
    private static final int SYNC_NOTIFICATION_ID = 4100;
    private final Handler handler = new Handler(Looper.getMainLooper());
    private final AtomicBoolean polling = new AtomicBoolean(false);

    private final Runnable pollTask = new Runnable() {
        @Override
        public void run() {
            if (polling.compareAndSet(false, true)) {
                new Thread(() -> {
                    try {
                        pollAlarms();
                    } finally {
                        polling.set(false);
                    }
                }, "neksus-alarm-poll").start();
            }
            handler.postDelayed(this, 8000L);
        }
    };

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannels();
        startForeground(SYNC_NOTIFICATION_ID, connectionNotification());
        handler.post(pollTask);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        handler.removeCallbacksAndMessages(null);
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private String token() {
        SharedPreferences prefs = getSharedPreferences(MainActivity.PREFS, MODE_PRIVATE);
        return prefs.getString(MainActivity.PREF_TOKEN, "");
    }

    private Notification connectionNotification() {
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(this, 10, open, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        return new Notification.Builder(this, SYNC_CHANNEL)
                .setSmallIcon(R.drawable.ic_neksus_logo)
                .setContentTitle("NEKSUS ELD")
                .setContentText("Driver alarm connection is active")
                .setContentIntent(pi)
                .setOngoing(true)
                .setCategory(Notification.CATEGORY_SERVICE)
                .build();
    }

    private void createNotificationChannels() {
        NotificationManager nm = getSystemService(NotificationManager.class);
        NotificationChannel sync = new NotificationChannel(SYNC_CHANNEL, "NEKSUS connection", NotificationManager.IMPORTANCE_LOW);
        sync.setSound(null, null);
        sync.setDescription("Keeps NEKSUS driver alarms connected while signed in");
        nm.createNotificationChannel(sync);

        createAlarmChannel(nm, "phone_ring", "Phone Ring", R.raw.neksus_phone_ring, new long[]{0, 500, 250, 500, 250, 900});
        createAlarmChannel(nm, "rooster", "Rooster", R.raw.neksus_rooster, new long[]{0, 700, 250, 700});
        createAlarmChannel(nm, "siren", "Siren", R.raw.neksus_siren, new long[]{0, 900, 200, 900});
        createAlarmChannel(nm, "alarm", "Alarm", R.raw.neksus_alarm, new long[]{0, 600, 180, 600, 180, 600});
    }

    private void createAlarmChannel(NotificationManager nm, String key, String label, int rawResId, long[] vibration) {
        String id = "neksus_alarm_" + key;
        NotificationChannel channel = new NotificationChannel(id, "NEKSUS alarm - " + label, NotificationManager.IMPORTANCE_HIGH);
        Uri sound = Uri.parse(ContentResolver.SCHEME_ANDROID_RESOURCE + "://" + getPackageName() + "/" + rawResId);
        AudioAttributes attrs = new AudioAttributes.Builder()
                .setUsage(AudioAttributes.USAGE_ALARM)
                .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                .build();
        channel.setSound(sound, attrs);
        channel.enableVibration(true);
        channel.setVibrationPattern(vibration);
        channel.setDescription("Audible NEKSUS dispatch alarm: " + label);
        nm.createNotificationChannel(channel);
    }

    private void pollAlarms() {
        String auth = token();
        if (auth.isEmpty()) {
            stopSelf();
            return;
        }
        HttpURLConnection conn = null;
        try {
            conn = open("GET", API + "/v1/driver/alarms", auth);
            int code = conn.getResponseCode();
            if (code == 401) {
                stopSelf();
                return;
            }
            if (code < 200 || code >= 300) {
                return;
            }
            JSONObject payload = new JSONObject(readAll(conn.getInputStream()));
            JSONArray alarms = payload.optJSONArray("alarms");
            if (alarms == null) return;
            for (int i = 0; i < alarms.length(); i++) {
                JSONObject alarm = alarms.optJSONObject(i);
                if (alarm == null) continue;
                if (showAlarm(alarm)) {
                    markDelivered(auth, alarm.optString("id", ""));
                }
            }
        } catch (Exception ignored) {
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private boolean showAlarm(JSONObject alarm) {
        String id = alarm.optString("id", "alarm");
        String title = alarm.optString("title", "NEKSUS alert");
        String message = alarm.optString("message", "Driver notification");
        String ringtone = alarm.optString("ringtone", "phone_ring").toLowerCase(Locale.US);
        if (!(ringtone.equals("phone_ring") || ringtone.equals("rooster") || ringtone.equals("siren") || ringtone.equals("alarm"))) {
            ringtone = "phone_ring";
        }
        Intent open = new Intent(this, MainActivity.class);
        open.addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP | Intent.FLAG_ACTIVITY_CLEAR_TOP);
        PendingIntent pi = PendingIntent.getActivity(this, id.hashCode(), open, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        NotificationManager manager = getSystemService(NotificationManager.class);
        if (manager == null || !manager.areNotificationsEnabled()) return false;
        Notification n = new Notification.Builder(this, "neksus_alarm_" + ringtone)
                .setSmallIcon(R.drawable.ic_neksus_logo)
                .setContentTitle(title)
                .setContentText(message)
                .setStyle(new Notification.BigTextStyle().bigText(message))
                .setContentIntent(pi)
                .setAutoCancel(true)
                .setCategory(Notification.CATEGORY_ALARM)
                .setVisibility(Notification.VISIBILITY_PUBLIC)
                .build();
        manager.notify(id.hashCode(), n);
        return true;
    }

    private void markDelivered(String auth, String id) {
        if (id == null || id.isEmpty()) return;
        HttpURLConnection conn = null;
        try {
            conn = open("POST", API + "/v1/driver/alarms/" + id + "/delivered", auth);
            conn.setDoOutput(true);
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(body.length);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }
            conn.getResponseCode();
        } catch (Exception ignored) {
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private HttpURLConnection open(String method, String target, String auth) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(target).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(7000);
        conn.setReadTimeout(7000);
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + auth);
        return conn;
    }

    private String readAll(InputStream stream) throws Exception {
        StringBuilder b = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) b.append(line);
        }
        return b.toString();
    }
}

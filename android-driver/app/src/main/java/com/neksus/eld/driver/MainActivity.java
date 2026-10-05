package com.neksus.eld.driver;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.webkit.JavascriptInterface;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import org.json.JSONObject;

public class MainActivity extends Activity {
    public static final String PREFS = "neksus_driver_native";
    public static final String PREF_TOKEN = "driver_token";
    public static final String PREF_LOCATION_PROMPTED = "location_prompted";
    public static final String PREF_ODOMETER = "odometer_miles";
    public static final String PREF_LAST_LAT = "last_latitude";
    public static final String PREF_LAST_LON = "last_longitude";
    public static final String PREF_LAST_ACCURACY = "last_accuracy";
    public static final String PREF_LAST_SPEED = "last_speed_mps";
    public static final String PREF_LAST_LOCATION_TIME = "last_location_time";
    public static final String PREF_LAST_LOCATION_TEXT = "last_location_text";
    private static final int REQ_LOCATION = 1002;
    private static final int REQ_NOTIFICATIONS = 1001;
    private WebView webView;

    @SuppressLint({"SetJavaScriptEnabled", "JavascriptInterface"})
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        if (!hasLocationPermission() && !prefs.getBoolean(PREF_LOCATION_PROMPTED, false)) {
            prefs.edit().putBoolean(PREF_LOCATION_PROMPTED, true).apply();
            requestPermissions(new String[]{Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION}, REQ_LOCATION);
        }
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, REQ_NOTIFICATIONS);
        }

        webView = new WebView(this);
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setDatabaseEnabled(true);
        settings.setAllowFileAccess(true);
        settings.setAllowContentAccess(false);
        settings.setAllowFileAccessFromFileURLs(true);
        settings.setAllowUniversalAccessFromFileURLs(true);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_NEVER_ALLOW);
        webView.setWebViewClient(new WebViewClient());
        webView.setWebChromeClient(new WebChromeClient());
        webView.setBackgroundColor(0xFF0B100D);
        webView.addJavascriptInterface(new NativeBridge(), "NeksusNative");
        setContentView(webView);
        webView.loadUrl("file:///android_asset/index.html");

        if (!prefs.getString(PREF_TOKEN, "").isEmpty()) {
            startBackgroundServices();
        }
    }

    private boolean hasLocationPermission() {
        return checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
                || checkSelfPermission(Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED;
    }

    private void startAlarmService() {
        Intent intent = new Intent(this, AlarmService.class);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) startForegroundService(intent); else startService(intent);
    }

    private void startDriveService() {
        if (!hasLocationPermission()) return;
        Intent intent = new Intent(this, DriveTrackingService.class);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) startForegroundService(intent); else startService(intent);
    }

    private void startBackgroundServices() {
        startAlarmService();
        startDriveService();
    }

    private void notifyPermissionState() {
        if (webView == null) return;
        String state = hasLocationPermission() ? "granted" : "denied";
        webView.post(() -> webView.evaluateJavascript("window.onNativeLocationPermissionChanged && window.onNativeLocationPermissionChanged('" + state + "');", null));
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQ_LOCATION) {
            if (hasLocationPermission() && !getSharedPreferences(PREFS, MODE_PRIVATE).getString(PREF_TOKEN, "").isEmpty()) {
                startDriveService();
            }
            notifyPermissionState();
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        notifyPermissionState();
        if (hasLocationPermission() && !getSharedPreferences(PREFS, MODE_PRIVATE).getString(PREF_TOKEN, "").isEmpty()) {
            startDriveService();
        }
    }

    public class NativeBridge {
        @JavascriptInterface
        public void setDriverToken(String token) {
            String value = token == null ? "" : token.trim();
            getSharedPreferences(PREFS, MODE_PRIVATE).edit().putString(PREF_TOKEN, value).apply();
            if (!value.isEmpty()) runOnUiThread(MainActivity.this::startBackgroundServices);
        }

        @JavascriptInterface
        public void clearDriverToken() {
            getSharedPreferences(PREFS, MODE_PRIVATE).edit().remove(PREF_TOKEN).apply();
            runOnUiThread(() -> {
                stopService(new Intent(MainActivity.this, AlarmService.class));
                stopService(new Intent(MainActivity.this, DriveTrackingService.class));
            });
        }

        @JavascriptInterface
        public String getLocationPermissionState() {
            return hasLocationPermission() ? "granted" : "denied";
        }

        @JavascriptInterface
        public void openLocationSettings() {
            runOnUiThread(() -> {
                Intent intent = new Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:" + getPackageName()));
                startActivity(intent);
            });
        }

        @JavascriptInterface
        public String getLastLocation() {
            SharedPreferences p = getSharedPreferences(PREFS, MODE_PRIVATE);
            JSONObject o = new JSONObject();
            try {
                o.put("permission", hasLocationPermission() ? "granted" : "denied");
                o.put("latitude", Double.longBitsToDouble(p.getLong(PREF_LAST_LAT, Double.doubleToLongBits(0))));
                o.put("longitude", Double.longBitsToDouble(p.getLong(PREF_LAST_LON, Double.doubleToLongBits(0))));
                o.put("accuracy_meters", Double.longBitsToDouble(p.getLong(PREF_LAST_ACCURACY, Double.doubleToLongBits(0))));
                o.put("speed_mps", Double.longBitsToDouble(p.getLong(PREF_LAST_SPEED, Double.doubleToLongBits(0))));
                o.put("recorded_at_ms", p.getLong(PREF_LAST_LOCATION_TIME, 0));
                o.put("location_text", p.getString(PREF_LAST_LOCATION_TEXT, ""));
            } catch (Exception ignored) {}
            return o.toString();
        }

        @JavascriptInterface
        public void setOdometer(String value) {
            try {
                double miles = Double.parseDouble(value == null ? "0" : value.trim());
                if (miles > 0) {
                    getSharedPreferences(PREFS, MODE_PRIVATE).edit().putLong(PREF_ODOMETER, Double.doubleToRawLongBits(miles)).apply();
                }
            } catch (Exception ignored) {}
        }

        @JavascriptInterface
        public String getOdometer() {
            SharedPreferences p = getSharedPreferences(PREFS, MODE_PRIVATE);
            double v = Double.longBitsToDouble(p.getLong(PREF_ODOMETER, Double.doubleToLongBits(0)));
            return String.valueOf(v);
        }

        @JavascriptInterface
        public void startDriveAnalyzer() {
            runOnUiThread(MainActivity.this::startDriveService);
        }

    }

    @Override
    public void onBackPressed() {
        if (webView != null && webView.canGoBack()) webView.goBack(); else super.onBackPressed();
    }

    @Override
    protected void onDestroy() {
        if (webView != null) webView.destroy();
        super.onDestroy();
    }
}

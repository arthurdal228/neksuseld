package com.neksus.eld.driver;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.webkit.JavascriptInterface;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;

public class MainActivity extends Activity {
    public static final String PREFS = "neksus_driver_native";
    public static final String PREF_TOKEN = "driver_token";
    private WebView webView;

    @SuppressLint({"SetJavaScriptEnabled", "JavascriptInterface"})
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, 1001);
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

        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        if (!prefs.getString(PREF_TOKEN, "").isEmpty()) {
            startAlarmService();
        }
    }

    private void startAlarmService() {
        Intent intent = new Intent(this, AlarmService.class);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent);
        } else {
            startService(intent);
        }
    }

    public class NativeBridge {
        @JavascriptInterface
        public void setDriverToken(String token) {
            String value = token == null ? "" : token.trim();
            getSharedPreferences(PREFS, MODE_PRIVATE).edit().putString(PREF_TOKEN, value).apply();
            if (!value.isEmpty()) {
                runOnUiThread(MainActivity.this::startAlarmService);
            }
        }

        @JavascriptInterface
        public void clearDriverToken() {
            getSharedPreferences(PREFS, MODE_PRIVATE).edit().remove(PREF_TOKEN).apply();
            runOnUiThread(() -> stopService(new Intent(MainActivity.this, AlarmService.class)));
        }
    }

    @Override
    public void onBackPressed() {
        if (webView != null && webView.canGoBack()) {
            webView.goBack();
        } else {
            super.onBackPressed();
        }
    }

    @Override
    protected void onDestroy() {
        if (webView != null) {
            webView.destroy();
        }
        super.onDestroy();
    }
}

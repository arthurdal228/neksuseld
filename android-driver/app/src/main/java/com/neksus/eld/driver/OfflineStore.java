package com.neksus.eld.driver;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.UUID;

/** Durable, driver-scoped outbox. Records are removed only after server acknowledgement. */
public final class OfflineStore extends SQLiteOpenHelper {
    private static OfflineStore instance;
    public static synchronized OfflineStore get(Context c){if(instance==null)instance=new OfflineStore(c.getApplicationContext());return instance;}
    private OfflineStore(Context c){super(c,"neksus_offline_v17.db",null,1);}
    public void onCreate(SQLiteDatabase db){
        db.execSQL("CREATE TABLE outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE NOT NULL,owner TEXT NOT NULL,path TEXT NOT NULL,payload TEXT NOT NULL)");
        db.execSQL("CREATE INDEX outbox_owner ON outbox(owner,seq)");
        db.execSQL("CREATE TABLE state(owner TEXT PRIMARY KEY,payload TEXT NOT NULL)");
        db.execSQL("CREATE TABLE changes(event_id TEXT PRIMARY KEY,owner TEXT NOT NULL,at INTEGER NOT NULL,status TEXT NOT NULL,synced_at INTEGER NOT NULL DEFAULT 0)");
    }
    public void onUpgrade(SQLiteDatabase db,int oldVersion,int newVersion){}
    private JSONObject state(String owner)throws Exception{try(Cursor c=getReadableDatabase().rawQuery("SELECT payload FROM state WHERE owner=?",new String[]{owner})){return c.moveToFirst()?new JSONObject(c.getString(0)):new JSONObject();}}
    private void saveState(String owner,JSONObject state){getWritableDatabase().execSQL("INSERT OR REPLACE INTO state(owner,payload) VALUES(?,?)",new Object[]{owner,state.toString()});}
    public synchronized void seed(String owner,String status,long since)throws Exception{
        if(owner.isEmpty())return;JSONObject state=state(owner);
        if(state.length()==0||pending(owner)==0){state.put("status",status);state.put("since",since);saveState(owner,state);}
    }
    public synchronized int pending(String owner){try(Cursor c=getReadableDatabase().rawQuery("SELECT count(*) FROM outbox WHERE owner=?",new String[]{owner})){return c.moveToFirst()?c.getInt(0):0;}}
    public synchronized String enqueue(String owner,String path,JSONObject body,long at,boolean manual)throws Exception{
        if(owner.isEmpty())throw new IllegalStateException("Driver identity is unavailable");
        SQLiteDatabase db=getWritableDatabase();db.beginTransaction();
        try{
            String id=UUID.randomUUID().toString();body.put("client_event_id",id);
            JSONObject state=state(owner);if(!manual&&at<=state.optLong("queued_at")){return "";}state.put("queued_at",at);String status=state.optString("status","OFF");
            long last=state.optLong("sample_at"),moving=state.optLong("moving_at"),stopped=state.optLong("stopped_at");int samples=state.optInt("moving_samples");
            String next=status;long since=at;
            if(manual){if(body.optDouble("speed_mps",0)>0.44704)throw new IllegalStateException("Vehicle is moving");next=body.getString("status");moving=0;stopped=0;samples=0;}
            else{
                boolean valid=body.optBoolean("speed_valid",false);double speed=body.optDouble("speed_mps",0);
                if(last==0||at-last>15000){moving=0;stopped=0;samples=0;}
                if(!valid){moving=0;stopped=0;samples=0;}
                else if(!"DR".equals(status)){
                    if(speed>1.34112){if(moving==0)moving=at;samples++;if(samples>=3&&at-moving>=6000){next="DR";since=moving;samples=0;}}
                    else{moving=0;samples=0;}
                }else if(speed<=0.44704){
                    if(stopped==0)stopped=at;int delay=body.optInt("auto_stop_seconds",60);
                    if(delay>0&&at-stopped>=delay*1000L){next=body.optString("auto_stop_status","ON");stopped=0;moving=0;samples=0;}
                }else{stopped=0;}
                state.put("sample_at",at);
            }
            if(!next.equals(status)){
                state.put("status",next);state.put("since",since);
                db.execSQL("INSERT INTO changes(event_id,owner,at,status) VALUES(?,?,?,?)",new Object[]{id,owner,since,next});
            }
            state.put("moving_at",moving);state.put("moving_samples",samples);state.put("stopped_at",stopped);saveState(owner,state);
            db.execSQL("INSERT INTO outbox(event_id,owner,path,payload) VALUES(?,?,?,?)",new Object[]{id,owner,path,body.toString()});
            db.setTransactionSuccessful();return id;
        }finally{db.endTransaction();}
    }
    public synchronized JSONObject first(String owner)throws Exception{
        try(Cursor c=getReadableDatabase().rawQuery("SELECT seq,path,payload FROM outbox WHERE owner=? ORDER BY seq LIMIT 1",new String[]{owner})){
            if(!c.moveToFirst())return null;return new JSONObject().put("seq",c.getLong(0)).put("path",c.getString(1)).put("payload",c.getString(2));
        }
    }
    public synchronized void acknowledge(String owner,long seq){
        SQLiteDatabase db=getWritableDatabase();db.beginTransaction();try{
            db.execSQL("UPDATE changes SET synced_at=? WHERE owner=? AND event_id IN (SELECT event_id FROM outbox WHERE owner=? AND seq=?)",new Object[]{System.currentTimeMillis(),owner,owner,seq});
            db.execSQL("DELETE FROM outbox WHERE owner=? AND seq=?",new Object[]{owner,seq});db.setTransactionSuccessful();
        }finally{db.endTransaction();}
    }
    public synchronized void error(String owner,String text)throws Exception{JSONObject x=state(owner);x.put("sync_error",text);saveState(owner,x);}
    public synchronized JSONObject snapshot(String owner)throws Exception{
        JSONObject x=state(owner);x.put("pending",pending(owner));JSONArray changes=new JSONArray();
        try(Cursor c=getReadableDatabase().rawQuery("SELECT c.at,c.status,c.synced_at,EXISTS(SELECT 1 FROM outbox o WHERE o.event_id=c.event_id) FROM changes c WHERE c.owner=? AND c.at>? ORDER BY c.at",new String[]{owner,String.valueOf(System.currentTimeMillis()-35L*86400000)})){
            while(c.moveToNext())changes.put(new JSONObject().put("at",c.getLong(0)).put("status",c.getString(1)).put("synced_at",c.getLong(2)).put("pending",c.getInt(3)>0));
        }
        return x.put("changes",changes);
    }
}

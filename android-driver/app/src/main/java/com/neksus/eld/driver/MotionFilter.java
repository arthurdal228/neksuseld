package com.neksus.eld.driver;

/** Pure movement filter, independent of Android and network availability. */
public final class MotionFilter {
    private String provider="";
    private long lastTime=0, anchorTime=0;
    private double anchorLat,anchorLon,anchorAccuracy;
    public double sample(String source,long time,double lat,double lon,double accuracy,boolean hasSpeed,double speed,double speedAccuracy){
        if(time<=lastTime)return -1;
        boolean reset=!provider.equals(source)||time-lastTime>15000;
        provider=source;lastTime=time;
        if(accuracy<=0||accuracy>35||!Double.isFinite(speed)){anchorTime=0;return -1;}
        if(reset||anchorTime==0){anchorTime=time;anchorLat=lat;anchorLon=lon;anchorAccuracy=accuracy;}
        // An available accurate zero is evidence of being stationary, not missing speed.
        if(hasSpeed&&speed>=0&&speed<=60&&(speedAccuracy<0||speedAccuracy<=0.8)){
            anchorTime=time;anchorLat=lat;anchorLon=lon;anchorAccuracy=accuracy;
            return speed<=0.44704?0:Math.max(0,speed-Math.max(0,speedAccuracy));
        }
        double dt=(time-anchorTime)/1000.0;
        if(dt<6)return -1;
        double meters=distance(anchorLat,anchorLon,lat,lon),uncertainty=anchorAccuracy+accuracy;
        double lower=Math.max(0,meters-uncertainty)/dt;
        if(dt>30||meters/dt>60){anchorTime=time;anchorLat=lat;anchorLon=lon;anchorAccuracy=accuracy;return -1;}
        // Coordinate-only fixes inside the error envelope are unknown, never a stop proof.
        return lower>1.34112?lower:-1;
    }
    static double distance(double a,double b,double c,double d){double p=Math.PI/180,dl=(c-a)*p,dn=(d-b)*p;double h=Math.sin(dl/2)*Math.sin(dl/2)+Math.cos(a*p)*Math.cos(c*p)*Math.sin(dn/2)*Math.sin(dn/2);return 6371000*2*Math.atan2(Math.sqrt(h),Math.sqrt(Math.max(0,1-h)));}
}

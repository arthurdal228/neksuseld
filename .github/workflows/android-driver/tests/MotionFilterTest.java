package com.neksus.eld.driver;
public class MotionFilterTest {
 public static void main(String[] args){
  MotionFilter zero=new MotionFilter();for(int i=0;i<100;i++){double speed=zero.sample("gps",1000+i*3000,41+(i%3)*.0001,-87,12,true,0,0.2);if(speed!=0)throw new AssertionError("zero became driving");}
  MotionFilter jitter=new MotionFilter();for(int i=0;i<100;i++){double speed=jitter.sample("gps",1000+i*3000,41+(i%3)*.00004,-87,15,false,0,-1);if(speed>1.34112)throw new AssertionError("drift became driving");}
  MotionFilter moving=new MotionFilter();int confirmed=0;for(int i=0;i<10;i++){double speed=moving.sample("gps",1000+i*3000,41+i*.00072,-87,5,false,0,-1);if(speed>1.34112)confirmed++;}if(confirmed<3)throw new AssertionError("sustained 60 mph movement missed");
  MotionFilter stale=new MotionFilter();stale.sample("gps",10000,41,-87,5,true,10,0.1);if(stale.sample("gps",10000,42,-87,5,true,10,.1)>=0)throw new AssertionError("duplicate accepted");
  if(stale.sample("gps",13000,41,-87,250,true,10,.1)>=0)throw new AssertionError("poor accuracy accepted");
  System.out.println("PASS: reported zero, stationary drift, 60 mph coordinate fallback, duplicates, poor accuracy");
 }
}

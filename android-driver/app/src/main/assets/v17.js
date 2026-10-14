let mobileLogAnchor=null;
function offlineState(){try{return JSON.parse(window.NeksusNative?.getOfflineState?.()||'{}')}catch{return {}}}
function seedNativeDriver(){if(!driver)return;try{window.NeksusNative?.setDriverIdentity?.(JSON.stringify({id:driver.id,token,current_status:driver.current_status,since_ms:Date.parse(driver.status_since)||Date.now()}))}catch{}}
function mobileCacheKey(){return 'neksus-offline-v17:'+driver?.id}
function cacheMobile(){if(!driver)return;try{localStorage.setItem('neksus-last-driver',driver.id);localStorage.setItem('neksus-session-driver',JSON.stringify({token,id:driver.id}));localStorage.setItem(mobileCacheKey(),JSON.stringify({driver,hos,days:mobileLogDays,anchor:mobileLogAnchor||driver}))}catch{toast('Phone storage is full. Native records remain queued.',true)}}
function restoreMobile(){try{const session=JSON.parse(localStorage.getItem('neksus-session-driver')||'null');if(!session||session.token!==token)return false;const id=session.id,x=JSON.parse(localStorage.getItem('neksus-offline-v17:'+id)||'null');if(!x?.driver||x.driver.id!==id)return false;driver=x.driver;mobileLogAnchor=x.anchor||x.driver;hos=x.hos;mobileLogDays=x.days||{};return true}catch{return false}}
const v17RenderDriver=renderDriver;
renderDriver=function(){const local=offlineState();if(driver&&local.status&&local.pending>0){driver={...driver,current_status:local.status,status_since:new Date(local.since||Date.now()).toISOString()}}v17RenderDriver();};
let previousQueueCount=0;
const v17Device=updateDeviceState;
updateDeviceState=function(){v17Device();const local=offlineState();if(driver&&local.status&&local.pending>0)renderDriver();if(driver&&previousQueueCount>0&&local.pending===0)loadLogs();previousQueueCount=local.pending||0;const badge=$('offlineQueue');if(badge){badge.textContent=local.sync_error|| (local.pending?local.pending+' records waiting to sync':'All records synced');badge.classList.toggle('warn',!!local.pending)}const loc=nativeLocation();if(loc.speed_valid===false||!loc.recorded_at_ms||Date.now()-Number(loc.recorded_at_ms)>15000){$('speedChip').textContent='Speed unavailable';$('vehicleSpeed').textContent='Unavailable'}};
refreshAll=async function(){if(!token)return;try{
 const [me,h,ds]=await Promise.all([api('/v1/driver/me'),api('/v1/driver/hos'),api('/v1/driver/drive-state')]);driver=me.driver;mobileLogAnchor={...me.driver};hos=h;driveState=ds;seedNativeDriver();renderDriver();renderHos();renderDriveState();cacheMobile();$('onlineIndicator').className='indicator ok';$('onlineIndicator').textContent='NET';$('lastRefresh').textContent=new Date().toLocaleTimeString();updateDeviceState();
 }catch(e){$('onlineIndicator').className='indicator bad';$('onlineIndicator').textContent=e.status===401?'SIGN IN':'OFFLINE';if(!driver)restoreMobile();if(driver){renderDriver();renderHos();updateDeviceState()}}
};
setStatus=async function(status,note=''){
 if(!['OFF','SB','ON'].includes(status))return;if(status==='ON'&&!note)return noteSheet();
 try{
  const loc=currentLocationForStatus();if(loc.speed_valid!==true||Date.now()-Number(loc.recorded_at_ms||0)>15000)throw new Error('Waiting for an accurate speed reading.');
  const body={status,note,latitude:loc.latitude,longitude:loc.longitude,accuracy_meters:loc.accuracy_meters,location_text:loc.location_text,speed_mps:loc.speed_mps};
  if(window.NeksusNative?.queueStatus){const result=JSON.parse(window.NeksusNative.queueStatus(JSON.stringify(body)));if(!result.ok)throw new Error(result.error||'Unable to save status');renderDriver();cacheMobile();resetStationaryPrompt();toast('Status saved on this phone');await loadLogs()}
  else{const result=await api('/v1/driver/status',{method:'POST',body:JSON.stringify(body)});driver=result.driver;renderDriver();await loadLogs()}
 }catch(e){toast(e.message,true)}
};
function dateAt(value){const tz=driver?.home_terminal_timezone||'America/Chicago',p=new Intl.DateTimeFormat('en-US',{timeZone:tz,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(value)),v=k=>p.find(x=>x.type===k).value;return v('year')+'-'+v('month')+'-'+v('day')}
function wallPosition(date,second){return Date.parse(date+'T00:00:00Z')/1000+second}
function projectOfflineDay(date,days,profile,changes=[],now=new Date()){
 const start=wallPosition(date,0),limit=date===dateAt(now)?secondInDriverZone(now):86400,end=start+limit;
 const pending=changes.filter(x=>x.pending!==false||!days[date]?.synced_at||Number(x.synced_at||0)>days[date].synced_at),firstPending=pending.length?Math.min(...pending.map(x=>wallPosition(dateAt(x.at),secondInDriverZone(x.at)))):Infinity;
 const points=[];
 for(const [day,payload] of Object.entries(days))for(const seg of payload.segments||[]){const pos=wallPosition(day,Number(seg.start_second??Number(seg.start_minute||0)*60));if(pos<firstPending)points.push({pos,seg})}
 if(profile?.status_since){const since=Date.parse(profile.status_since),pos=wallPosition(dateAt(since),secondInDriverZone(since));points.push({pos,seg:{duty_status:profile.current_status,episode_since:since}})}
 for(const change of pending)points.push({pos:wallPosition(dateAt(change.at),secondInDriverZone(change.at)),seg:{duty_status:change.status,episode_since:change.at}});
 points.sort((a,b)=>a.pos-b.pos);const unique=new Map();for(const point of points)unique.set(point.pos,point);const ordered=[...unique.values()].sort((a,b)=>a.pos-b.pos);
 let active=null;const result=[];let cursor=start;
 for(const point of ordered){if(point.pos<=start){active=point;continue}if(point.pos>=end)break;if(active&&point.pos>cursor)result.push({...active.seg,start_second:cursor-start,end_second:point.pos-start});active=point;cursor=point.pos}
 if(active&&end>cursor)result.push({...active.seg,start_second:cursor-start,end_second:date===dateAt(now)?null:limit,episode_duration_seconds:active.seg.episode_since?Math.floor((now-new Date(active.seg.episode_since))/1000):active.seg.episode_duration_seconds});
 return result;
}
function renderMobileLogs(){const today=driverToday(),dates=Array.from({length:7},(_,i)=>dateMinusMobile(today,i)),local=offlineState(),root=$('mobileLogsList');if(!root)return;
 root.innerHTML=dates.map(date=>{let day=mobileLogDays[date]||{segments:[],events:[]};const segs=projectOfflineDay(date,mobileLogDays,mobileLogAnchor||driver,local.changes||[]);day={...day,segments:segs};mobileRenderedDays[date]=day;
 return `<section class="card graph-card mobile-day-card" data-mobile-date="${date}"><div class="mobile-day-head"><b>${displayLogDate(date)}</b><span>${date===today?'Current day':'Duty log'}</span></div>${segs.length?`<div class="graph mobile-log-scroll"><div class="graph-plot mobile-log-plot">${mobileLogSvg(date,segs,day.events||[])}</div></div><div id="segmentDetail-${date}" class="segment-detail hidden"></div>`:'<div class="empty">No saved history available for this day.</div>'}</section>`}).join('');
}
let mobileRenderedDays={};
loadLogs=async function(){if(!token||!driver)return;renderMobileLogs();const dates=Array.from({length:7},(_,i)=>dateMinusMobile(driverToday(),i));await Promise.allSettled(dates.map(async date=>{try{const payload=await api('/v1/driver/logs/'+date);mobileLogDays[date]={segments:payload.segments||[],events:payload.events||[],synced_at:Date.now()}}catch{}}));cacheMobile();renderMobileLogs()};
checkStationaryPrompt=function(loc){const local=offlineState(),fresh=Date.now()-Number(loc.recorded_at_ms||0)<15000;if(!fresh||loc.speed_valid===false||Number(loc.speed_mps)>0.44704||driver?.current_status!=='DR'){if(document.querySelector('[data-stationary-sheet]'))closeSheet();resetStationaryPrompt();return}stationarySince=local.stopped_at||stationarySince||Date.now();if(Date.now()-stationarySince>=12000&&!stationaryPromptShown&&!document.querySelector('.sheet-backdrop')){stationaryPromptShown=true;showStationaryPrompt()}};
showStationaryPrompt=function(){const p=loadMobilePrefs(),seconds=Number(p.autoStopSeconds??60);openSheet(`<h2 data-stationary-sheet>Vehicle stopped</h2><p class="sub">${seconds?'Automatic '+statusName(p.autoStop)+' after '+seconds+' seconds stopped.':'Choose the next duty status.'}</p><div class="stationary-choice"><button data-stop-status="ON">ON</button><button data-stop-status="OFF">OFF</button><button data-stop-status="SB">SB</button></div><div class="sheet-actions"><button class="secondary" data-sheet-close>Dismiss</button></div>`)};
const v17Prefs=applyMobilePrefs;applyMobilePrefs=function(){v17Prefs();const seconds=Number(loadMobilePrefs().autoStopSeconds??60);if($('prefAutoStopSeconds'))$('prefAutoStopSeconds').value=seconds;try{window.NeksusNative?.setAutoStopSeconds?.(String(seconds))}catch{}};
document.addEventListener('change',e=>{if(e.target.id==='prefAutoStopSeconds')saveMobilePref('autoStopSeconds',Number(e.target.value))});
document.addEventListener('click',e=>{const hit=e.target.closest('[data-mobile-seg]');if(!hit)return;e.stopImmediatePropagation();const date=hit.dataset.mobileDate,i=Number(hit.dataset.mobileSeg),seg=mobileRenderedDays[date]?.segments[i];if(seg)renderSegmentDetail(seg,date,i)},true);
setInterval(()=>{if(driver&&!$('tabLogs').classList.contains('hidden')&&!document.querySelector('.sheet-backdrop'))renderMobileLogs()},15000);
applyMobilePrefs();
(async()=>{if(!token)return showLogin();nativeSetToken(token);const cached=restoreMobile();if(cached){seedNativeDriver();showApp();renderDriver();renderHos();startTimers()}try{const me=await api('/v1/driver/me');driver=me.driver;mobileLogAnchor={...me.driver};seedNativeDriver();showApp();await refreshAll();startTimers();await loadLogs()}catch(e){if(cached){$('onlineIndicator').textContent=e.status===401?'SIGN IN':'OFFLINE';loadLogs()}else{showLogin();$('loginError').textContent='Connect once to load your driver account.'}}})();

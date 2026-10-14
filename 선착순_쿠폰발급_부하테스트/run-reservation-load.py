"""Two reservation-path runs; requires isolated infrastructure from ReservationLoadEnvironment."""
import json,os,shutil,subprocess,sys,time,urllib.request,socket,threading
from pathlib import Path
from datetime import datetime,timedelta,timezone
out=Path(sys.argv[1]);cp=sys.argv[2];scripts=Path(__file__).resolve().parent
E=os.environ;group=E['LOAD_GROUP'];bootstrap=E['LOAD_KAFKA']
def free_port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def resource_samples(run):
 with (run/'docker-stats.jsonl').open('w') as f:
  while not (run/'monitor.stop').exists():
   start=time.time_ns();mono=time.monotonic_ns()
   r=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}',E['LOAD_DB_CONTAINER'],E['LOAD_REDIS_CONTAINER'],E['LOAD_KAFKA_CONTAINER']],capture_output=True,text=True,timeout=15)
   f.write(json.dumps(dict(time_ms=start/1e6,monotonic_ns=mono,end_ms=time.time_ns()/1e6,exit=r.returncode,rows=[json.loads(s) for s in r.stdout.splitlines()],error=r.stderr))+'\n');f.flush()
   time.sleep(max(0,2-(time.monotonic_ns()-mono)/1e9))
def last_sample(run):
 raw=(run/'progress.jsonl').read_text();return json.loads(raw[:raw.rfind('\n')].splitlines()[-1])
production_series=E.get('LOAD_PRODUCTION_BATCH')=='true'
batch_series=E.get('LOAD_BATCH_CONSUMER')=='true'
if production_series and any(E.get(k)=='true' for k in ['LOAD_BATCH_CONSUMER','LOAD_RESERVED_CONSUMER','LOAD_LUA_COMPARISON','LOAD_RELAY_PROFILE']):
 raise ValueError('Production comparison must not enable test candidate or profiling wrappers')
consumer_series=E.get('LOAD_RESERVED_CONSUMER')=='true'
lua_series=E.get('LOAD_LUA_COMPARISON')=='true' or consumer_series or batch_series
profile_series=E.get('LOAD_RELAY_PROFILE')=='true' or lua_series
labels=(['production-single-1','production-batch-1','production-batch-2','production-single-2'] if production_series else ['batch-one-1','batch-ten-1','batch-ten-2','batch-one-2'] if batch_series else ['consumer-off-1','consumer-on-1','consumer-on-2','consumer-off-2'] if consumer_series else ['legacy-1','lua-1','lua-2','legacy-2'] if lua_series else ['off-1','on-1','off-2','on-2'] if profile_series else ['run-1','run-2'])
if production_series and E.get('LOAD_PRODUCTION_RUNS'):
 labels=E['LOAD_PRODUCTION_RUNS'].split(',')
 if any(not label.startswith(('production-single-', 'production-batch-')) or '/' in label or '..' in label for label in labels):
  raise ValueError('Invalid production run label')
for label in labels:
 run=out/label;run.mkdir();processes=[];files=[];sampler_thread=None
 def start(cmd,name):
  f=(run/name).open('w');files.append(f);p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT);processes.append(p);return p
 def post(path,data):
  req=urllib.request.Request(base+path,json.dumps(data).encode(),{'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=20) as r:return json.load(r)
 def helper(mode,*args):
  return ['java','--add-modules','jdk.attach','-cp',cp,str(scripts/'ReservationLoadSampler.java'),str(coupon),str(run),bootstrap,group,mode,*map(str,args)]
 try:
  port=free_port();base='http://127.0.0.1:'+str(port)
  jvm=['java', '-Drelay.profile.enabled='+str(lua_series or label.startswith('on')).lower(), '-Drelay.profile.dir='+str(run)] if profile_series else ['java']
  if lua_series:jvm+=['-Drelay.prepare.lua='+str(batch_series or consumer_series or label.startswith('lua')).lower()]
  if consumer_series:jvm+=['-Dconsumer.profile.enabled='+str('consumer-on' in label).lower(), '-Dconsumer.profile.dir='+str(run)]
  if batch_series:jvm+=['-Dbatch.candidate.size='+('10' if 'batch-ten' in label else '1'),'-Dbatch.candidate.dir='+str(run),'-Dbatch.candidate.start=true']
  main='com.assignment.coupon_system.issuerequest.profiling.RelayProfileApplication' if profile_series else 'com.assignment.coupon_system.CouponSystemApplication'
  args=jvm+['-cp',cp,main,
   '--server.address=127.0.0.1','--server.port='+str(port),'--spring.jpa.show-sql=false','--spring.jpa.hibernate.ddl-auto=update',
   '--spring.datasource.url='+E['LOAD_DB_URL'],'--spring.datasource.username='+E['LOAD_DB_USER'],'--spring.datasource.password='+E['LOAD_DB_PASSWORD'],
   '--spring.datasource.hikari.maximum-pool-size=10','--spring.datasource.hikari.register-mbeans=true','--server.tomcat.mbeanregistry.enabled=true','--server.tomcat.threads.max=200',
   '--spring.data.redis.host='+E['LOAD_REDIS_HOST'],'--spring.data.redis.port='+E['LOAD_REDIS_PORT'],
   '--spring.kafka.bootstrap-servers='+bootstrap,'--spring.kafka.consumer.group-id='+group,
   '--coupon.relay.enabled=true','--coupon.legacy.enabled=false']
  if batch_series:args+=['--spring.kafka.listener.auto-startup=false']
  if production_series:args+=['--coupon.consumer.batch-enabled='+str('production-batch' in label).lower(),'--spring.kafka.listener.auto-startup=true']
  app=start(args,'application.log');deadline=time.monotonic()+60
  assignment_marker='Adding newly assigned partitions: coupon-issue-requests-0' if production_series else 'partitions assigned: [coupon-issue-requests-0]'
  while assignment_marker not in (run/'application.log').read_text():
   if app.poll() is not None or time.monotonic()>deadline:raise RuntimeError('App startup failed')
   time.sleep(.2)
  now=datetime.now(timezone.utc);start_at=now-timedelta(days=1);end_at=now+timedelta(days=30);kst=timezone(timedelta(hours=9))
  coupon=post('/api/coupons',dict(name='reservation-load-'+label,description='isolated open arrival',couponType='FIXED_AMOUNT',discountValue=1000,minOrderAmount=0,maxDiscountAmount=1000,totalQuantity=100000,validDays=30,startDateTime=start_at.astimezone(kst).replace(tzinfo=None).isoformat(timespec='seconds'),endDateTime=end_at.astimezone(kst).replace(tzinfo=None).isoformat(timespec='seconds')))['data']['id']
  # Same exact second boundaries as the DB fixture. Uses production initialization method.
  init=start(helper('init',start_at.replace(microsecond=0).isoformat().replace('+00:00','Z'),end_at.replace(microsecond=0).isoformat().replace('+00:00','Z')),'init.log')
  if init.wait(timeout=60):raise RuntimeError('Reservation initialization failed')
  meta=dict(mode='kafka',path='redis-reservation',coupon_id=coupon,group=group,app_pid=app.pid,http_port=port,git_head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),burst_seconds=10,client_timeout_seconds=5,vus=[50,6500,1000],profile='100/s 20s -> 1200/s 10s -> 100/s 30s',planned=17000,retries=0,stock=100000,result_observation_target_ms=250,db_status_sample_ms=1000,redis_queue_sample_ms=250,hikari_max=10,tomcat_max=200,aof='always',scope='isolated DB/Redis/Kafka; host app JVM; no app CPU limit/no custom heap; sequential runs with new JVM/coupon, shared warmed test infrastructure',comparison='historical direct Kafka used shared DB; current includes Redis approval/read/relay/fsync. Not a controlled single-variable comparison.')
  if production_series:meta.update(production_batch_enabled='production-batch' in label,consumer_implementation='CouponBatchConsumer' if 'production-batch' in label else 'CouponIssueConsumer Spring RECORD',relay_implementation='production existing Relay',comparison='Service single RECORD vs service max10/10ms manual consumer. Same production Relay, AOF always and external observation; collection, transaction and offset granularity/framework differ. Not commit-only effect.')
  (run/'run-meta.json').write_text(json.dumps(meta,indent=2));(run/'working-tree.patch').write_bytes(subprocess.check_output(['git','diff','HEAD','--','src/main','build.gradle','docker-compose.yml']))
  for name in ['run-reservation-load.py','ReservationLoadSampler.java','k6-overload-comparison.js','analyze-overload-comparison.py']:shutil.copy2(scripts/name,run/name)
  monitor=start(helper('kafka',app.pid),'monitor.log');deadline=time.monotonic()+40
  while not (run/'progress.jsonl').exists() or not (run/'progress.jsonl').stat().st_size:
   if monitor.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Monitor startup failed')
   time.sleep(.2)
  initial=last_sample(run)
  assert initial['lag']==initial['pending']==initial['processing']==initial['history']==0,initial
  meta['batch_candidate_max']=(10 if 'batch-ten' in label else 1) if batch_series else None;meta['batch_wait_ms']=10 if batch_series else None;meta['consumer_profile_enabled']=consumer_series and 'consumer-on' in label;meta['relay_prepare_lua']=lua_series and (batch_series or consumer_series or label.startswith('lua'));meta['relay_profile_enabled']=profile_series and (lua_series or label.startswith('on'));meta['initial_sample']=initial;(run/'run-meta.json').write_text(json.dumps(meta,indent=2))
  sampler_thread=threading.Thread(target=resource_samples,args=(run,),daemon=True);sampler_thread.start()
  run_id=os.urandom(4).hex()
  load=start(['k6','run','--quiet','--out',f'json={run}/metrics.json.gz','-e','BASE_URL='+base,'-e','MODE=kafka','-e',f'COUPON_ID={coupon}','-e','BURST_SECONDS=10','-e','RUN_ID='+run_id,'-e',f'RESULT_PATH={run}/k6-summary.json',str(scripts/'k6-overload-comparison.js')],'k6.log')
  print(label+' load started coupon='+str(coupon),flush=True)
  finished=None;stable=None;previous=None;last_notice=time.monotonic()
  while True:
   if app.poll() is not None or monitor.poll() is not None:raise RuntimeError('App/monitor exited')
   sample=last_sample(run)
   if time.monotonic()-last_notice>=15:
    print(label+' '+json.dumps({k:sample[k] for k in ['history','observed_count','pending','processing','lag','pool_waiting']}),flush=True);last_notice=time.monotonic()
   if load.poll() is not None:
    if load.returncode:raise RuntimeError('k6 failed')
    if finished is None:finished=time.monotonic();print(label+' requests stopped; observing drain',flush=True)
    idle=all(sample[k]==0 for k in ['pool_active','pool_waiting','http_busy','lag','pending','processing'])
    if idle and previous==sample['history']:
     if stable is None:stable=time.monotonic()
     if time.monotonic()-stable>=35:break
    else:stable=None
    previous=sample['history']
    if time.monotonic()-finished>900:raise RuntimeError('Drain deadline exceeded')
   time.sleep(.5)
  meta['drain_completed']=True;meta['seconds_observed_after_k6_exit']=time.monotonic()-finished
  (run/'run-meta.json').write_text(json.dumps(meta,indent=2));(run/'monitor.stop').touch();monitor.wait(timeout=15)
  snap=start(helper('snapshot'),'snapshot.log')
  if snap.wait(timeout=120):raise RuntimeError('Snapshot failed')
  for label2,cmd in [('redis-config.txt',['CONFIG','GET','appendonly','appendfsync','dir','no-appendfsync-on-rewrite']),('redis-persistence.txt',['INFO','persistence'])]:
   (run/label2).write_text(subprocess.check_output(['docker','exec',E['LOAD_REDIS_CONTAINER'],'redis-cli',*cmd],text=True))
  with (run/'analysis.log').open('w') as f:
   subprocess.run(['python3',str(scripts/'analyze-overload-comparison.py'),str(run)],stdout=f,stderr=subprocess.STDOUT,check=True)
  print(label+' final DB snapshot and baseline consistency passed',flush=True)
 finally:
  (run/'monitor.stop').touch()
  for p in reversed(processes):
   if p.poll() is None:
    p.terminate()
    try:p.wait(timeout=15)
    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=10)
  if sampler_thread:sampler_thread.join(timeout=20)
  for f in files:f.close()

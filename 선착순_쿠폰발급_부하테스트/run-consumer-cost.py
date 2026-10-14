"""Usage: python3 run-consumer-cost.py on|off EMPTY_DIRECTORY CLASSPATH_FILE [burst_seconds]
Requires local MySQL/Redis, isolated Kafka at localhost:39092; creates its own app on 28080.
KAFKA_GROUP must identify a caught-up test group if the broker already has messages.
"""
import json,os,shutil,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
from datetime import datetime,timedelta
enabled=sys.argv[1]; assert enabled in ('on','off')
mode='kafka'
out=Path(sys.argv[2]).resolve();out.mkdir(parents=True,exist_ok=True)
if any(out.iterdir()):raise SystemExit('Use empty output directory')
cp=Path(sys.argv[3]).read_text().strip();burst=int(sys.argv[4]) if len(sys.argv)>4 else 10
scripts=Path(__file__).resolve().parent; group=os.environ.get('KAFKA_GROUP','overload-'+uuid.uuid4().hex[:8])
processes=[];files=[]
def start(cmd,name):
 f=(out/name).open('w');files.append(f);p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT);processes.append(p);return p
def post(path,data):
 req=urllib.request.Request('http://localhost:28080'+path,json.dumps(data).encode(),{'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=20) as r:return json.load(r)
def mysql(query):
 return subprocess.run(['docker','compose','exec','-T','mysql','mysql','-uroot','-p1234','-N','-B','coupon_db','-e',query],capture_output=True,text=True,check=True).stdout
digest_query="SELECT DIGEST,DIGEST_TEXT,COUNT_STAR,SUM_TIMER_WAIT,SUM_LOCK_TIME,SUM_ROWS_AFFECTED,SUM_ROWS_EXAMINED FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='coupon_db' ORDER BY DIGEST;"
file_query="SELECT EVENT_NAME,COUNT_STAR,SUM_TIMER_WAIT,COUNT_WRITE,SUM_TIMER_WRITE,SUM_NUMBER_OF_BYTES_WRITE FROM performance_schema.file_summary_by_event_name WHERE EVENT_NAME LIKE 'wait/io/file/innodb/%' OR EVENT_NAME LIKE 'wait/io/file/sql/%';"
try:
 args=['java',f'-Dconsumer.profile.enabled={str(enabled=="on").lower()}',f'-Dconsumer.profile.dir={out}','-cp',cp,'com.assignment.coupon_system.issuerequest.profiling.ConsumerProfileApplication',
  '--server.port=28080','--spring.jpa.show-sql=false','--spring.datasource.hikari.register-mbeans=true',
  '--server.tomcat.mbeanregistry.enabled=true','--spring.kafka.bootstrap-servers=localhost:39092',
  '--spring.kafka.consumer.group-id='+group]
 if mode=='sync':args+=['--spring.kafka.listener.auto-startup=false']
 app=start(args,'application.log');deadline=time.monotonic()+60
 marker='Started CouponSystemApplication' if mode=='sync' else 'partitions assigned: [coupon-issue-requests-0]'
 while marker not in (out/'application.log').read_text():
  if app.poll() is not None or time.monotonic()>deadline:raise RuntimeError('App startup failed')
  time.sleep(.2)
 now=datetime.now();coupon=post('/api/coupons',dict(name='overload-'+mode+'-'+uuid.uuid4().hex[:8],description='Overload timeout vs final DB',couponType='FIXED_AMOUNT',discountValue=1000,minOrderAmount=0,maxDiscountAmount=1000,totalQuantity=100000,validDays=30,startDateTime=(now-timedelta(days=1)).isoformat(timespec='seconds'),endDateTime=(now+timedelta(days=30)).isoformat(timespec='seconds')))['data']['id']
 if mode=='sync':post(f'/api/coupons/{coupon}/stock/init',{})
 meta=dict(instrumentation=enabled,mode=mode,coupon_id=coupon,group=group,app_pid=app.pid,git_head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),burst_seconds=burst,client_timeout_seconds=5,vus=[50,6500,1000],profile=f'100/s 20s -> 1200/s {burst}s -> 100/s 30s',planned=5000+1200*burst,retries=0,db_status_sample_ms=1000,pool_sample_ms=250,kafka_result_observation_ms=250,drain_rule='after k6 exit: pool active/waiting and Tomcat busy all zero, DB history unchanged for 35s, plus lag=0 for Kafka; 600s limit',scope='local source paths with isolated Kafka; same local MySQL and Redis; no production source edits')
 (out/'run-meta.json').write_text(json.dumps(meta,indent=2))
 (out/'working-tree.patch').write_bytes(subprocess.check_output(['git','diff','HEAD','--','src/main','build.gradle']))
 for name in ['run-consumer-cost.py','ConsumerCostSampler.java','k6-overload-comparison.js','consumer-profile.init.gradle']:shutil.copy2(scripts/name,out/name)
 monitor=start(['java','--add-modules','jdk.attach','-cp',cp,str(scripts/'ConsumerCostSampler.java'),str(coupon),str(out),'localhost:39092',group,mode,str(app.pid)],'monitor.log')
 deadline=time.monotonic()+30
 while not (out/'progress.jsonl').exists() or not (out/'progress.jsonl').stat().st_size:
  if monitor.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Monitor startup failed')
  time.sleep(.2)
 (out/'db-digests-before.tsv').write_text(mysql(digest_query))
 (out/'db-file-io-before.tsv').write_text(mysql(file_query))
 (out/'db-settings.tsv').write_text(mysql("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('innodb_flush_log_at_trx_commit','sync_binlog','log_bin','performance_schema','innodb_buffer_pool_size');"))
 initial=json.loads((out/'progress.jsonl').read_text().splitlines()[-1])
 if mode=='kafka' and initial['lag']!=0:raise RuntimeError('Initial Kafka lag must be zero')
 meta['initial_sample']=initial;(out/'run-meta.json').write_text(json.dumps(meta,indent=2))
 resources=start(['python3',str(scripts/'sample-consumer-resources.py'),str(out)],'resources-monitor.log')
 load=start(['k6','run','--quiet','--out',f'json={out}/metrics.json.gz','-e','BASE_URL=http://localhost:28080','-e',f'MODE={mode}','-e',f'COUPON_ID={coupon}','-e',f'BURST_SECONDS={burst}','-e',f'RUN_ID={uuid.uuid4().hex[:8]}','-e',f'RESULT_PATH={out}/k6-summary.json',str(scripts/'k6-overload-comparison.js')],'k6.log')
 print(f'{enabled} load started, coupon={coupon}, burst={burst}s',flush=True)
 finished=None;stable=None;previous=None
 while True:
  if app.poll() is not None or monitor.poll() is not None:raise RuntimeError('App/monitor died')
  if load.poll() is not None:
   if load.returncode:raise RuntimeError('k6 failed')
   if finished is None:finished=time.monotonic();print('Requests stopped; observing server drain',flush=True)
   # Read complete lines only, as monitor may be appending concurrently.
   raw=(out/'progress.jsonl').read_text();lines=raw[:raw.rfind('\n')].splitlines();sample=json.loads(lines[-1])
   idle=sample['pool_active']==0 and sample['pool_waiting']==0 and sample['http_busy']==0 and sample.get('lag',0)==0
   if idle and previous==sample['history']:
    if stable is None:stable=time.monotonic()
    if time.monotonic()-stable>=35:break
   else:stable=None
   previous=sample['history']
   if time.monotonic()-finished>600:raise RuntimeError('Drain observation deadline exceeded')
  time.sleep(.5)
 (out/'db-digests-after.tsv').write_text(mysql(digest_query))
 (out/'db-file-io-after.tsv').write_text(mysql(file_query))
 (out/'final-history.tsv').write_text(mysql(f'SELECT id,user_id FROM issued_coupon WHERE coupon_id={coupon} ORDER BY id;'))
 (out/'final-results.tsv').write_text(mysql(f'SELECT request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id={coupon} ORDER BY id;'))
 (out/'final-stock.tsv').write_text(mysql(f'SELECT total_quantity,issued_quantity FROM coupon WHERE id={coupon};'))
 if mode=='sync':
  for key,command in [('redis-stock.txt',['GET',f'coupon:{coupon}:stock']),('redis-users.txt',['SMEMBERS',f'coupon:{coupon}:users'])]:
   (out/key).write_text(subprocess.check_output(['docker','compose','exec','-T','redis','redis-cli','--raw']+command,text=True))
 meta['drain_completed']=True;meta['seconds_observed_after_k6_exit']=time.monotonic()-finished
 (out/'run-meta.json').write_text(json.dumps(meta,indent=2))
 print('Drain stable 35s; final DB snapshot saved',flush=True)
finally:
 (out/'monitor.stop').touch()
 for p in reversed(processes):
  if p.poll() is None:
   if p in [locals().get('monitor'),locals().get('resources')]:
    try:p.wait(timeout=15);continue
    except subprocess.TimeoutExpired:pass
   p.terminate()
   try:p.wait(timeout=15)
   except subprocess.TimeoutExpired:p.kill();p.wait(timeout=10)
 for f in files:f.close()

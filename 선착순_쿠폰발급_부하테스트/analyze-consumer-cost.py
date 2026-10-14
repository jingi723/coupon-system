"""Consumer cost analysis. Run after each series run and base consistency analyzer.
Uses non-overlapping DB/offset/poll spans on the same Consumer thread; listener and
transaction-commit callback spans are nested and never added to that denominator.
"""
import collections,csv,json,math,sys
from pathlib import Path
out=Path(sys.argv[1]);base=json.loads((out/'analysis.json').read_text());origin=base['origin_ms']
meta=json.loads((out/'run-meta.json').read_text());ready=json.loads((out/'profile-ready.json').read_text())
requests=list(csv.DictReader((out/'requests.csv').open()))
observed=list(csv.DictReader((out/'observed-results.csv').open()))
progress=[json.loads(x) for x in (out/'progress.jsonl').read_text().splitlines()]
spans=list(csv.DictReader((out/'consumer-spans.csv').open()))
for s in spans:
 for k in ['start_ns','end_ns','thread_id','aux']:s[k]=int(s[k])
 s['start_s']=(ready['anchor_wall_ms']-origin)/1000+(s['start_ns']-ready['anchor_nano'])/1e9
 s['end_s']=(ready['anchor_wall_ms']-origin)/1000+(s['end_ns']-ready['anchor_nano'])/1e9
 s['duration_ms']=(s['end_ns']-s['start_ns'])/1e6
bykind=collections.defaultdict(list)
for s in spans:bykind[s['kind']].append(s)

def pct(values,p):
 values=sorted(values)
 if not values:return None
 x=(len(values)-1)*p/100;i=int(x);j=min(i+1,len(values)-1)
 return values[i]+(values[j]-values[i])*(x-i)
def stats(values):
 return dict(count=len(values),p50_ms=pct(values,50),p95_ms=pct(values,95),p99_ms=pct(values,99),sum_ms=sum(values),mean_ms=sum(values)/len(values) if values else None,max_ms=max(values,default=None))
def rate_interval(a,b):
 return sum(a<=(int(r['observed_ms'])-origin)/1000<b for r in observed)/(b-a)
def clip_sum(kind,a,b):
 return sum(max(0,min(s['end_s'],b)-max(s['start_s'],a)) for s in bykind[kind])
progress_times=[(p['time_ms']-origin)/1000 for p in progress]
windows=[]
for a in range(0,math.ceil(base['last_kafka_result_observed_s']/5)*5,5):
 b=a+5;samples=[p for p,t in zip(progress,progress_times) if a<=t<b]
 lag=[p['lag'] for p in samples]
 steady=len(lag)>=15 and min(lag)>=500
 w=dict(start_s=a,end_s=b,sample_count=len(lag),min_lag=min(lag,default=None),max_lag=max(lag,default=None),sustained_backlog=steady,db_observed_per_s=rate_interval(a,b))
 if meta['instrumentation']=='on':
  w['db_proxy_completions_per_s']=sum(a<=s['end_s']<b and s['aux']==1 for s in bykind['db_proxy'])/5
  w['db_s']=clip_sum('db_proxy',a,b);w['offset_s']=clip_sum('commitSync',a,b)+clip_sum('commitAsync',a,b)
  w['poll_s']=clip_sum('poll',a,b);w['other_s']=5-w['db_s']-w['offset_s']-w['poll_s']
  w['db_commit_subset_s']=clip_sum('db_commit_callbacks',a,b)
  w['listener_s_nested']=clip_sum('listener_method',a,b)
 windows.append(w)
steady=[w for w in windows if w['sustained_backlog']]
assert steady,'No sustained backlog windows'
# One-second rates provide the finer time series; observed rows are commit-visible, not entity timestamps.
with (out/'db-completions-per-second.csv').open('w') as f:
 w=csv.writer(f);w.writerow(['second','observed_completed','proxy_completed'])
 for sec in range(math.ceil(base['last_kafka_result_observed_s'])+1):
  w.writerow([sec,int(rate_interval(sec,sec+1)),sum(sec<=s['end_s']<sec+1 and s['aux']==1 for s in bykind['db_proxy']) if bykind['db_proxy'] else ''])
seconds=5*len(steady)
aggregate=dict(seconds=seconds,windows=len(steady),observed_per_s=sum(w['db_observed_per_s'] for w in steady)/len(steady),per_window_rate_min=min(w['db_observed_per_s'] for w in steady),per_window_rate_max=max(w['db_observed_per_s'] for w in steady))
if meta['instrumentation']=='on':
 for key in ['db_s','offset_s','poll_s','other_s','db_commit_subset_s']:
  aggregate[key]=sum(w[key] for w in steady)
  aggregate[key[:-2]+'_percent']=aggregate[key]/seconds*100
 aggregate['proxy_completed_per_s']=sum(w['db_proxy_completions_per_s'] for w in steady)/len(steady)
# Verify boundaries and non-overlap using raw nanoseconds, independently of wall-clock alignment.
validation={}
if spans:
 ids={r['request_id'] for r in observed}
 maps={kind:{s['request_id']:s for s in bykind[kind]} for kind in ['db_proxy','db_commit_callbacks','listener_method']}
 for kind,m in maps.items():assert set(m)==ids and len(bykind[kind])==len(ids),(kind,len(m),len(ids))
 for rid in ids:
  d=maps['db_proxy'][rid];l=maps['listener_method'][rid];c=maps['db_commit_callbacks'][rid]
  assert l['start_ns']<=d['start_ns']<=c['start_ns']<=c['end_ns']<=d['end_ns']<=l['end_ns']
 timed=sorted([s for kind in ['db_proxy','commitSync','commitAsync','poll'] for s in bykind[kind]],key=lambda s:s['start_ns'])
 assert len({s['thread_id'] for s in timed})==1,'Multiple threads cannot share time denominator'
 assert all(a['end_ns']<=b['start_ns'] for a,b in zip(timed,timed[1:])),'Overlapping exclusive spans'
 commits=[s for s in bykind['commitSync'] if s['request_id'] in ids]
 assert len(commits)==len(ids) and len({s['request_id'] for s in commits})==len(ids)
 assert all(s['start_ns']>=maps['listener_method'][s['request_id']]['end_ns'] for s in commits)
 assert not bykind['commitAsync'] and all(s['aux']>=0 for s in timed)
 validation=dict(nested_boundaries=True,exclusive_spans_nonoverlap=True,single_consumer_thread=True,one_commitSync_per_request=True,commit_after_listener=True,db_and_commit_coverage=len(ids))
# CPU is a separate concurrent resource metric, not a slice of the Consumer wall-time denominator.
def inside(t):return any(w['start_s']<=t<w['end_s'] for w in steady)
cpu=[]
for a,b in zip(progress,progress[1:]):
 t=((a['time_ms']+b['time_ms'])/2-origin)/1000
 if inside(t):cpu.append(100*(b['app_process_cpu_ns']-a['app_process_cpu_ns'])/(b['monotonic_ns']-a['monotonic_ns']))
docker=[json.loads(x) for x in (out/'docker-stats.jsonl').read_text().splitlines()]
db_cpu=[];broker_cpu=[]
for sample in docker:
 if not inside((sample['end_ms']-origin)/1000):continue
 for r in sample['rows']:
  if r['Name']=='coupon-mysql':db_cpu.append(float(r['CPUPerc'].strip('%')))
  else:broker_cpu.append(float(r['CPUPerc'].strip('%')))
def numeric(v):return dict(samples=len(v),mean=sum(v)/len(v) if v else None,p95=pct(v,95),max=max(v,default=None))
# Server-side query digests are global DB deltas; they overlap DB proxy spans and are not additive shares.
def digests(name):
 rows={}
 for line in (out/name).read_text().splitlines():
  r=line.split('\t');rows[r[0]]=(r[1],list(map(int,r[2:])))
 return rows
before=digests('db-digests-before.tsv');after=digests('db-digests-after.tsv');deltas=[]
for key,(query,v) in after.items():
 old=before.get(key,('',[0]*len(v)))[1];d=[x-y for x,y in zip(v,old)]
 if d[0]>0:deltas.append(dict(digest=key,sql=query,count=d[0],server_total_ms=d[1]/1e9,server_mean_ms=d[1]/1e9/d[0],server_lock_ms=d[2]/1e9,rows_affected=d[3],rows_examined=d[4]))
deltas.sort(key=lambda r:r['server_total_ms'],reverse=True)
(out/'sql-digest-deltas.json').write_text(json.dumps(deltas,indent=2))
def io_rows(name):
 return {r[0]:list(map(int,r[1:])) for r in (line.split('\t') for line in (out/name).read_text().splitlines())}
io_before=io_rows('db-file-io-before.tsv');io_after=io_rows('db-file-io-after.tsv');io_delta=[]
for event,values in io_after.items():
 d=[v-o for v,o in zip(values,io_before.get(event,[0]*len(values)))]
 if d[0]:io_delta.append(dict(event=event,operations=d[0],total_wait_ms=d[1]/1e9,writes=d[2],write_wait_ms=d[3]/1e9,bytes_written=d[4]))
io_delta.sort(key=lambda x:x['total_wait_ms'],reverse=True)
(out/'db-file-io-deltas.json').write_text(json.dumps(io_delta,indent=2))

# Per-second sampled global write counters.
status=[p for p in progress if p.get('db_sample')]
write_keys=['Innodb_os_log_written','Innodb_log_writes','Innodb_log_waits','Innodb_data_written','Innodb_data_writes','Com_commit','Com_insert','Com_update','Handler_commit']
write_delta={key:status[-1][key]-status[0][key] for key in write_keys if key in status[0] and key in status[-1]}
intervals=[(b['monotonic_ns']-a['monotonic_ns'])/1e6 for a,b in zip(progress,progress[1:])]
cohorts={}
for label,a,b in [('baseline',0,20),('burst',20,30),('recovery',30,60),('late',60,float('inf'))]:
 rs=[r for r in requests if a<=float(r['sent_s'])<b]
 cohorts[label]=dict(requests=len(rs),http=stats([float(r['response_ms']) for r in rs if r['response_ms']]),final_observed=stats([float(r['final_observed_ms']) for r in rs if r['final_observed_ms']]))
pre_db=[];post_db=[]
if spans:
 db_map={s['request_id']:s for s in bykind['db_proxy']}
 with (out/'request-timeline.csv').open('w') as f:
  writer=csv.writer(f);writer.writerow(['request_id','sent_s','db_start_s','db_end_s','pre_db_elapsed_ms','db_proxy_ms','result_observed_elapsed_ms'])
  for r in requests:
   d=db_map[r['request_id']];before=(d['start_s']-float(r['sent_s']))*1000
   after=float(r['final_observed_ms'])-before-d['duration_ms']
   pre_db.append(before);post_db.append(after)
   writer.writerow([r['request_id'],r['sent_s'],d['start_s'],d['end_s'],before,d['duration_ms'],r['final_observed_ms']])

summary=dict(instrumentation=meta['instrumentation'],cohorts_by_actual_send_time=cohorts,
 pre_db_elapsed_includes_admission_broker_and_local_queue=stats(pre_db),post_db_result_observation_elapsed=stats(post_db),
 all_http=stats([float(r['response_ms']) for r in requests if r['response_ms']]),all_final_observed=stats([float(r['final_observed_ms']) for r in requests if r['final_observed_ms']]),
 spans={kind:stats([s['duration_ms'] for s in rows]) for kind,rows in bykind.items()},
 backlog_windows=windows,sustained_backlog=aggregate,boundary_validation=validation,
 resource_cpu_percent_of_one_core=dict(app=numeric(cpu),db=numeric(db_cpu),broker=numeric(broker_cpu)),
 db_global_write_counter_deltas=write_delta,
 kafka_jmx_commit_total_first=status[0].get('kafka_commit-total'),kafka_jmx_commit_total_last=status[-1].get('kafka_commit-total'),
 kafka_jmx_commit_latency_avg_window_last_ms=status[-1].get('kafka_commit-latency-avg'),kafka_jmx_commit_latency_max_window_last_ms=status[-1].get('kafka_commit-latency-max'),
 observation_intervals_ms=numeric(intervals),observed_rows=len(observed),unique_observed_ids=len({r['request_id'] for r in observed}),
 observed_ids_equal_final={r['request_id'] for r in observed}=={l.split('\t')[0] for l in (out/'final-results.tsv').read_text().splitlines()},
 docker_samples=len(docker),docker_errors=sum(x['exit']!=0 for x in docker),max_clock_step_error_ms=base['max_clock_step_error_ms'])
assert summary['observed_ids_equal_final'] and len(observed)==len({r['request_id'] for r in observed})
(out/'cost-analysis.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
print(json.dumps(dict(mode=meta['instrumentation'],backlog=aggregate,spans=summary['spans'],top_sql=deltas[:6],cpu=summary['resource_cpu_percent_of_one_core']),indent=2))

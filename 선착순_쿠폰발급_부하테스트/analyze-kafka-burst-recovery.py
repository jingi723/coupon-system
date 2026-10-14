"""Analyze Kafka burst requests by their send time, joining committed DB observations."""
import collections
import csv
from datetime import datetime
import gzip
import json
from pathlib import Path
import re
import subprocess
import sys

out=Path(sys.argv[1]); meta=json.loads((out/'run-meta.json').read_text())
origin=int(re.search(r'RUN_ORIGIN_MS=(\d+)',(out/'k6.log').read_text()).group(1))
observed={r['request_id']:r for r in csv.DictReader((out/'observed-results.csv').open())}
scenario_origins=collections.defaultdict(set)
requests=[]; started=0; dropped=collections.Counter(); scenario_counts=collections.Counter()
with gzip.open(out/'metrics.json.gz','rt') as f:
 for line in f:
  x=json.loads(line)
  if x['type']!='Point': continue
  d=x['data']; tags=d.get('tags',{})
  if x['metric']=='issue_started': started+=int(d['value'])
  if x['metric']=='dropped_iterations': dropped[tags['scenario']]+=int(d['value'])
  if x['metric']!='issue_response_ms': continue
  scenario_origins[tags['scenario']].add(int(tags['origin_ms'])-origin)
  sent=int(tags['sent_ms']); relative=(sent-origin)/1000
  result=observed.get(tags['request_id'])
  r=dict(request_id=tags['request_id'],user_id=int(tags['user_id']),scenario=tags['scenario'],
         phase='baseline' if relative<20 else 'burst' if relative<23 else 'recovery' if relative<53 else 'late',
         sent_s=relative,api_ms=d['value'],status=int(tags['status']),outcome=tags['outcome'],error_code=int(tags['error_code']),
         final_status=result['status'] if result else None,
         final_observed_ms=int(result['observed_ms'])-sent if result else None,
         final_observed_s=(int(result['observed_ms'])-origin)/1000 if result else None)
  requests.append(r); scenario_counts[r['scenario']]+=1
progress=[json.loads(line) for line in (out/'progress.jsonl').read_text().splitlines()]
for p in progress: p['seconds']=(p['offset_sample_end_ms']-origin)/1000
pool=[dict(seconds=(int(r['time_ms'])-origin)/1000,waiting=int(r['waiting'])) for r in csv.DictReader((out/'pool.csv').open())]

def percentile(values,q):
 values=sorted(v for v in values if v is not None)
 if not values:return None
 pos=(len(values)-1)*q/100; lo=int(pos); hi=min(lo+1,len(values)-1)
 return values[lo]+(values[hi]-values[lo])*(pos-lo)

def window(a,b):
 rows=[r for r in requests if a<=r['sent_s']<b]
 waits=[p['waiting'] for p in pool if a<=p['seconds']<b]
 locks=[p['Innodb_row_lock_current_waits'] for p in progress if a<=(p['db_observation_end_ms']-origin)/1000<b and 'Innodb_row_lock_current_waits' in p]
 return dict(start_s=a,end_s=b,requests=len(rows),
  api_p95_ms=percentile([r['api_ms'] for r in rows],95),api_p99_ms=percentile([r['api_ms'] for r in rows],99),
  final_p95_ms=percentile([r['final_observed_ms'] for r in rows],95),final_p99_ms=percentile([r['final_observed_ms'] for r in rows],99),
  max_pool_waiting=max(waits,default=None),pool_samples=len(waits),max_row_lock_waits=max(locks,default=None))
baseline=[window(a,a+5) for a in [5,10,15]]
recovery=[window(a,a+5) for a in range(23,53,5)]
thresholds={kind:2*max(w[kind+'_p95_ms'] for w in baseline) for kind in ['api','final']}
confirmed={}
for kind,threshold in thresholds.items():
 for w in recovery:w[kind+'_meets_rule']=w[kind+'_p95_ms'] is not None and w[kind+'_p95_ms']<=threshold and w['pool_samples']>=15 and w['max_pool_waiting']==0
 confirmed[kind]=next((recovery[i]['end_s']-23 for i in range(1,len(recovery)) if recovery[i-1][kind+'_meets_rule'] and recovery[i][kind+'_meets_rule']),None)
after=[p for p in progress if p['seconds']>=23]
last_positive=max((i for i,p in enumerate(after) if p['lag']>0),default=-1)
first_empty=next((p['seconds']-23 for p in after if p['lag']==0),None)
sustained_empty=after[last_positive+1]['seconds']-23 if last_positive+1<len(after) else None
# A full final query validates the incremental observer and all user/request ID sets.
mysql=['docker','compose','exec','-T','mysql','mysql','-uroot','-p1234','-N','-B','coupon_db','-e']
def sql(query):return subprocess.run(mysql+[query],capture_output=True,text=True,check=True).stdout
coupon=meta['coupon_id']
final=sql(f'SELECT request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id={coupon} ORDER BY id;')
(out/'final-results.tsv').write_text(final)
rows=[line.split('\t') for line in final.splitlines()]
history=sql(f'SELECT id,user_id FROM issued_coupon WHERE coupon_id={coupon} ORDER BY id;')
(out/'final-history.tsv').write_text(history)
hist=[line.split('\t') for line in history.splitlines()]
stock,quantity=map(int,sql(f'SELECT total_quantity,issued_quantity FROM coupon WHERE id={coupon};').split())
accepted={r['request_id']:r['user_id'] for r in requests if r['outcome']=='accepted'}
result_map={r[0]:int(r[1]) for r in rows}
issued_rows=[r for r in rows if r[2]=='ISSUED']
history_map={int(r[0]):int(r[1]) for r in hist}
verification=dict(coupon_id=coupon,started=started,response_records=len(requests),accepted=len(accepted),
    results=len(rows),issued=len(issued_rows),rejected=sum(r[2]=='REJECTED' for r in rows),
    history=len(hist),unique_history_users=len({r[1] for r in hist}),issued_quantity=quantity,total_quantity=stock,
    accepted_request_user_map_matches_db=accepted==result_map,
    observed_result_ids_match_final_db=set(observed)==set(result_map),
    issued_results_match_history={int(r[3]):int(r[1]) for r in issued_rows}==history_map,
    missing_results=len(set(accepted)-set(result_map)),unexpected_results=len(set(result_map)-set(accepted)),
    final_lag=progress[-1]['lag'],counts_match=len(issued_rows)==len(hist)==len({r[1] for r in hist})==quantity)
intervals=[progress[i]['query_start_ms']-progress[i-1]['query_start_ms'] for i in range(1,len(progress))]
clock_errors=[abs((b['query_start_ms']-a['query_start_ms'])-(b['monotonic_ns']-a['monotonic_ns'])/1e6) for a,b in zip(progress,progress[1:]) if 'monotonic_ns' in a and 'monotonic_ns' in b]
summary=dict(max_clock_step_error_ms=max(clock_errors,default=None),origin_ms=origin,nominal_planned=8600,scenario_sent=dict(scenario_counts),scenario_origin_offsets_ms={k:sorted(v) for k,v in scenario_origins.items()},
 scenario_actual_send_ranges_s={k:dict(first=min(r['sent_s'] for r in requests if r['scenario']==k),last=max(r['sent_s'] for r in requests if r['scenario']==k)) for k in scenario_counts},dropped_by_scenario={k:dropped[k] for k in ['baseline','burst','recovery']},
 outcomes=dict(collections.Counter(r['outcome'] for r in requests)),timeouts=sum(r['error_code']==1050 for r in requests),
 baseline_windows=baseline,recovery_windows=recovery,thresholds_ms=thresholds,recovery_confirmed_after_burst_s=confirmed,
 phase_windows=[window(0,20),window(20,23),window(23,53)],
 max_lag=max(p['lag'] for p in progress),max_lag_at_s=max(progress,key=lambda p:p['lag'])['seconds'],
 burst_results_not_yet_observed_at_burst_end=sum(r['phase']=='burst' and r['final_observed_s']>23 for r in requests if r['final_observed_s'] is not None),first_zero_lag_after_burst_s=first_empty,
 sustained_zero_lag_after_burst_s=sustained_empty,
 max_pool_waiting=max(p['waiting'] for p in pool),max_row_lock_waits=max(p.get('Innodb_row_lock_current_waits',0) for p in progress),
 db_poll_intervals_ms=dict(min=min(intervals),p95=percentile(intervals,95),max=max(intervals)),
 last_send_s=max(r['sent_s'] for r in requests),last_result_observed_s=max(r['final_observed_s'] for r in requests if r['final_observed_s'] is not None),
 last_burst_result_after_burst_s=max(r['final_observed_s'] for r in requests if r['phase']=='burst' and r['final_observed_s'] is not None)-23,
 verification=verification)
(out/'analysis.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
with (out/'requests.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(requests[0]));w.writeheader();w.writerows(sorted(requests,key=lambda r:r['sent_s']))
print(json.dumps(summary,ensure_ascii=False,indent=2))
assert started==len(requests) and verification['missing_results']==0 and verification['counts_match'] and verification['accepted_request_user_map_matches_db'] and verification['observed_result_ids_match_final_db'] and verification['issued_results_match_history'] and verification['final_lag']==0

assert clock_errors and max(clock_errors)<50, 'Clock discontinuity invalidates timing comparison'

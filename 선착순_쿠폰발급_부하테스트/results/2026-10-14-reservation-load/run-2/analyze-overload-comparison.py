"""Analyze attempted HTTP calls vs independently snapshotted final DB state.
Usage: python3 analyze-overload-comparison.py RESULT_DIRECTORY
"""
import collections,csv,gzip,json,re,sys
from pathlib import Path
out=Path(sys.argv[1]);m=json.loads((out/'run-meta.json').read_text())
origin=int(re.search(r'RUN_ORIGIN_MS=(\d+)',(out/'k6.log').read_text()).group(1))
history={int(r.split('\t')[1]):int(r.split('\t')[0]) for r in (out/'final-history.tsv').read_text().splitlines()}
history_count=len((out/'final-history.tsv').read_text().splitlines())
final={r[0]:r[1:] for r in (line.split('\t') for line in (out/'final-results.tsv').read_text().splitlines())}
observed={r['request_id']:int(r['observed_ms']) for r in csv.DictReader((out/'observed-results.csv').open())}
started={};responses={};dropped=collections.Counter();origins=collections.defaultdict(set)
with gzip.open(out/'metrics.json.gz','rt') as f:
 for line in f:
  item=json.loads(line)
  if item['type']!='Point':continue
  d=item['data'];t=d.get('tags',{})
  if item['metric']=='dropped_iterations':dropped[t['scenario']]+=int(d['value'])
  if item['metric'] not in ('issue_started','issue_response_ms'):continue
  rid=t['request_id'];origins[t['scenario']].add(int(t['origin_ms'])-origin)
  if item['metric']=='issue_started':started[rid]=t
  else:responses[rid]=(t,d['value'])
requests=[];matrix=collections.Counter()
for rid,t in started.items():
 response=responses.get(rid);rt=response[0] if response else {};sent=int(t['sent_ms']);uid=int(t['user_id'])
 outcome=rt.get('outcome','no_response_record');issued=uid in history
 final_status=final[rid][1] if rid in final else 'ISSUED' if issued else 'NO_RECORD'
 matrix[outcome+' / '+final_status]+=1
 requests.append(dict(request_id=rid,user_id=uid,scenario=t['scenario'],sent_s=(sent-origin)/1000,
  outcome=outcome,status=int(rt.get('status',0)),error_code=int(rt.get('error_code',0)),
  response_ms=response[1] if response else None,issued=issued,final_status=final_status,
  final_observed_ms=observed[rid]-sent if rid in observed else None))
def percentile(values,p):
 v=sorted(x for x in values if x is not None)
 if not v:return None
 pos=(len(v)-1)*p/100;lo=int(pos);hi=min(lo+1,len(v)-1)
 return v[lo]+(v[hi]-v[lo])*(pos-lo)
def aggregate(rs):
 return dict(attempted=len(rs),outcomes=dict(collections.Counter(r['outcome'] for r in rs)),
  issued=sum(r['issued'] for r in rs),not_issued=sum(not r['issued'] for r in rs),
  response_p95_ms=percentile([r['response_ms'] for r in rs],95),response_p99_ms=percentile([r['response_ms'] for r in rs],99),
  final_observed_p95_ms=percentile([r['final_observed_ms'] for r in rs],95))
progress=[json.loads(l) for l in (out/'progress.jsonl').read_text().splitlines()]
clock_errors=[abs((b['time_ms']-a['time_ms'])-(b['monotonic_ns']-a['monotonic_ns'])/1e6) for a,b in zip(progress,progress[1:])]
stock,quantity=map(int,(out/'final-stock.tsv').read_text().split())
accepted={r['request_id']:r['user_id'] for r in requests if r['outcome']=='accepted'}
completed={r['user_id'] for r in requests if r['outcome']=='completed'}
sent_users={r['user_id'] for r in requests}
end=20+m['burst_seconds']
summary=dict(mode=m['mode'],origin_ms=origin,planned=m['planned'],attempted=len(started),response_records=len(responses),
 dropped_by_scenario={k:dropped[k] for k in ['baseline','burst','recovery']},matrix=dict(matrix),
 status_codes=dict(collections.Counter(str(r['status']) for r in requests)),error_codes=dict(collections.Counter(str(r['error_code']) for r in requests if r['error_code'])),
 timeouts=sum(r['error_code']==1050 for r in requests),
 scenarios={k:dict(**aggregate([r for r in requests if r['scenario']==k]),first_sent_s=min(r['sent_s'] for r in requests if r['scenario']==k),last_sent_s=max(r['sent_s'] for r in requests if r['scenario']==k)) for k in ['baseline','burst','recovery']},
 scenario_origin_offsets_ms={k:sorted(v) for k,v in origins.items()},
 nominal_burst_send_window=aggregate([r for r in requests if 20<=r['sent_s']<end]),
 after_burst_windows=[dict(start_after_burst_s=s,**aggregate([r for r in requests if end+s<=r['sent_s']<end+s+5])) for s in range(0,30,5)],
 max_pool_waiting=max(p['pool_waiting'] for p in progress),max_http_busy=max(p['http_busy'] for p in progress),
 max_row_lock_waits=max(p.get('Innodb_row_lock_current_waits',0) for p in progress),
 max_lag=max(p.get('lag',0) for p in progress) if m['mode']=='kafka' else None,
 initial_offset=progress[0].get('end'),final_offset=progress[-1].get('end'),final_lag=progress[-1].get('lag'),
 max_clock_step_error_ms=max(clock_errors),timing_valid=max(clock_errors)<50,
 drain_completed=m['drain_completed'],observation_after_k6_exit_s=m['seconds_observed_after_k6_exit'],
 final_db=dict(history=history_count,unique_users=len(history),issued_quantity=quantity,total_quantity=stock,results=len(final),
  completed_response_missing_history=len(completed-set(history)),issued_users_not_attempted=len(set(history)-sent_users),
  accepted_missing_result=len(set(accepted)-set(final)),
  accepted_user_map_matches_results=accepted=={rid:int(r[0]) for rid,r in final.items()} if m['mode']=='kafka' else None),
 last_request_sent_s=max(r['sent_s'] for r in requests),
 first_sample_all_final_history_s=next(((p['sample_end_ms']-origin)/1000 for p in progress if p['history']==history_count),None),
 last_kafka_result_observed_s=(max(observed.values())-origin)/1000 if observed else None)
if m['mode']=='sync':
 redis_users=set(map(int,(out/'redis-users.txt').read_text().split()));remaining=int((out/'redis-stock.txt').read_text())
 summary['redis']=dict(users=len(redis_users),remaining=remaining,user_set_matches_db=redis_users==set(history),stock_matches_db=remaining==stock-quantity)
with (out/'requests.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(requests[0]));w.writeheader();w.writerows(sorted(requests,key=lambda r:r['sent_s']))
(out/'analysis.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
print(json.dumps(summary,ensure_ascii=False,indent=2))
assert len(started)==len(responses),'Missing HTTP response records'
assert history_count==len(history)==quantity,'DB quantity/history mismatch'
assert not (completed-set(history)),'Success response without issuance'
assert not (set(history)-sent_users),'Unexpected DB issuance'
if m['mode']=='kafka':
 assert set(accepted)<=set(final),'Accepted but no final result'
 assert summary['final_lag']==0,'Kafka not drained'
 assert {int(r[2]):int(r[0]) for r in final.values() if r[1]=='ISSUED'}=={ident:uid for uid,ident in history.items()}

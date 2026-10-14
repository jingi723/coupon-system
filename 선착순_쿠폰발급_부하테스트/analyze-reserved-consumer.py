"""Disjoint current reserved-consumer spans; nested components reported separately."""
import csv,json,sys,collections,subprocess
from pathlib import Path
root=Path(sys.argv[1]); scripts=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(scripts/'analyze-relay-cost.py'),str(root)],check=True)
summary=json.loads((root/'comparison.json').read_text())
def stats(ns):
 v=sorted(x/1e6 for x in ns)
 def p(n):
  if not v:return None
  x=(len(v)-1)*n/100;i=int(x);return v[i]+(v[min(i+1,len(v)-1)]-v[i])*(x-i)
 return dict(count=len(v),p50_ms=p(50),p95_ms=p(95),p99_ms=p(99),cumulative_s=sum(v)/1000,mean_ms=sum(v)/len(v) if v else None)
for label,entry in summary.items():
 d=root/label;on='consumer-on' in label
 ready=json.loads((d/'profile-ready.json').read_text());counter=json.loads((d/'profile-counters.json').read_text())
 assert ready['enabled']==on and all(c['ackMode']=='RECORD' and c['syncCommits'] for c in ready['containers'])
 rows=list(csv.DictReader((d/'consumer-spans.csv').open()))
 for r in rows:
  for k in ['start_ns','end_ns','thread_id','aux']:r[k]=int(r[k])
  assert r['end_ns']>=r['start_ns']
 bykind=collections.defaultdict(list);byid=collections.defaultdict(lambda:collections.defaultdict(list))
 for r in rows:
  bykind[r['kind']].append(r)
  if r['request_id']:byid[r['request_id']][r['kind']].append(r)
 assert not any(n for k,n in counter['counters'].items() if k.startswith('violation') or k=='dropped_spans')
 result=dict(enabled=on,counters=counter,settings=ready,raw_spans={k:stats([r['end_ns']-r['start_ns'] for r in rs]) for k,rs in bykind.items()})
 if on:
  ids={r['request_id'] for r in csv.DictReader((d/'requests.csv').open())}
  assert set(byid)==ids
  assert counter['counters']['db_tx_begins']==len(ids)
  values=collections.defaultdict(list);cycles=[]
  for id,sp in byid.items():
   for k in ['approval','redis_lookup','db_proxy','db_commit_callbacks','listener_method','commitSync']:assert len(sp[k])==1,(id,k,len(sp[k]))
   a,r,b,t,l,c=[sp[k][0] for k in ['approval','redis_lookup','db_proxy','db_commit_callbacks','listener_method','commitSync']]
   assert a['start_ns']==l['start_ns']<=r['start_ns']<=r['end_ns']<=a['end_ns']==b['start_ns']<=t['start_ns']<=t['end_ns']<=b['end_ns']<=l['end_ns']<=c['start_ns']<=c['end_ns']
   assert all(s['aux']==1 for s in [a,r,b,t,l]) and c['aux']>0
   assert len({s['thread_id'] for s in [a,r,b,t,l,c]})==1
   for k,s in [('approval',a),('db_proxy',b),('commitSync',c)]:values[k].append(s['end_ns']-s['start_ns'])
   values['db_to_commit_gap'].append(c['start_ns']-b['end_ns'])
   values['approval_except_lookup'].append((a['end_ns']-a['start_ns'])-(r['end_ns']-r['start_ns']))
   cycles.append((a['start_ns'],c['end_ns'],id,b['end_ns'],b['start_ns']))
  cycles.sort();assert all(a[1]<=b[0] for a,b in zip(cycles,cycles[1:]))
  base=json.loads((d/'analysis.json').read_text())
  assert sorted(r['aux'] for r in bykind['commitSync'] if r['request_id'])==list(range(base['initial_offset']+1,base['final_offset']+1))
  assert not bykind['commitAsync']
  disjoint=['approval','db_proxy','db_to_commit_gap','commitSync']
  total=sum(sum(values[k]) for k in disjoint)
  result['disjoint']={k:dict(**stats(values[k]),percent_of_record_processing_span=sum(values[k])/total*100) for k in disjoint}
  result['approval_except_lookup']=stats(values['approval_except_lookup'])
  result['record_span_total_s']=total/1e9
  result['between_records_unattributed_s']=sum(b[0]-a[1] for a,b in zip(cycles,cycles[1:]))/1e9
  result['unattributed_commits']={k:stats([r['end_ns']-r['start_ns'] for r in bykind[k] if not r['request_id']]) for k in ['commitSync','commitAsync']}
  origin=base['origin_ms'];wall=ready['anchor_wall_ms'];nano=ready['anchor_nano']
  def wallms(n):return wall+(n-nano)/1e6
  requests={r['request_id']:r for r in csv.DictReader((d/'requests.csv').open())}
  result['request_start_to_listener_entry']=stats([(wallms(c[0])-origin-float(requests[c[2]]['sent_s'])*1000)*1e6 for c in cycles])
  result['consumer_record_span']=stats([c[1]-c[0] for c in cycles])
  result['pre_listener_scope']='HTTP + Redis reservation + Relay wait/work + Kafka wait; cannot isolate Kafka-only queue time.'
  progress=[json.loads(l) for l in (d/'progress.jsonl').read_text().splitlines()]
  busy=[(a,b) for a,b in zip(progress,progress[1:]) if a['time_ms']>=origin and a['lag']>0 and b['lag']>0]
  done=[wallms(c[3]) for c in cycles]
  busy_seconds=sum((b['monotonic_ns']-a['monotonic_ns'])/1e9 for a,b in busy)
  result['db_proxy_returns_per_s_while_sampled_lag']=sum(sum(a['time_ms']<=t<b['time_ms'] for t in done) for a,b in busy)/busy_seconds if busy_seconds else None
  result['backlog_rate_scope']='Proxy returns timestamp mapped via JVM monotonic anchor; lag sampled sequentially, selection approximate at boundaries.'
  counts=collections.Counter(int((wallms(c[3])-origin)//1000) for c in cycles)
  with (d/'db-proxy-returns-per-second.csv').open('w') as f:
   w=csv.writer(f);w.writerow(['second_from_start','committed_proxy_returns','sampled_lag_min','sampled_lag_max'])
   for sec in range(max(counts)+1):
    ps=[p['lag'] for p in progress if sec<=(p['time_ms']-origin)/1000<sec+1]
    w.writerow([sec,counts[sec],min(ps) if ps else '',max(ps) if ps else ''])
  result['validation']='All request IDs exactly once per span; one transaction and offset per request; monotonic nested boundaries and serial record cycles checked; no failures/dropped spans.'
  result['limits']=['DB proxy includes transaction begin, queries, flush, commit and cleanup; not SQL-only time.', 'DB commit callbacks are nested within DB proxy; may include ORM flush and synchronization, not pure fsync.', 'Redis lookup nested inside approval; approval residual includes proxy/instrumentation bookkeeping.', 'Offset measured synchronous client call wall time, not broker-only latency.', 'Listener span excludes offset commit; do not add nested listener/poll/callback spans to disjoint stages.', 'Between-record gaps include polling/framework/probe overhead; poll spans include idle waits and overlap these gaps.', 'Percentages describe serial per-record processing span only, not end-to-end latency or total application CPU.']
 else:assert not rows
 entry['load']['limits']=['Final latency includes external DB polling; see observation intervals.', 'Queue samples are sequential snapshots, not an atomic end-to-end backlog.', 'All runs retain Relay instrumentation and external samplers; Consumer OFF/ON is incremental overhead control.', 'Per-operation causal interpretation is limited to consumer-analysis.json boundaries; no SQL/fsync decomposition.']
 (d/'consumer-analysis.json').write_text(json.dumps(result,indent=2));entry['consumer']=result
(root/'consumer-comparison.json').write_text(json.dumps(summary,indent=2))
for label,e in summary.items():
 print(label,e['rates'])
 if e['consumer']['enabled']:print(json.dumps(e['consumer']['disjoint'],indent=2))

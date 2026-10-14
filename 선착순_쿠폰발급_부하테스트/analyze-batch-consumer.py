"""Validate bounded consumer batches and compare full-path latency, including low load."""
import csv,json,sys,subprocess,collections
from pathlib import Path
root=Path(sys.argv[1]);scripts=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(scripts/'analyze-relay-cost.py'),str(root)],check=True)
comparison=json.loads((root/'comparison.json').read_text())
def stats(v):
 v=sorted(v)
 def q(n):
  if not v:return None
  x=(len(v)-1)*n/100;i=int(x);return v[i]+(v[min(i+1,len(v)-1)]-v[i])*(x-i)
 return dict(count=len(v),mean=sum(v)/len(v) if v else None,p50=q(50),p95=q(95),p99=q(99),max=max(v) if v else None)
for name,e in comparison.items():
 d=root/name;ready=json.loads((d/'ready.jsonl').read_text().splitlines()[0]);counter=json.loads((d/'batch-counters.json').read_text());base=json.loads((d/'analysis.json').read_text());origin=base['origin_ms']
 rows=list(csv.DictReader((d/'batch-spans.csv').open()));requests={r['request_id']:r for r in csv.DictReader((d/'requests.csv').open())}
 assert counter['violations']==0 and not (d/'failure.jsonl').exists()
 assert len(rows)==counter['requests']==len(requests)==len({r['request_id'] for r in rows})
 assert set(requests)=={r['request_id'] for r in rows}
 batches=collections.defaultdict(list)
 for r in rows:
  for k in r:
   if k!='request_id':r[k]=int(r[k])
  assert r['received_ns']<=r['gathered_ns']<=r['db_start_ns']<=r['db_end_ns']<=r['commit_start_ns']<=r['commit_end_ns']
  assert r['collect_start_ns']<=r['gathered_ns']
  batches[r['batch_id']].append(r)
 assert list(batches)==list(range(1,len(batches)+1)) and len(batches)==counter['batches']
 assert [r['offset'] for r in rows]==list(range(base['initial_offset'],base['final_offset']))
 unique=[]
 for id,rs in batches.items():
  assert len(rs)==rs[0]['batch_size'] and 1<=len(rs)<=ready['batchMax']
  for k in ['collect_start_ns','gathered_ns','db_start_ns','db_end_ns','commit_start_ns','commit_end_ns']:assert len({r[k] for r in rs})==1
  unique.append(rs[0])
 assert all(a['commit_end_ns']<=b['collect_start_ns'] for a,b in zip(unique,unique[1:]))
 def wall(n):return ready['anchorWall']+(n-ready['anchorNano'])/1e6
 def phase(id):
  s=float(requests[id]['sent_s']);return 'baseline' if s<20 else 'burst' if s<30 else 'recovery' if s<60 else 'late'
 def summary(rs):return dict(count=len(rs),request_to_committed_return_ms=stats([wall(r['db_end_ns'])-origin-float(requests[r['request_id']]['sent_s'])*1000 for r in rs]),fetched_to_committed_return_ms=stats([(r['db_end_ns']-r['received_ns'])/1e6 for r in rs]))
 phases={p:summary([r for r in rows if phase(r['request_id'])==p]) for p in ['baseline','burst','recovery','late']}
 stages={k:dict(ms=stats([(r[b]-r[a])/1e6 for r in unique]),cumulative_s=sum(r[b]-r[a] for r in unique)/1e9) for k,a,b in [('collection','collect_start_ns','gathered_ns'),('approval','gathered_ns','db_start_ns'),('db_transaction','db_start_ns','db_end_ns'),('offset_commit','commit_start_ns','commit_end_ns')]}
 result=dict(settings=ready,requests=len(rows),transactions_and_offset_calls=len(unique),batch_size_histogram=dict(collections.Counter(r['batch_size'] for r in unique)),stages=stages,all=summary(rows),phases=phases,
  low_load_collection_ms=stats([(r['gathered_ns']-r['collect_start_ns'])/1e6 for r in unique if phase(r['request_id'])=='baseline']),
  collection_over_11ms=sum(r['gathered_ns']-r['collect_start_ns']>11_000_000 for r in unique),
  validation='Exact IDs/offsets, batch sizes, sequential nonoverlapping boundaries, zero failures/transaction-during-collection violations.',
  limits=['Both controls use same test-only manual consumer, not production Spring RECORD listener.', '10ms deadline is enforced before poll; runtime scheduling/client calls may overrun, observed separately.', 'Request-to-DB-return uses wall clock mapped from JVM monotonic anchor, quantized by wall ms. DB commit+cleanup upper boundary, not SQL-only commit timestamp.', 'External DB observation remains250ms; do not use it alone to identify10ms gathering cost.', 'Transaction counts inferred from successful TransactionTemplate boundaries, not JDBC interception in this whole-path experiment.', 'Per-request latency shares batch DB time; do not sum request latencies as serial CPU time.'])
 (d/'batch-analysis.json').write_text(json.dumps(result,indent=2));e['batch']=result
 e['load']['limits']=['See batch-analysis.json for new batch timing boundaries.', 'External observation includes polling delay; queues sampled sequentially.', 'All runs Relay and batch observation ON; not instrumentation OFF control.']
(root/'batch-comparison.json').write_text(json.dumps(comparison,indent=2))
for n,e in comparison.items():
 print(n,'p95 final',e['load']['all']['final_observed_ms']['p95']/1000,'low-commit',e['batch']['phases']['baseline']['request_to_committed_return_ms'],'sizes',e['batch']['batch_size_histogram'])

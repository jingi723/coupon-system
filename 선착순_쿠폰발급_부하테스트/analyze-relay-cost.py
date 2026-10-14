"""Analyze non-overlapping child call spans inside successful relayOne calls."""
import csv,json,sys,statistics,subprocess
from pathlib import Path
root=Path(sys.argv[1]);scripts=Path(__file__).resolve().parent

def q(v):
 v=sorted(v)
 def p(n):
  if not v:return None
  x=(len(v)-1)*n/100;i=int(x)
  return v[i]+(v[min(i+1,len(v)-1)]-v[i])*(x-i)
 return dict(p50=p(50),p95=p(95),p99=p(99))
summary={}
labels=['batch-one-1','batch-ten-1','batch-ten-2','batch-one-2'] if (root/'batch-one-1').exists() else ['consumer-off-1','consumer-on-1','consumer-on-2','consumer-off-2'] if (root/'consumer-off-1').exists() else ['legacy-1','lua-1','lua-2','legacy-2'] if (root/'legacy-1').exists() else ['off-1','on-1','off-2','on-2']
for label in labels:
 d=root/label
 if not (d/'relay-counters.txt').exists():continue
 with (d/'reservation-analysis.log').open('w') as log:
  subprocess.run([sys.executable,str(scripts/'analyze-reservation-load.py'),str(d)],stdout=log,stderr=subprocess.STDOUT,check=True)
 load=json.loads((d/'reservation-analysis.json').read_text())
 with (d/'relay-spans.csv').open() as f:rows=list(csv.DictReader(f))
 with (d/'requests.csv').open() as f:requests={r['request_id']:r for r in csv.DictReader(f)}
 counters=dict(line.split('=',1) for line in (d/'relay-counters.txt').read_text().splitlines())
 on=label.startswith(('on','legacy','lua','consumer','batch'))
 lua=label.startswith(('lua','consumer','batch'))
 assert counters['enabled']==str(on).lower()
 assert int(counters['failed'])==int(counters['dropped'])==0
 if on:
  assert len(rows)==len(requests)==len({r['request_id'] for r in rows})
  assert {r['request_id'] for r in rows}==set(requests)
  for r in rows:
   for k in r:
    if k!='request_id':r[k]=int(r[k])
   assert all(r[k]==1 for k in ['peek_calls','lookup_calls','kafka_calls','remove_calls']),r
   assert r['claim_calls']==(0 if lua else 1),r
   r['total_ns']=r['end_ns']-r['start_ns']
   r['prepare_ns']=r['peek_ns']+r['claim_ns']+r['lookup_ns']
   r['residual_ns']=r['total_ns']-r['prepare_ns']-r['kafka_ack_ns']-r['remove_ns']
   assert r['residual_ns']>=0
  assert all(a['end_ns']<=b['start_ns'] for a,b in zip(rows,rows[1:]))
 else:assert not rows
 def costs(rs):
  total=sum(r['total_ns'] for r in rs)
  return {k:dict(count=len(rs),ms=q([r[k]/1e6 for r in rs]),cumulative_s=sum(r[k] for r in rs)/1e9,
    percent_of_successful_relay_time=100*sum(r[k] for r in rs)/total if total else None)
    for k in ['total_ns','prepare_ns','peek_ns','claim_ns','lookup_ns','kafka_ack_ns','remove_ns','residual_ns']}
 result=dict(enabled=on,lua_preparation=lua,successful_rows=len(rows),counters=counters,all=costs(rows) if on else {},
  phases={phase:costs([r for r in rows if (lambda t: 'baseline' if t<20 else 'burst' if t<30 else 'recovery' if t<60 else 'late')(float(requests[r['request_id']]['sent_s']))==phase]) for phase in ['baseline','burst','recovery','late']} if on else {},
  validation='requestId sets, one call per stage, zero failed/dropped, non-overlapping relay attempts, nonnegative residual checked',
  scope='Successful relayOne wall times only. Empty polls/sleeps excluded. prepare contains peek/claim/lookup; do not add the grouped and component rows together. Producer.send includes KafkaTemplate work and Future.get, not broker-only commit time.')
 (d/'relay-analysis.json').write_text(json.dumps(result,indent=2))
 progress=[json.loads(l) for l in (d/'progress.jsonl').read_text().splitlines()]
 origin=json.loads((d/'analysis.json').read_text())['origin_ms']
 intervals=[(a,b,(b['monotonic_ns']-a['monotonic_ns'])/1e9) for a,b in zip(progress,progress[1:]) if a['time_ms']>=origin]
 def rate(key,predicate):
  selected=[(a,b,t) for a,b,t in intervals if predicate(a,b)]
  return sum(b[key]-a[key] for a,b,t in selected)/sum(t for a,b,t in selected) if selected else None
 rates=dict(relay_when_pending=rate('end',lambda a,b:a['pending']>0 and b['pending']>0),
            relay_during_burst=rate('end',lambda a,b:origin+20000<=a['time_ms'] and b['time_ms']<origin+30000),
            consumer_when_lag=rate('observed_count',lambda a,b:a['lag']>0 and b['lag']>0))
 summary[label]=dict(load=load,relay=result,rates=rates)
(root/'comparison.json').write_text(json.dumps(summary,indent=2))
for label,v in summary.items():
 l=v['load'];r=v['relay']
 print(label, 'HTTP p95',l['all']['response_ms']['p95'],'final p95 s',l['all']['final_observed_ms']['p95']/1000,'drain',l['final_result_drain_after_last_send_s'],'pending',l['peak_pending'],'lag',l['peak_kafka_lag'])
 if r['enabled']:
  for k in ['prepare_ns','kafka_ack_ns','remove_ns','residual_ns']:
   print(k,r['all'][k])

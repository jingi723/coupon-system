"""Sequential OFF/ON/ON/OFF experiment; broker must already be isolated and caught up.
Usage: KAFKA_GROUP=... python3 run-consumer-cost-series.py OUTPUT_PARENT TEST_CLASSPATH_FILE
"""
import csv,json,subprocess,sys
from pathlib import Path
base=Path(sys.argv[1]).resolve();cp=Path(sys.argv[2]).resolve();scripts=Path(__file__).resolve().parent
for label,mode in [('off1','off'),('on1','on'),('on2','on'),('off2','off')]:
 out=base/label
 subprocess.run([sys.executable,str(scripts/'run-consumer-cost.py'),mode,str(out),str(cp),'10'],check=True)
 with (out/'consistency-analysis.log').open('w') as f:
  subprocess.run([sys.executable,str(scripts/'analyze-overload-comparison.py'),str(out)],stdout=f,stderr=subprocess.STDOUT,check=True)
 a=json.loads((out/'analysis.json').read_text());c=json.loads((out/'profile-counters.json').read_text())
 spans=list(csv.DictReader((out/'consumer-spans.csv').open()))
 if mode=='on':
  assert not any(k.startswith('violation_') or k=='dropped_spans' for k in c['counters']),c
  assert c['counters']['db_tx_begins']==a['attempted'],(c,a['attempted'])
  for kind in ['db_proxy','db_commit_callbacks','listener_method']:
   rows=[r for r in spans if r['kind']==kind]
   assert len(rows)==a['attempted'] and all(r['aux']=='1' for r in rows),(kind,len(rows),a['attempted'])
 else:assert not spans
 assert a['timing_valid'] and a['final_db']['accepted_user_map_matches_results']
 (out/'execution-validation.json').write_text(json.dumps({'consistency':True,'clock_continuity':True,'instrumentation_counts':True}))
 print(label,'completed and validated; issued',a['attempted'],flush=True)

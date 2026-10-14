"""Docker CPU/Block I/O snapshots; identical in instrumented and control runs."""
import json,subprocess,sys,time
from pathlib import Path
out=Path(sys.argv[1])
with (out/'docker-stats.jsonl').open('w') as f:
 while not (out/'monitor.stop').exists():
  start=time.time_ns();mono=time.monotonic_ns()
  r=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}','coupon-mysql','coupon-kafka-profile-20261003'],capture_output=True,text=True,timeout=15)
  f.write(json.dumps(dict(time_ms=start/1e6,monotonic_ns=mono,end_ms=time.time_ns()/1e6,exit=r.returncode,rows=[json.loads(s) for s in r.stdout.splitlines()],error=r.stderr))+'\n');f.flush()
  time.sleep(max(0,2-(time.monotonic_ns()-mono)/1e9))

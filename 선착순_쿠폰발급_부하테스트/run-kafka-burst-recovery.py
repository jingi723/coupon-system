"""Run with an existing isolated Kafka broker; creates/stops its own app JVM.
Usage: python3 run-kafka-burst-recovery.py EMPTY_OUTPUT_DIRECTORY CLASSPATH_FILE
Defaults: Kafka localhost:29092, API localhost:18080, local compose MySQL.
"""
import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timedelta

out = Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
if any(out.iterdir()):
    raise SystemExit('Use an empty output directory')
scripts = Path(__file__).resolve().parent
cp = Path(sys.argv[2]).read_text().strip()
base = os.environ.get('BASE_URL', 'http://localhost:18080')
bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:29092')
group = os.environ.get('KAFKA_GROUP', 'coupon-burst-' + uuid.uuid4().hex[:8])
processes = []
files = []

def start(command, name):
    f = (out / name).open('w'); files.append(f)
    p = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
    processes.append(p)
    return p

def post(path, body):
    req = urllib.request.Request(base + path, json.dumps(body).encode(), {'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)

try:
    app = start(['java', '-cp', cp, 'com.assignment.coupon_system.CouponSystemApplication',
                 '--server.port=' + base.rsplit(':', 1)[1], '--spring.jpa.show-sql=false',
                 '--spring.datasource.hikari.register-mbeans=true',
                 '--spring.kafka.bootstrap-servers=' + bootstrap,
                 '--spring.kafka.consumer.group-id=' + group], 'application.log')
    deadline = time.monotonic() + 60
    while 'partitions assigned: [coupon-issue-requests-0]' not in (out/'application.log').read_text():
        if app.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError('App startup/partition assignment failed; see application.log')
        time.sleep(.2)
    now = datetime.now()
    coupon = post('/api/coupons', dict(name='kafka-burst-'+uuid.uuid4().hex[:8],
        description='Kafka burst recovery comparison', couponType='FIXED_AMOUNT', discountValue=1000,
        minOrderAmount=0, maxDiscountAmount=1000, totalQuantity=100000, validDays=30,
        startDateTime=(now-timedelta(days=1)).isoformat(timespec='seconds'),
        endDateTime=(now+timedelta(days=30)).isoformat(timespec='seconds')))['data']['id']
    meta = dict(coupon_id=coupon, app_pid=app.pid, group=group, bootstrap=bootstrap, base_url=base,
        git_head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        profile='100/s x 20s -> 1200/s x 3s -> 100/s x 30s', vus=[50,3600,1000],
        planned_requests=8600, result_poll_target_ms=250, offset_poll_target_ms=250,
        pool_poll_ms=250, row_lock_poll_ms=1000, timeout_seconds=60,
        completion_definition='first independent JDBC query observing a committed result; includes polling delay',
        recovery_rule='separately for acceptance/completion: two consecutive 5s send cohorts p95 <= 2*max(baseline 5-10,10-15,15-20 p95), pool wait samples all zero and >=15 samples per cohort',
        drain_rule='observe until all sent request IDs have committed results and lag=0, plus 10s; maximum 600s after k6 exit')
    (out/'run-meta.json').write_text(json.dumps(meta,indent=2))
    for name in ['k6-kafka-burst-recovery.js','KafkaBurstSampler.java','PoolSampler.java','run-kafka-burst-recovery.py']:
        shutil.copy2(scripts/name,out/name)
    (out/'working-tree.patch').write_bytes(subprocess.check_output(['git','diff','HEAD','--','src/main','build.gradle']))
    pool = start(['java','--add-modules','jdk.attach',str(scripts/'PoolSampler.java'),str(app.pid),str(out/'pool.csv'),str(out/'monitor.stop')],'pool-monitor.log')
    monitor = start(['java','-cp',cp,str(scripts/'KafkaBurstSampler.java'),str(coupon),str(out),bootstrap,group],'progress-monitor.log')
    deadline = time.monotonic()+30
    while not (out/'progress.jsonl').exists() or not (out/'progress.jsonl').stat().st_size or not (out/'pool.csv').exists():
        if monitor.poll() is not None or pool.poll() is not None or time.monotonic()>deadline:
            raise RuntimeError('Monitor startup failed; see monitor logs')
        time.sleep(.2)
    initial = json.loads((out/'progress.jsonl').read_text().splitlines()[-1])
    if initial['lag'] != 0: raise RuntimeError('Kafka must be caught up before load')
    meta['initial_offsets'] = initial
    (out/'run-meta.json').write_text(json.dumps(meta,indent=2))
    load = start(['k6','run','--quiet','--out',f'json={out}/metrics.json.gz',
                  '-e',f'BASE_URL={base}','-e',f'COUPON_ID={coupon}','-e',f'RUN_ID={uuid.uuid4().hex[:8]}',
                  '-e',f'RESULT_PATH={out}/k6-summary.json',str(scripts/'k6-kafka-burst-recovery.js')],'k6.log')
    print(f'Load started: coupon={coupon}, app={app.pid}, group={group}',flush=True)
    finished = None; drained = None
    while True:
        if app.poll() is not None or monitor.poll() is not None or pool.poll() is not None:
            raise RuntimeError('App or monitor stopped unexpectedly')
        if load.poll() is not None:
            if load.returncode: raise RuntimeError('k6 failed; see k6.log')
            if finished is None:
                finished = time.monotonic()
                expected = json.loads((out/'k6-summary.json').read_text())['metrics']['issue_started']['values']['count']
                print(f'k6 complete; waiting for {expected} final results and lag=0',flush=True)
            seen = list(csv.DictReader((out/'observed-results.csv').open()))
            progress = json.loads((out/'progress.jsonl').read_text().splitlines()[-1])
            if len(seen)==expected and progress['lag']==0:
                if drained is None: drained=time.monotonic()
                if time.monotonic()-drained>=10: break
            else: drained=None
            if time.monotonic()-finished>600: raise RuntimeError('Drain timeout; retain evidence')
        time.sleep(.5)
    print('All sent requests observed; lag=0; post-drain observation complete',flush=True)
finally:
    (out/'monitor.stop').touch()
    for p in reversed(processes):
        if p.poll() is None:
            if p in [locals().get('pool'),locals().get('monitor')]:
                try: p.wait(timeout=15); continue
                except subprocess.TimeoutExpired: pass
            p.terminate()
            try: p.wait(timeout=15)
            except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=10)
    for f in files: f.close()

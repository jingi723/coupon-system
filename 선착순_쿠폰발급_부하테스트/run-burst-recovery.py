"""Run from repo root after starting the app with Hikari register-mbeans=true.
Usage: python3 run-burst-recovery.py APP_PID NEW_OUTPUT_DIRECTORY
Requires k6, Java 17+, and the local docker compose MySQL service.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

pid, output = sys.argv[1:]
out = Path(output).resolve()
out.mkdir(parents=True, exist_ok=True)
if any(out.iterdir()):
    raise SystemExit('Use an empty output directory to preserve prior results')
scripts = Path(__file__).resolve().parent
stop = out / 'monitor.stop'
meta = {
    'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'app_pid': pid,
    'profile': '100/s x 20s -> 1200/s x 3s -> 100/s x 30s -> no requests',
    'recovery_rule': 'two consecutive 5s recovery-send bins: p95 <= 2 * max(baseline 5-10,10-15,15-20s p95), and every 250ms pool waiting sample in each bin is zero',
    'monitor_interval_seconds': {'pool': 0.25, 'db': 1},
    'post_run_observation_seconds': 10,
}
(out / 'run-meta.json').write_text(json.dumps(meta, indent=2))
mysql = ['docker', 'compose', 'exec', '-T', 'mysql', 'mysql', '-uroot', '-p1234', 'coupon_db', '-e']
query = """SELECT NOW(6),id,total_quantity,issued_quantity,
(SELECT COUNT(*) FROM issued_coupon i WHERE i.coupon_id=c.id) AS history_count
FROM coupon c ORDER BY id DESC LIMIT 1;
SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected',
'Innodb_row_lock_current_waits','Innodb_row_lock_time','Innodb_row_lock_waits');"""
sampler = None
load = None
try:
    with (out / 'pool-monitor.log').open('w') as pool_log:
        sampler = subprocess.Popen(['java', '--add-modules', 'jdk.attach', str(scripts / 'PoolSampler.java'),
                                    pid, str(out / 'pool.csv'), str(stop)], stdout=pool_log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while not (out / 'pool.csv').exists() or (out / 'pool.csv').stat().st_size == 0:
            if sampler.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError('Pool sampler failed: ' + (out / 'pool-monitor.log').read_text())
            time.sleep(0.1)
        with (out / 'k6.log').open('w') as log, (out / 'db-samples.jsonl').open('w') as db:
            load = subprocess.Popen(['k6', 'run', '--quiet', '--out', f'json={out}/metrics.json.gz',
                '-e', f'RESULT_PATH={out}/k6-summary.json', str(scripts / 'k6-burst-recovery.js')],
                stdout=log, stderr=subprocess.STDOUT)
            print('Load started; collecting pool and DB samples', flush=True)
            finished = None
            while finished is None or time.monotonic() - finished < 10:
                tick = time.monotonic()
                if load.poll() is not None and finished is None:
                    finished = tick
                    print('k6 completed; observing 10s without new requests', flush=True)
                if sampler.poll() is not None:
                    raise RuntimeError('Pool sampler stopped unexpectedly')
                result = subprocess.run(mysql + [query], capture_output=True, text=True, timeout=10)
                db.write(json.dumps({'time_ms': time.time() * 1000, 'returncode': result.returncode,
                                     'output': result.stdout, 'error': result.stderr if result.returncode else ''}) + '\n')
                db.flush()
                time.sleep(max(0, 1 - (time.monotonic() - tick)))
            if load.returncode:
                raise RuntimeError(f'k6 exit={load.returncode}: ' + (out / 'k6.log').read_text())
finally:
    if load is not None and load.poll() is None:
        load.terminate()
        load.wait(timeout=15)
    stop.touch()
    if sampler is not None:
        sampler.wait(timeout=15)
print('Measurement complete: ' + str(out), flush=True)

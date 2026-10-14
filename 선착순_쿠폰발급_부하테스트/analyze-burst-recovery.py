"""Analyze send-time cohorts; run after run-burst-recovery.py finishes."""
import collections
import csv
from datetime import datetime
import gzip
import json
from pathlib import Path
import re
import subprocess
import sys

out = Path(sys.argv[1])
log = (out / 'k6.log').read_text()
origin = int(re.search(r'RUN_ORIGIN_MS=(\d+)', log).group(1))
coupon = int(re.search(r'TEST_COUPON_ID=(\d+)', log).group(1))
records = []
started = collections.Counter()
dropped = collections.Counter()
scenario_origins = collections.defaultdict(set)

def sent_phase(seconds):
    return 'baseline' if seconds < 20 else 'burst' if seconds < 23 else 'recovery' if seconds < 53 else 'late'

with gzip.open(out / 'metrics.json.gz', 'rt') as f:
    for line in f:
        item = json.loads(line)
        if item['type'] != 'Point':
            continue
        data = item['data']; tags = data.get('tags', {})
        if item['metric'] == 'issue_started':
            seconds = (datetime.fromisoformat(data['time'].replace('Z', '+00:00')).timestamp() * 1000 - origin) / 1000
            started[sent_phase(seconds)] += int(data['value'])
        elif item['metric'] == 'dropped_iterations':
            dropped[tags['scenario']] += int(data['value'])
        elif item['metric'] == 'issue_response_ms':
            scenario_origins[tags['scenario']].add(int(tags['origin_ms']) - origin)
            sent = (int(tags['sent_ms']) - origin) / 1000
            records.append({'user_id': int(tags['user_id']), 'phase': sent_phase(sent),
                            'sent_s': sent, 'completed_s': sent + data['value'] / 1000,
                            'latency_ms': data['value'], 'outcome': tags['outcome'],
                            'status': int(tags['status']), 'error_code': int(tags['error_code'])})

pool = list(csv.DictReader((out / 'pool.csv').open()))
pool = [{'seconds': (int(x['time_ms']) - origin) / 1000,
         'waiting': int(x['waiting'])} for x in pool]
db = []
for line in (out / 'db-samples.jsonl').read_text().splitlines():
    sample = json.loads(line)
    assert sample['returncode'] == 0, sample
    m = re.search(r'Innodb_row_lock_current_waits\t(\d+)', sample['output'])
    db.append({'seconds': (sample['time_ms'] - origin) / 1000, 'lock_waits': int(m.group(1))})

def percentile(values, percent):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * percent / 100
    lo = int(position); hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (position - lo)

def window(start, end):
    requests = [r for r in records if start <= r['sent_s'] < end]
    waits = [p['waiting'] for p in pool if start <= p['seconds'] < end]
    locks = [p['lock_waits'] for p in db if start <= p['seconds'] < end]
    return {'start_s': start, 'end_s': end, 'requests': len(requests),
            'p95_ms': percentile([r['latency_ms'] for r in requests], 95),
            'p99_ms': percentile([r['latency_ms'] for r in requests], 99),
            'max_pool_waiting': max(waits) if waits else None,
            'pool_samples': len(waits), 'max_row_lock_waits': max(locks) if locks else None}

baseline = [window(s, s + 5) for s in [5, 10, 15]]
threshold = 2 * max(w['p95_ms'] for w in baseline)
recovery = [window(s, s + 5) for s in range(23, 53, 5)]
for w in recovery:
    w['meets_rule'] = bool(w['requests'] and w['p95_ms'] <= threshold
                           and w['pool_samples'] >= 15 and w['max_pool_waiting'] == 0)
qualified = next((i for i in range(1, len(recovery))
                  if recovery[i - 1]['meets_rule'] and recovery[i]['meets_rule']), None)

def sustained_zero(samples, key):
    after = [p for p in samples if p['seconds'] >= 23]
    last_positive = max((i for i, p in enumerate(after) if p[key] > 0), default=-1)
    return after[last_positive + 1]['seconds'] - 23 if last_positive + 1 < len(after) else None

cohorts = []
for phase, planned in [('baseline', 2000), ('burst', 3600), ('recovery', 3000), ('late', 0)]:
    requests = [r for r in records if r['phase'] == phase]
    cohorts.append({'phase': phase, 'nominal_planned': planned, 'started': started[phase],
                    'responded': len(requests), 'dropped_by_scheduled_scenario': dropped[phase],
                    'outcomes': dict(collections.Counter(r['outcome'] for r in requests)),
                    'p95_ms': percentile([r['latency_ms'] for r in requests], 95)})

sql = f'SELECT user_id FROM issued_coupon WHERE coupon_id={coupon} ORDER BY user_id;'
mysql = ['docker', 'compose', 'exec', '-T', 'mysql', 'mysql', '-uroot', '-p1234', '-N', '-B', 'coupon_db', '-e']
users = subprocess.run(mysql + [sql], capture_output=True, text=True, check=True).stdout
(out / 'db-user-ids.txt').write_text(users)
db_users = list(map(int, users.split()))
row = subprocess.run(mysql + [f'SELECT total_quantity,issued_quantity FROM coupon WHERE id={coupon};'],
                     capture_output=True, text=True, check=True).stdout
stock, quantity = map(int, row.split())
redis = ['docker', 'compose', 'exec', '-T', 'redis', 'redis-cli', '--raw']
remaining = int(subprocess.check_output(redis + ['GET', f'coupon:{coupon}:stock'], text=True).strip())
redis_users = set(map(int, subprocess.check_output(redis + ['SMEMBERS', f'coupon:{coupon}:users'], text=True).split()))
success_users = {r['user_id'] for r in records if r['outcome'] == 'completed'}
verification = {'coupon_id': coupon, 'success_responses': len(success_users), 'db_rows': len(db_users),
                'db_unique_users': len(set(db_users)), 'issued_quantity': quantity,
                'redis_remaining': remaining, 'redis_users': len(redis_users), 'stock': stock,
                'user_sets_equal': success_users == set(db_users) == redis_users,
                'all_counts_equal': len(success_users) == len(db_users) == len(set(db_users)) == quantity == len(redis_users) == stock - remaining}
summary = {'origin_ms': origin, 'cohorts_by_send_time': cohorts, 'baseline_windows': baseline,
           'scenario_start_offset_ms': {k: sorted(v) for k, v in scenario_origins.items()},
           'normal_p95_threshold_ms': threshold, 'recovery_windows': recovery,
           'stable_window_start_after_burst_s': recovery[qualified - 1]['start_s'] - 23 if qualified is not None else None,
           'recovery_confirmed_after_burst_s': recovery[qualified]['end_s'] - 23 if qualified is not None else None,
           'pool_sustained_zero_after_burst_s': sustained_zero(pool, 'waiting'),
           'row_lock_sustained_zero_after_burst_s': sustained_zero(db, 'lock_waits'),
           'burst_pending_at_burst_end': sum(r['phase'] == 'burst' and r['completed_s'] > 23 for r in records),
           'last_burst_response_after_burst_s': max(r['completed_s'] for r in records if r['phase'] == 'burst') - 23,
           'last_send_s': max(r['sent_s'] for r in records), 'last_response_s': max(r['completed_s'] for r in records),
           'started_without_response_record': sum(started.values()) - len(records),
           'timeouts': sum(r['error_code'] == 1050 for r in records), 'verification': verification}
(out / 'analysis.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
with (out / 'requests.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(records[0]))
    writer.writeheader(); writer.writerows(sorted(records, key=lambda r: r['sent_s']))
print(json.dumps(summary, ensure_ascii=False, indent=2))

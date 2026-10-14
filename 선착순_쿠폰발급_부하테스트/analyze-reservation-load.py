"""Reconcile reservation load evidence; run after analyze-overload-comparison.py."""
import collections, csv, json, math, sys
from pathlib import Path

out = Path(sys.argv[1])
base = json.loads((out / 'analysis.json').read_text())
meta = json.loads((out / 'run-meta.json').read_text())
origin = base['origin_ms']
def rows(name):
    with (out / name).open() as f:
        return list(csv.DictReader(f))
def tsv(name):
    return [s.split('\t') for s in (out / name).read_text().splitlines()]
def pct(values):
    v = sorted(values)
    def p(q):
        if not v: return None
        x = (len(v)-1)*q/100
        lo = int(x)
        return v[lo]+(v[min(lo+1,len(v)-1)]-v[lo])*(x-lo)
    return dict(zip(['p50','p95','p99'], [p(50),p(95),p(99)]))
requests = rows('requests.csv')
for r in requests:
    for key in ['sent_s','response_ms','final_observed_ms']:
        r[key] = float(r[key]) if r[key] else None
    r['phase'] = ('baseline' if r['sent_s'] < 20 else 'burst' if r['sent_s'] < 30
                  else 'recovery' if r['sent_s'] < 60 else 'after_60s')
def aggregate(rs):
    return dict(sent=len(rs),outcomes=dict(collections.Counter(r['outcome'] for r in rs)),
                response_ms=pct([r['response_ms'] for r in rs if r['response_ms'] is not None]),
                final_observed_ms=pct([r['final_observed_ms'] for r in rs if r['final_observed_ms'] is not None]))
reservations = rows('redis-reservations.csv')
observed = rows('observed-results.csv')
final_rows = tsv('final-results.tsv')
history_rows = tsv('final-history.tsv')
res = {r['request_id']:r for r in reservations}
final = {r[0]:r[1:] for r in final_rows}
history = {r[0]:r[1] for r in history_rows}
sent = {r['request_id']:r for r in requests}
accepted = {k for k,r in sent.items() if r['outcome']=='accepted'}
users = set((out/'redis-users.txt').read_text().split())
total, quantity = map(int,(out/'final-stock.tsv').read_text().split())
remaining = int((out/'redis-stock.txt').read_text())
queues = json.loads((out/'redis-queues.json').read_text())
checks = {
    'http_records_complete': base['attempted']==base['response_records']==len(sent),
    'accepted_has_reservation_and_result': accepted <= set(res) & set(final),
    'reservation_result_id_sets_equal': set(res)==set(final),
    'all_reserved_ids_were_sent': set(res)<=set(sent),
    'no_duplicate_reservation_or_result_ids': len(res)==len(reservations) and len(final)==len(final_rows),
    'all_results_issued': all(r[1]=='ISSUED' for r in final.values()),
    'request_coupon_user_mapping': all(r['coupon_id']==str(meta['coupon_id']) and k in sent and r['user_id']==sent[k]['user_id']==final[k][0] for k,r in res.items() if k in final),
    'result_history_mapping': {r[2]:r[0] for r in final.values()}==history,
    'unique_issuance': len(history)==len(history_rows)==len({r[1] for r in history_rows})==len(final),
    'users_match': users=={r['user_id'] for r in res.values()}==set(history.values()),
    'quantities_match': total==100000 and total-remaining==quantity==len(history)==len(res)==len(users),
    'all_results_observed_once': len(observed)==len(final) and {r['request_id'] for r in observed}==set(final),
    'queues_empty': queues=={'pending':[], 'processing':[]} and base['final_lag']==0,
    'kafka_record_count_matches_reservations': base['final_offset']-base['initial_offset']==len(res),
}
progress = [json.loads(l) for l in (out/'progress.jsonl').read_text().splitlines()]
db = [p for p in progress if p.get('db_sample')]
intervals = [b['result_query_start_ms']-a['result_query_start_ms'] for a,b in zip(progress,progress[1:])]
last_sent = max(r['sent_s'] for r in requests)
last_observed = (max(int(r['observed_ms']) for r in observed)-origin)/1000
drained = next(((p['sample_end_ms']-origin)/1000 for p in progress
                if p['time_ms']>=origin+last_sent*1000 and p['observed_count']==len(final)
                and p['pending']==p['processing']==p['lag']==0), None)
counts = collections.Counter(math.floor((int(r['observed_ms'])-origin)/1000) for r in observed)
with (out/'observed-completions-per-second.csv').open('w') as f:
    w=csv.writer(f);w.writerow(['second_from_start','observed_completions','pending_max','processing_max','lag_max'])
    for s in range(math.ceil((progress[-1]['sample_end_ms']-origin)/1000)):
        ps=[p for p in progress if s <= (p['time_ms']-origin)/1000 < s+1]
        w.writerow([s,counts[s],*[max((p[k] for p in ps),default='') for k in ['pending','processing','lag']]])
busy=[]
throughput_windows=[]
for s in range(0,math.ceil(last_observed),5):
    ps=[p for p in progress if s <= (p['time_ms']-origin)/1000 < s+5]
    if len(ps)<2: continue
    a,b=ps[0],ps[-1]
    duration=(b['monotonic_ns']-a['monotonic_ns'])/1e9
    throughput_windows.append(dict(start_s=s,sample_duration_s=duration,
        kafka_appended_per_s=(b['end']-a['end'])/duration,
        db_results_observed_per_s=(b['observed_count']-a['observed_count'])/duration,
        pending_first=a['pending'],pending_last=b['pending'],lag_first=a['lag'],lag_last=b['lag']))
for a,b in zip(progress,progress[1:]):
    # Both endpoints backlogged; this is sample-observed completion rate, not exact commit timestamps.
    if a['lag']>0 and b['lag']>0 and a['time_ms']>=origin:
        busy.append((b['observed_count']-a['observed_count'],(b['monotonic_ns']-a['monotonic_ns'])/1e9))
app_cpu=[(b['app_cpu_ns']-a['app_cpu_ns'])/(b['monotonic_ns']-a['monotonic_ns'])*100 for a,b in zip(progress,progress[1:])]
stats=[json.loads(s) for s in (out/'docker-stats.jsonl').read_text().splitlines()]
infra=json.loads((out.parent/'infrastructure.json').read_text())
container_cpu=collections.defaultdict(list)
for s in stats:
    for r in s['rows']:
        role=next((k for k in ['mysql','redis','kafka'] if infra[k]['id'].startswith(r['ID'])),r['Name'])
        container_cpu[role].append(float(r['CPUPerc'].rstrip('%')))
result=dict(
    planned=17000, sent=len(requests), dropped_by_scenario=base['dropped_by_scenario'],
    scheduling_note='Boundary scheduling may add a few iterations; report actual counts, not planned-minus-sent as dropped.',
    all=aggregate(requests), actual_send_phases={k:aggregate([r for r in requests if r['phase']==k]) for k in ['baseline','burst','recovery','after_60s']},
    recovery_windows=[dict(start_after_burst_s=s,**aggregate([r for r in requests if 30+s<=r['sent_s']<35+s])) for s in range(0,30,5)],
    last_sent_s=last_sent, last_result_observed_s=last_observed, final_result_drain_after_last_send_s=last_observed-last_sent,
    all_queues_empty_observed_s=drained, queues_drain_after_last_send_s=drained-last_sent if drained else None,
    peak_pending=max(p['pending'] for p in progress),peak_processing=max(p['processing'] for p in progress),peak_kafka_lag=max(p['lag'] for p in progress),
    observed_db_completions_per_second_while_kafka_backlogged=sum(n for n,t in busy)/sum(t for n,t in busy) if busy else None,
    backlogged_sample_seconds=sum(t for n,t in busy),
    throughput_windows=throughput_windows,
    db_pool_waiting_max=base['max_pool_waiting'],row_lock_waits_max=base['max_row_lock_waits'],
    row_lock_wait_count_delta=db[-1]['Innodb_row_lock_waits']-db[0]['Innodb_row_lock_waits'],
    db_write_bytes={k:db[-1][k]-db[0][k] for k in ['Innodb_os_log_written','Innodb_data_written']},
    app_cpu_single_core_percent={**pct(app_cpu),'max':max(app_cpu)},
    container_cpu_percent={k:{**pct(v),'max':max(v)} for k,v in container_cpu.items()},
    resource_sample_errors=sum(s['exit']!=0 or len(s['rows'])!=3 for s in stats),
    redis_peak_used_memory_bytes=max(p['redis_used_memory'] for p in db),
    redis_aof_delayed_fsync_delta=db[-1]['redis_aof_delayed_fsync']-db[0]['redis_aof_delayed_fsync'],
    observation=dict(target_interval_ms=250,actual_interval_ms={**pct(intervals),'max':max(intervals)},query_duration_ms=pct([p['result_query_end_ms']-p['result_query_start_ms'] for p in progress]),missing_result_observations=len(set(final)-{r['request_id'] for r in observed}),max_wall_vs_monotonic_step_ms=base['max_clock_step_error_ms']),
    final=dict(reservations=len(res),results=len(final),history=len(history),issued_quantity=quantity,redis_remaining=remaining,redis_users=len(users),kafka_records=base['final_offset']-base['initial_offset']),
    non_accepted_requests=[dict(request_id=k,outcome=r['outcome'],reserved=k in res,final_status=final[k][1] if k in final else None) for k,r in sent.items() if k not in accepted],
    checks=checks,passed=all(checks.values()),
    limits=['Final latency includes DB polling; not exact commit time.', 'Pending, processing and lag are sampled sequentially and can overlap in-flight; do not sum as exact backlog.', 'CPU and write counters include observation; no uninstrumented control run.', 'DB/offset/Relay per-operation time is not measured in this workload; no causal bottleneck attribution.'])
(out/'reservation-analysis.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
print(json.dumps(result,ensure_ascii=False,indent=2))
assert result['passed'], checks

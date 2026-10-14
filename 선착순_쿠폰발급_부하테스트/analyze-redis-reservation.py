import collections, csv, json, math, pathlib, sys
root=pathlib.Path(sys.argv[1])
def quantiles(values):
    v=sorted(values)
    return {f'p{p}':v[max(0,math.ceil(len(v)*p/100)-1)] if v else None for p in (50,95,99)}
def phase(ns):
    return 'baseline' if ns<20e9 else 'burst' if ns<30e9 else 'recovery' if ns<60e9 else 'after_stop'
summary={}
for folder in sorted(root.glob('run[12]')):
    rows=list(csv.DictReader((folder/'requests.csv').open()))
    for r in rows:
        for key in ('due_ns','start_ns','end_ns'): r[key]=int(r[key])
    sent=[r for r in rows if r['start_ns']>=0]
    ok=[r for r in sent if r['result']=='RESERVED']
    def stats(rs):
        return {'calls':len(rs),'reserved':sum(r['result']=='RESERVED' for r in rs),
                'service_ms':quantiles([(r['end_ns']-r['start_ns'])/1e6 for r in rs]),
                'schedule_delay_ms':quantiles([(r['start_ns']-r['due_ns'])/1e6 for r in rs])}
    stages={p:stats([r for r in sent if phase(r['start_ns'])==p]) for p in ('baseline','burst','recovery','after_stop')}
    bins=[]
    for sec in range(max(60,math.ceil(max(r['end_ns'] for r in sent)/1e9))):
        calls=[r for r in sent if int(r['start_ns']//1e9)==sec]
        bins.append({'second':sec,'planned':sum(int(r['due_ns']//1e9)==sec for r in rows),
                     'sent':len(calls),'reserved_completed':sum(int(r['end_ns']//1e9)==sec for r in ok),
                     'unsent':sum(r['start_ns']<0 and int(r['due_ns']//1e9)==sec for r in rows),
                     **quantiles([(r['end_ns']-r['start_ns'])/1e6 for r in calls])})
    with (folder/'per-second.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=bins[0].keys()); w.writeheader(); w.writerows(bins)
    resources=list(csv.DictReader((folder/'redis-info.csv').open()))
    valid=[r for r in resources if not r['error']]
    cpu=[]
    for a,b in zip(valid,valid[1:]):
        interval=(int(b['start_ns'])-int(a['start_ns']))/1e9
        usage=(float(b['cpu_user_s'])+float(b['cpu_sys_s'])-float(a['cpu_user_s'])-float(a['cpu_sys_s']))/interval*100
        cpu.append({'start_s':int(a['start_ns'])/1e9,'end_s':int(b['start_ns'])/1e9,'interval_s':interval,'one_core_percent':usage})
    (folder/'cpu-intervals.json').write_text(json.dumps(cpu,indent=2))
    burst=[r for r in cpu if r['start_s']>=20 and r['end_s']<=30]
    integrity=dict(line.split('=',1) for line in (folder/'integrity.txt').read_text().splitlines())
    data={'planned':len(rows),'sent':len(sent),'unsent':len(rows)-len(sent),'reserved':len(ok),
          'results':dict(collections.Counter(r['result'] for r in rows)),
          'errors':dict(collections.Counter(r['error'] for r in rows if r['error'])),
          'all':stats(sent),'stages_by_actual_start':stages,
          'sent_by_planned_phase':dict(collections.Counter(phase(r['due_ns']) for r in sent)),
          'burst_completions_per_second':sum(r['reserved_completed'] for r in bins[20:30])/10,
          'burst_completion_min_max':[min(r['reserved_completed'] for r in bins[20:30]),max(r['reserved_completed'] for r in bins[20:30])],
          'last_completion_s':max(r['end_ns'] for r in sent)/1e9,
          'resources':{'samples':len(resources),'errors':len(resources)-len(valid),
                       'sample_interval_max_s':max(r['interval_s'] for r in cpu),
                       'burst_cpu_mean_one_core_percent':sum(r['one_core_percent']*r['interval_s'] for r in burst)/sum(r['interval_s'] for r in burst),
                       'burst_cpu_max_one_core_percent':max(r['one_core_percent'] for r in burst),
                       'used_memory_first':int(valid[0]['used_memory']),'used_memory_last':int(valid[-1]['used_memory']),
                       'used_memory_max':max(int(r['used_memory']) for r in valid),
                       'rss_max':max(int(r['used_memory_rss']) for r in valid)},'integrity':integrity}
    summary[folder.name]=data
(root/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
print(json.dumps(summary,ensure_ascii=False,indent=2))

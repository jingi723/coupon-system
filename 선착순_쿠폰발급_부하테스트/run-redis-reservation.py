"""Run the test-only service benchmark in a disposable, resource-limited Redis."""
import json, pathlib, shutil, subprocess, sys, time, uuid
repo = pathlib.Path(__file__).resolve().parent.parent
out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=False)
name = 'coupon-reservation-bench-' + uuid.uuid4().hex[:8]
def run(args):
    return subprocess.check_output(args, cwd=repo, text=True)
(out/'head.txt').write_text(run(['git','rev-parse','HEAD']))
(out/'working-tree.patch').write_text(run(['git','diff','HEAD']))
(out/'status.txt').write_text(run(['git','status','--short']))
shutil.copy2(repo/'README.md',out/'README.plan.md')
shutil.copytree(repo/'src/main/java/com/assignment/coupon_system/coupon/redis',out/'production-source')
shutil.copy2(repo/'src/test/java/com/assignment/coupon_system/coupon/redis/profiling/RedisReservationBenchmark.java',out)
shutil.copy2(__file__,out)
shutil.copy2(repo/'선착순_쿠폰발급_부하테스트/consumer-profile.init.gradle',out)
(out/'containers-before.txt').write_text(run(['docker','ps','--format','{{.Names}} {{.Image}}']))
# Read only nonsecret Redis configuration and resource limits.
fields = '{{json .HostConfig.NanoCpus}} {{json .HostConfig.Memory}} {{json .Config.Cmd}} {{json .Image}}'
(out/'existing-redis.txt').write_text(run(['docker','inspect','--format',fields,'coupon-redis']))
(out/'existing-redis-config.txt').write_text(run(['docker','exec','coupon-redis','redis-cli','CONFIG','GET','appendonly','appendfsync','save','maxmemory','maxmemory-policy']))
classpath=(repo/'build/consumer-profile-classpath.txt').read_text().strip()
(out/'classpath.txt').write_text(classpath)
cmd=['docker','run','-d','--name',name,'--cpus','0.5','--memory','256m','-p','127.0.0.1::6379','redis:7-alpine']
(out/'container-command.json').write_text(json.dumps(cmd))
created=False
try:
    run(cmd); created=True
    port=int(run(['docker','port',name,'6379/tcp']).strip().rsplit(':',1)[1])
    for _ in range(50):
        try:
            if run(['docker','exec',name,'redis-cli','ping']).strip()=='PONG': break
        except subprocess.CalledProcessError: pass
        time.sleep(.1)
    else: raise RuntimeError('Redis not ready')
    (out/'test-redis.txt').write_text(run(['docker','inspect','--format',fields,name]))
    (out/'test-redis-config.txt').write_text(run(['docker','exec',name,'redis-cli','CONFIG','GET','appendonly','appendfsync','save','maxmemory','maxmemory-policy']))
    (out/'redis-server-info.txt').write_text(run(['docker','exec',name,'redis-cli','INFO','server']))
    for n in (1,2):
        dest=out/f'run{n}'; dest.mkdir()
        command=['java','-cp',classpath,'com.assignment.coupon_system.coupon.redis.profiling.RedisReservationBenchmark',str(port),str(dest),'DEDICATED_REDIS']
        (dest/'command.json').write_text(json.dumps(command))
        with (dest/'process.log').open('w') as log:
            subprocess.run(command,cwd=repo,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        print(f'run{n} complete',flush=True)
finally:
    if created: run(['docker','rm','-f',name])
    (out/'containers-after.txt').write_text(run(['docker','ps','--format','{{.Names}} {{.Image}}']))

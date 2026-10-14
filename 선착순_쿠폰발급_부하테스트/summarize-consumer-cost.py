"""Write the four-run comparison without re-running load or mutating services."""
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);labels=['off1','on1','on2','off2']
B={k:json.loads((p/k/'analysis.json').read_text()) for k in labels}
C={k:json.loads((p/k/'cost-analysis.json').read_text()) for k in labels}
M={k:json.loads((p/k/'run-meta.json').read_text()) for k in labels}
S={k:json.loads((p/k/'sql-digest-deltas.json').read_text()) for k in labels}
I={k:json.loads((p/k/'db-file-io-deltas.json').read_text()) for k in labels}
control=[C[k]['sustained_backlog']['observed_per_s'] for k in ['off1','off2']]
profile=[C[k]['sustained_backlog']['observed_per_s'] for k in ['on1','on2']]
summary={'runs':{k:{'base':B[k],'cost':C[k]} for k in labels},'overhead':{'control_rates':control,'instrumented_rates':profile,'ranges_overlap':max(min(control),min(profile))<=min(max(control),max(profile)),'both_profile_runs_at_least_10pct_slower_than_both_controls':max(profile)<.9*min(control)},'next_experiment':'DB-only benchmark, identical SQL/defenses and durability, 1 issue per transaction versus 10 issues per transaction; no Kafka or service changes in this task'}
(p/'comparison.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
def dist(d):return f"{d['p50_ms']:.3f}/{d['p95_ms']:.3f}/{d['p99_ms']:.3f}"
lines=['Kafka Consumer 처리 비용 계측 — 2026-10-03','',
'확인한 병목 범위: 현재 단일 Consumer의 순차 DB 서비스 호출 경로가 적체 시간의 약92%를 차지했다.',
'두 ON 실행에서 offset 커밋은 약7~8%, poll 약0.04%, 나머지 루프 약0.4~0.5%였다.',
'DB 커밋 단계는 DB 구간의 일부이며 전체 적체 시간의 약37~38%. 이를 DB 전체와 중복 합산하지 않았다.',
'JDBC 왕복/JPA/flush/서버 파일 동기화의 세부 기여는 아직 완전히 분리하지 못했다.',
'다음 비교 하나: Kafka를 제외한 DB 전용 벤치마크에서 트랜잭션당 1건 vs10건을 비교해 커밋 고정 비용을 검증한다.',
'이번 작업에서는 서비스 로직·트랜잭션 단위·ack-mode·파티션·Consumer 수를 변경하지 않았다. 커밋·배포도 하지 않았다.','',
'1. 실험 전 계획과 실행 조건',
'실험 전 README에 가설·계측 경계·집계 방식·판정값·OFF/ON 반복 순서를 기록했다. README.plan.md에 당시 원문을 보존했다.',
'가설 H1 DB 경로, H2 offset 커밋, H3 poll/기타 루프를 분리했다. 지배 비용은 같은 적체 창50% 초과, 유의미한 비용 후보20% 이상으로 사전 정의했다.',
'순서 OFF1→ON1→ON2→OFF2. OFF도 같은 테스트 앱/클래스패스/외부 관측이며 내부 타이밍 advice·Consumer wrapper·트랜잭션 콜백만 설치하지 않았다.',
'각 실행 새 앱 JVM, 새 쿠폰, 재고100000, 고유 사용자/requestId. 프로필100/s 20초→1200/s 10초→100/s 30초. timeout5초, 재시도0, VU50/6500/1000.',
'MySQL8.0 CPU1개/1GiB, Kafka3.7.0 CPU1개/1GiB, Hikari10. 파티션1, Consumer1, ack-mode RECORD, syncCommits=true, enable-auto-commit=false, pollTimeout5000ms.',
'기존과 동일하게 앱에 별도 CPU/heap 제한을 지정하지 않고 Java 기본값을 사용했다. 이번에는 테스트 클래스패스와 테스트 진입점을 사용했다.',
'별도 Kafka coupon-kafka-profile-20261003:39092, 앱28080, 그룹 consumer-cost-20261003. 기존 앱8080/브로커19092와 경쟁하지 않는다.',
'같은 독립 브로커/그룹을 재사용하며 매 실행 전 lag0을 확인했다. preflight의 3건은 별도 쿠폰이며 본 비교17,000건 목표에 포함하지 않는다.',
'같은 로컬 MySQL을 사용하고 기존 데이터는 삭제하지 않았다. 매 실행 테이블 크기·캐시·호스트 다른 작업의 상태가 달라 완벽한 통제 실험이 아니다.',
'이전 과부하 실험 대비 추가된 것은 테스트 프록시(ON), MySQL digest/파일 I/O 전후 조회, CPU 및 Docker stats 관측이다. 후자는 OFF/ON 공통이다.',
'Spring Kafka3.3.15, Kafka clients3.9.2, Spring TX6.2.18. 현재 소스·설정 전체는 source-state.tar.gz, 사용자 미커밋 변경은 working-tree-before.patch/각 run working-tree.patch에 보존했다.','',
'2. 측정 경계와 검증',
'DB: 실제 초기화된 CouponIssueProcessor AOP 프록시에서 TransactionInterceptor 존재를 검사하고 맨 바깥에 advice를 추가했다.',
'프록시 진입(트랜잭션 시작 전)→프록시 정상 반환(실제 커밋과 정리 후)을 nanoTime으로 측정한다. 시작 전/반환 후 활성 트랜잭션 없음, afterBegin 활성 상태를 확인했다.',
'서비스 메서드 본문 종료나 엔티티 processed_at을 커밋 완료로 간주하지 않는다. 호출 범위에는 시작/연결 확보/JPA/SQL/commit/정리가 포함된다.',
'DB commit 부분: TransactionExecutionListener.beforeCommit→afterCommit. Spring doCommit 및 commit 후 synchronization 처리가 포함된다. 순수 JDBC Connection.commit만의 시간이 아니다.',
'Listener: 사용자 CouponIssueConsumer.consume 메서드 진입→반환. DB 프록시 호출을 포함하지만 offset 커밋을 포함하지 않는다.',
'offset: DefaultKafkaConsumerFactory의 Consumer post-processor로 실제 Consumer.commitSync 호출→반환을 감싼다. commitAsync 호출도 감시했다.',
'대상 Spring 소스에서 doInvokeOnMessage→ackCurrent→commitSync 순서를 확인했으며 각 요청의 원시 구간에서도 Listener 반환 이후 offset 시작을 검증했다.',
'poll: 실제 Consumer.poll 호출→반환. fetch/역직렬화/대기를 함께 포함한다. 빈 poll 대기는 DB 처리시간으로 분류하지 않는다.',
'모든 내부 구간은 System.nanoTime. wall anchor는 클라이언트/외부 관측과 연결할 때만 쓴다. 외부 wall/monotonic 차이도 점검했다.',
'요청당 파일 로그 없이 메모리에 숫자 구간과 requestId를 저장하고 drain 후 종료 시 한 번 출력했다. 버퍼 상한500000, 초과/경계 위반/예외 카운터를 확인했다.',
'종료 때 진행 중인 마지막 idle poll은 dump 이후 종료돼 기록되지 않을 수 있다. 적체 창과 모든 DB/offset 구간은 그보다 먼저 끝나므로 해당 분석에는 누락이 없다.',
'preflight 3건에서 독립 JDBC로 실제 커밋을 확인하고 DB→Listener→offset 순서를 검사했다. 본 ON 두 실행에서도 요청/DB/commit/Listener 건수 및 ID가 모두 대응했다.',
'원문: https://github.com/spring-projects/spring-kafka/blob/v3.3.15/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java',
'원문: https://github.com/spring-projects/spring-framework/blob/v6.2.18/spring-tx/src/main/java/org/springframework/transaction/support/AbstractPlatformTransactionManager.java',
'해당 소스를 실험 디렉터리에도 보존했다.','',
'3. 예정·전송·접수·정합성',
'실행  쿠폰   명목예정  실제전송  baseline/burst/recovery 시나리오 전송  정확한[20,30)전송  미전송']
for k in labels:
 b=B[k];sc=b['scenarios'];lines.append(f"{k:4}  {M[k]['coupon_id']:4}  {b['planned']:7}  {b['attempted']:7}    {sc['baseline']['attempted']}/{sc['burst']['attempted']}/{sc['recovery']['attempted']}                      {b['nominal_burst_send_window']['attempted']:5}             {sum(b['dropped_by_scenario'].values())}")
lines += ['모든 실행에서 HTTP202 수=최종 ISSUED 결과=발급 이력=issuedQuantity=고유 사용자 수였다.',
'타임아웃/응답 불명/5xx/거절/중복/미완료/예상하지 못한 결과=모두0. 각 requestId→userId 및 결과 issuedCouponId→이력의 매핑까지 대조했다.',
'모든 실제 전송의 응답 기록과 최종 DB 관측 기록이 존재한다. 최종 lag0만으로 통과 판정하지 않았다.',
'전체 유효 부하 요청 합계 '+str(sum(B[k]['attempted'] for k in labels))+'건. 소량의 예정 초과/경계 편차는 k6 실제 기록 그대로 보존했다.',
'실제 전송 시각은 공통 RUN_ORIGIN_MS에 맞춰 분류했다. 완료 시각으로 폭주 요청을 회복 요청으로 옮기지 않았다.',
'실행  burst 첫/마지막 전송(s)     시나리오 원점 오차(ms)']
for k in labels:
 b=B[k];v=b['scenarios']['burst'];lines.append(f"{k:4}  {v['first_sent_s']:.3f}/{v['last_sent_s']:.3f}               {b['scenario_origin_offsets_ms']}")
lines += ['', '4. HTTP 접수 및 커밋된 최종 결과 최초 관측',
'전체 요청 분포. 단위ms, p50/p95/p99. 최종 관측은 독립 JDBC 조회 지연을 포함한다.',
'실행   HTTP 접수 p50/p95/p99          최종 결과 관측 p50/p95/p99     마지막 결과 시각(s)']
for k in labels:lines.append(f"{k:4}   {dist(C[k]['all_http']):28} {dist(C[k]['all_final_observed']):32} {B[k]['last_kafka_result_observed_s']:.3f}")
lines += ['마지막 요청은 약60초에 전송했다. 최종 처리 대기와 한 건 DB 서비스 시간을 혼동하지 않는다.',
'실제 전송 시각 구간별 분포(단위ms):']
for k in labels:
 for phase in ['baseline','burst','recovery','late']:
  c=C[k]['cohorts_by_actual_send_time'][phase]
  if c['requests']:lines.append(f"{k}/{phase}: n={c['requests']}, HTTP {dist(c['http'])}, final {dist(c['final_observed'])}")
for k in ['on1','on2']:
 c=C[k];lines.append(f"{k}: 요청 시작→DB 프록시 진입 p50/p95/p99={dist(c['pre_db_elapsed_includes_admission_broker_and_local_queue'])}ms. DB 한 건 평균={c['spans']['db_proxy']['mean_ms']:.3f}ms.")
lines += ['요청 시작→DB 진입에는 API/Producer/broker 대기와 poll 후 로컬 레코드 대기가 함께 들어간다. 순수 Kafka queue 체류시간이라고 부르지 않는다.',
'수십 초의 최종 지연과 수ms의 한 건 DB 처리시간은 다른 지표다. 누적된 앞선 요청을 순차 처리하는 대기가 대부분이다.','',
'5. DB·offset·Listener 구간별 전체 호출 분포',
'단위ms, 모든 요청 포함(워밍업/적체 외 구간도 포함). sum은 누적 호출시간이며 중첩 항목을 더하지 않는다.',
'실행  항목                      횟수      p50/p95/p99(ms)                 sum(ms)       평균(ms)']
for k in ['on1','on2']:
 for kind in ['db_proxy','db_commit_callbacks','listener_method','commitSync','poll']:
  d=C[k]['spans'][kind];lines.append(f"{k:4}  {kind:25} {d['count']:6}    {dist(d):30} {d['sum_ms']:11.3f}    {d['mean_ms']:.3f}")
lines += ['두 ON 모두 commitAsync0, 요청마다 commitSync1회. 재시도/커밋 오류0. Listener/DB와 offset 구간이 중첩되지 않았고 동일 Consumer 스레드1개였다.',
'commitSync 시간은 클라이언트 blocking 호출 벽시간이다. 네트워크·브로커 처리·클라이언트 스케줄링을 포함하며 broker 내부 저장/복제/디스크 단독 시간은 아니다.',
'Kafka JMX commit-total은 OFF에서도 관측했다. commit-latency-avg/max는 클라이언트 rolling 통계라 전체 실행의 p95/p99나 누적시간으로 사용하지 않았다.',
'OFF에서는 내부 구간 percentile/합계를 만들지 않았다. ON의 직접 호출 계측으로만 해당 값을 계산했다.',
'poll 전체 누적시간에는 초기/저부하와 요청 종료 후 idle 대기가 포함된다. 이를 적체 처리 병목 비율로 쓰지 않는다.','',
'6. 적체 중 비용과 실제 완료율',
'사전 기준: 각5초 구간의 모든 약250ms lag 샘플>=500, 15개 이상. DB/offset/poll은 같은 구간으로 잘라 합집합을 계산했다.',
'잔여=창 길이-DB-offset-poll. DB는 Listener 안에, DB commit 콜백은 DB 안에 포함되므로 별도 부분값이다.',
'실행   적체창(s)  독립DB완료/초   내부DB반환/초   DB%    offset%   poll%   기타%   DBcommit부분%(중첩)']
for k in labels:
 a=C[k]['sustained_backlog']
 if k.startswith('on'):lines.append(f"{k:4}  {a['seconds']:7}    {a['observed_per_s']:9.3f}    {a['proxy_completed_per_s']:9.3f}    {a['db_percent']:.3f}  {a['offset_percent']:.3f}     {a['poll_percent']:.3f}  {a['other_percent']:.3f}   {a['db_commit_subset_percent']:.3f}")
 else:lines.append(f"{k:4}  {a['seconds']:7}    {a['observed_per_s']:9.3f}       내부계측OFF")
lines += ['독립 조회와 내부 반환 차이는 약250ms의 관측 지연으로 경계 건수가 옮겨지기 때문이다.',
'기타에는 프레임워크 dispatch·메시지 어댑터·계측 기록·스케줄링 등 미분리 비용이 포함된다. 별도 CPU/서버 I/O/heartbeat 시간을 이 분모에 더하지 않았다.',
'시간대별 독립 DB 완료율(건/초), *는 위 적체 기준 충족. ON 괄호는 실제 DB 프록시 반환율이다.']
all_starts=sorted({w['start_s'] for k in labels for w in C[k]['backlog_windows']})
lines.append('구간(s)        OFF1              ON1(내부)            ON2(내부)            OFF2')
for t in all_starts:
 vals=[]
 for k in labels:
  w=next((w for w in C[k]['backlog_windows'] if w['start_s']==t),None)
  if not w:vals.append('-');continue
  value=f"{w['db_observed_per_s']:.1f}"+('*' if w['sustained_backlog'] else '')
  if k.startswith('on'):value+=f"({w['db_proxy_completions_per_s']:.1f})"
  vals.append(value)
 lines.append(f"{t:2}~{t+5:2}           "+'   '.join(f'{v:18}' for v in vals))
lines += ['각 run/db-completions-per-second.csv에 1초 단위 원시 집계도 보존했다. 적체 창의 min/max 완료율은 cost-analysis.json에 있다.','',
'7. CPU·연결·락·쓰기',
'CPU는 1코어=100% 기준이다. 앱은 여러 스레드가 있어100%를 넘을 수 있다. 이는 Consumer wall-time의 구성 비율이 아니다.',
'아래 CPU는 적체창 샘플 평균, Docker는 약2초 간격 스냅샷, 앱은 JMX ProcessCpuTime 차분 약250ms다.',
'실행  앱CPU평균%  DBCPU평균%  brokerCPU평균%  최대lag  연결대기  행락대기']
for k in labels:
 r=C[k]['resource_cpu_percent_of_one_core'];b=B[k];lines.append(f"{k:4}  {r['app']['mean']:10.2f}  {r['db']['mean']:10.2f}  {r['broker']['mean']:14.2f}  {b['max_lag']:7}   {b['max_pool_waiting']:4}     {b['max_row_lock_waits']:4}")
lines += ['평균CPU가 낮거나 연결 대기가0이어도 DB 여력이 충분하다는 뜻은 아니다. 단일 순차 호출은 왕복/commit을 기다리는 동안 CPU를 적게 쓸 수 있다.',
'MySQL 내구성 설정: innodb_flush_log_at_trx_commit=1, sync_binlog=1, log_bin=ON, buffer pool128MiB, performance_schema=ON. 변경하지 않았다.',
'글로벌 카운터 차분(시험만의 비용으로 엄밀히 귀속되지 않음):']
for k in labels:lines.append(k+' '+json.dumps(C[k]['db_global_write_counter_deltas'],ensure_ascii=False))
lines += ['Docker BlockIO 누적 표시값은 docker-stats.jsonl에 보존했다. 표시값의 반올림/전역 집계 때문에 요청당 물리 I/O 값으로 쓰지 않았다.',
'MySQL Performance Schema normalized statement digest 전후 차분:']
for k in labels:
 commit=next(x for x in S[k] if x['sql']=='COMMIT')
 lines.append(f"{k} COMMIT count={commit['count']}, 누적서버시간={commit['server_total_ms']:.3f}ms, 평균={commit['server_mean_ms']:.3f}ms")
for k in ['on1','on2']:
 lines.append(k+' 주요 나머지 SQL(전체 문장/나머지 항목은 sql-digest-deltas.json):')
 for row in [x for x in S[k] if x['sql']!='COMMIT'][:6]:
  lines.append(f"  count={row['count']}, mean={row['server_mean_ms']:.4f}ms, sum={row['server_total_ms']:.3f}ms, {row['sql']}")
 lines.append(k+' 파일 I/O 대기 상위(여러 서버 스레드가 병렬 수행하므로 합산 비율 금지):')
 for row in I[k][:4]:lines.append('  '+json.dumps(row,ensure_ascii=False))
lines += ['서버 COMMIT이 개별 SELECT/UPDATE/INSERT보다 큰 비용이며, redo/binlog 파일 관련 대기도 관측했다.',
'다만 파일 wait에는 write 외 기타 작업이 섞이고 background 작업과 겹친다. 이를 전부 fsync 시간으로 단정하거나 DB proxy 시간에서 빼지 않았다.',
'서버 digest에는 클라이언트/JDBC/JPA 시간은 포함되지 않는다. commit callback 구간에는 flush/synchronization이 섞여 둘의 차이를 순수 network 비용이라고 단정할 수 없다.','',
'8. 계측 누락·오버헤드·편차',
'OFF 적체 완료율246.04/279.40, ON267.60/300.175건/초. 범위가 겹치고 양 ON이 양 OFF보다10% 이상 느린 패턴은 없다.',
'ON이 빨라진 것을 계측의 성능 개선이라고 주장하지 않는다. 이4회만으로 오버헤드0 또는 정확한 오버헤드 비율을 추정할 수 없다.',
'외부 관측 비용은 OFF/ON에 공통이며 완전 무계측 실행과의 비용 차이는 분리하지 않았다. 캐시/호스트 경쟁/JIT/실행 순서가 남아 있다.',
'ON 두 실행의 완료율은 다르지만 DB91.79/92.21%, offset7.70/7.34%라는 비용 구성은 같은 경향이었다.',
'조회 간격 실측(평균/p95/최대ms), 계측 결과 수, wall/monotonic 최대차ms:']
for k in labels:
 c=C[k];q=c['observation_intervals_ms'];lines.append(f"{k}: {q['mean']:.3f}/{q['p95']:.3f}/{q['max']:.3f}ms, 관측행={c['observed_rows']}, 고유ID={c['unique_observed_ids']}, clock_error={c['max_clock_step_error_ms']:.3f}ms, Docker오류={c['docker_errors']}")
lines += ['결과 조회 ID 집합과 최종 DB ID 집합이 일치하고 중복 관측0. ON 두 실행은 버퍼 누락0/트랜잭션 경계 위반0/커밋 예외0/중첩 검증 통과.',
'JMX offset-total 최종값: '+', '.join(f"{k}={C[k]['kafka_jmx_commit_total_last']}" for k in labels)+'. 각 실행 새 Consumer의 누적값이다.',
'JMX rolling latency만으로 percentile이나 총시간을 재구성하지 않았다. 해당 원시값과 직접 span 자료를 모두 보존했다.',
'요청 중단 후 모든 결과가 조회되고 lag0, HTTP/DB 무활동·수량 안정35초를 확인한 뒤 종료했다. 요청별 DB span을 파일로 쓰는 작업은 이 뒤에 수행했다.','',
'9. 확인한 병목 / 미분리 비용 / 다음 비교 하나',
'확인: 현재 구조의 적체 처리율을 제한하는 지배 구간은 순차 DB 서비스 경로다. RECORD offset 커밋도 비용이 있지만 이번 측정의 주원인은 아니다.',
'확인: DB commit 단계가 큰 부분 비용이며 MySQL COMMIT 서버 통계도 이를 뒷받침한다. 특정 SELECT 하나가 지배한다는 결과는 없다.',
'미분리: DB 경로의 나머지 JDBC 왕복·ORM·flush·스케줄링, commit의 순수 디스크 동기화·network, broker 내부 offset 처리 비용.',
'추천 하나: Kafka를 제외한 DB 전용 순차 벤치마크에서 같은 유효 신청을 1건/트랜잭션과10건/트랜잭션으로 처리해 비교한다.',
'  두 군 모두 같은 JDBC 실행기에서 SQL 개수·순서, 재고 조건/고유 제약/요청 ID 멱등성/내구성/총 건수를 맞추고 트랜잭션당 건수만 바꾼다.',
'  JPA 1차 캐시로 SELECT 횟수까지 달라지는 혼선을 피하고, 실제 SQL/COMMIT 횟수가 의도한 조건과 일치하는지 검증한다.',
'  TPS, COMMIT 횟수/누적시간, 최종 정합성, 일괄 롤백 범위를 비교해 반복 commit 고정 비용을 실제로 줄일 수 있는지 검증한다.',
'  이는 다음 원인 검증 후보이며 실제 서비스에 배치 트랜잭션을 적용하라는 확정 결론이 아니다. 실패·재전달 의미가 바뀌므로 적용 전 별도 검증이 필요하다.',
'이번 작업에서 이 변경이나 offset BATCH 실험은 실행하지 않았다.','',
'10. 재현과 보존',
'소스: src/test/.../profiling/ConsumerProfileApplication.java, 부하 폴더의 ConsumerCostSampler.java, run-consumer-cost.py, run-consumer-cost-series.py, analyze-consumer-cost.py.',
'기존 k6-overload-comparison.js 및 analyze-overload-comparison.py를 재사용했다. 부하·서비스 설정은 그대로다.',
'준비: 기존 compose MySQL/Redis 유지, 독립 Kafka를39092에 준비(브로커 명 coupon-kafka-profile-20261003, CPU1/1GiB). 다른 Consumer와 그룹을 공유하지 않는다.',
'./gradlew -I 선착순_쿠폰발급_부하테스트/consumer-profile.init.gradle testClasses writeConsumerProfileClasspath',
'KAFKA_GROUP=<독립브로커의-lag0-그룹> python3 선착순_쿠폰발급_부하테스트/run-consumer-cost-series.py <새결과부모폴더> build/consumer-profile-classpath.txt',
'각 off1/on1/on2/off2에 대해: python3 선착순_쿠폰발급_부하테스트/analyze-consumer-cost.py <실행폴더>',
'python3 선착순_쿠폰발급_부하테스트/summarize-consumer-cost.py <결과부모폴더>',
'빈 브로커에서는 새 그룹 사용. 이번 그룹은 preflight 뒤 commit offset3에서 시작했고 각 실행 전 lag0을 확인했다.',
'이 환경은 기본 Gradle 홈 쓰기 제한으로 GRADLE_USER_HOME=/tmp/coupon-gradle-home과 설치된 Gradle8.14.4 실행 파일을 사용했다. 전체 classpath는 classpath.txt에 보존했다.',
'원시 자료: metrics.json.gz, requests.csv, observed-results.csv, consumer-spans.csv(ON), progress.jsonl, docker-stats.jsonl, DB digests/I/O 전후 TSV, DB 결과/이력 스냅샷, 로그.',
'분석: analysis.json, cost-analysis.json, request-timeline.csv(ON), db-completions-per-second.csv, sql-digest-deltas.json, db-file-io-deltas.json, comparison.json.',
'기준 상태: README.before.md/README.plan.md, git-status-before.txt, working-tree-before.patch, source-state.tar.gz, source-hashes.json, environment.json, 소스 사본.',
'테스트 앱/관측 프로세스와 새 브로커만 종료했다. 기존 앱/컨테이너 및 DB 테스트 자료는 유지했다. 서비스 소스 변경0, 커밋/배포0.','']
(p/'RESULTS.txt').write_text('\n'.join(lines))
print(p/'RESULTS.txt')

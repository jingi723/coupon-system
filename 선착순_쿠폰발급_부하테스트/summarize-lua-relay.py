from pathlib import Path
import json,sys
root=Path(sys.argv[1]);data=json.loads((root/'comparison.json').read_text())
assert list(data)==['legacy-1','lua-1','lua-2','legacy-2']
def triple(q,scale=1):return '/'.join(f'{q[k]/scale:.3f}' for k in ['p50','p95','p99'])
lines=['Relay 준비 호출 Lua 통합 비교 — 2026-10-14','',
'가설: Redis 왕복 횟수를 줄이면 Relay 준비 시간이 줄어드는가. fsync 원인 확정 또는 전체 지연 개선을 전제하지 않는다.',
'서비스 src/main 변경 없음. src/test 후보 어댑터로 기존 Relay의 준비 호출만 바꾼다. 실제 Producer.send 성공 확인 이후 processing 제거는 별도 유지.',
'기존: processing LINDEX → 필요하면 claim Lua/LMOVE → HGETALL. 통합: processing 우선/없으면 pending 선택/필드 검증/LMOVE/예약 반환 Lua.',
'Lua는 큐 자료형·Hash·필수필드·requestId·정확한 signed-long 범위를 쓰기 전에 검증한다. Lua 오류의 자동 롤백에 의존하지 않는다.',
'통합은 첫 스크립트 캐시 미스를 제외한 정상 준비 경로의 Redis 호출1회. 내부 Redis 명령 횟수와 네트워크 왕복 횟수는 다르다.',
'', '정합성·복구 사전 검증',
'- 신규 실제 Redis13개: 빈 큐/processing 우선/재처리/손상·자료형·누락·충돌·숫자 오류 시 전체 키 DUMP 동일/64bit 정밀도/어댑터 연결.',
'- 기존 실제 장애4개를 Lua 모드로 실행: Lua 준비 후 전송 전 종료, Kafka ack 후 제거 전 종료, DB 실패 롤백 후 재개, DB 커밋 후 offset 전 종료.',
'- 선택17개 성공(실패/오류/건너뜀0). 전체 JUnit 회귀 실행은 아니다. 실제 Redis AOF always/격리 MySQL/Kafka·별도 앱 JVM 사용.',
'- Lua 준비 완료 경계 marker 후 앱 강제 종료했으며 Lua 내부 중간 명령에서 멈춘 실험은 아니다. 재전송/동일 레코드 재전달과 DB 결과ID 보존을 기존 기준으로 검증.',
'', '성능 조건',
'- 기존1→통합1→통합2→기존2(ABBA), 전부 계측 ON. AOF always/동일 자원/동일 외부 관측 유지.',
'- 재고100000, 고유requestId/쿠폰별 사용자, 100/s20초→1200/s10초→100/s30초, timeout5초/자동재시도0.',
'- MySQL8 1CPU/1GiB, Redis7-alpine0.5CPU/256MiB, Kafka3.7 1CPU/1GiB. host JVM Java17 별도 CPU/heap 제한없음. Hikari10/Tomcat200, 단일Relay/Consumer, 1partition/RECORD.',
'- 매회 새 JVM/쿠폰, 동일 격리 DB/Redis/Kafka 재사용. 순서를 바꿨지만 캐시·데이터 증가·공유 호스트/디스크 영향을 완전히 제거하지 못했다.',
'- 관측은 DB 최종결과/큐/lag 약250ms, DB/Redis 지표1초, Docker약2초. 실제 간격/누락 및 단조시계-벽시계 변동은 회차별 JSON.',
'- 성공 Relay 구간은 System.nanoTime 및 메모리 기록, 안정35초·전량 처리 후 종료 시 CSV 기록. 상세 per-request 파일 쓰기는 부하 중 없음.',
'', '회차별 결과: p50/p95/p99']
for label,v in data.items():
 l=v['load'];r=v['relay'];c=r['all'];assert l['passed'] and r['enabled']
 lines += [label+':',f'  예정17000, 실제전송/승인/발급 {l["sent"]}, 미전송{sum(l["dropped_by_scenario"].values())}, 응답 {l["all"]["outcomes"]}',
 f'  준비(ms) {triple(c["prepare_ns"]["ms"])}, 평균 {c["prepare_ns"]["cumulative_s"]*1000/r["successful_rows"]:.6f}, 누적 {c["prepare_ns"]["cumulative_s"]:.6f}s',
 f'  Relay 전체(ms) {triple(c["total_ns"]["ms"])}, 평균 {c["total_ns"]["cumulative_s"]*1000/r["successful_rows"]:.6f}, 누적 {c["total_ns"]["cumulative_s"]:.6f}s',
 f'  Kafka 확인(ms) {triple(c["kafka_ack_ns"]["ms"])}, 제거(ms) {triple(c["remove_ns"]["ms"])}',
 f'  전달/완료 관측률(건/s) {v["rates"]}',
 f'  HTTP 응답(ms) {triple(l["all"]["response_ms"])}, 최종 발급 관측(s) {triple(l["all"]["final_observed_ms"],1000)}',
 f'  마지막 요청 후 전량관측 {l["final_result_drain_after_last_send_s"]:.3f}s, 모든 큐 해소 {l["queues_drain_after_last_send_s"]:.3f}s',
 f'  최대 pending/processing/lag {l["peak_pending"]}/{l["peak_processing"]}/{l["peak_kafka_lag"]}, DB 연결대기/행락대기 max {l["db_pool_waiting_max"]}/{l["row_lock_waits_max"]}',
 f'  DB조회 간격(ms) {triple(l["observation"]["actual_interval_ms"])} max{l["observation"]["actual_interval_ms"]["max"]}, 누락{l["observation"]["missing_result_observations"]}',
 f'  최종 정합성 {l["final"]}, 검사통과 {l["passed"]}, 계측ID {r["successful_rows"]}, counters {r["counters"]}',
 f'  앱CPU p95 {l["app_cpu_single_core_percent"]["p95"]:.2f}%, 컨테이너CPU p95 '+', '.join(f'{k} {val["p95"]:.2f}%' for k,val in l['container_cpu_percent'].items()),
 f'  Redis 최대 사용메모리 {l["redis_peak_used_memory_bytes"]:.0f}bytes, AOF delayed fsync 증가{l["redis_aof_delayed_fsync_delta"]}, 자원 표본 오류{l["resource_sample_errors"]}',
 f'  DB 쓰기 증가(bytes) {l["db_write_bytes"]}', '']
 (root/label/'RESULTS.txt').write_text('사후 정합성/계측 검증 통과. 전체 비교 ../RESULTS.txt\n\n'+json.dumps(v,ensure_ascii=False,indent=2)+'\n')
lines += ['결론과 다음 실험',
'- 준비 p95: 기존3.789~3.862ms → 통합2.075~2.157ms. 성공 요청당 평균 준비: 기존1.508~1.537ms → 통합0.946~0.994ms. 준비 비용 절감은 두 번 반복됐다.',
'- 동일한20~30초 폭주 구간의 Relay 전달률: 기존166.28/169.12건/s → 통합314.73/315.54건/s. 전체 pending 적체 표본의 평균은 기존275.93/277.52 → 통합288.77/287.65건/s로 차이가 작다. 서로 다른 집계 구간을 구분한다.',
'- 최종p95: 기존46.119/44.563s, 통합42.310/54.488s. 전량관측까지: 기존27.783/27.087s, 통합23.512/40.731s. 전체 지연 개선은 일관되게 입증되지 않았다.',
'- 최대 Kafka lag는 기존7051/7144 → 통합8253/8338로 증가, Redis pending은 기존10320/10284 → 통합8823/8878로 감소했다. 더 빠른 전달과 함께 적체 위치가 바뀌었다.',
'- 통합 두 실행의 Consumer 완료관측률은222.97/178.51건/s로 차이가 컸다. 이것은 동반 관측이며 DB/fsync/offset 중 어느 원인인지 분리한 결과는 아니다.',
'- 다음 실험 하나: 현재 Redis 예약 구조의 Consumer에서 Redis 승인조회/트랜잭션 프록시 반환까지의 DB 처리/offset 커밋을 분리 계측한다. 기존 구조의 DB92%를 재사용하지 않고 OFF/ON 반복으로 비용과 관측 영향을 다시 확인한다.',
'- 이번 산출물은 테스트 후보 및 근거다. 기본 서비스에 Lua 경로를 적용하지 않았고 최종 지연 개선을 주장하지 않는다.',
'', '해석 시 주의',
'- 준비 구간은 동일한 바깥 계측의 peek+claim+lookup 합계다. Lua 모드 peek에는 Lua와 디코딩, lookup에는 준비된 값 반환이 포함된다. 별도 claim 호출은0이다.',
'- 테스트 후보는 추가 내부 프록시/ThreadLocal 스냅샷 전달 및 Lua 검증 비용도 포함한다. 순수 네트워크 왕복 시간만 차감하는 실험은 아니다.',
'- 전체 준비 경로의 유효 입력은 동일하나, 통합은 손상 필드를 이동 전에 검증한다. processing이 존재해도 잘못된 pending 자료형을 발견하면 중단하는 보수적 정책이다.',
'- 성공 Relay 누적시간에는 빈 큐 확인·sleep이 빠진다. 이를 전체 발급/큐 대기시간으로 해석하지 않는다. DB 최종 관측에는 조회 주기가 포함된다.',
'- 각 큐/offset 조회는 순차 표본이다. pending+processing+lag를 단순 합산하지 않는다. 최대치 및 초별/5초별 추이는 원시 JSON/CSV 참조.',
'- 명목17000 대비 몇 건 추가된 k6 경계 반복은 실제 전송으로 집계하고 dropped_iterations와 구분한다.',
'- Redis 왕복 감소 결과로 fsync 자체가 원인이었다고 결론 내리지 않는다. DB 연결대기0도 DB 여력 증명이 아니다. 이전 구조의 DB92%를 적용하지 않는다.',
'- 현재 단일Redis/단일Relay용 후보이며 Redis Cluster/다중 Relay 안전성까지 검증한 것은 아니다. 서비스 반영은 하지 않았다.',
'- 불완전한 표본으로 주원인을 단정하지 않고 두 회차 수치와 순서 편차를 함께 남긴다.',
'- lua-2에서 전량 처리/최종 스냅샷 이후 앱 종료 중 LettuceConnectionFactory STOPPED 오류가 발생했다(application.log391행). 부하 중 실패와 구분하며 서비스 종료 순서는 수정하지 않았다. 종료 훅 counters 파일 이후의 정리 오류는 counters에 포함되지 않을 수 있다.',
'', '재현·원시 자료',
'REPRODUCE.txt, plan-readme.md, sources/, junit/, recovery-evidence/, 각 run-meta/metrics.json.gz/progress.jsonl/observed-results.csv/relay-spans.csv/최종 DB·Redis 스냅샷.',
'모든 계측ID/예약ID/DB결과/이력/사용자/수량을 대조했다. lag0만으로 통과하지 않는다. cleanup-verification.json은 시험 자원 정리 확인이다.']
(root/'RESULTS.txt').write_text('\n'.join(lines)+'\n')

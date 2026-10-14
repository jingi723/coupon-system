import json,sys
from pathlib import Path
p=Path(sys.argv[1]);data=json.loads((p/'batch-comparison.json').read_text());assert len(data)==4
lines=[]
def out(s=''):lines.append(s)
out('최대10건/10ms Consumer 전체 경로 비교 — 2026-10-14')
out('시험 후보: 동일 manual Kafka consumer에서 크기1↔최대10. 서비스 변경/적용/배포/커밋 없음. Spring RECORD listener는 비활성.')
out('고정조건: Lua Relay 후보/Relay 계측ON, AOFalways, 1partition/1consumer, max.poll.records10, 동일 외부관측. DB1CPU1GiB/Redis0.5CPU256MiB/Kafka1CPU1GiB, host 앱JVM.')
out('부하: stock100000, 고유user/requestId, 100/s20초→1200/s10초→100/s30초, timeout5초/재시도0. 첫20초 저부하. 실제 전송시각으로 분류.')
out('1건→10건→10건→1건 순서, 새 앱/쿠폰, 동일 격리 인프라 재사용. 호스트/디스크/캐시/순서 편차 남음.')
out()
out('run | 전송/미전송 | HTTP p50/p95/p99 ms | 최종 관측 p95/p99 s | 전량 관측drain s | 최대pending/lag | 적체중DB관측/s')
for n,e in data.items():
 l=e['load'];assert l['passed'] and not l['non_accepted_requests'];h=l['all']['response_ms'];f=l['all']['final_observed_ms']
 out(f"{n} | {l['sent']}/{sum(l['dropped_by_scenario'].values())} | {h['p50']}/{h['p95']}/{h['p99']} | {f['p95']/1000:.3f}/{f['p99']/1000:.3f} | {l['final_result_drain_after_last_send_s']:.3f} | {l['peak_pending']}/{l['peak_kafka_lag']} | {e['rates']['consumer_when_lag']:.2f}")
out(f"각 예정17000. 실제 총{sum(e['load']['sent'] for e in data.values())}건 승인/발급. 경계 스케줄링 추가분은 미전송과 구분. 모든 ID/사용자/이력/수량/Redis재고 대응 및 offset/큐0 검사 통과.")
out()
out('저부하(실제 전송0~20초) — 최종 외부 관측에는250ms 조회지연이 포함되어 커밋 반환 시각도 병기')
out('run | 요청→DB커밋정리 반환 p50/p95/p99 ms | poll수신→DB반환 p95 ms | 수집 p95 ms | 외부 관측p95 ms')
for n,e in data.items():
 b=e['batch'];v=b['phases']['baseline'];t=v['request_to_committed_return_ms'];out(f"{n} | {t['p50']:.3f}/{t['p95']:.3f}/{t['p99']:.3f} | {v['fetched_to_committed_return_ms']['p95']:.3f} | {b['low_load_collection_ms']['p95']:.3f} | {e['load']['actual_send_phases']['baseline']['final_observed_ms']['p95']:.3f}")
out()
for n,e in data.items():
 b=e['batch'];l=e['load'];out(n)
 out('  묶음수/성공transaction 경계/commitSync호출수='+str(b['transactions_and_offset_calls'])+'; batch크기분포='+str(b['batch_size_histogram']))
 out('  실제 수집11ms 초과 묶음수='+str(b['collection_over_11ms']))
 for k,v in b['stages'].items():out('  '+k+': '+json.dumps(v))
 out('  요청→DB반환 및 poll수신→DB반환 단계별 지연: '+json.dumps(b['phases']))
 for k in ['observation','db_pool_waiting_max','row_lock_waits_max','db_write_bytes','container_cpu_percent','app_cpu_single_core_percent','resource_sample_errors']:
  out('  '+k+': '+json.dumps(l[k]))
out()
out('규칙/장애 검증: 선택3개 통과(전체 회귀 아님). 미리 보관된13건은10+3으로 처리, 이후1건도 추가 요청 없이 처리. 수집 중transactionActive=false. DB5번째 INSERT 오류에서10건 전체 롤백/offset유지, 재시작 시 실패10건과후속3건 완료. DB10건 실제커밋 후 offset전 강제종료, 동일offset/요청ID 묶음 재전달에도 기존결과ID·발급ID·수량 유지.')
out('첫 장애시험은1통과/2실패: 오류주입 CHECK가 이전 쿠폰 데이터에도 적용된 fixture충돌과 Kafka session timeout45초보다 짧은30초 관찰 한도. CHECK를 해당쿠폰으로 한정하고 관찰90초로 수정한 뒤3개 통과. Consumer session설정/서비스로직은 변경하지 않음. 실패로그/성공XML/복구증거 모두 보존.')
out('계측경계: 수집→Redis검증→TransactionTemplate 시작/commit/정리→commitSync. 요청별 같은batch의DB시간은 중첩이므로 합산하지 않고 고유batch별 stage 누적만 계산. 성공트랜잭션 횟수는 경계 호출 수이며 이번 전체경로에서 JDBC commit을 별도 가로채지 않았다.')
out('10ms는 poll 호출 전 남은시간으로 제한한 수집deadline이며 OS/GC/Kafka클라이언트 호출이 지연되면 초과할 수 있다. observed collection max/p95/초과건수 보존. 이미 수신되어 앞 묶음 처리 동안 기다린 레코드는 추가10ms를 기다리지 않는다.')
out('저부하 지연은 Relay idle/HTTP/예약/DB/스케줄링을 포함한다. 요청→DB반환은 nano/wall anchor 매핑으로 ms단위 오차가 있고 외부조회보다 정확한 커밋 완료 상한이다. pure Kafka queue delay나 broker commit timestamp가 아니다.')
out('DB연결대기0은 여력 충분의 증거가 아니다. 묶음으로 JPA SELECT재사용/flush/offset호출/트랜잭션 수가 함께 달라진다. 순수commit만의 효과 또는 기존Spring RECORD 대비 개선으로 단정하지 않는다.')
out('문서: README는현재요약과링크, 상세설계/검증/성능/초기기록은docs하위로분리. 기존원시결과는보존. Relay 종료Redis접근 문제는별도과제로유지.')
(p/'RESULTS.txt').write_text('\n'.join(lines)+'\n');print('\n'.join(lines[:18]))

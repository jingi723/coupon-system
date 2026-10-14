"""Create a reviewable report from the four completed OFF/ON runs."""
from pathlib import Path
import json,sys
root=Path(sys.argv[1]);data=json.loads((root/'comparison.json').read_text())
assert list(data)==['off-1','on-1','off-2','on-2']
lines=['Relay 구간 계측 OFF/ON 비교 — 2026-10-14','',
'범위: 서비스 코드/발급 규칙/내구성 설정 변경 없이 테스트 전용 프록시와 기존 외부 관측으로 측정.',
'부하: stock100000, 100/s20초 → 1200/s10초 → 100/s30초, timeout5초, 재시도0, 고유 requestId/쿠폰별 사용자.',
'환경: AOF always; MySQL1CPU/1GiB, Redis0.5CPU/256MiB, Kafka1CPU/1GiB. Hikari10/Tomcat200, 단일Relay/Consumer, 1partition, RECORD.',
'앱: 호스트 JVM(Java17), 별도 heap/CPU 제한 없음. 전용 DB/Redis/Kafka이나 호스트/디스크는 기존 개발 컨테이너와 공유.',
'순서: OFF1→ON1→OFF2→ON2. 각 실행 새 JVM/쿠폰, 같은 인프라 재사용. 캐시·데이터 증가·공유 자원·시간순서 영향을 완전히 분리하지 못함.',
'OFF/ON은 Relay 계측의 활성 여부이며 AOF는 모두 always이다. ON도 상세 파일 쓰기는 부하 종료 후 JVM 종료 시에만 한다.',
'실험 전 가설/지점/판정기준: pre-experiment-readme.md. 현재 코드·원시 자료·명령: sources/, REPRODUCE.txt.','',
'전체 경로 결과: p50/p95/p99. 완료시간은 정확한 커밋시각이 아니라 커밋된 결과의 DB 조회 관측까지.']
def triple(d,scale=1):return '/'.join(f'{d[k]/scale:.3f}' for k in ['p50','p95','p99'])
for name,v in data.items():
 a=v['load'];assert a['passed']
 lines += [f'{name}: 전송/승인/최종발급 {a["sent"]}, dropped {sum(a["dropped_by_scenario"].values())}, 응답 분류 {a["all"]["outcomes"]}',
 f'  HTTP 응답 ms {triple(a["all"]["response_ms"])}, 최종 결과 관측 s {triple(a["all"]["final_observed_ms"],1000)}',
 f'  마지막 요청 후 전량 결과 관측 {a["final_result_drain_after_last_send_s"]:.3f}s, 모든 큐 해소 {a["queues_drain_after_last_send_s"]:.3f}s',
 f'  pending/processing/lag 최대 {a["peak_pending"]}/{a["peak_processing"]}/{a["peak_kafka_lag"]}, DB 연결대기 max{a["db_pool_waiting_max"]}, 행락대기 max{a["row_lock_waits_max"]}',
 '  전달/완료율(외부 관측 구간 차분 건/s): '+', '.join(f'{k}={n:.2f}' for k,n in v['rates'].items()),
 f'  DB 조회간격 ms {triple(a["observation"]["actual_interval_ms"])} (max{a["observation"]["actual_interval_ms"]["max"]}), 관측누락{a["observation"]["missing_result_observations"]}',
 f'  최종 수량 {a["final"]}; 모든 정합성 검사 {a["passed"]}; 비승인 요청 {len(a["non_accepted_requests"])}',
 f'  Redis 최대 사용메모리 {a["redis_peak_used_memory_bytes"]:.0f}bytes, AOF delayed_fsync 증분 {a["redis_aof_delayed_fsync_delta"]:.0f}; 자원 표본 오류 {a["resource_sample_errors"]}',
 f'  CPU p95: 앱 {a["app_cpu_single_core_percent"]["p95"]:.2f}%, '+', '.join(f'{k} {c["p95"]:.2f}%' for k,c in a['container_cpu_percent'].items()),
 f'  DB 쓰기 증가 bytes {a["db_write_bytes"]}', '']
lines += ['Relay 내부 비용: 성공한 relayOne 시간 합계만 분모로 사용. 전체 테스트 시간/사용자 대기시간의 비율이 아니다.',
'prepare는 peek+claim+lookup의 그룹이며 구성 항목과 중복 합산하지 않는다. 모든 요청에서 각 호출1회, ID 일치, dropped/failed0, 비중첩/음수잔여 없음 검증.']
for name in ['on-1','on-2']:
 r=data[name]['relay'];lines.append(name+' 성공 건수 '+str(r['successful_rows']))
 for key,value in r['all'].items():
  lines.append(f'  {key}: p50/p95/p99(ms) {triple(value["ms"])}, 누적 {value["cumulative_s"]:.6f}s, 성공 Relay 시간 중 {value["percent_of_successful_relay_time"]:.3f}%')
 lines.append('')
lines += ['계측 ON/OFF 관측 차이: ON/OFF-1 백분율. 실행 순서와 캐시 등이 섞여 있어 이 값을 순수 오버헤드로 추정하지 않는다.']
for i in [1,2]:
 off=data[f'off-{i}'];on=data[f'on-{i}']
 for label,a,b in [('최종p95',off['load']['all']['final_observed_ms']['p95'],on['load']['all']['final_observed_ms']['p95']),
                    ('drain',off['load']['final_result_drain_after_last_send_s'],on['load']['final_result_drain_after_last_send_s']),
                    ('Relay 전달률',off['rates']['relay_when_pending'],on['rates']['relay_when_pending']),
                    ('폭주 중 Relay 전달률',off['rates']['relay_during_burst'],on['rates']['relay_during_burst']),
                    ('Consumer 완료관측률',off['rates']['consumer_when_lag'],on['rates']['consumer_when_lag'])]:
  lines.append(f'  pair{i} {label}: {a:.3f} → {b:.3f} ({(b/a-1)*100:+.2f}%)')
lines += ['', '확인한 결론',
'- 두 ON 실행에서 성공한 Relay 시간 중 Redis 준비+제거 합계는85.41%/86.22%, Kafka 전송 확인은14.42%/13.63%였다. 동일한 경향이 반복됐다.',
'- OFF2/ON2의 최종p95는46.280s/46.353s로 가까웠다. 그러나 폭주 중 Relay 전달률은 OFF165/178건/s 대비 ON153/153건/s로7~14% 낮았다.',
'- 따라서 최종 완료 지연의 큰 계측 악화는 관측되지 않았지만, 폭주 전달률에서 계측 영향 가능성이 남는다. 고정 순서·공유 자원과 분리하지 못하므로 오버헤드가 무시 가능하다고 결론 내리지 않는다.',
'- 이후 Kafka lag도7천건 이상 쌓였다. Relay 비용이 줄어도 Consumer가 따라오지 못하면 최종 발급시간은 그대로일 수 있다.',
'', '해석 및 한계',
'- 준비/제거/전송 확인은 같은 Relay 스레드의 순차 구간이며 total에서 비중첩 하위 구간을 뺀 residual만 잔여로 보고했다.',
'- residual에는 DTO 생성/프록시 및 계측 book-keeping이 섞인다. 빈 큐 확인·100ms sleep·성공 작업 밖 스케줄링 시간은 성공 누적시간에서 제외한다.',
'- Redis Service 명령 반환 시간에는 클라이언트/네트워크/서버 실행/디스크 응답 대기가 포함된다. fsync 시간을 직접 분리한 수치가 아니다.',
'- Producer.send 구간은 KafkaTemplate 호출과 Future.get 성공 확인 전체다. 브로커 전용 처리시간으로 해석하지 않는다.',
'- ON에서는 공유 Redis 빈이 프록시를 통과하므로 API/Consumer에도 ThreadLocal 확인 비용은 생긴다. 해당 호출의 시간은 수집하지 않으며 이 오버헤드도 OFF/ON 비교에 포함된다.',
'- 요청별 파일 로그 없이 메모리 보관(최대100000건) 후 종료 시 출력. 성공 집합과 5개 호출 수를 대조했다. OFF는 프록시/시간수집 없음.',
'- DB 연결대기0/낮은CPU로 DB 여력 또는 I/O 병목 부재를 단정하지 않는다. DB/offset 비용은 이번 범위에서 재계측하지 않았다. 이전 구조의 DB92%는 적용하지 않는다.',
'- pending/processing/lag는 순차 표본이다. 이를 합해 정확한 총적체로 사용하지 않는다. 요청 구간별 비용은 실제 HTTP 전송 시각으로 분류하며 Relay 실행 당시 구간과 구분한다.',
'- 완료율은 커밋된 DB 결과의 약250ms 관측 차분이다. 1초 CSV/5초 창·두 큐 추이는 각 reservation-analysis.json/progress.jsonl에 있다.',
'- 계측 오버헤드 0을 증명한 것이 아니다. 2쌍과 고정 순서만으로 작은 오버헤드·캐시·공유 디스크 변동을 엄밀히 분리할 수 없다.',
'- 앱/컨테이너 종료 단계 오류는 부하 중 실패와 구분하며 application.log를 보존한다. 원래 장애 복구/전체 JUnit 회귀를 이번에 재실행하지는 않았다.',
'', '다음 비교 실험 제안',
'AOF always와 단일Relay/동일 장애 보장을 유지한 채 processing 조회·claim·예약 조회를 한 번의 Lua 호출로 묶는 테스트 전용 비교를 추천한다.',
'현재 개별 명령 방식과 준비 구간 시간·전달률·최종 발급p95/전량 처리시간을 함께 비교한다. 장애 시 processing 보존 및 재전달 멱등성도 유지돼야 한다.',
'이번 작업에서는 이 변경을 구현하지 않았다. Relay 전달만 빨라져 Kafka lag가 늘면 최종 발급 개선으로 판정하지 않는다.',
'', '커밋: git add가 .git/index.lock Operation not permitted로 거절되어 신규 커밋 없음. 기존 미커밋 변경과 산출물을 보존했다.']
(root/'RESULTS.txt').write_text('\n'.join(lines)+'\n')
for name,v in data.items():
 (root/name/'RESULTS.txt').write_text('상세 비교: ../RESULTS.txt\n정합성: 통과\n\n'+json.dumps(v,ensure_ascii=False,indent=2)+'\n')

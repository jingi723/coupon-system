import json,sys
from pathlib import Path
p=Path(sys.argv[1]);all=json.loads((p/'consumer-comparison.json').read_text());lines=[]
def out(s=''):lines.append(s)
assert len(all)==4
out('Redis 예약 경로 / Lua Relay 고정 / Consumer OFF-ON 비교 — 2026-10-14')
out('서비스 코드/Consumer 수/파티션/RECORD 변경 없음. 테스트 전용 계측 추가. 배포·커밋 없음.')
out('사전 가설·경계: PLAN-README.md. 재현: REPRODUCE.txt. 소스: sources/, 미커밋: git-before.diff/status.')
out('순서 OFF1→ON1→ON2→OFF2, 각 새 JVM/쿠폰. 동일 격리 인프라 누적 데이터/캐시와 호스트 공유 자원 영향은 남음.')
out('Relay Lua 후보와 Relay 계측 ON, Redis AOF always, 1partition/Consumer, RECORD syncCommits=true, Hikari10/Tomcat200.')
out('MySQL/Kafka 각각 1CPU 1GiB, Redis0.5CPU 256MiB. 앱 host JVM 별도 CPU/heap 제한 없음. 상세 infrastructure.json/docker-inspect/run-meta.json/redis-config.txt/application.log.')
out('100/s20초→1200/s10초→100/s30초, 재고100000, 고유user/requestId, timeout5초/자동재시도0.')
out()
out('전체 경로 결과 (HTTP ms / 최종 관측 s / 마지막 전송 뒤 전량 관측 s)')
out('run | 예정/실제/미전송 | HTTP p50/p95/p99 | 최종 p50/p95/p99 | drain | Kafka 적체 중 완료관측/s | max pending/lag')
for label,e in all.items():
 l=e['load'];h=l['all']['response_ms'];f=l['all']['final_observed_ms'];assert l['passed'] and not l['non_accepted_requests']
 out(f"{label} | 17000/{l['sent']}/{sum(l['dropped_by_scenario'].values())} | "+'/'.join(f'{h[k]:.3f}' for k in ['p50','p95','p99'])+' | '+'/'.join(f'{f[k]/1000:.3f}' for k in ['p50','p95','p99'])+f" | {l['final_result_drain_after_last_send_s']:.3f} | {e['rates']['consumer_when_lag']:.2f} | {l['peak_pending']}/{l['peak_kafka_lag']}")
out('시나리오 경계 스케줄링으로 예정보다 몇 건 많다. 미전송은 k6 dropped_iterations이며 예정-실제 차이가 아니다.')
out(f"총 {sum(e['load']['sent'] for e in all.values()):,}건 승인/발급. 전 실행 비승인·타임아웃·미전송·중복·미완료0. 14개 정합성 검사 통과.")
out('승인 requestId/Redis 예약/DB 결과·이력/사용자 대응 및 stock 감소량=issuedQuantity=이력 수, Kafka offset 증가량, pending/processing/lag0 확인. lag0만으로 판정하지 않음.')
out()
for label,e in all.items():
 c=e['consumer'];l=e['load']
 if not c['enabled']:continue
 out(label+' — 단일 레코드 직렬 처리 구간 (ms; 누적 s; 구간 내 비율)')
 for k,v in c['disjoint'].items():out(f"  {k}: n={v['count']}, p50/p95/p99={v['p50_ms']:.6f}/{v['p95_ms']:.6f}/{v['p99_ms']:.6f}, 누적={v['cumulative_s']:.6f}s, 평균={v['mean_ms']:.6f}ms, 비율={v['percent_of_record_processing_span']:.3f}%")
 for k in ['redis_lookup','db_commit_callbacks','listener_method','poll']:
  v=c['raw_spans'][k];out(f"  중첩/보조 {k}: n={v['count']}, p50/p95/p99={v['p50_ms']:.6f}/{v['p95_ms']:.6f}/{v['p99_ms']:.6f}ms, 누적={v['cumulative_s']:.6f}s (위 합계에 더하지 않음)")
 out('  Redis 제외 승인 구간(검증/프록시/계측 포함): '+json.dumps(c['approval_except_lookup']))
 out('  요청 시작→Listener 진입(HTTP+예약+Relay+Kafka 대기/작업 포함): '+json.dumps(c['request_start_to_listener_entry']))
 out('  Listener 진입→offset 반환: '+json.dumps(c['consumer_record_span']))
 out(f"  sampled lag>0 구간 DB 프록시 반환률={c['db_proxy_returns_per_s_while_sampled_lag']:.2f}/s; 별도 db-proxy-returns-per-second.csv에 1초별 완료/lag.")
 out(f"  레코드 사이 미분리 시간={c['between_records_unattributed_s']:.6f}s; polling/idle/framework/probe 비용 포함, DB 시간에 넣지 않음.")
 out('  비대응 commit: '+json.dumps(c['unattributed_commits']))
 out('  모든 requestId에 승인/Redis조회/DB/DBcommit콜백/Listener/commitSync 각각1개, transaction begin1개, 연속offset 및 동일thread/비중첩 순서 검증. violations/dropped0. commitAsync0.')
 out()
out('계측 경계와 해석')
out('DB advice는 실제 TransactionInterceptor 앞 index0: 외부 transaction entry/return false, begin callback 활성 확인. 메서드 몸체 종료가 아니라 실제 commit/정리 뒤 반환을 측정했다.')
out('Listener 종료<=offset 호출 시작을 전 requestId 검증했다. Listener 시간에는 offset commit이 없다. 승인+DB+DB후간격+offset만 직렬로 합산; 비율은 전체 실행/최종대기/CPU 비율이 아니다.')
out('DB 콜백 구간은 flush/동기화/commit을 포함할 수 있어 순수 디스크 fsync로 해석하지 않는다. Redis 호출은 네트워크/서버/디코딩/스케줄링 포함; offset은 동기 client 호출 wall time.')
out('poll에는 부하 전/후 idle 대기가 포함된다. 종료 직전 진행 중 poll은 dump에 없을 수 있다. 요청별 업무 span은 누락0. System.nanoTime을 쓰며 완료초별 시계는 JVM anchor로 매핑했다.')
out('신청→Listener 진입은 Relay/Kafka 대기와 HTTP/예약 처리까지 합친 값이며 Kafka 대기만 분리한 값은 아니다. 여러 요청의 대기 누적을 직렬 DB 시간과 합산하지 않는다.')
out()
out('관측 간격·자원 (CPU는 코어1개=100%, 전체 관측 구간에 idle 포함)')
for label,e in all.items():
 l=e['load'];out(label)
 for k in ['observation','db_pool_waiting_max','row_lock_waits_max','row_lock_wait_count_delta','app_cpu_single_core_percent','container_cpu_percent','db_write_bytes','resource_sample_errors','redis_peak_used_memory_bytes','redis_aof_delayed_fsync_delta']:
  out('  '+k+': '+json.dumps(l[k]))
out('DB 결과 조회 목표250ms. 실제 간격/조회소요/누락은 위 수치, 요청별 최종 지연은 커밋 확인 관측 상한이며 polling 지연 포함. 초별 CSV와 5초 throughput_windows를 함께 보라.')
out('DB 연결/행락 대기0은 DB 용량 여유의 증거가 아니다. 단일 순차 소비의 건당 SQL/트랜잭션 왕복 비용이 남는다. 쓰기 bytes는 관측 가능한 엔진 카운터이며 fsync 지연을 직접 측정하지 않았다.')
out()
out('확인한 병목 / 아직 분리하지 못한 비용 / 다음 실험')
on=[e['consumer'] for e in all.values() if e['consumer']['enabled']]
out('현재 두 ON 실행에서 가장 큰 직렬 비용은 DB 트랜잭션 포함 호출: '+', '.join(f"{c['disjoint']['db_proxy']['percent_of_record_processing_span']:.2f}%" for c in on)+'. 과거 직접 Kafka 구조의92%와 별도 결과다.')
out('RECORD offset은 건당1회지만 현재 지배 비용이라고 볼 수 없다. Redis 승인 조회도 측정됐으며 상세는 위 표. 단일 Consumer가 주원인이라는 병렬 비교는 하지 않았다.')
out('OFF/ON은 각각2회, 모든 실행 Relay/외부 관측은 ON. 차이를 순수 계측 오버헤드로 단정하거나 오버헤드0으로 주장하지 않는다. 실행 순서/공유 디스크/캐시/호스트 부하 혼재.')
out('미분리: DB 내부 SQL별 실행/ORM flush/transaction begin·cleanup/스토리지 fsync, 스레드 스케줄링과 GC, 레코드 사이 framework 시간의 세부 구성. DB commit 콜백은 DB의 부분집합이다.')
out('추천하는 다음 비교 하나: 테스트 전용 DB 재생에서 트랜잭션당1건 vs10건. 동일 승인 데이터/SQL/동시성1/내구성으로 processReserved를 외부 transaction에 참여시켜 commit 횟수 효과를 비교하고 처리량·지연·정합성/전체 batch rollback을 검증한다. Kafka offset 정책은 이 격리 비교에서 바꾸지 않는다. DB 외부 비교이므로 전체 경로 개선은 이후 따로 검증해야 한다. 이번에는 구현하지 않았다.')
out('전체 회귀/JUnit 재실행은 아님: testClasses 컴파일 성공, 실제 부하4회와 정합성·계측 경계 자동검증. 이전17개 장애/Lua 검증 결과는 이전 디렉터리 참조.')
(p/'RESULTS.txt').write_text('\n'.join(lines)+'\n')

"""Summarize production single/batch ABBA runs after reservation reconciliation."""
import json, sys
from pathlib import Path

root = Path(sys.argv[1])
labels = ['production-single-1', 'production-batch-1', 'production-batch-2', 'production-single-2']
data = {name: json.loads((root/name/'reservation-analysis.json').read_text()) for name in labels}
repeat = root/'timing-repeat/production-single-repeat/reservation-analysis.json'
if repeat.exists(): data['production-single-repeat'] = json.loads(repeat.read_text())
assert all(run['passed'] for run in data.values())
lines = [
    '서비스 단건 RECORD ↔ 최대10건·10ms Consumer 전체 부하 비교 — 2026-10-15',
    '실제 CouponSystemApplication/CouponBatchConsumer 사용. 테스트 후보/Lua/내부 상세 계측 없음.',
    '동일 기존 Relay, AOF always, 단일 파티션·Consumer, 풀10. DB1CPU/1GiB Redis0.5CPU/256MiB Kafka1CPU/1GiB.',
    '호스트 앱 JVM 제한 없음. 전용 인프라/그룹 재사용, 실행별 새 JVM/쿠폰. 개발 앱/컨테이너와 격리.',
    '부하: 재고100000, 고유 사용자/requestId, 100/s20초→1200/s10초→100/s30초, 제한5초·재시도0.',
    '예정17000/실행. 실제 전송시각으로 구간 분류. 경계 스케줄링 추가분과 미전송은 별개.',
    '',
    '실행 | 실제전송/미전송 | HTTP p50/p95/p99 ms | 최종관측 p50/p95/p99 s | 마지막전송 후 전량관측 s | 최대pending/lag',
]
for name, run in data.items():
    h, f = run['all']['response_ms'], run['all']['final_observed_ms']
    lines.append(f"{name}{'' if run['timing_valid'] else ' [시계오차: 지연비교 제외]'} | {run['sent']}/{sum(run['dropped_by_scenario'].values())} | "
                 + '/'.join(f'{h[k]:.3f}' for k in ['p50','p95','p99']) + ' | '
                 + '/'.join(f'{f[k]/1000:.3f}' for k in ['p50','p95','p99'])
                 + f" | {run['final_result_drain_after_last_send_s']:.3f} | {run['peak_pending']}/{run['peak_kafka_lag']}")
lines += ['', f"실제 총전송 {sum(r['sent'] for r in data.values())}건. 각 실행14개 정합성 검사 통과.",
          '승인 ID/Redis예약/DB결과/발급이력/사용자/쿠폰 대응, issuedQuantity와 재고 감소, 중복·미완료·조회누락, 두 큐 및 lag 대조.']
for name, run in data.items():
    lines += ['', name]
    for key in ['all','actual_send_phases','recovery_windows','final','non_accepted_requests','checks',
                'queues_drain_after_last_send_s','observed_db_completions_per_second_while_kafka_backlogged',
                'observation','db_pool_waiting_max','row_lock_waits_max','row_lock_wait_count_delta',
                'db_write_bytes','app_cpu_single_core_percent','container_cpu_percent','resource_sample_errors',
                'redis_peak_used_memory_bytes','redis_aof_delayed_fsync_delta']:
        lines.append(key + ': ' + json.dumps(run[key], ensure_ascii=False))
singles = [r for n,r in data.items() if 'single' in n and r['timing_valid']]
batches = [r for n,r in data.items() if 'batch' in n and r['timing_valid']]
consistent = (len(singles)>=2 and len(batches)>=2 and max(r['all']['final_observed_ms']['p95'] for r in batches) < min(r['all']['final_observed_ms']['p95'] for r in singles)
              and max(r['final_result_drain_after_last_send_s'] for r in batches) < min(r['final_result_drain_after_last_send_s'] for r in singles))
lines += ['', '판정: ' + ('묶음2회 모두 최종p95 및 전량 완료시간이 단건2회보다 낮음.' if consistent else '최종p95/전량 완료시간의 일관된 개선 기준은 만족하지 않음. 실행별 편차를 함께 해석.'),
          '시계 검증: 첫 단건 실행12.126→14.376초 사이 wall/monotonic 차이1994.7ms. 수량 결과는 유효하나 해당 실행 지연은 비교 제외. 새 격리 인프라에서 단건 추가 재측정, cold start/실행순서 차이를 기록.',
          '한계: 최종 지연은250ms 목표 DB 조회 관측값이며 정확한 커밋 시각 아님. 저부하10ms 차이를 이 관측만으로 정밀 판정하지 않음.',
          '스케줄링/캐시/공유 호스트 편차가 남으며 실행별 실제 조회 간격 기록. CPU는1코어=100%, 관측 비용 포함.',
          'DB 연결 대기0은 충분한 DB 여력의 증거 아님. pending/processing/lag는 순차 샘플이어서 정확한 동시 합계로 더하지 않음.',
          '이번 비교는 서비스 단건 Listener와 수동 묶음 Consumer의 전체 차이. 트랜잭션/offset/수집/프레임워크가 함께 달라져 순수 커밋 효과로 해석 불가.',
          '이전 Lua Relay+테스트 후보의 성능을 이번 서비스 결과와 직접 혼합하지 않음.',
          '기존34개 선택 기능/장애 테스트는 2026-10-15-production-batch/ 참조. 이번에는 컴파일 및 ABBA 부하4회와 시계 오류에 따른 단건 추가 측정/정합성 검사 수행.',
          '커밋: git add는 완료됐으나 git commit이 .git/index.lock Operation not permitted로 실패. 커밋/배포 완료 아님.']
(root/'comparison.json').write_text(json.dumps(data, ensure_ascii=False, indent=2))
(root/'RESULTS.txt').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines[:14]))

import csv,json,sys
from pathlib import Path
p=Path(sys.argv[1]);runs={}
def stats(v):
 v=sorted(v)
 def q(n):
  x=(len(v)-1)*n/100;i=int(x);return v[i]+(v[min(i+1,len(v)-1)]-v[i])*(x-i)
 return dict(mean=sum(v)/len(v),p50=q(50),p95=q(95),p99=q(99),max=max(v))
for name in ['one-1','ten-1','ten-2','one-2']:
 r=json.loads((p/(name+'-summary.json')).read_text());rows=list(csv.DictReader((p/(name+'-times.csv')).open()));n=r['requests'];b=r['batch']
 assert len(rows)==n==len({x['request_id'] for x in rows})
 assert r['jdbc']['commitCalls']==r['jdbc']['commitSuccess']==r['hibernate']['successfulTransactions']==n//b
 assert not r['jdbc'].get('rollbackCalls') and not r['jdbc'].get('commitFailures')
 assert sum(r['sql'].values())==r['hibernate']['prepareStatements']==r['jdbc']['jdbcExecuteStatementEnd']
 for row in rows:
  for k in ['call_start_ns','call_end_ns','committed_return_ns','origin_ns']:row[k]=int(row[k])
  assert row['origin_ns']<=row['call_start_ns']<=row['call_end_ns']<=row['committed_return_ns']
 for i in range(0,n,b):
  chunk=rows[i:i+b];assert len({x['committed_return_ns'] for x in chunk})==1
  if i:assert rows[i-1]['committed_return_ns']<=chunk[0]['call_start_ns']
 r['call_start_to_commit_ms']=stats([(x['committed_return_ns']-x['call_start_ns'])/1e6 for x in rows])
 r['processor_call_ms_not_commit']=stats([(x['call_end_ns']-x['call_start_ns'])/1e6 for x in rows])
 r['preloaded_ready_to_completion_s']=stats([(x['committed_return_ns']-x['origin_ns'])/1e9 for x in rows])
 r['jdbc_commit_mean_ms']=r['jdbcCommitNs']/r['jdbc']['commitSuccess']/1e6
 r['sql_total']=sum(r['sql'].values());r['sql_by_kind']={k:sum(v for s,v in r['sql'].items() if s.startswith(k)) for k in ['select','insert','update','delete']}
 runs[name]=r
(p/'comparison.json').write_text(json.dumps(runs,indent=2))
lines=['DBトランザクション比較 / DB 트랜잭션당 1건 vs10건 — 2026-10-14','서비스 변경 없음. 테스트 전용 TransactionTemplate으로 기존 processReserved 호출. Redis/Kafka/HTTP 제외.',
 '17,000개 동일 requestId/userId/예약시각/쿠폰, 한도100,000, 동시성1, 500건 워밍업 뒤 1→10→10→1. 회차 사이 DB 이력/결과 삭제·수량0. 같은 JVM/DB/테이블, auto_increment와 cache는 누적.',
 'MySQL8.0.46 1CPU/1GiB, Hikari10. innodb_flush_log_at_trx_commit=1/sync_binlog=1/log_bin=1 실제 조회 확인. JVM 옵션 test-jvm.txt, DB docker inspect 참조.',
 '계측은 전 실행 동일하게 ON. JDBC 실제 commit 호출/반환과 Hibernate StatementInspector·SessionEventListener·Statistics 집계. 파일은 측정 종료 후 출력. 무계측 처리량 아님.',
 '', 'run | 처리량/s | 전체 s | 실제 commits | SQL | flush / partialFlush | 호출 시작→커밋완료 p50/p95/p99 ms | JDBC commit 누적s/평균ms']
for name,r in runs.items():
 t=r['call_start_to_commit_ms'];lines.append(f"{name} | {r['perSecond']:.2f} | {r['elapsedNs']/1e9:.3f} | {r['jdbc']['commitSuccess']} | {r['sql_total']} | {r['jdbc']['flushEnd']}/{r['jdbc']['partialFlushEnd']} | {t['p50']:.3f}/{t['p95']:.3f}/{t['p99']:.3f} | {r['jdbcCommitNs']/1e9:.3f}/{r['jdbc_commit_mean_ms']:.3f}")
lines+=['','정합성: 배치1 실패1번째/배치10 실패5번째 DB INSERT CHECK 오류. 해당 트랜잭션 성공commit0/rollback1, 수량/이력/결과0. 원인 제거 후 같은 요청 재시도 성공, 묶음 재전달 후 issued ID/수량 유지(correctness.json).',
 '네 부하 실행 각17,000건(총68,000회 신규 발급 처리), 모든 요청ID/user/coupon/ISSUED/발급이력 대응과 issuedQuantity 검증. 매 회차 데이터 초기화이므로 동시 보존된 고유 쿠폰68,000개라는 뜻은 아니다. 원시 최종 결과와 monotonic ns 시각을 보존.',
 '실제 commit 성공횟수=17,000/batch, Hibernate 성공 트랜잭션 수와 일치. SQL 합계=prepare=execute 이벤트 수 및 요청별 커밋 경계·중복누락 검증.',
 '', '해석과 한계:',
 '처리량은 준비된 입력을 순차 처리한 DB 전용 값. 요청마다 processReserved의 논리적 작업은 동일하지만 트랜잭션 공유로 영속성 컨텍스트도 공유된다. coupon findById SELECT 감소와 flush 감소가 동반되므로 개선을 커밋만의 효과로 볼 수 없다.',
 'INSERT/UPDATE 횟수와 상세 SQL별 횟수는 각 summary.json/sql 및 comparison.json 참조. JDBC executeBatch는 사용하지 않았다. saveAndFlush 호출을 제거한 실험도 아니다.',
 '건당 완료시간=그 건의 서비스 호출 직전→해당 묶음 외부 TransactionTemplate 반환(실제 commit/정리 이후). 묶음 앞 요청은 뒤 요청 처리를 기다리므로 이 값이 늘 수 있다. 메서드 몸체/내부 프록시 반환을 커밋으로 간주하지 않는다.',
 '전체 입력을 미리 준비한 시점→완료 분포는 comparison.json/preloaded_ready_to_completion_s. 실제 HTTP 유입/묶음 채우기 대기는 없고 Kafka offset/Redis 승인 조회도 제외. 기존 전체 발급 p95와 직접 비교 불가.',
 'JDBC commit 시간은 드라이버+DB 왕복 wall time이며 순수 fsync가 아니다. Hibernate flush/partialFlush 이벤트는 비어 있는 검사도 포함할 수 있고 DB 쓰기 횟수가 아니다. flushEntities는 동일 entity를 여러 번 셀 수 있다.',
 '동일 계측이지만 ON/OFF 대조는 이번에 하지 않음. 순서/GC/JIT/cache/공유 호스트 디스크 편차는 남는다. 기존 Consumer 계측 실험과 JVM/동시실행 자원 부하가 다르다.',
 '', '다음: 결과가 반복 개선이면 전체 예약 경로에 테스트 전용 묶음 후보를 연결한 비교를 별도로 설계한다. 최대 대기시간·묶음 실패 재처리 범위·DB commit 이후 offset 경계와 정합성을 먼저 정의한다. 이번에는 서비스 배치/offset 변경을 구현하지 않는다.',
 'Relay 종료 중 Redis 접근은 별도 미해결 과제 유지. 이번 DB-only 실험은 Redis/Relay를 실행하지 않아 종료 오류를 수정하거나 재검증하지 않는다.']
lines[0]='DB 트랜잭션당 1건 vs10건 — 2026-10-14'
(p/'RESULTS.txt').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines[:12]))

# 공개 코드와 측정 근거

현재 구현은 Redis 예약 → 단일 Relay → Kafka → DB 발급 → requestId 결과 조회이다. 기본 Consumer는 단건이며 최대10건·10ms 배치는 선택 옵션이다. 실제 소스는 `src/main`, 실험 후보·장애 주입·계측 코드는 `src/test`에 있다. Lua Relay는 서비스 기본 구현이 아니다.

## 결과를 읽는 순서

1. [현재 배치 구현과 실제 앱 결과](batch-consumer.md): 2026-10-15 서비스 부하5회, 총85,012건. 첫 단건 시계 오류는 지연 비교 제외. DB 결과 조회 간격·실행 조건·반복 편차를 함께 읽는다.
2. 같은 문서의 **과거 테스트 후보**: 2026-10-14 Lua Relay와 수동 Consumer 후보4회, 총68,010건. 실제 앱 결과와 섞어 개선 배율을 계산하지 않는다.
3. [예약·장애 검증 기록](reservation-tests.md) 및 [성능 실험 기록](performance.md): 앞 단계별 측정 조건과 범위.

## 공개 범위

새 실험 자료는 연결된 RESULTS/REPRODUCE, 집계 JSON, 정합성 검사 결과, 테스트별 통과 내역과 재현 코드를 공개한다. 최신 상세 application 로그, docker inspect, JVM/개인 경로 덤프, 중복 소스 스냅샷, 캐시, 요청별 대용량 원본은 추가 게시하지 않는다. 원문 RESULTS에 등장하는 `sources/`, 원시 CSV/로그 등은 측정 당시 로컬 보존 자료이며 모두 공개 파일이라는 뜻은 아니다. 이전에 커밋된 합성 부하 원본은 이력의 재현 근거로 유지한다.

실험의 사용자 ID/requestId는 생성한 테스트 데이터이다. Compose의 `coupon`/`1234` 등은 로컬 실습용 기본값이다. 실제 인증 토큰·개인 키·운영 접속정보는 공개 대상에 포함하지 않는다. 결과의 수치는 변경하지 않으며 공개용 재현 안내에서는 개인 절대 경로를 저장소 상대 경로로 바꾼다.

## 검증 및 재현

Java17, Docker, k6가 필요하다. 테스트가 만든 별도 MySQL/Redis/Kafka를 사용하며 기존 개발 컨테이너를 종료하지 않는다.

```bash
./gradlew --no-daemon test \
  --tests '*CouponBatchConsumerTest' \
  --tests '*ProductionBatchConsumerRecoveryTest' \
  --tests '*ReservedCouponIssueIntegrationTest' \
  --tests '*ReservationFailureRecoveryTest'
```

선택34개가 대상이다. 전체 프로젝트 테스트 통과를 의미하지 않는다. 최신 공개 준비 검증 결과는 [검증 요약](publication-validation.json)을 참조한다. 성능 재현은 [실제 앱 재현 명령](../../선착순_쿠폰발급_부하테스트/results/2026-10-15-production-batch-load/REPRODUCE.txt)을 따른다. 공개 정리 작업에서는 새 성능 개선이나 부하 재측정을 하지 않는다.

## 남은 문제

- Redis pending 적체와 최종 발급 대기는 여전히 존재한다.
- Relay 종료 중 Redis 자원 접근 오류는 별도 수정 과제다. 최신 부하에서 ERROR 로그0이라는 사실만으로 해결됐다고 보지 않는다.
- 단일 Relay·단일 파티션/Consumer 전제, Redis AOF always/동일 볼륨 복구 범위이다. 다중 노드 장애 보장은 검증하지 않았다.
- 처리 실패 후 원인을 제거하고 같은 Consumer 그룹으로 명시적으로 재시작한다. 영구 오류 격리·운영 자동 복구는 이번 범위 밖이다.

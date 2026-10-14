# coupon-system

순간적으로 몰리는 쿠폰 신청을 빠르게 예약하고, 장애 후에도 중복 없이 최종 발급하는 학습 프로젝트입니다. 큐는 초과 유입을 보관하고 DB 부하를 조절하며, DB 자체의 처리량을 자동으로 높이지 않습니다.

## 현재 구조

신청 → **Redis 예약**(재고·중복 확인, 차감·예약·pending 원자 등록) → 예약 응답 → 단일 Relay → Kafka → Consumer DB 트랜잭션 → 최종 결과 조회.

- `RESERVED`: Redis에서 예약 승인. `ISSUED`: DB 반영이 끝나 사용 가능한 발급.
- 행사 전 수량·기간 고정. 승인된 예약은 행사 종료 후에도 발급하며, DB 장애 시 유지·재처리합니다.
- DB 조건부 수량 증가·고유 제약·requestId 멱등성으로 수량과 중복 발급을 방어합니다.
- Redis AOF always와 같은 볼륨 재시작을 검증했습니다. 다중 노드 장애/데이터 유실까지 보장하는 구성은 아닙니다.
- Consumer는 기본 단건이며 `COUPON_BATCH_ENABLED=true`로 최대10건·10ms 묶음 모드를 선택합니다. Lua Relay 통합은 아직 테스트 후보입니다.

| 실행 모드 | 설정 | Consumer 처리 | Relay |
| --- | --- | --- | --- |
| 기본 단건 | `COUPON_BATCH_ENABLED=false` (기본값) | Spring Listener, 건별 DB 트랜잭션·RECORD offset 커밋 | 기존 서비스 구현 |
| 선택 배치 | `COUPON_BATCH_ENABLED=true` | 최대10건·첫 수신 후10ms 수집, 묶음 DB 커밋 후 offset 커밋 | 기존 서비스 구현 |

배치 모드에서는 단건 Listener가 자동 시작되지 않습니다. 두 모드 모두 Relay를 실행하려면 `COUPON_RELAY_ENABLED=true`가 필요합니다. 테스트 전용 Lua Relay·Consumer 후보의 결과는 실제 앱 결과와 구분합니다.

## 검증된 내용

| 범위 | 확인한 내용 | 근거 |
| --- | --- | --- |
| 전체 연결·장애 복구 | Redis 재시작, Relay 전달 경계, DB 실패·커밋 후 재전달에서도 승인 예약 정합성 | [예약 검증 기록](docs/testing/reservation-tests.md) |
| Consumer 비용 | 단건 Consumer·Lua Relay 테스트 후보에서 DB 구간이 직렬 처리 시간의 약84%. 실제 배치 앱의 비용 비율은 아님 | [Consumer 계측](선착순_쿠폰발급_부하테스트/results/2026-10-14-reserved-consumer-cost/RESULTS.txt) |
| DB 전용 묶음 비교 | 1건236~360건/s, 10건841~848건/s. 커밋뿐 아니라 SELECT·flush도 감소 | [DB 비교](선착순_쿠폰발급_부하테스트/results/2026-10-14-db-batch/RESULTS.txt) |

성능 수치는 로컬 조건별 결과입니다. 예약 응답과 최종 완료, 단계별 개선과 전체 개선을 구분하며 서로 다른 구조·내구성의 수치를 직접 합치지 않습니다.

## 현재 작업

[최대10건·10ms Consumer](docs/testing/batch-consumer.md)를 서비스 선택 옵션으로 반영하고 선택 테스트34개를 통과했습니다. 서비스 전체 부하5회에서85,012건을 모두 발급했습니다. 시계 오류가 있던 단건1회를 지연 비교에서 제외한 결과, 최종 발급 p95는 단건63.8~67.3초·묶음40.7~48.1초였습니다. 재측정은 새 격리 인프라였으므로 조건·편차를 상세 문서와 함께 봐야 합니다. Redis pending은 여전히 크게 남았고, Relay 종료 순서 문제는 별도 과제로 유지합니다.

## 실행과 테스트

Java17, Spring Boot/JPA, MySQL8, Redis7, Kafka3.7, Docker Compose, k6를 사용합니다.

```bash
docker compose up -d
./gradlew bootRun
./gradlew test
```

앱은8080, MySQL은13306, Redis는6379, Kafka는19092를 사용합니다. Relay는 기본 비활성이며 실행 시 `COUPON_RELAY_ENABLED=true`로 켭니다. 묶음 모드는 `COUPON_BATCH_ENABLED=true COUPON_RELAY_ENABLED=true ./gradlew bootRun`으로 실행합니다. 단일 파티션·단일 Consumer 전제이며 처리 실패 후에는 원인을 제거하고 앱을 재시작합니다. 행사 사전 예약 초기화가 필요하며, 기존 `/stock/init`·동기 발급 경로는 기본 차단됩니다. 현재 테스트는 격리 인프라와 사전 초기화 fixture를 사용합니다. 성능 시험은 일반 단위 테스트와 별도이며 각 결과의 재현 명령을 따릅니다.

## 문서 안내

| 문서 | 내용 |
| --- | --- |
| [예약 정합성 설계](docs/design/reservation-consistency.md) | 첫 버전 정책, 내구성·오류 처리, 미확정 확장안 |
| [예약 검증 기록](docs/testing/reservation-tests.md) | 단계별 테스트 계획·결과와 원시 자료 |
| [성능 실험 기록](docs/testing/performance.md) | 전체 부하, Relay·Consumer 계측, DB 묶음 비교 |
| [현재 묶음 실험](docs/testing/batch-consumer.md) | 10ms 규칙, 장애 기준, 비교 조건과 결과 |
| [동기·Kafka 초기 기록](docs/history/synchronous-kafka.md) | 과거 구현/측정/도입 판단. 현재 상태와 구분 |

공개 근거의 포함 범위와 검증 명령은 [공개 자료 안내](docs/testing/evidence-publication.md)를 참조하세요. Compose의 DB 계정은 로컬 실습용 기본값이며 운영 자격증명이 아닙니다.

README에는 현재 상태와 핵심 근거만 유지합니다. 설계 변경은 설계 문서, 실험 계획·결과는 해당 주제 문서, 원시 자료는 기존 `선착순_쿠폰발급_부하테스트/results/`에 기록합니다. 기존 README의 상세 기록은 위 문서로 이동했으며 결과 파일은 유지했습니다.

사용자가 직접 구현하며 배우는 실습입니다. 코드 작성·수정·실행은 사용자 요청 범위에서 진행하고, 문서 정리 자체를 서비스 변경 승인으로 해석하지 않습니다.

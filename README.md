# coupon-system — 선착순 쿠폰 발급 동시성 제어

대량 동시 요청에서 재고 초과 발급과 중복 발급 없이 선착순 쿠폰을 발급하는 것을 목표로,
동시성 문제를 단계적으로 재현하고 해결한 학습 프로젝트입니다.

- 상세 정리: [선착순 쿠폰 발급 1 — 문제 정의와 해결 과정](https://three-atom-0d5.notion.site/1-37bb6bf97c4380e394a6d7f0a74acca0)
- 부하 실측: [k6 결과 (2026-08-17)](선착순_쿠폰발급_부하테스트/K6_RESULTS_2026-08-17.md)

## 문제와 해결 단계

1. 조회 → 검증 → 증가 흐름에서 발생하는 재고 초과·중복 발급 race를 동시성 테스트로 재현
2. 조건부 atomic UPDATE (`issuedQuantity < totalQuantity`일 때만 증가)로 재고 초과 방지
3. `(coupon_id, user_id)` unique constraint로 중복 발급을 DB 레벨에서 차단
4. Redis Lua 스크립트로 중복 체크·재고 확인·차감·등록을 원자 실행해 발급 판정을 DB 앞으로 이동,
   DB 저장 실패 시 Redis 재고 rollback

## k6 부하 실측 요약 (300 VUs · 30초, 로컬)

| 선착순 재고 100 | atomic UPDATE 경로 | Redis Lua 경로 |
|---|---|---|
| 처리량 | 3,325 RPS | 5,112 RPS |
| p50 / p95 | 88ms / 102ms | 56ms / 94ms |
| 재고 소진 후 거절 | 전부 MySQL 도달 | Redis에서 흡수, DB 도달 0건 |
| 발급 수 | 정확히 100 | 정확히 100 |

재고가 남아 있는 구간은 두 경로 모두 DB 쓰기가 지배해 사실상 동일하다(약 665 RPS).
Redis 도입의 이득은 재고 소진 후 거절 트래픽이 DB에 도달하지 않는 데 있다.

## 기술

Java, Spring Boot, JPA, MySQL, Redis(Lua), Docker Compose, k6

## 실행

```bash
docker compose up -d          # MySQL(13306)·Redis·Kafka
./gradlew bootRun

# 부하 테스트
k6 run -e RUN_SEED=1 -e VUS=300 -e DURATION=30s -e STOCK=100 \
  선착순_쿠폰발급_부하테스트/k6-load-test.js
```

## 다음 단계

- Kafka 기반 비동기 DB 저장 분리 — compose에 broker(KRaft) 구성만 완료, 애플리케이션 연결은 미구현
- 락 방식(비관적 락)과의 HTTP 부하 비교 — 현재 락 구현은 JUnit 동시성 테스트에서만 사용

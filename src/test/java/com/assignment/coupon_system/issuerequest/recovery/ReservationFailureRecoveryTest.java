package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.coupon.redis.CouponRedisKey;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ReservationFailureRecoveryTest extends ReservationTestSupport {
    long coupon;
    long baseline;
    String prefix;
    final List<String> ids = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    void prepareCoupon(String boundary) throws Exception {
        launch("before", true, boundary, boundary == null ? null : ids.get(0));
        baseline = endOffset();
        // Consumer가 아직 시작하지 않은 새 그룹만 이전 테스트 데이터 끝으로 이동한다.
        admin.alterConsumerGroupOffsets(group, Map.of(PARTITION, new OffsetAndMetadata(baseline)))
                .all().get(10, TimeUnit.SECONDS);
        Instant start = Instant.now().minusSeconds(3600), end = Instant.now().plusSeconds(3600);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "failure-boundary"); body.put("description", "isolated fixture");
        body.put("couponType", "FIXED_AMOUNT"); body.put("minOrderAmount", 0);
        body.put("maxDiscountAmount", 1000); body.put("totalQuantity", 10); body.put("validDays", 30);
        body.put("startDateTime", LocalDateTime.ofInstant(start, ZoneId.of("Asia/Seoul")).toString());
        body.put("endDateTime", LocalDateTime.ofInstant(end, ZoneId.of("Asia/Seoul")).toString());
        coupon = request("POST", "/api/coupons", body, 200).path("data").path("id").asLong();
        assertThat(reservations.initializeReservation(coupon, 10, start, end)).isTrue();
        prefix = "/api/coupons/" + coupon;
        save("initial.json", Map.of("couponId", coupon, "requestIds", ids, "group", group, "baseline", baseline));
    }

    void startConsumer() throws Exception {
        assertThat(request("POST", "/__test/consumer/start", Map.of(), 200).path("running").asBoolean()).isTrue();
    }

    void waitForMarker(String name) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(app.isAlive()).isTrue();
            assertThat(evidence.resolve("before").resolve(name)).exists();
        });
    }

    void killAtBoundary() throws Exception {
        long pid = app.pid();
        app.destroyForcibly();
        assertThat(app.waitFor(10, TimeUnit.SECONDS)).isTrue();
        save("killed.json", Map.of("pid", pid, "exitCode", app.exitValue(), "redis", snapshot(),
                "endOffset", endOffset(), "committedOffset", committedOffset()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"after-claim", "after-kafka-ack"})
    void relayCrashResendsWithoutLosingOrDuplicatingIssuance(String boundary) throws Exception {
        // 파라미터 실행별로 별도 로그 경로를 둔다.
        evidence = evidence.resolve(boundary); Files.createDirectories(evidence);
        prepareCoupon(boundary);
        submit(prefix, ids.get(0), 100);
        waitForMarker("relay-boundary.jsonl");
        JsonNode marker = JSON.readTree(Files.readAllLines(evidence.resolve("before/relay-boundary.jsonl")).get(0));
        assertThat(marker.path("boundary").asText()).isEqualTo(boundary);
        assertThat(marker.path("requestId").asText()).isEqualTo(ids.get(0));
        assertThat(marker.path("pid").asLong()).isEqualTo(app.pid());
        assertThat(redis.opsForList().range(CouponRedisKey.processingReservations(), 0, -1)).containsExactly(ids.get(0));
        submit(prefix, ids.get(1), 101);
        assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1)).containsExactly(ids.get(1));
        assertThat(committedOffset()).isEqualTo(baseline);
        Long firstIssued = null;
        if (boundary.equals("after-kafka-ack")) {
            assertThat(endOffset()).isEqualTo(baseline + 1);
            startConsumer();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertCounts(coupon, 1));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(baseline + 1));
            firstIssued = query(prefix, ids.get(0)).path("issuedCouponId").asLong();
        } else {
            assertThat(endOffset()).isEqualTo(baseline);
            assertCounts(coupon, 0);
        }
        save("at-boundary.json", Map.of("marker", marker, "redis", snapshot(), "db", dbResults(),
                "endOffset", endOffset(), "committedOffset", committedOffset()));
        killAtBoundary();
        launch("after", true);
        startConsumer();
        verifyFinal(boundary.equals("after-kafka-ack") ? 3 : 2);
        if (firstIssued != null) assertThat(query(prefix, ids.get(0)).path("issuedCouponId").asLong()).isEqualTo(firstIssued);
        var records = deliveries();
        var firstRecords = records.stream().filter(row -> row.path("requestId").asText().equals(ids.get(0))).toList();
        if (boundary.equals("after-kafka-ack")) {
            assertThat(firstRecords).hasSize(2);
            assertThat(firstRecords.get(0).path("offset").asLong()).isNotEqualTo(firstRecords.get(1).path("offset").asLong());
        } else assertThat(firstRecords).hasSize(1);
        save("deliveries.json", records);
    }

    @Test
    void dbFailureRollsBackThenExplicitResumeProcessesFailedAndFollowingRequests() throws Exception {
        prepareCoupon(null);
        startConsumer();
        jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT reservation_db_failure CHECK (request_id <> '" + ids.get(0) + "')");
        try {
            submit(prefix, ids.get(0), 100);
            await().atMost(Duration.ofSeconds(30)).until(() ->
                    !request("GET", "/__test/consumer/status", null, 200).path("running").asBoolean());
            assertCounts(coupon, 0);
            assertThat(committedOffset()).isEqualTo(baseline);
            submit(prefix, ids.get(1), 101);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(endOffset()).isEqualTo(baseline + 2));
            assertCounts(coupon, 0);
            for (String id : ids) assertThat(query(prefix, id).path("status").asText()).isEqualTo("RESERVED");
            assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("8");
            assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(coupon))).containsExactlyInAnyOrder("100", "101");
            save("db-failed.json", Map.of("redis", snapshot(), "db", dbResults(), "issuedQuantity", 0,
                    "issuedHistoryCount", number("SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=?", coupon),
                    "endOffset", endOffset(), "committedOffset", committedOffset()));
        } finally {
            jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK reservation_db_failure");
        }
        assertThat(request("GET", "/__test/consumer/status", null, 200).path("running").asBoolean()).isFalse();
        startConsumer();
        verifyFinal(2);
        var records = deliveries();
        assertThat(records).hasSize(3);
        assertThat(records.get(0)).isEqualTo(records.get(1));
        assertThat(records.get(0).path("requestId").asText()).isEqualTo(ids.get(0));
        assertThat(records.get(2).path("requestId").asText()).isEqualTo(ids.get(1));
        save("deliveries.json", records);
    }

    @Test
    void committedDbResultSurvivesSameKafkaRecordRedeliveryAfterKill() throws Exception {
        prepareCoupon("after-db-commit");
        startConsumer();
        submit(prefix, ids.get(0), 100);
        waitForMarker("after-db-commit.jsonl");
        JsonNode marker = JSON.readTree(Files.readAllLines(evidence.resolve("before/after-db-commit.jsonl")).get(0));
        assertThat(marker.path("transactionActive").asBoolean()).isFalse();
        assertCounts(coupon, 1); // 별도 JDBC 연결로 실제 커밋 가시성 확인
        var original = dbResults().get(0);
        assertThat(committedOffset()).isEqualTo(baseline);
        submit(prefix, ids.get(1), 101);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(endOffset()).isEqualTo(baseline + 2));
        save("db-committed-offset-pending.json", Map.of("marker", marker, "result", original,
                "redis", snapshot(), "endOffset", endOffset(), "committedOffset", committedOffset()));
        killAtBoundary();
        launch("after", true);
        startConsumer();
        verifyFinal(2);
        var replay = dbResults().stream().filter(row -> row.get("request_id").equals(ids.get(0))).findFirst().orElseThrow();
        assertThat(replay).isEqualTo(original); // 결과ID·발급ID·처리시각까지 그대로
        var records = deliveries();
        assertThat(records).hasSize(3);
        assertThat(records.get(0)).isEqualTo(records.get(1));
        assertThat(records.get(0).path("requestId").asText()).isEqualTo(ids.get(0));
        assertThat(records.get(2).path("requestId").asText()).isEqualTo(ids.get(1));
        save("deliveries.json", records);
    }

    List<Map<String, Object>> dbResults() {
        return jdbc.queryForList("SELECT id,request_id,coupon_id,user_id,status,issued_coupon_id,CAST(processed_at AS CHAR) AS processed_at FROM coupon_issue_result WHERE coupon_id=? ORDER BY id", coupon);
    }

    List<JsonNode> deliveries() throws Exception {
        List<JsonNode> records = new ArrayList<>();
        for (String phase : List.of("before", "after")) {
            Path p = evidence.resolve(phase).resolve("deliveries.jsonl");
            if (Files.exists(p)) for (String line : Files.readAllLines(p)) records.add(JSON.readTree(line));
        }
        return records;
    }

    void verifyFinal(int kafkaRecords) throws Exception {
        await().pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            assertCounts(coupon, 2);
            for (int i = 0; i < ids.size(); i++) {
                var result = query(prefix, ids.get(i));
                assertThat(result.path("status").asText()).isEqualTo("ISSUED");
                assertThat(result.path("userId").asLong()).isEqualTo(100 + i);
                assertThat(number("SELECT COUNT(*) FROM issued_coupon WHERE id=? AND coupon_id=? AND user_id=?",
                        result.path("issuedCouponId").asLong(), coupon, 100 + i)).isEqualTo(1);
                assertThat(reservations.findReservation(ids.get(i)).userId()).isEqualTo(100 + i);
            }
            assertThat(redis.opsForList().size(CouponRedisKey.pendingReservations())).isZero();
            assertThat(redis.opsForList().size(CouponRedisKey.processingReservations())).isZero();
            assertThat(endOffset()).isEqualTo(baseline + kafkaRecords);
            assertThat(committedOffset()).isEqualTo(endOffset());
        });
        assertThat(dbResults()).extracting(row -> row.get("request_id")).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("8");
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(coupon))).containsExactlyInAnyOrder("100", "101");
        save("final.json", Map.of("requestIds", ids, "results", dbResults(), "redis", snapshot(),
                "baseline", baseline, "endOffset", endOffset(), "committedOffset", committedOffset(),
                "issuedQuantity", 2, "issuedHistoryCount", number("SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=?", coupon)));
    }
}

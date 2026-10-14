package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.coupon.redis.*;
import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ReservationEndToEndTest extends ReservationTestSupport {
    @Test
    void redisKeepsReservationsAcrossRestartAndCrashRecreation() throws Exception {
        Instant now = Instant.now();
        assertThat(reservations.initializeReservation(900L, 10, now.minusSeconds(60), now.plusSeconds(3600))).isTrue();
        String first = UUID.randomUUID().toString(), second = UUID.randomUUID().toString();
        assertThat(reservations.reserve(first, 900L, 1L)).isEqualTo(CouponReservationStatus.RESERVED);
        assertThat(reservations.reserve(second, 900L, 2L)).isEqualTo(CouponReservationStatus.RESERVED);
        assertThat(reservations.claimPendingReservation()).isEqualTo(first);
        var before = snapshot(); save("before.json", before);
        connection.destroy();
        DockerClientFactory.instance().client().restartContainerCmd(redisContainer.getContainerId()).exec();
        connectRedis();
        assertThat(snapshot()).isEqualTo(before); save("after-normal-restart.json", snapshot());
        crashAndRecreateRedis();
        assertThat(snapshot()).isEqualTo(before); save("after-crash-recreation.json", snapshot());
        assertThat(reservations.reserve(first, 900L, 1L)).isEqualTo(CouponReservationStatus.ALREADY_RESERVED);
        assertThat(snapshot()).isEqualTo(before);
        savePersistence();
    }

    @Test
    void httpReservationsSurviveRedisCrashAndReachDbThroughRealRelayAndKafka() throws Exception {
        launch("accept", false);
        Instant start = Instant.now().minusSeconds(3600), end = Instant.now().plusSeconds(3600);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "reservation-e2e"); body.put("description", "isolated fixture");
        body.put("couponType", "FIXED_AMOUNT"); body.put("minOrderAmount", 0);
        body.put("maxDiscountAmount", 1000); body.put("totalQuantity", 10); body.put("validDays", 30);
        body.put("startDateTime", LocalDateTime.ofInstant(start, ZoneId.of("Asia/Seoul")).toString());
        body.put("endDateTime", LocalDateTime.ofInstant(end, ZoneId.of("Asia/Seoul")).toString());
        long coupon = request("POST", "/api/coupons", body, 200).path("data").path("id").asLong();
        assertThat(coupon).isPositive();
        assertThat(reservations.initializeReservation(coupon, 10, start, end)).isTrue();
        List<String> ids = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String prefix = "/api/coupons/" + coupon;
        List<JsonNode> approvals = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            approvals.add(submit(prefix, ids.get(i), 100 + i));
            assertThat(query(prefix, ids.get(i)).path("status").asText()).isEqualTo("RESERVED");
        }
        assertThat(submit(prefix, ids.get(0), 100)).isEqualTo(approvals.get(0));
        assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1)).containsExactlyElementsOf(ids);
        assertThat(reservations.claimPendingReservation()).isEqualTo(ids.get(0));
        var before = snapshot(); save("approved-before-restart.json", Map.of("approvals", approvals, "redis", before));
        assertCounts(coupon, 0);
        stopApp();
        crashAndRecreateRedis();
        assertThat(snapshot()).isEqualTo(before); save("redis-after-recreation.json", snapshot());
        Path run = launch("deliver", true);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(redis.opsForList().size(CouponRedisKey.pendingReservations())).isZero();
            assertThat(redis.opsForList().size(CouponRedisKey.processingReservations())).isZero();
            assertThat(endOffset()).isEqualTo(3);
        });
        assertCounts(coupon, 0);
        for (String id : ids) assertThat(query(prefix, id).path("status").asText()).isEqualTo("RESERVED");
        request("POST", prefix + "/stock/init", Map.of(), 409);
        request("POST", prefix + "/issue", Map.of("userId", 999), 409);
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("7");
        request("POST", "/__test/consumer/start", Map.of(), 200);
        await().pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            for (String id : ids) assertThat(query(prefix, id).path("status").asText()).isEqualTo("ISSUED");
        });
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(endOffset()));
        assertCounts(coupon, 3);
        List<Map<String, Object>> results = jdbc.queryForList(
                "SELECT request_id, coupon_id, user_id, status, issued_coupon_id FROM coupon_issue_result WHERE coupon_id=?", coupon);
        assertThat(results).extracting(row -> row.get("request_id")).containsExactlyInAnyOrderElementsOf(ids);
        for (int i = 0; i < ids.size(); i++) {
            var result = query(prefix, ids.get(i));
            assertThat(result.path("userId").asLong()).isEqualTo(100 + i);
            assertThat(number("SELECT COUNT(*) FROM issued_coupon WHERE id=? AND coupon_id=? AND user_id=?",
                    result.path("issuedCouponId").asLong(), coupon, 100 + i)).isEqualTo(1);
            assertThat(submit(prefix, ids.get(i), 100 + i)).isEqualTo(approvals.get(i));
            assertThat(reservations.findReservation(ids.get(i)).userId()).isEqualTo(100 + i);
        }
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(coupon))).containsExactlyInAnyOrder("100", "101", "102");
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("7");
        assertThat(redis.opsForList().size(CouponRedisKey.pendingReservations())).isZero();
        assertThat(redis.opsForList().size(CouponRedisKey.processingReservations())).isZero();
        assertThat(endOffset()).isEqualTo(3);
        assertCounts(coupon, 3);
        var deliveries = Files.readAllLines(run.resolve("deliveries.jsonl")).stream().map(line -> {
            try { return JSON.readTree(line); } catch (Exception e) { throw new RuntimeException(e); }
        }).toList();
        assertThat(deliveries).extracting(row -> row.path("requestId").asText()).containsExactlyElementsOf(ids);
        save("final.json", Map.of("couponId", coupon, "requestIds", ids, "results", results,
                "redis", snapshot(), "endOffset", endOffset(), "committedOffset", committedOffset(), "deliveries", deliveries));
        savePersistence();
    }

}

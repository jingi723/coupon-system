package com.assignment.coupon_system.issuerequest.consumer;

import com.assignment.coupon_system.coupon.entity.*;
import com.assignment.coupon_system.coupon.redis.*;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static com.assignment.coupon_system.issuerequest.entity.CouponIssueResultStatus.ISSUED;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@DataJpaTest(showSql = false, properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({CouponIssueProcessor.class, CouponIssueConsumer.class, CouponIssueRedisService.class,
        ReservedCouponIssueIntegrationTest.RedisConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@DisplayName("승인 예약 Consumer→DB 연결: 실제 Redis/MySQL, Kafka 전송 제외")
class ReservedCouponIssueIntegrationTest {
    static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    @Container @ServiceConnection
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @TestConfiguration(proxyBeanMethods = false)
    static class RedisConfig {
        @Bean
        LettuceConnectionFactory redisConnectionFactory() {
            return new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        }
        @Bean
        StringRedisTemplate redisTemplate(LettuceConnectionFactory f) { return new StringRedisTemplate(f); }
    }

    @Autowired CouponIssueConsumer consumer;
    @Autowired CouponIssueProcessor processor;
    @Autowired CouponIssueRedisService reservations;
    @Autowired StringRedisTemplate redis;
    @Autowired CouponRepository coupons;
    @Autowired IssuedCouponRepository issues;
    @Autowired CouponIssueResultRepository results;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        results.deleteAll(); issues.deleteAll(); coupons.deleteAll();
        try (var c = redis.getConnectionFactory().getConnection()) { c.serverCommands().flushDb(); }
    }

    @Test
    void reservationMadeDuringEventIsIssuedAfterActualEventEnd() {
        Instant end = Instant.now().plusSeconds(2).truncatedTo(ChronoUnit.MILLIS);
        Coupon c = create(1, end.minusSeconds(3600), end);
        var message = reserve(c, 10L);
        assertThat(message.receivedAt()).isBefore(end);
        await().atMost(Duration.ofSeconds(5)).until(() -> Instant.now().isAfter(end));
        assertThat(coupons.findById(c.getId()).orElseThrow().isAvailable()).isFalse();

        consumer.consume(message);
        assertIssued(c, message);
        assertReservationRemains(c, message, "0");
    }

    @Test
    void committedReservationReplayDoesNotIssueAgain() {
        Coupon c = active(2);
        var message = reserve(c, 10L);
        consumer.consume(message);
        var first = results.findByRequestId(message.requestId()).orElseThrow();
        consumer.consume(message);
        var replay = results.findByRequestId(message.requestId()).orElseThrow();
        assertThat(replay.getId()).isEqualTo(first.getId());
        assertThat(replay.getIssuedCouponId()).isEqualTo(first.getIssuedCouponId());
        assertIssued(c, message);
        assertReservationRemains(c, message, "1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"coupon", "user", "time"})
    void mismatchedMessageIsNotIssued(String field) {
        Coupon c = active(1);
        var original = reserve(c, 10L);
        var changed = new CouponIssueMessage(original.requestId(),
                field.equals("coupon") ? c.getId()+1 : c.getId(),
                field.equals("user") ? 11L : 10L,
                field.equals("time") ? original.receivedAt().plusMillis(1) : original.receivedAt());
        assertThatThrownBy(() -> consumer.consume(changed))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Redis 예약 정보가 다릅니다");
        assertCounts(c, 0);
        assertReservationRemains(c, original, "0");
    }

    @Test
    void messageWithoutReservationCannotBypassRedisApproval() {
        Coupon c = active(1);
        var message = new CouponIssueMessage(UUID.randomUUID().toString(), c.getId(), 10L, Instant.now());
        assertThatThrownBy(() -> consumer.consume(message))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("예약 정보가 없습니다");
        assertCounts(c, 0);
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(c.getId()))).isEqualTo("1");
    }

    @Test
    void missingApprovalTimeIsNotIssued() {
        Coupon c = active(1);
        var message = reserve(c, 10L);
        assertThatThrownBy(() -> consumer.consume(new CouponIssueMessage(
                message.requestId(), c.getId(), 10L, null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertCounts(c, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"beforeStart", "atEnd"})
    void processorRejectsOutOfPeriodApprovalEvenIfRedisRecordIsCorrupted(String boundary) {
        Coupon c = active(1);
        var message = reserve(c, 10L);
        Instant invalid = boundary.equals("beforeStart")
                ? c.getStartDate().atZone(ZONE).toInstant().minusMillis(1)
                : c.getEndDate().atZone(ZONE).toInstant();
        // Fault injection: not a legitimate Redis approval. Message matches damaged Redis data.
        redis.opsForHash().put(CouponRedisKey.reservation(message.requestId()),
                "reservedAtMillis", Long.toString(invalid.toEpochMilli()));
        assertThatThrownBy(() -> consumer.consume(new CouponIssueMessage(
                message.requestId(), c.getId(), 10L, invalid)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("행사 기간 밖");
        assertCounts(c, 0);
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(c.getId()))).isEqualTo("0");
    }

    @Test
    void processorIncludesExactStartBoundary() {
        Coupon c = active(1);
        // DB rule boundary test; direct Processor call does not prove Redis approval.
        var result = processor.processReserved(UUID.randomUUID().toString(), c.getId(), 10L,
                c.getStartDate().atZone(ZONE).toInstant());
        assertThat(result.getStatus()).isEqualTo(ISSUED);
        assertCounts(c, 1);
    }

    @Test
    void dbResultInsertFailureRollsBackAndSameReservationCanBeRetried() {
        Coupon c = active(1);
        var message = reserve(c, 10L);
        jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT reserved_result_failure CHECK (user_id <> 10)");
        try {
            assertThatThrownBy(() -> consumer.consume(message)).hasStackTraceContaining("reserved_result_failure");
            assertCounts(c, 0); // independent reads after service transaction has rolled back
            assertReservationRemains(c, message, "0");
        } finally {
            jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK reserved_result_failure");
        }
        consumer.consume(message);
        assertIssued(c, message);
        assertReservationRemains(c, message, "0");
    }

    @Test
    void dbLimitViolationStopsWithoutRejectingOrRefundingReservation() {
        Coupon c = active(1);
        var message = reserve(c, 10L);
        // Inject inconsistent DB count to verify the final upper-bound defense.
        jdbc.update("UPDATE coupon SET issued_quantity = total_quantity WHERE id = ?", c.getId());
        assertThatThrownBy(() -> consumer.consume(message))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("DB 수량이 부족");
        assertThat(coupons.findById(c.getId()).orElseThrow().getIssuedQuantity()).isEqualTo(1);
        assertThat(issues.count()).isZero(); assertThat(results.count()).isZero();
        assertReservationRemains(c, message, "0");
    }

    private Coupon active(int stock) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return create(stock, now.minusSeconds(3600), now.plusSeconds(3600));
    }
    private Coupon create(int stock, Instant start, Instant end) {
        Coupon c = coupons.save(Coupon.create("예약 테스트", "고정 행사", CouponType.FIXED_AMOUNT,
                0, 1000, stock, 30, LocalDateTime.ofInstant(start, ZONE), LocalDateTime.ofInstant(end, ZONE)));
        assertThat(reservations.initializeReservation(c.getId(), stock, start, end)).isTrue();
        return c;
    }
    private CouponIssueMessage reserve(Coupon c, long user) {
        String id = UUID.randomUUID().toString();
        assertThat(reservations.reserve(id, c.getId(), user)).isEqualTo(CouponReservationStatus.RESERVED);
        var r = reservations.findReservation(id);
        return new CouponIssueMessage(id, c.getId(), user, r.reservedAt());
    }
    private void assertCounts(Coupon c, int count) {
        assertThat(coupons.findById(c.getId()).orElseThrow().getIssuedQuantity()).isEqualTo(count);
        assertThat(issues.count()).isEqualTo(count);
        assertThat(results.count()).isEqualTo(count);
    }
    private void assertIssued(Coupon c, CouponIssueMessage message) {
        assertCounts(c, 1);
        var result = results.findByRequestId(message.requestId()).orElseThrow();
        assertThat(result.getStatus()).isEqualTo(ISSUED);
        assertThat(result.getCouponId()).isEqualTo(c.getId());
        assertThat(result.getUserId()).isEqualTo(message.userId());
        var issued = issues.findById(result.getIssuedCouponId()).orElseThrow();
        assertThat(issued.getCoupon().getId()).isEqualTo(c.getId());
        assertThat(issued.getUserId()).isEqualTo(message.userId());
    }
    private void assertReservationRemains(Coupon c, CouponIssueMessage message, String stock) {
        assertThat(reservations.findReservation(message.requestId()).reservedAt()).isEqualTo(message.receivedAt());
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(c.getId()))).isEqualTo(stock);
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(c.getId())))
                .containsExactly(message.userId().toString());
    }
}

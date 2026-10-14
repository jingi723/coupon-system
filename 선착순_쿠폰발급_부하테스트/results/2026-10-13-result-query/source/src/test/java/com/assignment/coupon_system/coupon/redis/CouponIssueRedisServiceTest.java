package com.assignment.coupon_system.coupon.redis;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static com.assignment.coupon_system.coupon.redis.CouponReservationStatus.*;

@Testcontainers
@DisplayName("실제 Redis 예약 및 동시성 검증")
class CouponIssueRedisServiceTest {
    // 앱의 Redis와 분리된 일회용 컨테이너만 사용한다.
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    static LettuceConnectionFactory connectionFactory;
    static StringRedisTemplate redis;
    CouponIssueRedisService service;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redis = new StringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void disconnect() {
        if (connectionFactory != null) connectionFactory.destroy();
    }

    @BeforeEach
    void setUp() {
        try (var connection = connectionFactory.getConnection()) {
            connection.serverCommands().flushDb();
        }
        service = new CouponIssueRedisService(redis);
        assertThat(service.initializeReservation(1L, 2,
                Instant.now().minusSeconds(3600), Instant.now().plusSeconds(3600))).isTrue();
    }

    @Test
    void optionalLookupDistinguishesMissingAndExistingReservation() {
        String id = UUID.randomUUID().toString();
        assertThat(service.findReservationIfPresent(id)).isEmpty();
        assertThatThrownBy(() -> service.findReservation(id)).isInstanceOf(IllegalStateException.class);
        service.reserve(id, 1L, 10L);
        assertThat(service.findReservationIfPresent(id)).contains(service.findReservation(id));
        assertState("1", Set.of("10"), List.of(id));
    }

    @Test
    void optionalLookupDoesNotHideMissingFields() {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        redis.opsForHash().delete(CouponRedisKey.reservation(id), "reservedAtMillis");
        assertThatThrownBy(() -> service.findReservationIfPresent(id))
                .isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"couponId", "userId", "reservedAtMillis", "policyVersion"})
    void optionalLookupReportsMalformedNumbersAsDataError(String field) {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        redis.opsForHash().put(CouponRedisKey.reservation(id), field, "invalid");
        assertThatThrownBy(() -> service.findReservationIfPresent(id))
                .isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void createsReservationAndPendingTogether() {
        String id = UUID.randomUUID().toString();
        long before = redisTimeMillis();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        assertState("1", Set.of("10"), List.of(id));
        var reservation = redis.opsForHash().entries(CouponRedisKey.reservation(id));
        assertThat(reservation).containsEntry("requestId", id)
                .containsEntry("couponId", "1").containsEntry("userId", "10");
        assertThat(Long.parseLong((String) reservation.get("reservedAtMillis")))
                .isBetween(before, redisTimeMillis());
        assertThat(reservation).containsEntry("policyVersion", "1").doesNotContainKey("reservedAt");
    }

    @Test
    void retryAfterSoldOutPreservesOriginalReservation() {
        redis.opsForValue().set(CouponRedisKey.stock(1L), "1");
        String id = UUID.randomUUID().toString();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        var original = redis.opsForHash().entries(CouponRedisKey.reservation(id));
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(ALREADY_RESERVED);
        assertState("0", Set.of("10"), List.of(id));
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservation(id))).isEqualTo(original);
    }

    @Test
    void differentRequestForSameUserIsRejected() {
        String first = UUID.randomUUID().toString();
        String second = UUID.randomUUID().toString();
        assertThat(service.reserve(first, 1L, 10L)).isEqualTo(RESERVED);
        assertThat(service.reserve(second, 1L, 10L)).isEqualTo(DUPLICATE_USER);
        assertState("1", Set.of("10"), List.of(first));
        assertThat(redis.hasKey(CouponRedisKey.reservation(second))).isFalse();
    }

    @Test
    void requestIdCannotBeReusedForAnotherUserOrCoupon() {
        String id = UUID.randomUUID().toString();
        redis.opsForValue().set(CouponRedisKey.stock(2L), "5");
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        var original = redis.opsForHash().entries(CouponRedisKey.reservation(id));
        assertThat(service.reserve(id, 1L, 11L)).isEqualTo(REQUEST_CONFLICT);
        assertThat(service.reserve(id, 2L, 10L)).isEqualTo(REQUEST_CONFLICT);
        assertState("1", Set.of("10"), List.of(id));
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(2L))).isEqualTo("5");
        assertThat(redis.hasKey(CouponRedisKey.issueUsers(2L))).isFalse();
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservation(id))).isEqualTo(original);
    }

    @Test
    void soldOutDoesNotCreateReservation() {
        redis.opsForValue().set(CouponRedisKey.stock(1L), "0");
        String id = UUID.randomUUID().toString();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(SOLD_OUT);
        assertState("0", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @Test
    void missingStockIsInitializationError() {
        redis.delete(CouponRedisKey.stock(1L));
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("초기화");
        assertState(null, Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @Test
    void wrongPendingTypeFailsBeforeStockIsChanged() {
        redis.opsForValue().set(CouponRedisKey.pendingReservations(), "wrong-type");
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .hasStackTraceContaining("Unexpected key type");
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(1L))).isEqualTo("2");
        assertThat(redis.hasKey(CouponRedisKey.issueUsers(1L))).isFalse();
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
        assertThat(redis.opsForValue().get(CouponRedisKey.pendingReservations())).isEqualTo("wrong-type");
    }

    @Test
    void invalidStockFailsWithoutCreatingReservation() {
        redis.opsForValue().set(CouponRedisKey.stock(1L), "invalid");
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .hasStackTraceContaining("Invalid stock value");
        assertState("invalid", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @Test
    void concurrentUniqueRequestsCannotExceedStock() throws Exception {
        redis.opsForValue().set(CouponRedisKey.stock(1L), "10");
        List<String> ids = new ArrayList<>();
        List<Callable<CouponReservationStatus>> calls = new ArrayList<>();
        for (long user = 1; user <= 50; user++) {
            String id = UUID.randomUUID().toString();
            long userId = user;
            ids.add(id);
            calls.add(() -> service.reserve(id, 1L, userId));
        }
        var results = runConcurrently(calls);
        assertThat(Collections.frequency(results, RESERVED)).isEqualTo(10);
        assertThat(Collections.frequency(results, SOLD_OUT)).isEqualTo(40);
        Set<String> expectedUsers = new HashSet<>();
        List<String> expectedPending = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            boolean accepted = results.get(i) == RESERVED;
            String id = ids.get(i);
            assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isEqualTo(accepted);
            if (accepted) {
                expectedUsers.add(String.valueOf(i + 1));
                expectedPending.add(id);
                assertThat(redis.opsForHash().entries(CouponRedisKey.reservation(id)))
                        .containsEntry("requestId", id).containsEntry("couponId", "1")
                        .containsEntry("userId", String.valueOf(i + 1));
            }
        }
        assertState("0", expectedUsers, expectedPending);
    }

    @Test
    void concurrentRetriesReserveOnlyOnce() throws Exception {
        String id = UUID.randomUUID().toString();
        List<Callable<CouponReservationStatus>> calls = new ArrayList<>();
        for (int i = 0; i < 20; i++) calls.add(() -> service.reserve(id, 1L, 10L));
        var results = runConcurrently(calls);
        assertThat(Collections.frequency(results, RESERVED)).isEqualTo(1);
        assertThat(Collections.frequency(results, ALREADY_RESERVED)).isEqualTo(19);
        assertState("1", Set.of("10"), List.of(id));
    }

    @Test
    void emptyPendingReturnsNullWithoutChangingProcessing() {
        redis.opsForList().rightPush(CouponRedisKey.processingReservations(), "already-processing");
        assertThat(service.claimPendingReservation()).isNull();
        assertThat(redis.hasKey(CouponRedisKey.pendingReservations())).isFalse();
        assertThat(processing()).containsExactly("already-processing");
    }

    @Test
    void claimMovesOldestPendingAndPreservesReservation() {
        String first = UUID.randomUUID().toString();
        String second = UUID.randomUUID().toString();
        service.reserve(first, 1L, 10L);
        service.reserve(second, 1L, 11L);
        var original = redis.opsForHash().entries(CouponRedisKey.reservation(first));

        assertThat(service.claimPendingReservation()).isEqualTo(first);
        assertState("0", Set.of("10", "11"), List.of(second));
        assertThat(processing()).containsExactly(first);
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservation(first))).isEqualTo(original);

        assertThat(service.claimPendingReservation()).isEqualTo(second);
        assertThat(service.claimPendingReservation()).isNull();
        assertState("0", Set.of("10", "11"), List.of());
        assertThat(processing()).containsExactly(first, second);
    }

    @Test
    void invalidProcessingTypeDoesNotRemovePending() {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        redis.opsForValue().set(CouponRedisKey.processingReservations(), "wrong-type");

        assertThatThrownBy(service::claimPendingReservation).hasStackTraceContaining("WRONGTYPE");
        assertState("1", Set.of("10"), List.of(id));
        assertThat(redis.opsForValue().get(CouponRedisKey.processingReservations())).isEqualTo("wrong-type");
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isTrue();
    }

    @Test
    void concurrentClaimsMoveEachRequestOnlyOnce() throws Exception {
        redis.opsForValue().set(CouponRedisKey.stock(1L), "10");
        List<String> ids = new ArrayList<>();
        Set<String> users = new HashSet<>();
        for (long user = 1; user <= 10; user++) {
            String id = UUID.randomUUID().toString();
            assertThat(service.reserve(id, 1L, user)).isEqualTo(RESERVED);
            ids.add(id);
            users.add(String.valueOf(user));
        }
        List<Callable<String>> calls = new ArrayList<>();
        for (int i = 0; i < 20; i++) calls.add(service::claimPendingReservation);
        var results = runConcurrently(calls);
        assertThat(Collections.frequency(results, null)).isEqualTo(10);
        assertThat(results.stream().filter(Objects::nonNull).toList())
                .containsExactlyInAnyOrderElementsOf(ids);
        assertThat(processing()).containsExactlyElementsOf(ids);
        assertState("0", users, List.of());
    }

    @Test
    void findsAllReservationFieldsAfterClaimWithoutRemovingProcessing() {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        Instant reservedAt = Instant.ofEpochMilli(Long.parseLong((String) redis.opsForHash()
                .get(CouponRedisKey.reservation(id), "reservedAtMillis")));
        assertThat(service.claimPendingReservation()).isEqualTo(id);

        assertThat(service.findReservation(id))
                .isEqualTo(new CouponReservation(id, 1L, 10L, reservedAt, 1L));
        assertThat(processing()).containsExactly(id);
        assertState("1", Set.of("10"), List.of());
    }

    @Test
    void missingReservationFailsAndLeavesProcessingForInvestigation() {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        service.claimPendingReservation();
        redis.delete(CouponRedisKey.reservation(id));

        assertThatThrownBy(() -> service.findReservation(id))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("예약 정보가 없습니다");
        assertThat(processing()).containsExactly(id);
        assertState("1", Set.of("10"), List.of());
    }

    @ParameterizedTest
    @ValueSource(strings = {"requestId", "couponId", "userId", "reservedAtMillis", "policyVersion"})
    void missingFieldFailsWithoutRemovingProcessing(String field) {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        service.claimPendingReservation();
        redis.opsForHash().delete(CouponRedisKey.reservation(id), field);

        assertThatThrownBy(() -> service.findReservation(id))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("field=" + field);
        assertThat(processing()).containsExactly(id);
    }

    @Test
    void mismatchedStoredRequestIdFailsWithoutRemovingProcessing() {
        String id = UUID.randomUUID().toString();
        service.reserve(id, 1L, 10L);
        service.claimPendingReservation();
        redis.opsForHash().put(CouponRedisKey.reservation(id), "requestId", UUID.randomUUID().toString());

        assertThatThrownBy(() -> service.findReservation(id))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("일치하지 않습니다");
        assertThat(processing()).containsExactly(id);
    }

    @Test
    void initializesPolicyAndStockForNewCoupon() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = start.plusSeconds(3600);
        assertThat(service.initializeReservation(2L, 100, start, end)).isTrue();
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(2L))).isEqualTo("100");
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservationPolicy(2L)))
                .containsEntry("status", "ACTIVE").containsEntry("version", "1")
                .containsEntry("startAtMillis", Long.toString(start.toEpochMilli()))
                .containsEntry("endAtMillis", Long.toString(end.toEpochMilli()));
    }

    @Test
    void reinitializationDoesNotOverwriteExistingData() {
        var original = redis.opsForHash().entries(CouponRedisKey.reservationPolicy(1L));
        assertThat(service.initializeReservation(1L, 999,
                Instant.now(), Instant.now().plusSeconds(7200))).isFalse();
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservationPolicy(1L))).isEqualTo(original);
        assertState("2", Set.of(), List.of());
    }

    @ParameterizedTest
    @ValueSource(strings = {"policy", "stock", "users"})
    void partialExistingDataPreventsInitialization(String remaining) {
        redis.delete(List.of(CouponRedisKey.reservationPolicy(1L), CouponRedisKey.stock(1L)));
        String key = switch (remaining) {
            case "policy" -> CouponRedisKey.reservationPolicy(1L);
            case "stock" -> CouponRedisKey.stock(1L);
            default -> CouponRedisKey.issueUsers(1L);
        };
        redis.opsForValue().set(key, "sentinel");
        assertThat(service.initializeReservation(1L, 99,
                Instant.now(), Instant.now().plusSeconds(60))).isFalse();
        assertThat(redis.opsForValue().get(key)).isEqualTo("sentinel");
        assertThat(redis.keys("coupon:*" )).containsExactly(key);
    }

    @Test
    void sameMillisecondPeriodIsRejectedBeforeWriting() {
        Instant start = Instant.parse("2026-01-01T00:00:00.000100Z");
        assertThatThrownBy(() -> service.initializeReservation(2L, 10, start, start.plusNanos(100)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("밀리초");
        assertThat(redis.hasKey(CouponRedisKey.stock(2L))).isFalse();
        assertThat(redis.hasKey(CouponRedisKey.reservationPolicy(2L))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"future", "expired", "inactive"})
    void unavailablePolicyDoesNotConsumeStock(String scenario) {
        String key = CouponRedisKey.reservationPolicy(1L);
        if (scenario.equals("inactive")) {
            redis.opsForHash().put(key, "status", "INACTIVE");
        } else {
            long start = scenario.equals("future")
                    ? Instant.now().plusSeconds(3600).toEpochMilli()
                    : Instant.now().minusSeconds(7200).toEpochMilli();
            redis.opsForHash().put(key, "startAtMillis", Long.toString(start));
            redis.opsForHash().put(key, "endAtMillis", Long.toString(start + 1800000));
        }
        String id = UUID.randomUUID().toString();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(NOT_AVAILABLE);
        assertState("2", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @Test
    void missingPolicyFailsWithoutConsumingStock() {
        redis.delete(CouponRedisKey.reservationPolicy(1L));
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("예약 정책이 초기화되지 않았습니다");
        assertState("2", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"status", "startAtMillis", "endAtMillis"})
    void missingPolicyFieldFailsWithoutConsumingStock(String field) {
        redis.opsForHash().delete(CouponRedisKey.reservationPolicy(1L), field);
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .hasStackTraceContaining("Missing or invalid reservation policy");
        assertState("2", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"text", "fraction", "reversed"})
    void invalidPolicyPeriodFailsWithoutConsumingStock(String scenario) {
        String start = switch (scenario) {
            case "text" -> "invalid";
            case "fraction" -> "1.5";
            default -> "10";
        };
        redis.opsForHash().put(CouponRedisKey.reservationPolicy(1L), "startAtMillis", start);
        redis.opsForHash().put(CouponRedisKey.reservationPolicy(1L), "endAtMillis", "2");
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L)).hasStackTraceContaining(
                scenario.equals("text") ? "Missing or invalid reservation policy" : "Invalid reservation period");
        assertState("2", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
    }

    @Test
    void retryOfAcceptedRequestStillWorksAfterPolicyExpires() {
        String id = UUID.randomUUID().toString();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        var original = service.findReservation(id);
        redis.opsForHash().put(CouponRedisKey.reservationPolicy(1L), "startAtMillis", "0");
        redis.opsForHash().put(CouponRedisKey.reservationPolicy(1L), "endAtMillis", "1");
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(ALREADY_RESERVED);
        assertThat(service.findReservation(id)).isEqualTo(original);
        assertState("1", Set.of("10"), List.of(id));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "", "0", "-1", "1.5", "abc", "01", "2147483648"})
    void invalidPolicyVersionFailsBeforeAnyReservationWrite(String version) {
        String key = CouponRedisKey.reservationPolicy(1L);
        if (version.equals("missing")) redis.opsForHash().delete(key, "version");
        else redis.opsForHash().put(key, "version", version);
        String id = UUID.randomUUID().toString();
        assertThatThrownBy(() -> service.reserve(id, 1L, 10L))
                .hasStackTraceContaining("Invalid reservation policy version");
        assertState("2", Set.of(), List.of());
        assertThat(redis.hasKey(CouponRedisKey.reservation(id))).isFalse();
        assertThat(processing()).isEmpty();
    }

    @Test
    void retryPreservesApprovalTimeAndVersionDespiteLaterPolicyChanges() {
        String key = CouponRedisKey.reservationPolicy(1L);
        redis.opsForHash().put(key, "version", "7");
        String id = UUID.randomUUID().toString();
        long before = redisTimeMillis();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        long after = redisTimeMillis();
        var original = service.findReservation(id);
        assertThat(original.policyVersion()).isEqualTo(7L);
        assertThat(original.reservedAt().toEpochMilli()).isBetween(before, after);
        var hash = redis.opsForHash().entries(CouponRedisKey.reservation(id));

        // Defensive retry check; changing policy during the event is not a supported feature.
        redis.opsForHash().put(key, "version", "8");
        redis.opsForHash().put(key, "status", "INACTIVE");
        redis.opsForHash().put(key, "startAtMillis", "0");
        redis.opsForHash().put(key, "endAtMillis", "1");
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(ALREADY_RESERVED);
        assertThat(service.findReservation(id)).isEqualTo(original);
        assertThat(redis.opsForHash().entries(CouponRedisKey.reservation(id))).isEqualTo(hash);
        assertState("1", Set.of("10"), List.of(id));
    }

    private long redisTimeMillis() {
        Long time = redis.execute(new DefaultRedisScript<>("""
                local t = redis.call('TIME')
                return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                """, Long.class), List.of());
        return Objects.requireNonNull(time);
    }

    private List<String> processing() {
        return redis.opsForList().range(CouponRedisKey.processingReservations(), 0, -1);
    }

    private <T> List<T> runConcurrently(List<Callable<T>> calls) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(calls.size());
        CountDownLatch ready = new CountDownLatch(calls.size());
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (var call : calls) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new TimeoutException("start barrier");
                    return call.call();
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            List<T> results = new ArrayList<>();
            for (var future : futures) results.add(future.get(10, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void assertState(String stock, Set<String> users, List<String> pending) {
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(1L))).isEqualTo(stock);
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(1L)))
                .containsExactlyInAnyOrderElementsOf(users);
        assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1))
                .containsExactlyInAnyOrderElementsOf(pending);
    }
}

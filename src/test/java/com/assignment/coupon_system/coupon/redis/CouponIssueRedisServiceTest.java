package com.assignment.coupon_system.coupon.redis;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
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
        redis.opsForValue().set(CouponRedisKey.stock(1L), "2");
    }

    @Test
    void createsReservationAndPendingTogether() {
        String id = UUID.randomUUID().toString();
        Instant before = Instant.now();
        assertThat(service.reserve(id, 1L, 10L)).isEqualTo(RESERVED);
        assertState("1", Set.of("10"), List.of(id));
        var reservation = redis.opsForHash().entries(CouponRedisKey.reservation(id));
        assertThat(reservation).containsEntry("requestId", id)
                .containsEntry("couponId", "1").containsEntry("userId", "10");
        assertThat(Instant.parse((String) reservation.get("reservedAt")))
                .isBetween(before, Instant.now());
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
        Instant reservedAt = Instant.parse((String) redis.opsForHash()
                .get(CouponRedisKey.reservation(id), "reservedAt"));
        assertThat(service.claimPendingReservation()).isEqualTo(id);

        assertThat(service.findReservation(id))
                .isEqualTo(new CouponReservation(id, 1L, 10L, reservedAt));
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
    @ValueSource(strings = {"requestId", "couponId", "userId", "reservedAt"})
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

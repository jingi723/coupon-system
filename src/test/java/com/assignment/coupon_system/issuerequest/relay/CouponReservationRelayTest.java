package com.assignment.coupon_system.issuerequest.relay;

import com.assignment.coupon_system.coupon.redis.*;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.producer.CouponIssueProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@Testcontainers
@DisplayName("Redis 전달 작업자 흐름 검증 — 실제 Redis, Producer 대역")
class CouponReservationRelayTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);
    static LettuceConnectionFactory factory;
    static StringRedisTemplate redis;
    CouponIssueRedisService service;
    CouponIssueProducer producer;
    CouponReservationRelay relay;

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void disconnect() {
        if (factory != null) factory.destroy();
    }

    @BeforeEach
    void setUp() {
        try (var c = factory.getConnection()) { c.serverCommands().flushDb(); }
        redis.opsForValue().set(CouponRedisKey.stock(1L), "10");
        service = new CouponIssueRedisService(redis);
        producer = mock(CouponIssueProducer.class);
        relay = new CouponReservationRelay(service, producer);
    }

    @Test
    void emptyQueuesReturnFalseWithoutSending() throws Exception {
        assertThat(relay.relayOne()).isFalse();
        verifyNoInteractions(producer);
        assertQueues(List.of(), List.of());
    }

    @Test
    void successfulSendPreservesMessageAndReservationButRemovesProcessing() throws Exception {
        var message = reserve(11L);
        doAnswer(invocation -> {
            assertQueues(List.of(), List.of(message.requestId()));
            return null;
        }).when(producer).send(message);

        assertThat(relay.relayOne()).isTrue();
        verify(producer).send(message);
        verifyNoMoreInteractions(producer);
        assertQueues(List.of(), List.of());
        assertReservationPreserved(message);
        // Completing again is idempotent and does not restore stock.
        service.completeDelivery(message.requestId());
        assertReservationPreserved(message);
    }

    @ParameterizedTest
    @ValueSource(strings = {"execution", "timeout", "interrupted"})
    void sendFailureKeepsRequestAndNextCallRetriesSameMessage(String type) throws Exception {
        var message = reserve(11L);
        Exception failure = switch (type) {
            case "execution" -> new ExecutionException(new IllegalStateException("injected send failure"));
            case "timeout" -> new TimeoutException("injected unknown send result");
            default -> new InterruptedException("injected interruption");
        };
        doThrow(failure).doNothing().when(producer).send(message);

        assertThatThrownBy(() -> relay.relayOne()).isSameAs(failure);
        assertQueues(List.of(), List.of(message.requestId()));
        assertReservationPreserved(message);

        assertThat(relay.relayOne()).isTrue();
        verify(producer, times(2)).send(message);
        assertQueues(List.of(), List.of());
        assertReservationPreserved(message);
    }

    @Test
    void processingIsDeliveredBeforePending() throws Exception {
        var first = reserve(11L);
        var second = reserve(12L);
        assertThat(service.claimPendingReservation()).isEqualTo(first.requestId());

        assertThat(relay.relayOne()).isTrue();
        assertQueues(List.of(second.requestId()), List.of());
        verify(producer).send(first);
        verify(producer, never()).send(second);
        assertThat(relay.relayOne()).isTrue();
        var order = inOrder(producer);
        order.verify(producer).send(first);
        order.verify(producer).send(second);
        assertQueues(List.of(), List.of());
    }

    @Test
    void processingRemainsUntilSendReturns() throws Exception {
        var message = reserve(11L);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        doAnswer(invocation -> {
            entered.countDown();
            if (!release.await(10, TimeUnit.SECONDS)) throw new TimeoutException("test release not received");
            return null;
        }).when(producer).send(message);
        try {
            Future<Boolean> result = executor.submit(relay::relayOne);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(result.isDone()).isFalse();
            assertQueues(List.of(), List.of(message.requestId()));
            release.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS)).isTrue();
            assertQueues(List.of(), List.of());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sendSuccessThenCleanupFailureIsRedeliveredByNewRelayInstance() throws Exception {
        var message = reserve(11L);
        var failingService = spy(service);
        var failure = new IllegalStateException("injected failure before LREM");
        doThrow(failure).when(failingService).completeDelivery(message.requestId());
        var failingRelay = new CouponReservationRelay(failingService, producer);

        assertThatThrownBy(() -> failingRelay.relayOne()).isSameAs(failure);
        verify(producer).send(message); // Producer returned normally before cleanup failed.
        assertQueues(List.of(), List.of(message.requestId()));

        // Object recreation verifies Redis-backed state, not a real JVM crash/restart.
        var restarted = new CouponReservationRelay(new CouponIssueRedisService(redis), producer);
        assertThat(restarted.relayOne()).isTrue();
        verify(producer, times(2)).send(message);
        assertQueues(List.of(), List.of());
        assertReservationPreserved(message);
    }

    @Test
    void missingReservationStopsBeforeSendingAndPreservesProcessing() {
        var message = reserve(11L);
        redis.delete(CouponRedisKey.reservation(message.requestId()));

        assertThatThrownBy(() -> relay.relayOne())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("예약 정보가 없습니다");
        verifyNoInteractions(producer);
        assertQueues(List.of(), List.of(message.requestId()));
    }

    private CouponIssueMessage reserve(long user) {
        String id = UUID.randomUUID().toString();
        assertThat(service.reserve(id, 1L, user)).isEqualTo(CouponReservationStatus.RESERVED);
        var r = service.findReservation(id);
        return new CouponIssueMessage(r.requestId(), r.couponId(), r.userId(), r.reservedAt());
    }

    private void assertQueues(List<String> pending, List<String> processing) {
        assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1))
                .containsExactlyElementsOf(pending);
        assertThat(redis.opsForList().range(CouponRedisKey.processingReservations(), 0, -1))
                .containsExactlyElementsOf(processing);
    }

    private void assertReservationPreserved(CouponIssueMessage message) {
        assertThat(service.findReservation(message.requestId())).isEqualTo(new CouponReservation(
                message.requestId(), message.couponId(), message.userId(), message.receivedAt()));
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(1L))).isEqualTo("9");
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(1L)))
                .containsExactly(message.userId().toString());
    }
}

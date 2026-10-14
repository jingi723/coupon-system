package com.assignment.coupon_system.issuerequest.consumer;

import com.assignment.coupon_system.coupon.redis.CouponReservation;
import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/** Single partition, single worker. A failed batch requires explicit restart. */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "coupon.consumer", name = "batch-enabled", havingValue = "true")
public class CouponBatchConsumer implements SmartLifecycle {
    private static final int MAX_BATCH_SIZE = 10;
    private static final long WAIT_NANOS = Duration.ofMillis(10).toNanos();
    private final ConsumerFactory<String, CouponIssueMessage> factory;
    private final CouponIssueConsumer validator;
    private final CouponIssueProcessor processor;
    private final TransactionTemplate transaction;
    private final boolean autoStartup;
    private volatile boolean running;
    private volatile boolean stopRequested;
    private volatile Consumer<String, CouponIssueMessage> consumer;
    private final List<Runnable> stopCallbacks = new ArrayList<>();

    public CouponBatchConsumer(ConsumerFactory<String, CouponIssueMessage> factory,
                               CouponIssueConsumer validator, CouponIssueProcessor processor,
                               PlatformTransactionManager manager,
                               @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
        this.factory = factory;
        this.validator = validator;
        this.processor = processor;
        this.transaction = new TransactionTemplate(manager);
        this.autoStartup = autoStartup;
    }

    @Override
    public synchronized void start() {
        if (running) return;
        stopRequested = false;
        running = true;
        new Thread(this::runConsumer, "coupon-batch-consumer").start();
    }

    private void runConsumer() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(MAX_BATCH_SIZE));
        try (var kafka = factory.createConsumer(null, "coupon-batch", null, overrides)) {
            consumer = kafka;
            Session session = new Session();
            kafka.subscribe(List.of(CouponKafkaConfig.ISSUE_TOPIC), session);
            while (!stopRequested) {
                if (session.buffer.isEmpty()) {
                    poll(kafka, session, Duration.ofMillis(100));
                    if (session.buffer.isEmpty()) continue;
                }
                long version = session.assignmentVersion;
                long deadline = session.buffer.peekFirst().receivedNanos() + WAIT_NANOS;
                List<Received> batch = new ArrayList<>(MAX_BATCH_SIZE);
                while (!stopRequested && batch.size() < MAX_BATCH_SIZE) {
                    while (!session.buffer.isEmpty() && batch.size() < MAX_BATCH_SIZE) {
                        batch.add(session.buffer.removeFirst());
                    }
                    long remaining = deadline - System.nanoTime();
                    if (batch.size() == MAX_BATCH_SIZE || remaining <= 0) break;
                    poll(kafka, session, Duration.ofNanos(remaining));
                    // Never commit a mixture of records from different assignments.
                    if (version != session.assignmentVersion) {
                        throw new IllegalStateException("수집 중 파티션 소유권이 변경되어 Consumer를 중지합니다.");
                    }
                }
                if (stopRequested) break;
                List<CouponReservation> approved = batch.stream()
                        .map(item -> validator.validateReservation(item.record().value())).toList();
                // Collection and Redis validation happen before opening the DB transaction.
                transaction.executeWithoutResult(status -> {
                    for (var reservation : approved) {
                        processor.processReserved(reservation.requestId(), reservation.couponId(),
                                reservation.userId(), reservation.reservedAt());
                    }
                });
                // The outer transaction has committed and released resources at this boundary.
                var last = batch.get(batch.size() - 1).record();
                kafka.commitSync(Map.of(new TopicPartition(last.topic(), last.partition()),
                        new OffsetAndMetadata(last.offset() + 1)));
            }
        } catch (WakeupException e) {
            if (!stopRequested) log.error("묶음 Consumer가 예기치 않게 중지됐습니다. 재시작이 필요합니다.", e);
        } catch (Exception e) {
            log.error("묶음 Consumer를 중지합니다. DB/offset 커밋 결과와 원인을 확인한 뒤 재시작하세요.", e);
        } finally {
            List<Runnable> callbacks;
            synchronized (this) {
                consumer = null;
                running = false;
                callbacks = List.copyOf(stopCallbacks);
                stopCallbacks.clear();
            }
            callbacks.forEach(Runnable::run);
        }
    }

    private void poll(Consumer<String, CouponIssueMessage> kafka, Session session, Duration timeout) {
        var records = kafka.poll(timeout);
        long received = System.nanoTime();
        for (var record : records) session.buffer.addLast(new Received(record, received));
    }

    private record Received(ConsumerRecord<String, CouponIssueMessage> record, long receivedNanos) {}

    private static class Session implements ConsumerRebalanceListener {
        private final Deque<Received> buffer = new ArrayDeque<>();
        private long assignmentVersion;

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            assignmentVersion++;
            buffer.clear();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assignmentVersion++;
            if (partitions.size() != 1) {
                throw new IllegalStateException("묶음 Consumer는 단일 파티션·단일 Consumer만 지원합니다.");
            }
        }
    }

    @Override
    public synchronized void stop(Runnable callback) {
        if (!running) {
            callback.run();
            return;
        }
        stopCallbacks.add(callback);
        stopRequested = true;
        var current = consumer;
        if (current != null) current.wakeup();
    }

    @Override
    public void stop() {
        CountDownLatch stopped = new CountDownLatch(1);
        stop(stopped::countDown);
        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override public boolean isRunning() { return running; }
    @Override public boolean isAutoStartup() { return autoStartup; }
    @Override public int getPhase() { return Integer.MAX_VALUE; }
}

package com.assignment.coupon_system.issuerequest.consumer;

import com.assignment.coupon_system.coupon.redis.CouponReservation;
import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.*;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.awaitility.Awaitility.await;

class CouponBatchConsumerTest {
    final TopicPartition partition = new TopicPartition(CouponKafkaConfig.ISSUE_TOPIC, 0);
    final ConsumerFactory<String, CouponIssueMessage> factory = mock(ConsumerFactory.class);
    final Consumer<String, CouponIssueMessage> kafka = mock(Consumer.class);
    final CouponIssueConsumer validator = mock(CouponIssueConsumer.class);
    final CouponIssueProcessor processor = mock(CouponIssueProcessor.class);
    final PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
    final AtomicReference<ConsumerRebalanceListener> rebalance = new AtomicReference<>();
    final AtomicBoolean wakeup = new AtomicBoolean();
    final CouponBatchConsumer worker = new CouponBatchConsumer(factory, validator, processor, manager, true);
    final Instant approvedAt = Instant.parse("2026-10-15T00:00:00Z");

    @BeforeEach void setup() {
        when(factory.createConsumer(isNull(), eq("coupon-batch"), isNull(), any(Properties.class))).thenReturn(kafka);
        when(manager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        doAnswer(call -> { rebalance.set(call.getArgument(1)); rebalance.get().onPartitionsAssigned(List.of(partition)); return null; })
                .when(kafka).subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
        doAnswer(call -> { wakeup.set(true); return null; }).when(kafka).wakeup();
        when(validator.validateReservation(any())).thenAnswer(call -> {
            CouponIssueMessage message = call.getArgument(0);
            return new CouponReservation(message.requestId(), 1L, message.userId(), approvedAt, 1L);
        });
    }
    @AfterEach void stop() { worker.stop(); }

    ConsumerRecords<String, CouponIssueMessage> records(int count) {
        List<ConsumerRecord<String, CouponIssueMessage>> items = new ArrayList<>();
        for (int i=0; i<count; i++) items.add(new ConsumerRecord<>(partition.topic(), 0, i,
                "1", new CouponIssueMessage("request-"+i, 1L, (long)i+1, approvedAt)));
        return new ConsumerRecords<>(Map.of(partition, items));
    }
    void deliver(int count) {
        AtomicBoolean first = new AtomicBoolean(true);
        when(kafka.poll(any(Duration.class))).thenAnswer(call -> {
            if (wakeup.get()) throw new WakeupException();
            if (first.getAndSet(false)) return records(count);
            Thread.sleep(2);
            return ConsumerRecords.empty();
        });
    }
    void stopped() { await().atMost(Duration.ofSeconds(5)).until(() -> !worker.isRunning()); }

    @Test void fullBatchCommitsDatabaseBeforeOffsetAndClosesBeforeStopCallback() {
        deliver(10);
        worker.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(kafka).commitSync(Map.of(partition, new OffsetAndMetadata(10))));
        worker.stop();
        var order = inOrder(validator, manager, processor, kafka);
        order.verify(validator, times(10)).validateReservation(any());
        order.verify(manager).getTransaction(any());
        order.verify(processor, times(10)).processReserved(anyString(), eq(1L), anyLong(), eq(approvedAt));
        order.verify(manager).commit(any());
        order.verify(kafka).commitSync(anyMap());
        order.verify(kafka).close();
        verify(factory).createConsumer(isNull(), eq("coupon-batch"), isNull(), argThat(props ->
                "false".equals(props.getProperty("enable.auto.commit")) && "10".equals(props.getProperty("max.poll.records"))));
    }
    @Test void singleRecordFlushesAtDeadlineWithoutFurtherTraffic() {
        deliver(1);
        worker.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(kafka).commitSync(Map.of(partition, new OffsetAndMetadata(1))));
        verify(processor).processReserved("request-0", 1L, 1L, approvedAt);
    }
    @Test void validationFailureDoesNotOpenTransactionOrAdvanceOffset() {
        deliver(10);
        doThrow(new IllegalArgumentException("bad approval")).when(validator).validateReservation(any());
        worker.start(); stopped();
        verifyNoInteractions(manager, processor);
        verify(kafka, never()).commitSync(anyMap());
    }
    @Test void processingFailureRollsBackAndDoesNotAdvanceOffset() {
        deliver(10);
        when(processor.processReserved("request-4", 1L, 5L, approvedAt)).thenThrow(new IllegalStateException("DB failure"));
        worker.start(); stopped();
        verify(manager).rollback(any());
        verify(manager, never()).commit(any());
        verify(kafka, never()).commitSync(anyMap());
        verify(processor, never()).processReserved("request-5", 1L, 6L, approvedAt);
    }
    @Test void rebalanceDuringCollectionStopsWithoutProcessingStaleRecords() {
        AtomicInteger polls = new AtomicInteger();
        when(kafka.poll(any(Duration.class))).thenAnswer(call -> {
            if (polls.getAndIncrement() == 0) return records(1);
            rebalance.get().onPartitionsRevoked(List.of(partition));
            rebalance.get().onPartitionsAssigned(List.of(partition));
            return records(1);
        });
        worker.start(); stopped();
        verifyNoInteractions(manager, processor);
        verify(kafka, never()).commitSync(anyMap());
    }
    @Test void commitFailureStopsAfterDatabaseCommit() {
        deliver(10);
        doThrow(new IllegalStateException("unknown offset outcome")).when(kafka).commitSync(anyMap());
        worker.start(); stopped();
        verify(manager).commit(any());
        verify(manager, never()).rollback(any());
        verify(kafka, times(1)).commitSync(anyMap());
    }
    @Test void batchBeanIsOptInAndDoesNotStartWhenAutoStartupIsDisabled() {
        var context = new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withUserConfiguration(CouponBatchConsumer.class)
                .withBean(ConsumerFactory.class, () -> factory)
                .withBean(CouponIssueConsumer.class, () -> validator)
                .withBean(CouponIssueProcessor.class, () -> processor)
                .withBean(PlatformTransactionManager.class, () -> manager)
                .withPropertyValues("spring.kafka.listener.auto-startup=false");
        context.run(app -> assertThat(app).doesNotHaveBean(CouponBatchConsumer.class));
        context.withPropertyValues("coupon.consumer.batch-enabled=true").run(app -> {
            assertThat(app).hasSingleBean(CouponBatchConsumer.class);
            assertThat(app.getBean(CouponBatchConsumer.class).isRunning()).isFalse();
        });
        verifyNoInteractions(factory);
    }
    @Test void batchSettingDisablesSingleListenerAndStartupSettingIsRespected() throws Exception {
        String expression = CouponIssueConsumer.class.getMethod("consume", CouponIssueMessage.class)
                .getAnnotation(KafkaListener.class).autoStartup();
        for (boolean batch : List.of(false, true)) for (boolean startup : List.of(false, true)) {
            var environment = new StandardEnvironment();
            environment.getPropertySources().addFirst(new MapPropertySource("test", Map.of(
                    "coupon.consumer.batch-enabled", batch, "spring.kafka.listener.auto-startup", startup)));
            Object value = new StandardBeanExpressionResolver().evaluate(environment.resolveRequiredPlaceholders(expression),
                    new BeanExpressionContext(new DefaultListableBeanFactory(), null));
            assertThat(value).isEqualTo(!batch && startup);
            assertThat(new CouponBatchConsumer(factory, validator, processor, manager, startup).isAutoStartup()).isEqualTo(startup);
        }
    }
}

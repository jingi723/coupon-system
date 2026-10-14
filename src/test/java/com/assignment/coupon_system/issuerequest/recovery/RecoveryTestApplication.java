package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.CouponSystemApplication;
import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.issuerequest.producer.CouponIssueProducer;
import com.assignment.coupon_system.issuerequest.consumer.CouponIssueConsumer;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** 테스트 클래스패스에서만 실행한다. 운영 코드에는 제어 API와 장애 주입을 추가하지 않는다. */
public class RecoveryTestApplication {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(new Class<?>[]{CouponSystemApplication.class, Controls.class}, args);
    }

    static synchronized void record(String file, Map<String, ?> value) {
        try {
            Files.writeString(Path.of(System.getProperty("recovery.dir"), file),
                    JSON.writeValueAsString(value) + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot record recovery evidence", e);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class Controls {
        @Bean
        ApplicationListener<ApplicationReadyEvent> readyFile() {
            return event -> record("ready.jsonl", Map.of(
                    "port", event.getApplicationContext().getEnvironment().getProperty("local.server.port"),
                    "pid", ProcessHandle.current().pid()));
        }

        @Bean
        static RecoveryProbe recoveryProbe(org.springframework.data.redis.core.StringRedisTemplate template) {
            return new RecoveryProbe(template);
        }
    }

    static class RecoveryProbe implements BeanPostProcessor, Ordered {
        final org.springframework.data.redis.core.StringRedisTemplate template;
        RecoveryProbe(org.springframework.data.redis.core.StringRedisTemplate template){this.template=template;}
        @Override
        public int getOrder() {
            // KafkaListener 등록 전에 감싸야 실제 Listener가 이 프록시를 호출한다.
            return Ordered.HIGHEST_PRECEDENCE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
                ((ConcurrentKafkaListenerContainerFactory<Object, Object>) factory)
                        .setRecordInterceptor((rec, consumer) -> {
                            CouponIssueMessage message = (CouponIssueMessage) rec.value();
                            record("deliveries.jsonl", Map.of("topic", rec.topic(),
                                    "partition", rec.partition(), "offset", rec.offset(),
                                    "requestId", message.requestId()));
                            return rec;
                        });
            }
            if (Boolean.getBoolean("relay.prepare.lua") && bean instanceof CouponIssueRedisService service) {
                bean = new com.assignment.coupon_system.issuerequest.profiling.LuaReservationPreparation(template).wrap(service);
            }
            String boundary = System.getProperty("recovery.hold.boundary");
            if (bean instanceof CouponIssueRedisService && "after-claim".equals(boundary)) {
                ProxyFactory proxy = new ProxyFactory(bean);
                proxy.setProxyTargetClass(true);
                proxy.addAdvice((MethodInterceptor) invocation -> {
                    Object result = invocation.proceed();
                    if (invocation.getMethod().getName().equals(Boolean.getBoolean("relay.prepare.lua") ? "peekProcessingReservation" : "claimPendingReservation")
                            && System.getProperty("recovery.hold.request").equals(result)) {
                        holdBoundary(boundary, (String) result);
                    }
                    return result;
                });
                return proxy.getProxy();
            }
            if (bean instanceof CouponIssueProducer && "after-kafka-ack".equals(boundary)) {
                ProxyFactory proxy = new ProxyFactory(bean);
                proxy.setProxyTargetClass(true);
                proxy.addAdvice((MethodInterceptor) invocation -> {
                    Object result = invocation.proceed();
                    if (invocation.getMethod().getName().equals("send")) {
                        var message = (CouponIssueMessage) invocation.getArguments()[0];
                        if (message.requestId().equals(System.getProperty("recovery.hold.request"))) {
                            // Producer.send의 Future.get 성공 이후이며 Relay의 LREM 이전이다.
                            holdBoundary(boundary, message.requestId());
                        }
                    }
                    return result;
                });
                return proxy.getProxy();
            }
            if (bean instanceof CouponIssueConsumer) {
                ProxyFactory proxy = new ProxyFactory(bean);
                proxy.setProxyTargetClass(true);
                proxy.addAdvice((MethodInterceptor) invocation -> {
                    Object result = invocation.proceed();
                    if (invocation.getMethod().getName().equals("consume")) {
                        CouponIssueMessage message = (CouponIssueMessage) invocation.getArguments()[0];
                        if (message.requestId().equals(System.getProperty("recovery.hold.request"))
                                && (boundary == null || "after-db-commit".equals(boundary))) {
                            // 실제 Consumer 메서드는 DB 서비스의 커밋 이후 반환했다.
                            // Kafka 컨테이너로는 아직 반환하지 않아 RECORD offset 커밋 이전이다.
                            record("after-db-commit.jsonl", Map.of("requestId", message.requestId(),
                                    "transactionActive", TransactionSynchronizationManager.isActualTransactionActive()));
                            new CountDownLatch(1).await(); // 부모 테스트가 이 JVM을 강제 종료한다.
                        }
                    }
                    return result;
                });
                return proxy.getProxy();
            }
            return bean;
        }
    }

    static void holdBoundary(String boundary, String requestId) throws InterruptedException {
        record("relay-boundary.jsonl", Map.of("boundary", boundary, "requestId", requestId,
                "pid", ProcessHandle.current().pid()));
        new CountDownLatch(1).await();
    }

    @RestController
    @Profile("recovery-probe")
    public static class ConsumerControl {
        private final KafkaListenerEndpointRegistry registry;

        ConsumerControl(KafkaListenerEndpointRegistry registry) {
            this.registry = registry;
        }

        @PostMapping("/__test/consumer/stop")
        Map<String, Boolean> stop() throws InterruptedException {
            CountDownLatch stopped = new CountDownLatch(1);
            registry.stop(stopped::countDown);
            if (!stopped.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("Stop timed out");
            return status();
        }

        @PostMapping("/__test/consumer/start")
        Map<String, Boolean> start() {
            registry.start();
            return status();
        }

        @GetMapping("/__test/consumer/status")
        Map<String, Boolean> status() {
            return Map.of("running", registry.getListenerContainers().stream().anyMatch(c -> c.isRunning()));
        }
    }
}

package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.CouponSystemApplication;
import com.assignment.coupon_system.issuerequest.consumer.CouponBatchConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.aop.framework.ProxyFactory;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.bind.annotation.*;
import java.lang.reflect.*;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Only observes production Kafka calls; collection, validation and transactions use src/main. */
public class ProductionBatchTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(new Class<?>[]{CouponSystemApplication.class,
                RecoveryTestApplication.Controls.class, Probe.class}, args);
    }
    @TestConfiguration(proxyBeanMethods = false)
    public static class Probe {
        @Bean static BeanPostProcessor batchProbe() {
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    if (!(bean instanceof ConsumerFactory<?, ?>)) return bean;
                    ProxyFactory proxy = new ProxyFactory(bean);
                    proxy.addAdvice((MethodInterceptor) invocation -> {
                        Object result = invocation.proceed();
                        if (!invocation.getMethod().getName().equals("createConsumer")) return result;
                        return Proxy.newProxyInstance(Consumer.class.getClassLoader(), new Class[]{Consumer.class}, (p, method, args) -> {
                            if (method.getName().equals("poll") || method.getName().equals("commitSync")) {
                                if (TransactionSynchronizationManager.isActualTransactionActive())
                                    throw new AssertionError("Kafka operation inside DB transaction");
                            }
                            if (method.getName().equals("commitSync") && args != null && args[0] instanceof Map<?, ?> offsets) {
                                var offset = (OffsetAndMetadata) offsets.values().iterator().next();
                                RecoveryTestApplication.record("commit-attempts.jsonl", Map.of("nextOffset", offset.offset(), "transactionActive", false));
                                if ("after-db-commit".equals(System.getProperty("recovery.hold.boundary"))) {
                                    RecoveryTestApplication.record("after-db-commit.jsonl", Map.of("nextOffset", offset.offset()));
                                    new CountDownLatch(1).await();
                                }
                            }
                            try { return method.invoke(result, args); }
                            catch (InvocationTargetException e) { throw e.getCause(); }
                        });
                    });
                    return proxy.getProxy();
                }
            };
        }
    }
    @RestController @Profile("production-batch-probe")
    public static class Controls {
        private final CouponBatchConsumer worker;
        private final KafkaListenerEndpointRegistry registry;
        public Controls(CouponBatchConsumer worker, KafkaListenerEndpointRegistry registry) {
            this.worker = worker; this.registry = registry;
        }
        @PostMapping("/__test/batch/start") public Map<String, Boolean> start() { worker.start(); return status(); }
        @PostMapping("/__test/batch/stop") public Map<String, Boolean> stop() { worker.stop(); return status(); }
        @GetMapping("/__test/batch/status") public Map<String, Boolean> status() {
            return Map.of("running", worker.isRunning(), "singleRunning",
                    registry.getListenerContainers().stream().anyMatch(c -> c.isRunning()));
        }
    }
}

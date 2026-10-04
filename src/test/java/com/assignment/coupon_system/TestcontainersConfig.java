package com.assignment.coupon_system;

import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.mockito.Mockito.mock;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfig {

    // test 프로필은 Kafka 자동 구성을 끈다. 기존 DB/Redis 테스트의 빈 구성만 지원하며,
    // 실제 Kafka 전송 검증에는 이 대역을 사용하지 않는다.
    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, CouponIssueMessage> kafkaTemplate() {
        return mock(KafkaTemplate.class);
    }

    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redisContainer() {
        return new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379);
    }
}

package com.assignment.coupon_system.issuerequest.config;

import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;

@Configuration
public class CouponKafkaConfig {

    public static final String ISSUE_TOPIC = "coupon-issue-requests";

    @Bean
    public NewTopic couponIssueTopic() {
        return TopicBuilder.name(ISSUE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return new CommonContainerStoppingErrorHandler();
    }
}

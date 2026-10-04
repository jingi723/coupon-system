package com.assignment.coupon_system.issuerequest.producer;

import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
public class CouponIssueProducer {

    private final KafkaTemplate<String, CouponIssueMessage> kafkaTemplate;

    public void send(CouponIssueMessage message)
            throws ExecutionException, InterruptedException, TimeoutException {

        kafkaTemplate.send(
                CouponKafkaConfig.ISSUE_TOPIC,
                message.couponId().toString(),
                message
        ).get(10, TimeUnit.SECONDS);
    }
}

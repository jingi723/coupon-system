package com.assignment.coupon_system.issuerequest.consumer;

import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CouponIssueConsumer {

    private final CouponIssueProcessor processor;

    @KafkaListener(topics = CouponKafkaConfig.ISSUE_TOPIC)
    public void consume(CouponIssueMessage message) {
        processor.process(
                message.requestId(),
                message.couponId(),
                message.userId()
        );
    }
}

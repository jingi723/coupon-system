package com.assignment.coupon_system.issuerequest.consumer;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.redis.CouponReservation;
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
    private final CouponIssueRedisService redisService;

    @KafkaListener(topics = CouponKafkaConfig.ISSUE_TOPIC,
            autoStartup = "#{!${coupon.consumer.batch-enabled:false} && ${spring.kafka.listener.auto-startup:true}}")
    public void consume(CouponIssueMessage message) {
        CouponReservation reservation = validateReservation(message);
        processor.processReserved(
                reservation.requestId(),
                reservation.couponId(),
                reservation.userId(),
                reservation.reservedAt()
        );
    }

    public CouponReservation validateReservation(CouponIssueMessage message) {
        if(message == null
                || message.requestId() == null
                || message.receivedAt() == null) {
            throw new IllegalArgumentException("예약 메시지가 올바르지 않습니다.");
        }

        CouponReservation reservation = redisService.findReservation(message.requestId());

        if(!reservation.couponId().equals(message.couponId())
                || !reservation.userId().equals(message.userId())
                || ! reservation.reservedAt().equals(message.receivedAt())) {
            throw  new IllegalArgumentException("메시지와 Redis 예약 정보가 다릅니다. requestId="
                    + message.requestId());
        }

        return reservation;
    }
}

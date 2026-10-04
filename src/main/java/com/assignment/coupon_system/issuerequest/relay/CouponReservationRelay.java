package com.assignment.coupon_system.issuerequest.relay;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.redis.CouponReservation;
import com.assignment.coupon_system.coupon.redis.CouponReservationStatus;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.producer.CouponIssueProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
public class CouponReservationRelay {

    private final CouponIssueRedisService redisService;
    private final CouponIssueProducer producer;

    public synchronized boolean relayOne()
            throws ExecutionException, InterruptedException, TimeoutException {

        String requestId = redisService.peekProcessingReservation();

        if(requestId == null) {
            requestId = redisService.claimPendingReservation();
        }

        if(requestId == null) {
            return false;
        }

        CouponReservation reservation =
                redisService.findReservation(requestId);

        CouponIssueMessage message = new CouponIssueMessage(
                reservation.requestId(),
                reservation.couponId(),
                reservation.userId(),
                reservation.reservedAt()
        );

        producer.send(message);

        redisService.completeDelivery(requestId);

        return true;
    }
}

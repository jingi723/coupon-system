package com.assignment.coupon_system.coupon.redis;

import java.time.Instant;

public record CouponReservation(
        String requestId,
        Long couponId,
        Long userId,
        Instant reservedAt
) {
}

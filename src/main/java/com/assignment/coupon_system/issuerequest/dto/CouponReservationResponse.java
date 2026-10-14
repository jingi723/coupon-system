package com.assignment.coupon_system.issuerequest.dto;

import com.assignment.coupon_system.coupon.redis.CouponReservation;

import java.time.Instant;

public record CouponReservationResponse(
        String requestId,
        Long couponId,
        Long userId,
        String status,
        Instant reservedAt
) {
    public static CouponReservationResponse from(
            CouponReservation reservation
    ) {
        return new CouponReservationResponse(
                reservation.requestId(),
                reservation.couponId(),
                reservation.userId(),
                "RESERVED",
                reservation.reservedAt()
        );
    }
}

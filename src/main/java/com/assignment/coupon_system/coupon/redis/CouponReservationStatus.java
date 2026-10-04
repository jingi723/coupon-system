package com.assignment.coupon_system.coupon.redis;

public enum CouponReservationStatus {
    RESERVED,
    ALREADY_RESERVED,
    DUPLICATE_USER,
    SOLD_OUT,
    REQUEST_CONFLICT
}

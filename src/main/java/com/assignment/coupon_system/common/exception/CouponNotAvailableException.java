package com.assignment.coupon_system.common.exception;

public class CouponNotAvailableException extends RuntimeException {
    public CouponNotAvailableException() {
        super("발급 가능한 쿠폰이 아닙니다.");
    }
}

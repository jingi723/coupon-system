package com.assignment.coupon_system.exception;

public class CouponNotAvailableException extends RuntimeException {

    public CouponNotAvailableException(Long couponId) {
        super("발급 가능한 기간이 아닙니다. id=" + couponId);
    }
}

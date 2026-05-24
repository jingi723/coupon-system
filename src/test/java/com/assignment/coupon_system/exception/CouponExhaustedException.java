package com.assignment.coupon_system.exception;

public class CouponExhaustedException extends RuntimeException {

    public CouponExhaustedException(Long couponId) {
        super("쿠폰 재고가 소진되었습니다. id=" + couponId);
    }
}

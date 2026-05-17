package com.assignment.coupon_system.common.exception;

public class CouponExhaustedException extends RuntimeException {
    public CouponExhaustedException() {
        super("쿠폰 재고가 소진되었습니다.");
    }
}

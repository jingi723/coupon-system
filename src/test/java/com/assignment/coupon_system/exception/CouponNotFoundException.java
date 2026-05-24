package com.assignment.coupon_system.exception;

public class CouponNotFoundException extends RuntimeException {

    public CouponNotFoundException(Long id) {
        super("쿠폰을 찾을 수 없습니다. id=" + id);
    }

    public CouponNotFoundException(String message) {
        super(message);
    }
}

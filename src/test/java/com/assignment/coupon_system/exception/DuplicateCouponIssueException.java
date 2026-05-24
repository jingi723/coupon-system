package com.assignment.coupon_system.exception;

public class DuplicateCouponIssueException extends RuntimeException {

    public DuplicateCouponIssueException(Long couponId, Long userId) {
        super("이미 발급받은 쿠폰입니다. couponId=" + couponId + ", userId=" + userId);
    }
}

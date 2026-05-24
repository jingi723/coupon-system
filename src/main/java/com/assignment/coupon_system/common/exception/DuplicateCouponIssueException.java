package com.assignment.coupon_system.common.exception;

public class DuplicateCouponIssueException extends RuntimeException {
    public DuplicateCouponIssueException() {
        super("이미 발급받은 쿠폰입니다.");
    }
}

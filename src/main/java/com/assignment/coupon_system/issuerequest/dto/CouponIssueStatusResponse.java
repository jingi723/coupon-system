package com.assignment.coupon_system.issuerequest.dto;

public record CouponIssueStatusResponse(
        String requestId,
        Long couponId,
        Long userId,
        String status,
        Long issuedCouponId
) {
}

package com.assignment.coupon_system.issuerequest.dto;

import java.time.Instant;

public record CouponIssueMessage(
        String requestId,
        Long couponId,
        Long userId,
        Instant receivedAt
) {
}

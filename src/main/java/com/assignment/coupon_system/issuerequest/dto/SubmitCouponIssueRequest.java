package com.assignment.coupon_system.issuerequest.dto;

import java.util.UUID;

public record SubmitCouponIssueRequest(
        UUID requestId,
        Long userId
) {
}

package com.assignment.coupon_system.issuerequest.dto;

public record CouponIssueAcceptedResponse (
        String requestId,
        String status
){
}

package com.assignment.coupon_system.couponusagehistory.controller;

import com.assignment.coupon_system.common.response.ApiResponse;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryRequest;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryResponse;
import com.assignment.coupon_system.couponusagehistory.service.CouponUsageHistoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class CouponUsageHistoryController {

    private final CouponUsageHistoryService couponUsageHistoryService;

    @PostMapping("/api/coupons/usage/{issuedCouponId}")
    public ApiResponse<CouponUsageHistoryResponse> useCoupon(
            @PathVariable Long issuedCouponId,
            @RequestBody CouponUsageHistoryRequest couponUsageHistoryRequest
    ) {
        return ApiResponse.success(couponUsageHistoryService.useCoupon(issuedCouponId, couponUsageHistoryRequest));
    }

    @GetMapping("/api/coupons/usage/history/{userId}/")
    public ApiResponse<List<CouponUsageHistoryResponse>> getCouponUsageHistory(
            @PathVariable Long userId
    )  {
        return ApiResponse.success(couponUsageHistoryService.getUsageHistory(userId));
    }
}

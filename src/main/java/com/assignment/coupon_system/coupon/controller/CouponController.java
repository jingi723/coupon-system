package com.assignment.coupon_system.coupon.controller;

import com.assignment.coupon_system.common.response.ApiResponse;
import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.dto.CreateCouponRequest;
import com.assignment.coupon_system.coupon.service.CouponService;
import com.assignment.coupon_system.issuedcoupon.dto.IssueCouponRequest;
import com.assignment.coupon_system.issuedcoupon.dto.IssuedCouponResponse;
import com.assignment.coupon_system.issuedcoupon.service.IssuedCouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin(origins = "*")
@RestController
@RequiredArgsConstructor
public class CouponController {

    private final CouponService couponService;
    private final IssuedCouponService issuedCouponService;


    @PostMapping("api/coupons")
    public ApiResponse<CouponResponse> createCoupon(@RequestBody CreateCouponRequest request) {
        return ApiResponse.success(couponService.createCoupon(request));
    }

    @GetMapping("api/coupons/available")
    public ApiResponse<List<CouponResponse>> getAvailableCoupons() {
        return ApiResponse.success(couponService.getAvailableCoupons());
    }

    @GetMapping("api/coupons/{couponId}/stock")
    public ApiResponse<Integer> getStock(
            @PathVariable Long couponId
    ) {
        return ApiResponse.success(
                couponService.getCouponStock(couponId)
        );
    }

    @PostMapping("api/coupons/{couponId}/issue")
    public ApiResponse<IssuedCouponResponse> issueCoupon(
            @PathVariable Long couponId,
            @RequestBody IssueCouponRequest request
    ) {
        return ApiResponse.success(issuedCouponService.issueCoupon(couponId, request));
    }

    @GetMapping("api/coupons/users/{userId}")
    public ApiResponse<List<IssuedCouponResponse>> getUserCoupons(
            @PathVariable Long userId
    ) {
        return ApiResponse.success(issuedCouponService.getUserCoupons(userId));
    }

    @GetMapping("api/coupons/users/{userId}/usable")
    public ApiResponse<List<IssuedCouponResponse>> getUsableCoupons(
            @PathVariable Long userId
    ) {
        return ApiResponse.success(issuedCouponService.getUsableCoupons(userId));
    }

    @PostMapping("api/coupons/{couponId}/stock/init")
    public ApiResponse<Integer> initStock(
            @PathVariable Long couponId
    ) {
        return ApiResponse.success(couponService.initStock(couponId));
    }

}
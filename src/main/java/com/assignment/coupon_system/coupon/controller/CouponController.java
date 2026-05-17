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
@RequestMapping("/api/coupons")
public class CouponController {

    private final CouponService couponService;
    private final IssuedCouponService issuedCouponService;

    // 쿠폰 생성
    @PostMapping
    public ApiResponse<CouponResponse> createCoupon(
            @RequestBody CreateCouponRequest request
    ) {
        return ApiResponse.success(couponService.createCoupon(request));
    }

    // 발급가능한 쿠폰 목록
    @GetMapping("/available")
    public ApiResponse<List<CouponResponse>> getAvailableCoupons() {
        return ApiResponse.success(couponService.getAvailableCoupons());
    }

    // 선착순 쿠폰 발급 요청
    @PostMapping("/{couponId}/issue")
    public ApiResponse<IssuedCouponResponse> issueCoupon(
            @PathVariable Long couponId,
            @RequestBody IssueCouponRequest request
    ) {
        return ApiResponse.success(
                issuedCouponService.issueCoupon(couponId, request)
        );
    }

    // 잔여 수량 조회
    @GetMapping("/{couponId}/stock")
    public ApiResponse<Integer> getStock(
            @PathVariable Long couponId
    ) {
        return ApiResponse.success(
                couponService.getStock(couponId)
        );
    }

    // 유저 쿠폰 목록 조회
    @GetMapping("/users/{userId}")
    public ApiResponse<List<IssuedCouponResponse>> getUserCoupons(
            @PathVariable Long userId
    ) {
        return ApiResponse.success(
                issuedCouponService.getUserCoupons(userId)
        );
    }

    // 사용 가능한 쿠폰 조회
    @GetMapping("/users/{userId}/usable")
    public ApiResponse<List<IssuedCouponResponse>> getUsableCoupons(
            @PathVariable Long userId
    ) {
        return ApiResponse.success(
                issuedCouponService.getUsableCoupons(userId)
        );
    }

    // 쿠폰 초기화
    @PostMapping("/{couponId}/stock/init")
    public ApiResponse<Integer> initStock(
            @PathVariable Long couponId
    ) {
        return ApiResponse.success(
                couponService.initStock(couponId)
        );
    }
}

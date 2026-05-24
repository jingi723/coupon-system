package com.assignment.coupon_system.coupon.controller;

import com.assignment.coupon_system.coupon.dto.CreateCouponRequest;
import com.assignment.coupon_system.coupon.service.CouponService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CouponController {

    private final CouponService couponService;

    public CouponController(CouponService couponService) {
        this.couponService = couponService;
    }

    @PostMapping("/coupon")
    public Long createCoupon(@RequestBody CreateCouponRequest request) {
        return couponService.createCoupon(request);
    }
}
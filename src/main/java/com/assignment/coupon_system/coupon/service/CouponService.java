package com.assignment.coupon_system.coupon.service;

import com.assignment.coupon_system.CouponSystemApplication;
import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.dto.CreateCouponRequest;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.dto.IssueCouponRequest;
import com.assignment.coupon_system.issuedcoupon.dto.IssuedCouponResponse;
import org.springframework.stereotype.Service;

@Service
public class CouponService {
    private final CouponRepository couponRepository;

    public CouponService(CouponRepository couponRepository) {
        this.couponRepository = couponRepository;
    }

    public Long createCoupon(CreateCouponRequest request) {
        Coupon coupon = new Coupon(
                request.getDescription(),
                request.getCouponType(),
                request.getMinOrderAmount(),
                request.getMaxDiscountAmount(),
                request.getTotalQuantity(),
                request.getValidDays(),
                request.getStartDateTime(),
                request.getEndDateTime()
        );

        Coupon savedCoupon = couponRepository.save(coupon);

        return savedCoupon.getId();
    }
}

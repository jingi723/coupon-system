package com.assignment.coupon_system.coupon.service;

import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.dto.CreateCouponRequest;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class CouponService {
    private final CouponRepository couponRepository;

    public CouponService(CouponRepository couponRepository) {
        this.couponRepository = couponRepository;
    }

    public CouponResponse createCoupon(CreateCouponRequest request) {
        Coupon coupon = Coupon.create(
                request.getName(),
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

        return CouponResponse.from(savedCoupon);
    }

    public List<CouponResponse> getAvailableCoupons() {
        LocalDateTime now = LocalDateTime.now();
        return couponRepository.findByStatusAndStartDateLessThanAndEndDateGreaterThan(
                        CouponStatus.ACTIVE, now, now)
                .stream()
                .map(CouponResponse::from)
                .toList();
    }

    public Integer getCouponStock(Long couponId) {
        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(CouponNotFoundException::new);
        return coupon.getTotalQuantity() - coupon.getIssuedQuantity();
    }

    @Transactional
    public Integer initStock(Long couponId) {
        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(CouponNotFoundException::new);

        coupon.resetIssuedQuantity();

        return coupon.getTotalQuantity();
    }

}

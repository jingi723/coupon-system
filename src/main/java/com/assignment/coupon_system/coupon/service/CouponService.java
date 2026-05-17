package com.assignment.coupon_system.coupon.service;

import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.dto.CreateCouponRequest;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import jakarta.persistence.criteria.CriteriaBuilder;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CouponService {

    private final CouponRepository couponRepository;

    @Transactional
    public CouponResponse createCoupon(CreateCouponRequest request) {
        Coupon coupon = Coupon.create(
                request.getName(),
                request.getDescription(),
                request.getCouponType(),
                request.getDiscountValue(),
                request.getMinOrderAmount(),
                request.getMaxDiscountAmount(),
                request.getTotalQuantity(),
                request.getValidDays(),
                request.getStartDate(),
                request.getEndDate()
        );

        Coupon savedCoupon = couponRepository.save(coupon);

        return CouponResponse.from(savedCoupon);
    }

    public List<CouponResponse> getAvailableCoupons() {
        return couponRepository.findAll()
                .stream()
                .map(CouponResponse::from)
                .toList();
    }

    public Integer getStock(Long couponId) {
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

package com.assignment.coupon_system.issuedcoupon.service;

import com.assignment.coupon_system.common.exception.CouponExhaustedException;
import com.assignment.coupon_system.common.exception.CouponNotAvailableException;
import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.common.exception.DuplicateCouponIssueException;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.dto.IssueCouponRequest;
import com.assignment.coupon_system.issuedcoupon.dto.IssuedCouponResponse;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;


@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class IssuedCouponService {

    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRepository couponRepository;


    @Transactional
    public List<IssuedCouponResponse> getUserCoupons(Long userId) {
        return issuedCouponRepository
                .findByUserId(userId)
                .stream()
                .map(IssuedCouponResponse::from)
                .toList();
    }

    @Transactional
    public List<IssuedCouponResponse> getUsableCoupons(Long userId) {
        return issuedCouponRepository
                .findByUserIdAndStatus(userId, IssuedCouponStatus.ISSUED)
                .stream()
                .map(IssuedCouponResponse::from)
                .toList();
    }

    @Transactional
    public IssuedCouponResponse issueCoupon(Long couponId, IssueCouponRequest request) {
        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(CouponNotFoundException::new);

        LocalDateTime now = LocalDateTime.now();
        if (now.isBefore(coupon.getStartDate()) || now.isAfter(coupon.getEndDate())) {
            throw new CouponNotAvailableException();
        }

        int updated = couponRepository.tryIncreaseIssueQuantity(couponId, CouponStatus.ACTIVE);
        if (updated == 0) {
            throw new CouponExhaustedException();
        }

        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, request.getUserId());
        try {
            // 충돌 시 감지
            IssuedCoupon saved = issuedCouponRepository.saveAndFlush(issuedCoupon);
            return IssuedCouponResponse.from(saved);
        } catch (DataIntegrityViolationException e) {
            // (쿠폰, 유저) 유니크 제약
            throw new DuplicateCouponIssueException();
        }
    }
}

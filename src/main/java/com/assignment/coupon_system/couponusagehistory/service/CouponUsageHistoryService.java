package com.assignment.coupon_system.couponusagehistory.service;

import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryRequest;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryResponse;
import com.assignment.coupon_system.couponusagehistory.entity.CouponUsageHistory;
import com.assignment.coupon_system.couponusagehistory.repository.CouponUsageHistoryRepository;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CouponUsageHistoryService {

    private CouponUsageHistoryRepository couponUsageHistoryRepository;
    private IssuedCouponRepository issuedCouponRepository;

    @Transactional
    public CouponUsageHistoryResponse useCoupon(
            Long issuedCouponId,
            CouponUsageHistoryRequest couponUsageHistoryRequest
    ) {

        IssuedCoupon issuedCoupon = issuedCouponRepository.findById(issuedCouponId)
                .orElseThrow(CouponNotFoundException::new);

        int discountAmount = 0;

        CouponUsageHistory couponUsageHistory = CouponUsageHistory.create(
                issuedCoupon,
                couponUsageHistoryRequest.getOrderId(),
                discountAmount
        );

        return CouponUsageHistoryResponse.from(couponUsageHistoryRepository.save(couponUsageHistory));
    }


}

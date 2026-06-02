package com.assignment.coupon_system.couponusagehistory.service;

import com.assignment.coupon_system.common.exception.CouponNotAvailableException;
import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryRequest;
import com.assignment.coupon_system.couponusagehistory.dto.CouponUsageHistoryResponse;
import com.assignment.coupon_system.couponusagehistory.entity.CouponUsageHistory;
import com.assignment.coupon_system.couponusagehistory.repository.CouponUsageHistoryRepository;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CouponUsageHistoryService {

    private final CouponUsageHistoryRepository couponUsageHistoryRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    @Transactional
    public CouponUsageHistoryResponse useCoupon(
            Long issuedCouponId,
            CouponUsageHistoryRequest couponUsageHistoryRequest
    ) {

        IssuedCoupon issuedCoupon = issuedCouponRepository.findById(issuedCouponId)
                .orElseThrow(CouponNotFoundException::new);

         if(issuedCoupon.getStatus() == IssuedCouponStatus.USED) {
             throw new CouponNotAvailableException();
         }
         if(LocalDateTime.now().isAfter(issuedCoupon.getExpiredAt())) {
             throw new CouponNotAvailableException();
         }
         if(couponUsageHistoryRequest.getOrderAmount() < issuedCoupon.getCoupon().getMinOrderAmount()) {
             throw new CouponNotAvailableException();
         }

        Coupon coupon = issuedCoupon.getCoupon();
         int discountAmount;

         if(coupon.getCouponType() == CouponType.FIXED_AMOUNT) {
             discountAmount = coupon.getDiscountValue();
         } else {
             discountAmount = couponUsageHistoryRequest.getOrderAmount() * coupon.getDiscountValue() / 100;
             discountAmount = Math.min(discountAmount, coupon.getMaxDiscountAmount());
         }

         issuedCoupon.use();

        CouponUsageHistory couponUsageHistory = CouponUsageHistory.create(
                issuedCoupon,
                couponUsageHistoryRequest.getOrderId(),
                discountAmount
        );

        return CouponUsageHistoryResponse.from(couponUsageHistoryRepository.save(couponUsageHistory));
    }

    public List<CouponUsageHistoryResponse> getUsageHistory(Long userId) {
        return couponUsageHistoryRepository.findByIssuedCouponUserId(userId)
                .stream()
                .map(CouponUsageHistoryResponse::from)
                .toList();
    }
}

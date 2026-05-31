package com.assignment.coupon_system.couponusagehistory.dto;

import com.assignment.coupon_system.couponusagehistory.entity.CouponUsageHistory;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class CouponUsageHistoryResponse {

    private Long id;
    private Long issuedCouponId;
    private Long orderId;
    private Integer discountAmount;
    private LocalDateTime usedAt;


    public static CouponUsageHistoryResponse from(CouponUsageHistory couponUsageHistory) {
        return CouponUsageHistoryResponse.builder()
                .id(couponUsageHistory.getId())
                .issuedCouponId(couponUsageHistory.getIssuedCoupon().getId())
                .orderId(couponUsageHistory.getOrderId())
                .discountAmount(couponUsageHistory.getDiscountAmount())
                .usedAt(couponUsageHistory.getUsedAt())
                .build();
    }
}

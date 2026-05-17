package com.assignment.coupon_system.issuedcoupon.dto;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class IssuedCouponResponse {

    private Long id;
    private Long couponId;
    private Long userId;
    private IssuedCouponStatus status;
    private LocalDateTime issuedAt;
    private LocalDateTime expiredAt;
    private LocalDateTime usedAt;

    public static IssuedCouponResponse from(IssuedCoupon issuedCoupon) {
        return IssuedCouponResponse.builder()
                .id(issuedCoupon.getId())
                .couponId(issuedCoupon.getCoupon().getId())
                .userId(issuedCoupon.getUserId())
                .status(issuedCoupon.getStatus())
                .issuedAt(issuedCoupon.getIssuedAt())
                .expiredAt(issuedCoupon.getExpiredAt())
                .usedAt(issuedCoupon.getUsedAt())
                .build();
    }
}

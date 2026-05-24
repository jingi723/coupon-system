package com.assignment.coupon_system.issuedcoupon.dto;

import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
public class IssueCouponRequest {
    private Long userId;
}

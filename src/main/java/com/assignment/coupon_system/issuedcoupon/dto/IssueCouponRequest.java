package com.assignment.coupon_system.issuedcoupon.dto;

import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class IssueCouponRequest {
    private Long userId;
}

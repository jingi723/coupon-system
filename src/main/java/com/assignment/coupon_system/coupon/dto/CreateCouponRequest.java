package com.assignment.coupon_system.coupon.dto;

import com.assignment.coupon_system.coupon.entity.CouponType;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
public class CreateCouponRequest {

    private String name;
    private String description;
    private CouponType couponType;
    private Integer discountValue;
    private Integer minOrderAmount;
    private Integer maxDiscountAmount;
    private Integer totalQuantity;
    private Integer validDays;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
}

package com.assignment.coupon_system.coupon.dto;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.entity.CouponType;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class CouponResponse {

    private Long id;
    private String description;
    private CouponType couponType;
    private CouponStatus status;
    private Integer discountValue;
    private Integer minOrderAmount;
    private Integer maxDiscountAmount;
    private Integer totalQuantity;
    private Integer issuedQuantity;
    private Integer validDays;
    private LocalDateTime startDate;
    private LocalDateTime endDate;

    public static CouponResponse from(Coupon coupon) {
        return CouponResponse.builder()
                .id(coupon.getId())
                .description(coupon.getDescription())
                .couponType(coupon.getCouponType())
                .status(coupon.getStatus())
                .discountValue(coupon.getDiscountValue())
                .minOrderAmount(coupon.getMinOrderAmount())
                .maxDiscountAmount(coupon.getMaxDiscountAmount())
                .totalQuantity(coupon.getTotalQuantity())
                .issuedQuantity(coupon.getIssuedQuantity())
                .validDays(coupon.getValidDays())
                .startDate(coupon.getStartDate())
                .endDate(coupon.getEndDate())
                .build();
    }

}

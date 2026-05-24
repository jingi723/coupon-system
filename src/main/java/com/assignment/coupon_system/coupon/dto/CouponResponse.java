package com.assignment.coupon_system.coupon.dto;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.entity.Status;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class CouponResponse {

    private Long id;
    private String description;
    private CouponType couponType;
    private Status status;
    private Integer discountValue;
    private Integer minOrderAmount;
    private Integer maxDiscountAmount;
    private Integer totalQuantity;
    private Integer issuedQuantity;
    private Integer validDays;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;

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
                .startDateTime(coupon.getStartDateTime())
                .endDateTime(coupon.getEndDateTime())
                .build();
    }

}

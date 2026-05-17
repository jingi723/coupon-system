package com.assignment.coupon_system.coupon.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDateTime;

@Entity
@Getter
@NoArgsConstructor
public class Coupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private String description;

    @Enumerated(EnumType.STRING)
    private CouponType couponType;

    @Enumerated(EnumType.STRING)
    private CouponStatus status;

    private Integer discountValue;

    private Integer minOrderAmount;

    private Integer maxDiscountAmount;

    private Integer totalQuantity;

    private Integer issuedQuantity;

    private Integer validDays;

    private LocalDateTime startDate;

    private LocalDateTime endDate;

    public static Coupon create(
        String name,
        String description,
        CouponType couponType,
        Integer discountValue,
        Integer minOrderAmount,
        Integer maxDiscountAmount,
        Integer totalQuantity,
        Integer validDays,
        LocalDateTime startDate,
        LocalDateTime endDate
    ) {
        Coupon coupon = new Coupon();
        coupon.name = name;
        coupon.description = description;
        coupon.couponType = couponType;
        coupon.status = CouponStatus.ACTIVE;
        coupon.discountValue = discountValue;
        coupon.minOrderAmount = minOrderAmount;
        coupon.maxDiscountAmount = maxDiscountAmount;
        coupon.totalQuantity = totalQuantity;
        coupon.issuedQuantity = 0;
        coupon.validDays = validDays;
        coupon.startDate = startDate;
        coupon.endDate = endDate;

        return coupon;
    }

    public void increaseIssuedQuantity() {
        this.issuedQuantity += 1;
    }

    public void resetIssuedQuantity() {
        this.issuedQuantity = 0;
        this.status = CouponStatus.ACTIVE;
    }
}

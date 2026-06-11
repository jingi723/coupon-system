package com.assignment.coupon_system.coupon.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


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
            Integer minOrderAmount,
            Integer maxDiscountAmount,
            Integer totalQuantity,
            Integer validDays,
            LocalDateTime startDateTime,
            LocalDateTime endDateTime
    ) {
        Coupon coupon = new Coupon();
        coupon.name = name;
        coupon.description = description;
        coupon.couponType = couponType;
        coupon.status = CouponStatus.ACTIVE;
        coupon.minOrderAmount = minOrderAmount;
        coupon.maxDiscountAmount = maxDiscountAmount;
        coupon.totalQuantity = totalQuantity;
        coupon.issuedQuantity = 0;
        coupon.validDays = validDays;
        coupon.startDate = startDateTime;
        coupon.endDate = endDateTime;

        return coupon;
    }

    public void increaseIssuedQuantity() {
        if(this.issuedQuantity >= this.totalQuantity) {
            throw new IllegalArgumentException("sold out");
        }
        this.issuedQuantity += 1;
    }

    public void resetIssuedQuantity() {
        this.issuedQuantity = 0;
        this.status = CouponStatus.ACTIVE;
    }

    public boolean isAvailable() {
        LocalDateTime now = LocalDateTime.now();
        return this.status == CouponStatus.ACTIVE
                && now.isAfter(this.startDate)
                && now.isBefore(this.endDate);
    }

    public boolean isExhausted() {
        return this.issuedQuantity >= this.totalQuantity;
    }

    public void markAsExhausted() {
        this.status = CouponStatus.EXHAUSTED;
    }

}

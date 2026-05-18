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

    private String description;

    @Enumerated(EnumType.STRING)
    private CouponType couponType;

    @Enumerated(EnumType.STRING)
    private Status status;

    private Integer minOrderAmount;

    private Integer maxDiscountAmount;

    private Integer totalQuantity;

    private Integer issuedQuantity;

    private Integer validDays;

    private LocalDateTime startDateTime;

    private LocalDateTime endDateTime;


    public  Coupon(
            String description,
            CouponType couponType,
            Integer minOrderAmount,
            Integer maxDiscountAmount,
            Integer totalQuantity,
            Integer validDays,
            LocalDateTime startDateTime,
            LocalDateTime endDateTime
    ) {
        this.description = description;
        this.couponType = couponType;
        this.status = Status.ACTIVE;
        this.minOrderAmount = minOrderAmount;
        this.maxDiscountAmount = maxDiscountAmount;
        this.totalQuantity = totalQuantity;
        this.issuedQuantity = 0;
        this.validDays = validDays;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
    }

    public void increaseIssuedQuantity() {
        issuedQuantity += 1;
    }

    public void resetIssuedQuantity() {
        this.issuedQuantity = 0;
        this.status = Status.ACTIVE;
    }
}

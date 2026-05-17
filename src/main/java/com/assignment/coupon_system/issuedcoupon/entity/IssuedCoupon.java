package com.assignment.coupon_system.issuedcoupon.entity;

import com.assignment.coupon_system.coupon.entity.Coupon;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor
public class IssuedCoupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "coupon_id")
    private Coupon coupon;

    private Long userId;

    @Enumerated(EnumType.STRING)
    private IssuedCouponStatus status;

    private LocalDateTime issuedAt;

    private LocalDateTime expiredAt;

    private LocalDateTime usedAt;

    public static IssuedCoupon issue(Coupon coupon, Long userId) {
        IssuedCoupon issuedCoupon = new IssuedCoupon();
        issuedCoupon.coupon = coupon;
        issuedCoupon.userId = userId;
        issuedCoupon.status = IssuedCouponStatus.ISSUED;
        issuedCoupon.issuedAt = LocalDateTime.now();
        issuedCoupon.expiredAt = issuedCoupon.issuedAt.plusDays(coupon.getValidDays());
        return issuedCoupon;
    }


}

package com.assignment.coupon_system.couponusagehistory.entity;

import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor
public class CouponUsageHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "issued_coupon_id")
    private IssuedCoupon issuedCoupon;

//    private Long userId;

    private Long orderId;

    private Integer discountAmount;

    private LocalDateTime usedAt;

    public static CouponUsageHistory create(
            IssuedCoupon issuedCoupon,
            Long orderId,
            Integer discountAmount
    ) {
        CouponUsageHistory couponUsageHistory = new CouponUsageHistory();
        couponUsageHistory.issuedCoupon = issuedCoupon;
        couponUsageHistory.orderId = orderId;
        couponUsageHistory.discountAmount = discountAmount;
        couponUsageHistory.usedAt = LocalDateTime.now();

        return couponUsageHistory;
    }

}

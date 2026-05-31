package com.assignment.coupon_system.couponusagehistory.dto;

import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CouponUsageHistoryRequest {

    private IssuedCoupon issuedCoupon;
    private Long orderId;

}

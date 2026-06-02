package com.assignment.coupon_system.couponusagehistory.repository;

import com.assignment.coupon_system.couponusagehistory.entity.CouponUsageHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CouponUsageHistoryRepository extends JpaRepository<CouponUsageHistory, Long> {
    List<CouponUsageHistory> findByIssuedCouponUserId(Long userId);
}

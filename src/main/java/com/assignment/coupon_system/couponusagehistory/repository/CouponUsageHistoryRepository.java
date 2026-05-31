package com.assignment.coupon_system.couponusagehistory.repository;

import com.assignment.coupon_system.couponusagehistory.entity.CouponUsageHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponUsageHistoryRepository extends JpaRepository<CouponUsageHistory, Long> {
}

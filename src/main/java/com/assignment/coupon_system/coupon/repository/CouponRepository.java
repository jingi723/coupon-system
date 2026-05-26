package com.assignment.coupon_system.coupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface CouponRepository extends JpaRepository<Coupon, Long> {
    List<Coupon> findByStatusAndStartDateLessThanAndEndDateGreaterThan(
            CouponStatus status, LocalDateTime startBefore, LocalDateTime endAfter);
}

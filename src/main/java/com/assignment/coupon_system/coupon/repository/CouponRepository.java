package com.assignment.coupon_system.coupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponRepository extends JpaRepository<Coupon, Long> {
}

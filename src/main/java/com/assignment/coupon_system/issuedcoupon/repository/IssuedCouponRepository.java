package com.assignment.coupon_system.issuedcoupon.repository;

import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IssuedCouponRepository extends JpaRepository<IssuedCoupon, Long> {

    boolean existsByCouponIdAndUserId(Long couponId, Long userId);

    List<IssuedCoupon> findByUserId(Long userId);

    List<IssuedCoupon> findByUserIdAndStatus(Long userId, IssuedCouponStatus status);
}

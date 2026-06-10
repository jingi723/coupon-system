package com.assignment.coupon_system.coupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import io.lettuce.core.dynamic.annotation.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface CouponRepository extends JpaRepository<Coupon, Long> {
    List<Coupon> findByStatusAndStartDateLessThanAndEndDateGreaterThan(
            CouponStatus status, LocalDateTime startBefore, LocalDateTime endAfter);

    @Modifying
    @Query("""
        update Coupon c
        set c.issuedQuantity = c.issuedQuantity +1
        where c.id = :couponId
            and c.status = :activeStatus
            and c.issuedQuantity < c.totalQuantity
        """)
    int tryIncreaseIssueQuantity(@Param("couponId") Long couponId,
                                 @Param("activeStatus") CouponStatus activeStatus);
}

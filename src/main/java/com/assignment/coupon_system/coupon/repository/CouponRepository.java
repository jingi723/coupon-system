package com.assignment.coupon_system.coupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

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

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from Coupon c where c.id = :couponId")
    Optional<Coupon> findByIdForUpdate(@Param("couponId") Long couponId);
}

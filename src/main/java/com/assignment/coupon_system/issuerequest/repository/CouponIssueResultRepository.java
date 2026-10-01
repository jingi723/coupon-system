package com.assignment.coupon_system.issuerequest.repository;

import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface CouponIssueResultRepository extends JpaRepository<CouponIssueResult, Long> {
    Optional<CouponIssueResult> findByRequestId(String requestId);
}

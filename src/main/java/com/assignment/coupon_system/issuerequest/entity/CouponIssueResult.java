package com.assignment.coupon_system.issuerequest.entity;

import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.awt.image.LookupOp;
import java.beans.ConstructorProperties;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(
        name = "coupon_issue_result",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_coupon_result_request_id",
                        columnNames = "request_id"
                )
        }
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CouponIssueResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", nullable = false, length = 36)
    private String requestId;

    @Column(name = "coupon_id", nullable = false)
    private Long couponId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private CouponIssueResultStatus status;

    @Column(name = "issued_coupon_id")
    private Long issuedCouponId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    private CouponIssueResult(
            String requestId,
            Long couponId,
            Long userId,
            CouponIssueResultStatus status,
            Long issuedCouponId
    ) {
        this.requestId = Objects.requireNonNull(requestId);
        this.couponId = Objects.requireNonNull(couponId);
        this.userId = Objects.requireNonNull(userId);
        this.status = status;
        this.issuedCouponId = issuedCouponId;
        this.processedAt = LocalDateTime.now();
    }

    public static CouponIssueResult issued(
            String requestId,
            Long couponId,
            Long userId,
            Long issuedCouponId
    ) {
        return new CouponIssueResult(
                requestId,
                couponId,
                userId,
                CouponIssueResultStatus.ISSUED,
                Objects.requireNonNull(issuedCouponId)
        );
    }

    public static CouponIssueResult rejected(
            String requestId,
            Long couponId,
            Long userId
    ) {
        return new CouponIssueResult(
                requestId,
                couponId,
                userId,
                CouponIssueResultStatus.REJECTED,
                null
        );
    }
}

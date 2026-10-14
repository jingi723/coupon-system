package com.assignment.coupon_system.issuerequest.service;

import com.assignment.coupon_system.common.LegacyCouponGuard;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import com.assignment.coupon_system.issuerequest.entity.CouponIssueResultStatus;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class CouponIssueProcessor {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponIssueResultRepository resultRepository;
    private final LegacyCouponGuard legacyCouponGuard;

    private static final ZoneId EVENT_ZONE = ZoneId.of("Asia/Seoul");

    @Transactional
    public CouponIssueResult process(
            String requestId,
            Long couponId,
            Long userId
    ) {
        legacyCouponGuard.checkEnabled();

        // 1. 이미 처리한 요청인지 확인
        var existing = resultRepository.findByRequestId(requestId);

        if(existing.isPresent()) {
            CouponIssueResult result = existing.get();

            if(!result.getCouponId().equals(couponId)
                || !result.getUserId().equals(userId)) {
                throw new IllegalArgumentException(
                        "같은 요청 ID에 다른 요청 신청 내용을 사용할 수 없습니다."
                );
            }
            return result;
        }

        // 2. 쿠폰 존재 여부와 발급 가능 여부 확인
        Coupon coupon = couponRepository.findById(couponId).orElse(null);

        if (coupon == null || !coupon.isAvailable()) {
            return reject(requestId, couponId, userId);
        }

        // 3. 이미 발급 받은 사용자 거절
        if(issuedCouponRepository.existsByCouponIdAndUserId(
                couponId, userId)) {
            return reject(requestId, couponId, userId);
        }

        // 4. 재과가 남아있고 ACTIVE인 경우에만 수량 증가
        int updated = couponRepository.tryIncreaseIssueQuantity(
                couponId, CouponStatus.ACTIVE
        );

        if(updated == 0) {
            return reject(requestId, couponId, userId);
        }

        // 5. 발급 이력 저장
        IssuedCoupon issuedCoupon = issuedCouponRepository.saveAndFlush(
                IssuedCoupon.issue(coupon, userId)
        );

        // 6. 신청의 성공 결과 저장
        return resultRepository.save(
                CouponIssueResult.issued(
                        requestId,
                        couponId,
                        userId,
                        issuedCoupon.getId()
                )
        );
    }

    private CouponIssueResult reject(
            String requestId,
            Long couponId,
            Long userId
    ) {
        return resultRepository.save(
                CouponIssueResult.rejected(
                        requestId,
                        couponId,
                        userId
                )
        );
    }

    @Transactional
    public CouponIssueResult processReserved(
            String requestId,
            Long couponId,
            Long userId,
            Instant reservedAt
    ) {
        if(requestId == null || requestId.isBlank()
            || couponId == null || couponId <= 0
            || userId == null || userId <= 0
            || reservedAt == null) {
            throw new IllegalArgumentException("예약 승인 정보가 올바르지 않습니다.");
        }

        var existing = resultRepository.findByRequestId(requestId);

        if(existing.isPresent()) {
            CouponIssueResult result = existing.get();

            if(!result.getCouponId().equals(couponId)
                    || !result.getUserId().equals(userId)) {
                throw new IllegalArgumentException("같은 requestId에 다른 신청 정보를 사용할 수 없습니다.");
            }

            if(result.getStatus() != CouponIssueResultStatus.ISSUED) {
                throw new IllegalArgumentException("승인 예약과 기존 처리 결과가 충돌합니다. requestId=" + requestId);
            }

            return result;
        }

        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(() -> new IllegalStateException(
                        "예약 대상 쿠폰이 없습니다. requestId=" + requestId
                ));

        Instant startsAt = coupon.getStartDate()
                .atZone(EVENT_ZONE)
                .toInstant();

        Instant endsAt = coupon.getEndDate()
                .atZone(EVENT_ZONE)
                .toInstant();

        if(reservedAt.isBefore(startsAt)
                || !reservedAt.isBefore(endsAt)) {
            throw new IllegalArgumentException(
                    "행사 기간 밖의 예약 승인입니다. requestId=" + requestId
            );
        }

        if(issuedCouponRepository.existsByCouponIdAndUserId(
                couponId, userId)) {
            throw new IllegalStateException("다른 요청으로 이미 발급된 사용자입니다. requestId=" + requestId);
        }

        int update =
                couponRepository.tryIncreaseReservedIssueQuantity(couponId);

        if(update != 1) {
            throw new IllegalStateException("승인 예약을 반영할 DB 수량이 부족합니다. requestId=" + requestId);
        }

        IssuedCoupon issuedCoupon = issuedCouponRepository.saveAndFlush(
                IssuedCoupon.issue(coupon, userId)
        );

        return resultRepository.save(
                CouponIssueResult.issued(
                        requestId,
                        couponId,
                        userId,
                        issuedCoupon.getId()
                )
        );
    }
}

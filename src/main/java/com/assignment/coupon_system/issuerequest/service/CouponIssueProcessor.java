package com.assignment.coupon_system.issuerequest.service;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CouponIssueProcessor {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponIssueResultRepository resultRepository;

    @Transactional
    public CouponIssueResult process(
            String requestId,
            Long couponId,
            Long userId
    ) {
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
}

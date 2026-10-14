package com.assignment.coupon_system.issuerequest.service;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueStatusResponse;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CouponIssueQueryService {

    private final CouponIssueResultRepository resultRepository;
    private final CouponIssueRedisService redisService;

    public Optional<CouponIssueStatusResponse> find(
            Long couponId,
            String requestId
    ) {
        var result = resultRepository.findByRequestId(requestId);

        if(result.isPresent()) {
            var saved = result.get();

            if(!couponId.equals(saved.getCouponId())) {
                return Optional.empty();
            }

            return Optional.of(new CouponIssueStatusResponse(
                    saved.getRequestId(),
                    saved.getCouponId(),
                    saved.getUserId(),
                    saved.getStatus().name(),
                    saved.getIssuedCouponId()
            ));
        }

        return redisService.findReservationIfPresent(requestId)
                .filter(reservation ->
                        couponId.equals(reservation.couponId()))
                .map(reservation -> new CouponIssueStatusResponse(
                        reservation.requestId(),
                        reservation.couponId(),
                        reservation.userId(),
                        "RESERVED",
                        null
                ));
    }
}

package com.assignment.coupon_system.issuerequest.controller;

import com.assignment.coupon_system.common.response.ApiResponse;
import com.assignment.coupon_system.issuerequest.service.CouponIssueQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class CouponIssueQueryController {

    private final CouponIssueQueryService queryService;

    @GetMapping("/api/coupons/{couponId}/issue-requests/{requestId}")
    public ResponseEntity<?> find(
            @PathVariable Long couponId,
            @PathVariable UUID requestId
    ) {
        if(couponId <= 0) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.fail("couponId는 양수여야 합니다.")
            );
        }

        try {
            var result = queryService.find(
                    couponId,
                    requestId.toString()
            );

            if(result.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(
                        ApiResponse.fail("해당 쿠폰의 신청 정보를 찾을 수 없습니다.")
                );
            }

            return ResponseEntity.ok(
                    ApiResponse.success(result.get())
            );
        } catch (DataAccessException | IllegalStateException e) {
            log.error(
                    "발급 상태 조회 오류. requestId={}, couponId={}",
                    requestId,
                    couponId,
                    e
            );

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(ApiResponse.fail(
                            "발급 상태를 확인하지 못했습니다. "
                            + "같은 requestId로 다시 조회하세요."
                    ));
        }
    }
}

package com.assignment.coupon_system.issuerequest.controller;

import com.assignment.coupon_system.common.response.ApiResponse;
import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.redis.CouponReservationStatus;
import com.assignment.coupon_system.issuerequest.dto.CouponReservationResponse;
import com.assignment.coupon_system.issuerequest.dto.SubmitCouponIssueRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequiredArgsConstructor
public class CouponIssueRequestController {

    private final CouponIssueRedisService redisService;

    @PostMapping("/api/coupons/{couponId}/issue-requests")
    public ResponseEntity<?> submit(
            @PathVariable Long couponId,
            @RequestBody SubmitCouponIssueRequest request
    ) {
        if (couponId == null || couponId <= 0
                || request == null || request.requestId() == null
                || request.userId() == null || request.userId() <= 0) {
            return ResponseEntity.badRequest().body(ApiResponse.fail(
                    "requestId와 양수인 couponId, userId가 필요합니다."
            ));
        }

        String requestId = request.requestId().toString();
        try {
            CouponReservationStatus status = redisService.reserve(
                    requestId, couponId, request.userId()
            );
            return switch (status) {
                case RESERVED, ALREADY_RESERVED -> ResponseEntity.accepted().body(
                        ApiResponse.success(CouponReservationResponse.from(
                                redisService.findReservation(requestId)
                        ))
                );
                case SOLD_OUT -> conflict("SOLD_OUT: 쿠폰 재고가 소진됐습니다.");
                case DUPLICATE_USER -> conflict(
                        "DUPLICATE_USER: 이미 다른 요청으로 예약한 사용자입니다.");
                case REQUEST_CONFLICT -> conflict(
                        "REQUEST_CONFLICT: 같은 requestId의 신청 내용이 다릅니다.");
                case NOT_AVAILABLE -> conflict(
                        "NOT_AVAILABLE: 현재 예약 가능한 상태나 기간이 아닙니다.");
            };
        } catch (DataAccessException e) {
            log.error("Redis 예약 처리 오류. requestId={}, couponId={}", requestId, couponId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ApiResponse.fail(
                    "예약 여부를 확인하지 못했습니다. 같은 requestId로 재시도하세요."
            ));
        } catch (IllegalStateException e) {
            log.error("Redis 예약 데이터 확인 필요. requestId={}, couponId={}", requestId, couponId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ApiResponse.fail(
                    "예약 정보를 확인할 수 없습니다. 서버 상태 확인이 필요합니다. "
                            + "재시도 시 같은 requestId를 사용하세요."
            ));
        }
    }

    private ResponseEntity<ApiResponse<Void>> conflict(String message) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiResponse.fail(message));
    }
}

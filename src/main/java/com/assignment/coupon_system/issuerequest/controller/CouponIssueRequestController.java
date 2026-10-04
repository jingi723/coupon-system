package com.assignment.coupon_system.issuerequest.controller;

import com.assignment.coupon_system.common.response.ApiResponse;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueAcceptedResponse;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.dto.SubmitCouponIssueRequest;
import com.assignment.coupon_system.issuerequest.producer.CouponIssueProducer;
import com.sun.net.httpserver.HttpsServer;
import lombok.RequiredArgsConstructor;
import org.apache.coyote.Response;
import org.apache.kafka.common.KafkaException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@RequiredArgsConstructor
public class CouponIssueRequestController {

    private final CouponIssueProducer producer;

    @PostMapping("/api/coupons/{couponId}/issue-requests")
    public ResponseEntity<ApiResponse<CouponIssueAcceptedResponse>> submit(
            @PathVariable Long couponId,
            @RequestBody SubmitCouponIssueRequest request
    ) {
        if (couponId <= 0
                || request.requestId() == null
                || request.userId() == null
                || request.userId() <= 0
        ) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "requestId와 양수인 couponId, userId가 필요합니다."
            );
        }

        String requestId = request.requestId().toString();

        CouponIssueMessage message = new CouponIssueMessage(
                requestId,
                couponId,
                request.userId(),
                Instant.now()
        );

        try {
            producer.send(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "접수 여부를 확인하지 못했습니다. 같은 requestId로 재시도하세요,",
                    e
            );
        } catch (ExecutionException | TimeoutException | KafkaException e) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "접수 여부를 확인하지 못했습니다. 같은 requestId로 재시도하세요.",
                    e
            );
        }

        return ResponseEntity.accepted().body(
                ApiResponse.success(
                        new CouponIssueAcceptedResponse(requestId, "ACCEPTED")
                )
        );
    }
}

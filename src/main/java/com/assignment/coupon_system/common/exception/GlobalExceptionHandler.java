package com.assignment.coupon_system.common.exception;

import com.assignment.coupon_system.common.response.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CouponNotFoundException.class)
    public ResponseEntity<ApiResponse<Void>> handleCouponNotFound(
            CouponNotFoundException e
    ) {
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ApiResponse.fail(e.getMessage()));
    }

    @ExceptionHandler(CouponExhaustedException.class)
    public ResponseEntity<ApiResponse<Void>> handleCouponExhausted(
            CouponExhaustedException e
    ) {
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(ApiResponse.fail(e.getMessage()));
    }

    @ExceptionHandler(CouponNotAvailableException.class)
    public ResponseEntity<ApiResponse<Void>> handleCouponNotAvailable(
            CouponNotAvailableException e
    ) {
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.fail(e.getMessage()));
    }

    @ExceptionHandler(DuplicateCouponIssueException.class)
    public ResponseEntity<ApiResponse<Void>> handleDuplicateCouponIssue(
            DuplicateCouponIssueException e
    ) {
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(ApiResponse.fail(e.getMessage()));
    }
}

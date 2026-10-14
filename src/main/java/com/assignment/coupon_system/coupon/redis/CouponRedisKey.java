package com.assignment.coupon_system.coupon.redis;

public final class CouponRedisKey {

    private CouponRedisKey() {}

    public static String stock(Long couponId) {
        return "coupon:" + couponId + ":stock";
    }

    public static String issueUsers(Long couponId) {
        return "coupon:" + couponId + ":users";
    }

    // 요청 ID는 쿠폰관 관계없이 전역으로 식별 가능하게 한다.
    public  static  String reservation(String requestId) {
        return "coupon:reservation:" + requestId;
    }

    // 모든 쿠폰의 Kafka 미절달 예약을 모아두는 List
    public static String pendingReservations() {
        return "coupon:reservation:pending";
    }

    // 작업자가 가져갔지만 Kafka 전달 확인, 정리가 끝나지 않은 예약
    public static String processingReservations() {
        return "coupon:reservation:processing";
    }

    // 쿠폰 예약 접수 정책을 저장하는 Hash
    public static String reservationPolicy(Long couponId) {
        return "coupon:" + couponId + ":reservation-policy";
    }
}

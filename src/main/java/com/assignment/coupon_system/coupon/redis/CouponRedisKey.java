package com.assignment.coupon_system.coupon.redis;

import com.assignment.coupon_system.coupon.entity.CouponStatus;

public final class CouponRedisKey {

    private CouponRedisKey() {}

    public static String stock(Long couponId) {
        return "coupon:" + couponId + ":stock";
    }

    public static String issueUsers(Long couponId) {
        return "coupon:" + couponId + ":users";
    }
}

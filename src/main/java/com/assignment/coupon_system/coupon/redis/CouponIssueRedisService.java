package com.assignment.coupon_system.coupon.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CouponIssueRedisService {

    private final StringRedisTemplate redisTemplate;

    private static final String ISSUE_SCRIPT = """
            local stock = tonumber(redis.call('GET', KEYS[1]))
            
            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
                return -1
            end
            
            if stock == nil or stock <= 0 then
                return -2
            end
            
            redis.call('DECR', KEYS[1])
            redis.call('SADD', KEYS[2], ARGV[1])
            
            return 1
            """;

    public CouponIssueResult tryIssue(Long couponId, Long userId) {
        String stockKey = CouponRedisKey.stock(couponId);
        String usersKey = CouponRedisKey.issueUsers(couponId);

        Long result = redisTemplate.execute(
                new DefaultRedisScript<>(ISSUE_SCRIPT, Long.class),
                List.of(stockKey, usersKey),
                userId.toString()
        );

        if (result == 1L) return CouponIssueResult.SUCCESS;
        if (result == -1L) return CouponIssueResult.DUPLICATE;
        if (result == -2L) return CouponIssueResult.SOLD_OUT;

        throw new IllegalStateException("Unknown Redis result: " + result);
    }

    public void rollbackIssue(Long couponId, Long userId) {
        String stockKey = CouponRedisKey.stock(couponId);
        String usersKey = CouponRedisKey.issueUsers(couponId);

        Long removed = redisTemplate.opsForSet()
                .remove(usersKey, userId.toString());

        if(removed != null && removed > 0) {
            redisTemplate.opsForValue().increment(stockKey);
        }
    }
}

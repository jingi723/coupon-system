package com.assignment.coupon_system.coupon.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

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

    private static final DefaultRedisScript<Long> RESERVE_SCRIPT =

            new DefaultRedisScript<>("""
                    -- KEYS[1]: 쿠폰 재고 (String)
                    -- KEYS[2]: 신청한 사용자 (Set)
                    -- KeyS[3]: requestId별 예약 (Hash)
                    -- KEYS[4]: Kafka 전달 대기 목록 (List)
                    
                    -- ARGV[1]: requestId
                    -- ARGV[2]: couponId
                    -- ARGV[3]: userId
                    -- ARGV[4]: reservedAt
                    
                    -- 쓰기 전에 키자료형을 검사한다.
                    
                    local expectedTypes = {'string', 'set', 'hash', 'list', 'hash'}
                    
                    for i = 1, 5 do
                        local actualType = redis.call('TYPE', KEYS[i]).ok
                        
                        if actualType ~= 'none'
                            and actualType ~= expectedTypes[i] then
                            return redis.error_reply('Unexpected key type: ' .. KEYS[i])
                        end
                    end
                    
                    -- 1. 같은 requestId의 기존 예약 확인
                    if redis.call('EXISTS', KEYS[3]) == 1 then 
                        local couponId = redis.call('HGET', KEYS[3], 'couponId')
                        local userId = redis.call('HGET', KEYS[3], 'userId')
                        
                        if couponId == ARGV[2] and userId == ARGV[3] then
                            return 2
                        end
                        
                        return -3
                    end
                    
                    -- 신규 신청에만 예약 정책을 정용한다.
                    if redis.call('EXISTS', KEYS[5]) == 0 then
                        return -5
                    end
                    
                    local status = redis.call('HGET', KEYS[5], 'status')
                    local startAt = tonumber(redis.call('HGET', KEYS[5], 'startAtMillis'))
                    local endAt = tonumber(redis.call('HGET', KEYS[5], 'endAtMillis'))
                    
                    local policyVersion = redis.call('HGET', KEYS[5], 'version')
                    
                    if not policyVersion
                        or not string.match(policyVersion, '^[1-9]%d*$')
                        or tonumber(policyVersion) > 2147483647 then
                        return redis.error_reply('Invalid reservation policy version')
                    end
                    
                    if not status or not startAt or not endAt then
                        return redis.error_reply('Missing or invalid reservation policy')
                    end
                    
                    if startAt ~= math.floor(startAt)
                        or endAt ~= math.floor(endAt)
                        or startAt >= endAt then
                        return redis.error_reply('Invalid reservation period')
                    end
                    
                    if status ~= 'ACTIVE' then
                        return -6
                    end
                    
                    -- Redis 서버의 현재 시각: 초 + 마이크로초
                    local currentTime = redis.call('TIME')
                    local nowMillis =
                        tonumber(currentTime[1]) * 1000
                        + math.floor(tonumber(currentTime[2]) / 1000)
                    
                    -- 시작 시각 이상, 종료 시각 미만
                    if nowMillis < startAt or nowMillis >= endAt then
                        return -6
                    end
                    
                    -- 2. 다른 requestId로 같은 사용자가 신청했는지 확인
                    if redis.call('SISMEMBER', KEYS[2], ARGV[3]) == 1 then
                        return -1
                    end
                    
                    -- 3. 재고 확인
                    local rawStock = redis.call('GET', KEYS[1])
                    
                    if not rawStock then 
                        return -4
                    end
                    
                    local stock = tonumber(rawStock)
                    
                    if stock == nil or stock ~= math.floor(stock) then
                        return redis.error_reply('Invalid stock value')
                    end
                    
                    if stock <= 0 then
                        return -2
                    end
                    
                    -- 4. 재고 차감과 사용자 등록
                    redis.call('DECR', KEYS[1])
                    redis.call('SADD', KEYS[2], ARGV[3])
                    
                    -- 5. 예약 내용 저장
                    redis.call('HSET', KEYS[3],
                        'requestId', ARGV[1],
                        'couponId', ARGV[2],
                        'userId', ARGV[3],
                        'reservedAtMillis', string.format('%.0f', nowMillis),
                        'policyVersion', policyVersion
                    )
                    
                    -- 6. Kafka 전달 대기 목록에 등록
                    redis.call('RPUSH', KEYS[4], ARGV[1])
                    
                    return 1
                    """, Long.class);

    private static final DefaultRedisScript<String> CLAIM_PENDING_SCRIPT =
            new DefaultRedisScript<>("""
                    return redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
                    """, String.class);

    private static final DefaultRedisScript<Long> INITIALIZE_RESERVATION_SCRIPT =
            new DefaultRedisScript<>("""
                    -- KEYS[1]: 예약 정책 Hash
                    -- KEYS[2]: 예약 가능한 재고
                    -- KEYS[3]: 신청 사용자 SET
                    
                    -- ARGV[1]: 초기재고
                    -- ARGV[2]: 시작 시각(epoch milliseconds)
                    -- ARGV[3]: 종료 시각(epoch milliseconds
                    
                    -- 기존 데이터가 있으면 초기화하지 않는다.
                    if redis.call('EXISTS', KEYS[1], KEYS[2], KEYS[3]) > 0 then
                        return 0
                    end
                    
                    redis.call('HSET', KEYS[1],
                        'status', 'ACTIVE',
                        'startAtMillis', ARGV[2],
                        'endAtMillis', ARGV[3],
                        'version', '1'
                    )
                    
                    redis.call('SET', KEYS[2], ARGV[1])
                    
                    return 1
                    """, Long.class);

    public String claimPendingReservation() {
        return redisTemplate.execute(
                CLAIM_PENDING_SCRIPT,
                List.of(
                        CouponRedisKey.pendingReservations(),
                        CouponRedisKey.processingReservations()
                )
        );
    }

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

    public CouponReservationStatus reserve(
            String requestId,
            Long couponId,
            Long userId
    ) {
        if (requestId == null || couponId == null || userId == null) {
            throw new IllegalArgumentException("예약 요청 값은 필수입니다.");
        }

        if (couponId <= 0 || userId <=  0) {
            throw new IllegalArgumentException("쿠폰 ID와 사용자 ID는 양수여야 합니다.");
        }

        String nomalizedRequestId = UUID.fromString(requestId).toString();

        Long result = redisTemplate.execute(
                RESERVE_SCRIPT,
                List.of(
                        CouponRedisKey.stock(couponId),
                        CouponRedisKey.issueUsers(couponId),
                        CouponRedisKey.reservation(nomalizedRequestId),
                        CouponRedisKey.pendingReservations(),
                        CouponRedisKey.reservationPolicy(couponId)
                ),
                nomalizedRequestId,
                couponId.toString(),
                userId.toString()
        );

        if (result == null) {
            throw new IllegalStateException("Redis 예약 결과가 없습니다.");
        }

        if(result == 1L) return CouponReservationStatus.RESERVED;
        if(result == 2L) return CouponReservationStatus.ALREADY_RESERVED;
        if(result == -1L) return CouponReservationStatus.DUPLICATE_USER;
        if(result == -2L) return CouponReservationStatus.SOLD_OUT;
        if(result == -3L) return CouponReservationStatus.REQUEST_CONFLICT;

        if(result == -4L) {
            throw new IllegalStateException(
                    "Redis 재고가 초기화되지 않았습니다. couponId=" + couponId
            );
        }
        if(result == -5L) {
            throw new IllegalStateException(
                    "Redis 예약 정책이 초기화되지 않았습니다. couponId=" + couponId
            );
        }
        if(result == -6L) {
            return CouponReservationStatus.NOT_AVAILABLE;
        }

            throw new IllegalStateException("알 수 없는 Redis 예약 결과: " + result);
    }

    // Consumer Relay: 예약이 존재해야 한다.
    public CouponReservation findReservation(String requestId) {
        return findReservationIfPresent(requestId)
                .orElseThrow(()-> new IllegalStateException(
                        "예약 정보가 없습니다. requestId=" + requestId
                ));
    }

    public Optional<CouponReservation> findReservationIfPresent(
            String requestId
    ) {
        String key = CouponRedisKey.reservation(requestId);

        Map<Object, Object> values =
                redisTemplate.opsForHash().entries(key);

        if(values.isEmpty()) {
            return Optional.empty();
        }

        for(String field : List.of(
                "requestId",
                "couponId",
                "userId",
                "reservedAtMillis",
                "policyVersion"
        )) {
            if(!values.containsKey(field)) {
                throw new IllegalStateException(
                        "예약 필드가 누락됐습니다. requestId="
                            + requestId + ", field=" + field
                );
            }
        }

        String storeRequestId = (String) values.get("requestId");

        if(!requestId.equals(storeRequestId)) {
            throw new IllegalStateException(
                    "예약 requestId가 일치하지 않습니다. requestId=" + requestId
            );
        }

        try {
            CouponReservation reservation = new CouponReservation(
                    storeRequestId,
                    Long.parseLong((String) values.get("couponId")),
                    Long.parseLong((String) values.get("userId")),
                    Instant.ofEpochMilli(
                            Long.parseLong((String) values.get("reservedAtMillis"))
                    ),
                    Long.parseLong((String) values.get("policyVersion"))
            );

            return Optional.of(reservation);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "예약 숫자 필드가 잘못됐습니다. requestId=" + requestId, e
            );
        }
    }

//    public CouponReservation findReservation(String requestId) {
//        String key = CouponRedisKey.reservation(requestId);
//
//        Map<Object, Object> values = redisTemplate.opsForHash().entries(key);
//
//        if(values.isEmpty()) {
//            throw new IllegalStateException(
//                    "예약 정보가 없습니다. requestId=" + requestId
//            );
//        }
//
//        for (String field : List.of(
//                "requestId", "couponId", "userId", "reservedAtMillis", "policyVersion"
//        )) {
//            if(!values.containsKey(field)) {
//                throw new IllegalStateException(
//                        "예약 필드가 누락됐습니다. requestId=" + requestId + ", field=" + field
//                );
//            }
//        }
//
//        String storedRequestId = (String) values.get("requestId");
//
//        if(!requestId.equals(storedRequestId)) {
//            throw new IllegalStateException(
//                    "예약 requestId가 일치하지 않습니다. requestId=" + requestId
//            );
//        }
//
//        return new CouponReservation(
//                storedRequestId,
//                Long.parseLong((String) values.get("couponId")),
//                Long.parseLong((String) values.get("userId")),
//                Instant.ofEpochMilli(
//                            Long.parseLong((String) values.get("reservedAtMillis"))
//                ),
//                Long.parseLong((String) values.get("policyVersion"))
//        );
//    }

    public void completeDelivery(String requestId) {
        if(requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId는 필수입니다.");
        }

        Long removed = redisTemplate.opsForList().remove(
                CouponRedisKey.processingReservations(),
                1,
                requestId
        );

        if(removed == null) {
            throw new IllegalStateException(
                    "처리 결과가 없습니다.requestId=" + requestId
            );
        }
    }

    public String peekProcessingReservation() {
        return redisTemplate.opsForList().index(
                CouponRedisKey.processingReservations(),
                0
        );
    }

    public boolean initializeReservation(
            Long couponId,
            int totalQuantity,
            Instant startAt,
            Instant endAt
    ) {
        if(couponId == null || couponId <= 0) {
            throw new IllegalArgumentException("양수인 couponId가 필요합니다.");
        }

        if(totalQuantity <= 0) {
            throw new IllegalArgumentException("초기 재고는 양수여야 합니다.");
        }

        if(startAt == null || endAt == null || !startAt.isBefore((endAt))) {
            throw new IllegalArgumentException("시작 시각은 종료시각보다 빨라야 합니다.");
        }

        long startMillis = startAt.toEpochMilli();
        long endMillis = endAt.toEpochMilli();

        if(startMillis >= endMillis) {
            throw new IllegalArgumentException("밀리초 기준으로 시작 시각과 종료 시각을 구분해야 합니다.");
        }

        Long result = redisTemplate.execute(
                INITIALIZE_RESERVATION_SCRIPT,
                List.of(
                        CouponRedisKey.reservationPolicy(couponId),
                        CouponRedisKey.stock(couponId),
                        CouponRedisKey.issueUsers(couponId)
                ),
                Integer.toString(totalQuantity),
                Long.toString(startMillis),
                Long.toString(endMillis)
        );

        if(result == null) {
            throw new IllegalStateException("Redis 초기화 결과가 없습니다.");
        }

        if(result == 1L) {
            return true;
        }

        if(result == 0L) {
            return false;
        }

        throw new IllegalStateException("알 수 없는 초기화 결과: " + result);
    }
}

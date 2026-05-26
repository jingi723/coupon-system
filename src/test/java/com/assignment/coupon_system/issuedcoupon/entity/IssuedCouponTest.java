package com.assignment.coupon_system.issuedcoupon.entity;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * TDD - IssuedCoupon 엔티티 단위 테스트
 *
 * [BUG] issue() 메서드의 expiredAt 계산 시 this.issuedAt(null) 참조로 NPE 발생
 *       → issuedCoupon.issuedAt.plusDays(...) 으로 수정 필요
 *
 * [RED] use() 메서드 미구현 → IssuedCoupon에 use() 추가 필요
 */
@DisplayName("IssuedCoupon 엔티티 단위 테스트")
class IssuedCouponTest {

    private static final int VALID_DAYS = 7;

    private Coupon createCoupon() {
        return Coupon.create(
                "테스트 쿠폰",
                "테스트 쿠폰",
                CouponType.FIXED_AMOUNT,
                1000,
                5000,
                100,
                VALID_DAYS,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30)
        );
    }

    // ───────── 버그 검출 테스트 (현재 NPE로 실패) ─────────

    @Test
    @DisplayName("[BUG] issue() 호출 시 NPE 없이 IssuedCoupon이 생성되어야 한다")
    void issue_shouldNotThrowNullPointerException() {
        Coupon coupon = createCoupon();

        // 현재 코드: issuedAt.plusDays(...) 에서 this.issuedAt = null → NPE
        // 수정 방법: issuedCoupon.issuedAt.plusDays(...) 으로 변경
        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, 1L);

        assertThat(issuedCoupon).isNotNull();
    }

    @Test
    @DisplayName("[BUG] issue() 호출 시 쿠폰, userId, ISSUED 상태, issuedAt이 올바르게 설정된다")
    void issue_setsFieldsCorrectly() {
        Coupon coupon = createCoupon();
        Long userId = 42L;

        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, userId);

        assertThat(issuedCoupon.getCoupon()).isEqualTo(coupon);
        assertThat(issuedCoupon.getUserId()).isEqualTo(userId);
        assertThat(issuedCoupon.getStatus()).isEqualTo(IssuedCouponStatus.ISSUED);
        assertThat(issuedCoupon.getIssuedAt()).isNotNull();
        assertThat(issuedCoupon.getUsedAt()).isNull();
    }

    @Test
    @DisplayName("[BUG] issue() 호출 시 expiredAt = issuedAt + validDays로 설정된다")
    void issue_setsExpiredAtFromIssuedAt() {
        Coupon coupon = createCoupon();

        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, 1L);

        assertThat(issuedCoupon.getExpiredAt())
                .isCloseTo(
                        issuedCoupon.getIssuedAt().plusDays(VALID_DAYS),
                        within(1, ChronoUnit.SECONDS)
                );
    }

    // ───────── 구현 필요 (RED) ─────────
    // 아래 테스트는 IssuedCoupon에 use() 메서드 추가 후 동작합니다.

    @Test
    @DisplayName("[RED] use() 호출 시 상태가 USED로 변경되고 usedAt이 기록된다")
    void use_changesStatusToUsedAndSetsUsedAt() {
        Coupon coupon = createCoupon();
        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, 1L);

        issuedCoupon.use();

        assertThat(issuedCoupon.getStatus()).isEqualTo(IssuedCouponStatus.USED);
        assertThat(issuedCoupon.getUsedAt()).isNotNull();
        assertThat(issuedCoupon.getUsedAt()).isBeforeOrEqualTo(LocalDateTime.now());
    }
}

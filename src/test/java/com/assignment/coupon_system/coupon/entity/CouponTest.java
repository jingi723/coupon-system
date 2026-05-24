package com.assignment.coupon_system.coupon.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD - Coupon 엔티티 단위 테스트
 *
 * [RED] isAvailable(), isExhausted(), markAsExhausted() 은 아직 미구현
 *       → Coupon 엔티티에 해당 메서드를 추가해서 GREEN으로 만드세요.
 */
@DisplayName("Coupon 엔티티 단위 테스트")
class CouponTest {

    private Coupon createCoupon(int totalQuantity, LocalDateTime start, LocalDateTime end) {
        return new Coupon(
                "10% 할인 쿠폰",
                CouponType.PERCENTAGE,
                1000,
                5000,
                totalQuantity,
                30,
                start,
                end
        );
    }

    // ───────── 기존 메서드 (GREEN) ─────────

    @Test
    @DisplayName("쿠폰 생성 시 초기 상태는 ACTIVE, 발급 수량은 0이다")
    void createCoupon_initialState() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));

        assertThat(coupon.getStatus()).isEqualTo(Status.ACTIVE);
        assertThat(coupon.getIssuedQuantity()).isZero();
    }

    @Test
    @DisplayName("increaseIssuedQuantity() 호출마다 발급 수량이 1씩 증가한다")
    void increaseIssuedQuantity_incrementsByOne() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));

        coupon.increaseIssuedQuantity();
        coupon.increaseIssuedQuantity();

        assertThat(coupon.getIssuedQuantity()).isEqualTo(2);
    }

    @Test
    @DisplayName("resetIssuedQuantity() 호출 시 발급 수량이 0으로 초기화되고 상태가 ACTIVE로 변경된다")
    void resetIssuedQuantity_resetsToZeroAndActive() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));
        coupon.increaseIssuedQuantity();

        coupon.resetIssuedQuantity();

        assertThat(coupon.getIssuedQuantity()).isZero();
        assertThat(coupon.getStatus()).isEqualTo(Status.ACTIVE);
    }

    // ───────── 구현 필요 (RED) ─────────
    // 아래 테스트들은 Coupon에 isAvailable(), isExhausted(), markAsExhausted() 추가 후 동작합니다.

    @Test
    @DisplayName("[RED] isAvailable() - ACTIVE 상태이고 발급 기간 내이면 true")
    void isAvailable_whenActiveAndWithinPeriod_returnsTrue() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));

        assertThat(coupon.isAvailable()).isTrue();
    }

    @Test
    @DisplayName("[RED] isAvailable() - 발급 시작 전이면 false")
    void isAvailable_whenBeforeStartDate_returnsFalse() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().plusDays(1),
                LocalDateTime.now().plusDays(30));

        assertThat(coupon.isAvailable()).isFalse();
    }

    @Test
    @DisplayName("[RED] isAvailable() - 발급 종료 후이면 false")
    void isAvailable_whenAfterEndDate_returnsFalse() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(30),
                LocalDateTime.now().minusDays(1));

        assertThat(coupon.isAvailable()).isFalse();
    }

    @Test
    @DisplayName("[RED] isAvailable() - EXHAUSTED 상태이면 false")
    void isAvailable_whenExhausted_returnsFalse() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));
        coupon.markAsExhausted();

        assertThat(coupon.isAvailable()).isFalse();
    }

    @Test
    @DisplayName("[RED] isExhausted() - 발급 수량이 총 수량보다 적으면 false")
    void isExhausted_whenNotFull_returnsFalse() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));
        coupon.increaseIssuedQuantity();

        assertThat(coupon.isExhausted()).isFalse();
    }

    @Test
    @DisplayName("[RED] isExhausted() - 발급 수량이 총 수량에 도달하면 true")
    void isExhausted_whenFull_returnsTrue() {
        Coupon coupon = createCoupon(1,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));
        coupon.increaseIssuedQuantity();

        assertThat(coupon.isExhausted()).isTrue();
    }

    @Test
    @DisplayName("[RED] markAsExhausted() 호출 시 상태가 EXHAUSTED로 변경된다")
    void markAsExhausted_changesStatusToExhausted() {
        Coupon coupon = createCoupon(100,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30));

        coupon.markAsExhausted();

        assertThat(coupon.getStatus()).isEqualTo(Status.EXHAUSTED);
    }
}

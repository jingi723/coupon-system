package com.assignment.coupon_system.coupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.entity.Status;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD - CouponRepository 통합 테스트 (@DataJpaTest + H2)
 *
 * [RED] findByStatusAndStartDateTimeLessThanAndEndDateTimeGreaterThan() 미구현
 *       → CouponRepository에 해당 메서드 추가 필요
 */
@DataJpaTest
@DisplayName("CouponRepository 테스트")
class CouponRepositoryTest {

    @Autowired
    private CouponRepository couponRepository;

    private Coupon saveCoupon(Status status, LocalDateTime start, LocalDateTime end) {
        Coupon coupon = new Coupon(
                "테스트 쿠폰",
                CouponType.FIXED_AMOUNT,
                1000,
                5000,
                100,
                30,
                start,
                end
        );
        Coupon saved = couponRepository.save(coupon);
        if (status == Status.EXHAUSTED) {
            saved.markAsExhausted(); // RED: markAsExhausted() 미구현 시 컴파일 오류
            couponRepository.save(saved);
        }
        return saved;
    }

    // ───────── 기존 메서드 (GREEN) ─────────

    @Test
    @DisplayName("저장한 쿠폰을 ID로 조회할 수 있다")
    void saveAndFindById() {
        Coupon coupon = new Coupon(
                "조회 테스트 쿠폰",
                CouponType.PERCENTAGE,
                500,
                3000,
                50,
                7,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(10)
        );

        Coupon saved = couponRepository.save(coupon);
        Optional<Coupon> found = couponRepository.findById(saved.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getDescription()).isEqualTo("조회 테스트 쿠폰");
        assertThat(found.get().getStatus()).isEqualTo(Status.ACTIVE);
    }

    @Test
    @DisplayName("존재하지 않는 ID로 조회하면 empty를 반환한다")
    void findById_whenNotExists_returnsEmpty() {
        Optional<Coupon> found = couponRepository.findById(9999L);

        assertThat(found).isEmpty();
    }

    // ───────── 구현 필요 (RED) ─────────
    // 아래 테스트는 CouponRepository에 findByStatusAndStartDateTimeLessThanAndEndDateTimeGreaterThan() 추가 필요

    @Test
    @DisplayName("[RED] 현재 발급 가능한(ACTIVE + 기간 내) 쿠폰만 조회된다")
    void findAvailableCoupons_returnsOnlyActiveAndWithinPeriod() {
        LocalDateTime now = LocalDateTime.now();
        saveCoupon(Status.ACTIVE, now.minusDays(1), now.plusDays(30));  // 발급 가능
        saveCoupon(Status.ACTIVE, now.plusDays(1), now.plusDays(30));   // 시작 전 → 제외
        saveCoupon(Status.ACTIVE, now.minusDays(30), now.minusDays(1)); // 종료 후 → 제외

        List<Coupon> available = couponRepository
                .findByStatusAndStartDateTimeLessThanAndEndDateTimeGreaterThan(
                        Status.ACTIVE, now, now);

        assertThat(available).hasSize(1);
        assertThat(available.get(0).getStatus()).isEqualTo(Status.ACTIVE);
    }

    @Test
    @DisplayName("[RED] ACTIVE 상태의 쿠폰이 없으면 빈 목록을 반환한다")
    void findAvailableCoupons_whenNoneActive_returnsEmptyList() {
        LocalDateTime now = LocalDateTime.now();

        List<Coupon> available = couponRepository
                .findByStatusAndStartDateTimeLessThanAndEndDateTimeGreaterThan(
                        Status.ACTIVE, now, now);

        assertThat(available).isEmpty();
    }
}

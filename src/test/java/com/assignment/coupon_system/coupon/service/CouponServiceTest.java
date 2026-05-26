package com.assignment.coupon_system.coupon.service;

import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.entity.CouponStatus;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@DisplayName("CouponService 단위 테스트")
class CouponServiceTest {

    @Mock
    private CouponRepository couponRepository;

    @InjectMocks
    private CouponService couponService;

    private Coupon activeCoupon(int total) {
        return Coupon.create(
                "테스트 쿠폰",
                "테스트 쿠폰",
                CouponType.FIXED_AMOUNT,
                1000,
                5000,
                total,
                30,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30)
        );
    }

    // ───────── getAvailableCoupons ─────────

    @Test
    @DisplayName("getAvailableCoupons() - 현재 발급 가능한 쿠폰 목록을 반환한다")
    void getAvailableCoupons_returnsActiveCoupons() {
        Coupon coupon = activeCoupon(100);
        given(couponRepository.findByStatusAndStartDateLessThanAndEndDateGreaterThan(
                any(CouponStatus.class), any(LocalDateTime.class), any(LocalDateTime.class)))
                .willReturn(List.of(coupon));

        List<CouponResponse> result = couponService.getAvailableCoupons();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo(CouponStatus.ACTIVE);
    }

    @Test
    @DisplayName("getAvailableCoupons() - 발급 가능한 쿠폰이 없으면 빈 목록을 반환한다")
    void getAvailableCoupons_whenNone_returnsEmpty() {
        given(couponRepository.findByStatusAndStartDateLessThanAndEndDateGreaterThan(
                any(CouponStatus.class), any(LocalDateTime.class), any(LocalDateTime.class)))
                .willReturn(List.of());

        List<CouponResponse> result = couponService.getAvailableCoupons();

        assertThat(result).isEmpty();
    }

    // ───────── getCouponStock ─────────

    @Test
    @DisplayName("getCouponStock() - 잔여 수량(totalQuantity - issuedQuantity)을 반환한다")
    void getCouponStock_returnsRemainingCount() {
        Long couponId = 1L;
        Coupon coupon = activeCoupon(100);
        coupon.increaseIssuedQuantity();
        coupon.increaseIssuedQuantity();

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));

        Integer remaining = couponService.getCouponStock(couponId);

        assertThat(remaining).isEqualTo(98);
    }

    @Test
    @DisplayName("getCouponStock() - 존재하지 않는 쿠폰이면 CouponNotFoundException 발생")
    void getCouponStock_whenNotFound_throwsCouponNotFoundException() {
        given(couponRepository.findById(999L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> couponService.getCouponStock(999L))
                .isInstanceOf(CouponNotFoundException.class);
    }

    // ───────── initStock ─────────

    @Test
    @DisplayName("initStock() - 발급 수량을 0으로 초기화하고 총 수량을 반환한다")
    void initStock_resetsIssuedQuantityAndReturnsTotalQuantity() {
        Long couponId = 1L;
        Coupon coupon = activeCoupon(100);
        coupon.increaseIssuedQuantity();

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));

        Integer result = couponService.initStock(couponId);

        assertThat(result).isEqualTo(100);
        assertThat(coupon.getIssuedQuantity()).isZero();
    }

    @Test
    @DisplayName("initStock() - 존재하지 않는 쿠폰이면 CouponNotFoundException 발생")
    void initStock_whenNotFound_throwsCouponNotFoundException() {
        given(couponRepository.findById(999L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> couponService.initStock(999L))
                .isInstanceOf(CouponNotFoundException.class);
    }
}

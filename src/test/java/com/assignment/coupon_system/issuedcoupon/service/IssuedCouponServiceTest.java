package com.assignment.coupon_system.issuedcoupon.service;

import com.assignment.coupon_system.common.exception.CouponExhaustedException;
import com.assignment.coupon_system.common.exception.CouponNotAvailableException;
import com.assignment.coupon_system.common.exception.CouponNotFoundException;
import com.assignment.coupon_system.common.exception.DuplicateCouponIssueException;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.dto.IssueCouponRequest;
import com.assignment.coupon_system.issuedcoupon.dto.IssuedCouponResponse;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
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
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
@DisplayName("IssuedCouponService 단위 테스트")
class IssuedCouponServiceTest {

    @Mock
    private IssuedCouponRepository issuedCouponRepository;

    @Mock
    private CouponRepository couponRepository;

    @InjectMocks
    private IssuedCouponService issuedCouponService;

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

    // ───────── issueCoupon ─────────

    @Test
    @DisplayName("issueCoupon() - 정상 발급 시 IssuedCouponResponse를 반환한다")
    void issueCoupon_success() {
        Long couponId = 1L;
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, userId);
        IssueCouponRequest request = new IssueCouponRequest(userId);

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));
        given(issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId)).willReturn(false);
        given(issuedCouponRepository.save(any())).willReturn(issuedCoupon);

        IssuedCouponResponse response = issuedCouponService.issueCoupon(couponId, request);

        assertThat(response).isNotNull();
        assertThat(response.getUserId()).isEqualTo(userId);
        assertThat(response.getStatus()).isEqualTo(IssuedCouponStatus.ISSUED);
    }

    @Test
    @DisplayName("issueCoupon() - 존재하지 않는 쿠폰이면 CouponNotFoundException 발생")
    void issueCoupon_whenCouponNotFound_throwsCouponNotFoundException() {
        Long couponId = 999L;
        given(couponRepository.findById(couponId)).willReturn(Optional.empty());

        assertThatThrownBy(() -> issuedCouponService.issueCoupon(couponId, new IssueCouponRequest(1L)))
                .isInstanceOf(CouponNotFoundException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    @Test
    @DisplayName("issueCoupon() - 발급 기간이 아니면 CouponNotAvailableException 발생")
    void issueCoupon_whenNotAvailablePeriod_throwsCouponNotAvailableException() {
        Long couponId = 1L;
        Coupon expiredCoupon = Coupon.create(
                "만료 쿠폰", "만료 쿠폰", CouponType.FIXED_AMOUNT, 1000, 5000, 100, 30,
                LocalDateTime.now().minusDays(30),
                LocalDateTime.now().minusDays(1)
        );
        given(couponRepository.findById(couponId)).willReturn(Optional.of(expiredCoupon));

        assertThatThrownBy(() -> issuedCouponService.issueCoupon(couponId, new IssueCouponRequest(1L)))
                .isInstanceOf(CouponNotAvailableException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    @Test
    @DisplayName("issueCoupon() - 재고 소진 시 CouponExhaustedException 발생")
    void issueCoupon_whenExhausted_throwsCouponExhaustedException() {
        Long couponId = 1L;
        Long userId = 1L;
        Coupon coupon = activeCoupon(1);
        coupon.increaseIssuedQuantity(); // issuedQuantity == totalQuantity → 소진

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));
        given(issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId)).willReturn(false);

        assertThatThrownBy(() -> issuedCouponService.issueCoupon(couponId, new IssueCouponRequest(userId)))
                .isInstanceOf(CouponExhaustedException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    @Test
    @DisplayName("issueCoupon() - 이미 발급받은 사용자면 DuplicateCouponIssueException 발생")
    void issueCoupon_whenDuplicate_throwsDuplicateCouponIssueException() {
        Long couponId = 1L;
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));
        given(issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId)).willReturn(true);

        assertThatThrownBy(() -> issuedCouponService.issueCoupon(couponId, new IssueCouponRequest(userId)))
                .isInstanceOf(DuplicateCouponIssueException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    // ───────── getUserCoupons ─────────

    @Test
    @DisplayName("getUserCoupons() - 사용자가 보유한 모든 쿠폰 목록을 반환한다")
    void getUserCoupons_returnsAllCouponsForUser() {
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon ic1 = IssuedCoupon.issue(coupon, userId);
        IssuedCoupon ic2 = IssuedCoupon.issue(coupon, userId);

        given(issuedCouponRepository.findByUserId(userId)).willReturn(List.of(ic1, ic2));

        List<IssuedCouponResponse> result = issuedCouponService.getUserCoupons(userId);

        assertThat(result).hasSize(2);
    }

    // ───────── getUsableCoupons ─────────

    @Test
    @DisplayName("getUsableCoupons() - ISSUED 상태의 쿠폰만 반환한다")
    void getUsableCoupons_returnsOnlyIssuedStatus() {
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon issuedCoupon = IssuedCoupon.issue(coupon, userId);

        given(issuedCouponRepository.findByUserIdAndStatus(userId, IssuedCouponStatus.ISSUED))
                .willReturn(List.of(issuedCoupon));

        List<IssuedCouponResponse> result = issuedCouponService.getUsableCoupons(userId);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo(IssuedCouponStatus.ISSUED);
    }
}

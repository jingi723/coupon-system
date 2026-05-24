package com.assignment.coupon_system.coupon.service;

import com.assignment.coupon_system.coupon.dto.CouponResponse;
import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.entity.Status;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.exception.CouponExhaustedException;
import com.assignment.coupon_system.exception.CouponNotAvailableException;
import com.assignment.coupon_system.exception.CouponNotFoundException;
import com.assignment.coupon_system.exception.DuplicateCouponIssueException;
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

/**
 * TDD - CouponService 단위 테스트 (Mockito)
 *
 * [RED] CouponService, 예외 클래스들 미구현 → 컴파일 오류
 *
 * 구현해야 할 클래스:
 *   - com.assignment.coupon_system.coupon.service.CouponService
 *   - com.assignment.coupon_system.exception.CouponNotFoundException
 *   - com.assignment.coupon_system.exception.CouponExhaustedException
 *   - com.assignment.coupon_system.exception.DuplicateCouponIssueException
 *   - com.assignment.coupon_system.exception.CouponNotAvailableException
 *
 * CouponService가 가져야 할 메서드:
 *   - List<CouponResponse>   getAvailableCoupons()
 *   - IssuedCouponResponse   issueCoupon(Long couponId, Long userId)
 *   - Integer                getRemainingStock(Long couponId)
 *   - List<IssuedCouponResponse> getUserCoupons(Long userId)
 *   - List<IssuedCouponResponse> getUsableCoupons(Long userId)
 *   - IssuedCouponResponse   useCoupon(Long issuedCouponId)
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("CouponService 단위 테스트")
class CouponServiceTest {

    @Mock
    private CouponRepository couponRepository;

    @Mock
    private IssuedCouponRepository issuedCouponRepository;

    @InjectMocks
    private CouponService couponService;

    // ───────── 헬퍼 ─────────

    private Coupon activeCoupon(int total) {
        return new Coupon(
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
        LocalDateTime now = LocalDateTime.now();
        Coupon coupon = activeCoupon(100);
        given(couponRepository.findByStatusAndStartDateTimeLessThanAndEndDateTimeGreaterThan(
                Status.ACTIVE, now, now))
                .willReturn(List.of(coupon));

        List<CouponResponse> result = couponService.getAvailableCoupons();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo(Status.ACTIVE);
    }

    // ───────── issueCoupon ─────────

    @Test
    @DisplayName("issueCoupon() - 정상 발급 시 IssuedCouponResponse를 반환하고 수량이 증가한다")
    void issueCoupon_success() {
        Long couponId = 1L;
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon issuedCoupon = new IssuedCoupon().issue(coupon, userId);

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));
        given(issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId)).willReturn(false);
        given(issuedCouponRepository.save(any())).willReturn(issuedCoupon);

        IssuedCouponResponse response = couponService.issueCoupon(couponId, userId);

        assertThat(response).isNotNull();
        assertThat(response.getUserId()).isEqualTo(userId);
        assertThat(response.getStatus()).isEqualTo(IssuedCouponStatus.ISSUED);
        then(couponRepository).should().save(coupon); // issuedQuantity 증가 후 저장
    }

    @Test
    @DisplayName("issueCoupon() - 존재하지 않는 쿠폰이면 CouponNotFoundException 발생")
    void issueCoupon_whenCouponNotFound_throwsCouponNotFoundException() {
        Long couponId = 999L;
        given(couponRepository.findById(couponId)).willReturn(Optional.empty());

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, 1L))
                .isInstanceOf(CouponNotFoundException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    @Test
    @DisplayName("issueCoupon() - 발급 기간이 아니면 CouponNotAvailableException 발생")
    void issueCoupon_whenNotAvailablePeriod_throwsCouponNotAvailableException() {
        Long couponId = 1L;
        // 종료된 쿠폰
        Coupon expiredCoupon = new Coupon(
                "만료 쿠폰", CouponType.FIXED_AMOUNT, 1000, 5000, 100, 30,
                LocalDateTime.now().minusDays(30),
                LocalDateTime.now().minusDays(1)
        );
        given(couponRepository.findById(couponId)).willReturn(Optional.of(expiredCoupon));

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, 1L))
                .isInstanceOf(CouponNotAvailableException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    @Test
    @DisplayName("issueCoupon() - 재고 소진 시 CouponExhaustedException 발생")
    void issueCoupon_whenExhausted_throwsCouponExhaustedException() {
        Long couponId = 1L;
        Coupon coupon = activeCoupon(1);
        coupon.increaseIssuedQuantity(); // totalQuantity == issuedQuantity → 소진

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));
        given(issuedCouponRepository.existsByCouponIdAndUserId(couponId, 1L)).willReturn(false);

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, 1L))
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

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, userId))
                .isInstanceOf(DuplicateCouponIssueException.class);

        then(issuedCouponRepository).should(never()).save(any());
    }

    // ───────── getRemainingStock ─────────

    @Test
    @DisplayName("getRemainingStock() - 잔여 수량(totalQuantity - issuedQuantity)을 반환한다")
    void getRemainingStock_returnsRemainingCount() {
        Long couponId = 1L;
        Coupon coupon = activeCoupon(100);
        coupon.increaseIssuedQuantity();
        coupon.increaseIssuedQuantity(); // issuedQuantity = 2

        given(couponRepository.findById(couponId)).willReturn(Optional.of(coupon));

        Integer remaining = couponService.getRemainingStock(couponId);

        assertThat(remaining).isEqualTo(98);
    }

    @Test
    @DisplayName("getRemainingStock() - 존재하지 않는 쿠폰이면 CouponNotFoundException 발생")
    void getRemainingStock_whenNotFound_throwsCouponNotFoundException() {
        given(couponRepository.findById(999L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> couponService.getRemainingStock(999L))
                .isInstanceOf(CouponNotFoundException.class);
    }

    // ───────── getUserCoupons ─────────

    @Test
    @DisplayName("getUserCoupons() - 사용자가 보유한 모든 쿠폰 목록을 반환한다")
    void getUserCoupons_returnsAllCouponsForUser() {
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon ic1 = new IssuedCoupon().issue(coupon, userId);
        IssuedCoupon ic2 = new IssuedCoupon().issue(coupon, userId);

        given(issuedCouponRepository.findByUserId(userId)).willReturn(List.of(ic1, ic2));

        List<IssuedCouponResponse> result = couponService.getUserCoupons(userId);

        assertThat(result).hasSize(2);
    }

    // ───────── getUsableCoupons ─────────

    @Test
    @DisplayName("getUsableCoupons() - ISSUED 상태의 쿠폰만 반환한다")
    void getUsableCoupons_returnsOnlyIssuedStatus() {
        Long userId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon issuedCoupon = new IssuedCoupon().issue(coupon, userId);

        given(issuedCouponRepository.findByUserIdAndStatus(userId, IssuedCouponStatus.ISSUED))
                .willReturn(List.of(issuedCoupon));

        List<IssuedCouponResponse> result = couponService.getUsableCoupons(userId);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo(IssuedCouponStatus.ISSUED);
    }

    // ───────── useCoupon ─────────

    @Test
    @DisplayName("useCoupon() - 정상 사용 시 상태가 USED로 변경된다")
    void useCoupon_success() {
        Long issuedCouponId = 1L;
        Coupon coupon = activeCoupon(100);
        IssuedCoupon issuedCoupon = new IssuedCoupon().issue(coupon, 1L);

        given(issuedCouponRepository.findById(issuedCouponId)).willReturn(Optional.of(issuedCoupon));
        given(issuedCouponRepository.save(any())).willReturn(issuedCoupon);

        IssuedCouponResponse response = couponService.useCoupon(issuedCouponId);

        assertThat(response.getStatus()).isEqualTo(IssuedCouponStatus.USED);
    }

    @Test
    @DisplayName("useCoupon() - 존재하지 않는 발급 쿠폰이면 CouponNotFoundException 발생")
    void useCoupon_whenNotFound_throwsCouponNotFoundException() {
        given(issuedCouponRepository.findById(999L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> couponService.useCoupon(999L))
                .isInstanceOf(CouponNotFoundException.class);
    }
}

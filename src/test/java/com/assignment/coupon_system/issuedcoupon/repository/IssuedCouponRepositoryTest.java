package com.assignment.coupon_system.issuedcoupon.repository;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCoupon;
import com.assignment.coupon_system.issuedcoupon.entity.IssuedCouponStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD - IssuedCouponRepository 통합 테스트 (@DataJpaTest + H2)
 *
 * [주의] issue() 버그 수정 후에 정상 동작합니다.
 *        IssuedCoupon.issue()의 this.issuedAt → issuedCoupon.issuedAt 수정 필요
 */
@DataJpaTest
@DisplayName("IssuedCouponRepository 테스트")
class IssuedCouponRepositoryTest {

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    @Autowired
    private CouponRepository couponRepository;

    private Coupon savedCoupon;

    @BeforeEach
    void setUp() {
        savedCoupon = couponRepository.save(new Coupon(
                "테스트 쿠폰",
                CouponType.FIXED_AMOUNT,
                1000,
                5000,
                100,
                30,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30)
        ));
    }

    private IssuedCoupon issueFor(Long userId) {
        return issuedCouponRepository.save(new IssuedCoupon().issue(savedCoupon, userId));
    }

    @Test
    @DisplayName("쿠폰을 발급받은 사용자면 existsByCouponIdAndUserId()가 true를 반환한다")
    void existsByCouponIdAndUserId_whenIssued_returnsTrue() {
        issueFor(1L);

        boolean exists = issuedCouponRepository.existsByCouponIdAndUserId(savedCoupon.getId(), 1L);

        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("쿠폰을 발급받지 않은 사용자면 existsByCouponIdAndUserId()가 false를 반환한다")
    void existsByCouponIdAndUserId_whenNotIssued_returnsFalse() {
        boolean exists = issuedCouponRepository.existsByCouponIdAndUserId(savedCoupon.getId(), 999L);

        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("동일 쿠폰을 다른 사용자가 발급받았을 때 해당 사용자는 false를 반환한다")
    void existsByCouponIdAndUserId_differentUser_returnsFalse() {
        issueFor(1L);

        boolean exists = issuedCouponRepository.existsByCouponIdAndUserId(savedCoupon.getId(), 2L);

        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("findByUserId() 는 해당 사용자의 모든 발급 쿠폰을 반환한다")
    void findByUserId_returnsAllIssuedCouponsForUser() {
        issueFor(1L);
        issueFor(1L); // 같은 사용자 다른 쿠폰 시나리오는 실제론 다른 쿠폰이어야 하지만 구조상 허용
        issueFor(2L);

        List<IssuedCoupon> result = issuedCouponRepository.findByUserId(1L);

        assertThat(result).hasSize(2);
        assertThat(result).allMatch(ic -> ic.getUserId().equals(1L));
    }

    @Test
    @DisplayName("findByUserId() 는 해당 사용자의 발급 쿠폰이 없으면 빈 목록을 반환한다")
    void findByUserId_whenNone_returnsEmpty() {
        List<IssuedCoupon> result = issuedCouponRepository.findByUserId(999L);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("findByUserIdAndStatus() 는 ISSUED 상태 쿠폰만 반환한다")
    void findByUserIdAndStatus_returnsOnlyMatchingStatus() {
        IssuedCoupon issued = issueFor(1L);

        List<IssuedCoupon> issuedList = issuedCouponRepository
                .findByUserIdAndStatus(1L, IssuedCouponStatus.ISSUED);
        List<IssuedCoupon> usedList = issuedCouponRepository
                .findByUserIdAndStatus(1L, IssuedCouponStatus.USED);

        assertThat(issuedList).hasSize(1);
        assertThat(usedList).isEmpty();
    }
}

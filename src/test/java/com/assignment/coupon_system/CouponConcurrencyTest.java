package com.assignment.coupon_system;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.coupon.service.CouponService;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD - 동시성 테스트 (과제 8번)
 *
 * [RED] CouponService 미구현 → 컴파일 오류
 *       동시성 제어 미구현 → 테스트 실패
 *
 * 실행 조건:
 *   - application-test.yaml 의 H2 설정으로 실행 가능
 *   - 실제 MySQL 동시성 검증은 Docker Compose 환경에서 수행
 *
 * 동시성 제어 구현 방법 (힌트):
 *   1. Redis SETNX + Lua Script
 *   2. JPA 비관적 락 (SELECT ... FOR UPDATE)
 *   3. JPA 낙관적 락 (@Version)
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("쿠폰 발급 동시성 테스트")
class CouponConcurrencyTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    @BeforeEach
    void setUp() {
        issuedCouponRepository.deleteAll();
        couponRepository.deleteAll();
    }

    @Test
    @DisplayName("1,000명이 동시에 요청해도 100개 쿠폰만 정확히 발급된다")
    void concurrentIssuance_exactQuantityIssued() throws InterruptedException {
        // given
        int totalQuantity = 100;
        int totalRequests = 1_000;
        Coupon coupon = couponRepository.save(new Coupon(
                "선착순 쿠폰", CouponType.FIXED_AMOUNT, 1000, 5000,
                totalQuantity, 30,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30)
        ));

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(50);

        // when
        for (int i = 0; i < totalRequests; i++) {
            final long userId = i + 1L;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    couponService.issueCoupon(coupon.getId(), userId);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        // then
        long issuedCount = issuedCouponRepository.count();
        assertThat(successCount.get())
                .as("정확히 100명만 성공해야 한다")
                .isEqualTo(totalQuantity);
        assertThat(issuedCount)
                .as("DB에 저장된 발급 쿠폰 수는 100개여야 한다")
                .isEqualTo(totalQuantity);
        assertThat(failCount.get())
                .as("나머지 900명은 실패해야 한다")
                .isEqualTo(totalRequests - totalQuantity);
    }

    @Test
    @DisplayName("동일 사용자가 100번 동시에 요청해도 정확히 1번만 발급된다")
    void concurrentIssuance_duplicatePreventionForSameUser() throws InterruptedException {
        // given
        int totalQuantity = 100;
        int totalRequests = 100;
        Long userId = 1L;
        Coupon coupon = couponRepository.save(new Coupon(
                "선착순 쿠폰", CouponType.FIXED_AMOUNT, 1000, 5000,
                totalQuantity, 30,
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().plusDays(30)
        ));

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(50);

        // when
        for (int i = 0; i < totalRequests; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    couponService.issueCoupon(coupon.getId(), userId);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // 중복 발급 예외 또는 재고 소진 예외 → 정상 흐름
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        // then
        long issuedCount = issuedCouponRepository.findByUserId(userId).size();
        assertThat(successCount.get())
                .as("동일 사용자는 단 1번만 성공해야 한다")
                .isEqualTo(1);
        assertThat(issuedCount)
                .as("DB에는 해당 사용자의 쿠폰이 1개만 존재해야 한다")
                .isEqualTo(1);
    }
}

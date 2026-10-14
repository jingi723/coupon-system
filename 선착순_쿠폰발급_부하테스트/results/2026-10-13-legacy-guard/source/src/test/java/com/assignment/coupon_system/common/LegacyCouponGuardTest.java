package com.assignment.coupon_system.common;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.coupon.service.CouponService;
import com.assignment.coupon_system.issuedcoupon.dto.IssueCouponRequest;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import com.assignment.coupon_system.issuedcoupon.service.IssuedCouponService;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class LegacyCouponGuardTest {
    @ParameterizedTest
    @ValueSource(strings = {"absent", "false", "true"})
    void configurationIsClosedByDefault(String setting) {
        var runner = new ApplicationContextRunner().withUserConfiguration(LegacyCouponGuard.class);
        if (!setting.equals("absent")) {
            runner = runner.withPropertyValues("coupon.legacy.enabled=" + setting);
        }
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var guard = context.getBean(LegacyCouponGuard.class);
            if (setting.equals("true")) {
                assertThatCode(guard::checkEnabled).doesNotThrowAnyException();
            } else {
                assertThatThrownBy(guard::checkEnabled).isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT));
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"init", "issue", "issueCoupon", "createIssued", "process"})
    void blockedServicesDoNotCallRepositoriesOrRedis(String path) {
        var coupons = mock(CouponRepository.class);
        var issues = mock(IssuedCouponRepository.class);
        var results = mock(CouponIssueResultRepository.class);
        var redis = mock(CouponIssueRedisService.class);
        var template = mock(StringRedisTemplate.class);
        var guard = new LegacyCouponGuard(false);
        var issueService = new IssuedCouponService(issues, coupons, redis, guard);
        var couponService = new CouponService(coupons, issueService, template, guard);
        var processor = new CouponIssueProcessor(coupons, issues, results, guard);
        org.assertj.core.api.ThrowableAssert.ThrowingCallable action = switch (path) {
            case "init" -> () -> couponService.initStock(1L);
            case "issue" -> () -> couponService.issue(1L, 10L);
            case "issueCoupon" -> () -> issueService.issueCoupon(1L, new IssueCouponRequest(10L));
            case "createIssued" -> () -> issueService.createIssued(1L, 10L);
            case "process" -> () -> processor.process("request", 1L, 10L);
            default -> throw new AssertionError(path);
        };
        assertThatThrownBy(action).isInstanceOfSatisfying(ResponseStatusException.class,
                e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT));
        verifyNoInteractions(coupons, issues, results, redis, template);
    }
}

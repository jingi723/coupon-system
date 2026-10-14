package com.assignment.coupon_system.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class LegacyCouponGuard {

    private final boolean enabled;

    public LegacyCouponGuard(
            @Value("${coupon.legacy.enabled:false}") boolean enabled
    ) {
        this.enabled = enabled;
    }

    public void checkEnabled() {
        if(!enabled) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "예약 방식에서는 기존 재초기화, 동기 발급을 사용할 수 없습니다."
            );
        }
    }
}

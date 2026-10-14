package com.assignment.coupon_system.issuerequest.relay;


import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "coupon.relay.enabled",
        havingValue = "true",
        matchIfMissing = false
)
public class CouponReservationRelayRunner implements ApplicationRunner {

    private final CouponReservationRelay relay;

    private volatile boolean running;
    private volatile boolean paused;

    private Thread worker;

    @Override
    public synchronized void run(ApplicationArguments args) {
        if(worker != null) {
            return;
        }

        running = true;
        worker = new Thread(this::runLoop, "coupon-reservation-relay");
        worker.start();
    }

    private void runLoop() {
        log.info("쿠폰 예약 전달 작업자를 시작합니다");

        try {
            while(running) {
                if(paused) {
                    Thread.sleep(200);
                    continue;
                }

                try {
                    boolean delivered = relay.relayOne();

                    if(!delivered) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    paused = true;

                    log.error("쿠폰 예약 전달을 일시 중단합니다. 원인 해결후 재개하거나 앱을 재시작하세요.", e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            log.info("쿠폰 예약 전달 작업자를 종료합니다.");
        }
    }

    public void resume() {
        if(!running) {
            throw new IllegalStateException("작업자가 실행 중이 아닙니다");
        }

        paused  = false;
        log.info("쿠폰 예약 전달을 재개합니다.");
    }

    public boolean isPaused() {
        return paused;
    }

    @PreDestroy
    public void shutdown() {
        running = false;

        if(worker == null) {
            return;
        }

        worker.interrupt();

        try {
            worker.join(10_000);

            if(worker.isAlive()) {
                log.warn("쿠폰 예약 전달 작업자가 종료 대기시간을 초과했습니다.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

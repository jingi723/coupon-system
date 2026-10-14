package com.assignment.coupon_system.issuerequest.relay;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@DisplayName("단일 Relay 반복 실행·중단·재개·종료")
class CouponReservationRelayRunnerTest {
    CouponReservationRelay relay;
    CouponReservationRelayRunner runner;
    static final DefaultApplicationArguments ARGS = new DefaultApplicationArguments(new String[0]);

    @BeforeEach
    void setUp() {
        relay = mock(CouponReservationRelay.class);
        runner = new CouponReservationRelayRunner(relay);
    }

    @AfterEach
    void stop() { runner.shutdown(); }

    @Test
    void continuesAfterEachSuccessfulDelivery() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch fourth = new CountDownLatch(1);
        doAnswer(invocation -> {
            int n = calls.incrementAndGet();
            if (n == 4) {
                fourth.countDown();
                new CountDownLatch(1).await();
            }
            return true;
        }).when(relay).relayOne();
        runner.run(ARGS);
        assertThat(fourth.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(calls.get()).isEqualTo(4);
        assertThat(runner.isPaused()).isFalse();
    }

    @Test
    void emptyQueueWaitsBeforePollingAgain() throws Exception {
        List<Long> starts = new CopyOnWriteArrayList<>();
        CountDownLatch third = new CountDownLatch(1);
        doAnswer(invocation -> {
            starts.add(System.nanoTime());
            if (starts.size() == 3) {
                third.countDown();
                new CountDownLatch(1).await();
            }
            return false;
        }).when(relay).relayOne();
        runner.run(ARGS);
        assertThat(third.await(3, TimeUnit.SECONDS)).isTrue();
        // Generous lower bound for the configured 100ms idle delay; no upper latency assertion.
        assertThat(starts.get(1)-starts.get(0)).isGreaterThanOrEqualTo(80_000_000L);
        assertThat(starts.get(2)-starts.get(1)).isGreaterThanOrEqualTo(80_000_000L);
    }

    @Test
    void failurePausesUntilExplicitResumeAndShutdownStopsPausedWorker() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Thread> worker = new AtomicReference<>();
        doAnswer(invocation -> {
            worker.set(Thread.currentThread());
            calls.incrementAndGet();
            throw new TimeoutException("injected send timeout");
        }).when(relay).relayOne();
        runner.run(ARGS);
        await().atMost(Duration.ofSeconds(3)).until(runner::isPaused);
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(calls.get()).isEqualTo(1));
        runner.resume();
        await().atMost(Duration.ofSeconds(3)).until(() -> calls.get()==2 && runner.isPaused());
        runner.shutdown();
        assertThat(worker.get().isAlive()).isFalse();
        assertThatThrownBy(runner::resume).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void duplicateStartDoesNotCreateSecondWorkerAndShutdownInterruptsSend() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            worker.set(Thread.currentThread()); entered.countDown();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException e) { interrupted.set(true); throw e; }
            return true;
        }).when(relay).relayOne();
        runner.run(ARGS);
        assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
        runner.run(ARGS);
        runner.shutdown();
        verify(relay, times(1)).relayOne();
        assertThat(interrupted.get()).isTrue();
        assertThat(worker.get().isAlive()).isFalse();
    }

    @Test
    void cannotResumeBeforeStartAndShutdownBeforeStartIsSafe() {
        assertThatThrownBy(runner::resume).isInstanceOf(IllegalStateException.class);
        runner.shutdown();
        verifyNoInteractions(relay);
    }

    @Test
    void closingSpringContextStopsWorkerThroughPreDestroy() throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        doAnswer(invocation -> { worker.set(Thread.currentThread()); return false; }).when(relay).relayOne();
        new ApplicationContextRunner()
                .withBean(CouponReservationRelay.class, () -> relay)
                .withUserConfiguration(CouponReservationRelayRunner.class)
                .withPropertyValues("coupon.relay.enabled=true")
                .run(context -> {
                    var bean = context.getBean(CouponReservationRelayRunner.class);
                    bean.run(ARGS); // ContextRunner does not invoke ApplicationRunner automatically.
                    await().atMost(Duration.ofSeconds(3)).until(() -> worker.get()!=null);
                });
        assertThat(worker.get().isAlive()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"default", "false", "true"})
    void actualApplicationYamlAndFlagControlBeanCreation(String flag) throws Exception {
        var properties = new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yaml"));
        assertThat(properties).anySatisfy(p ->
                assertThat(p.getProperty("coupon.relay.enabled"))
                        .isEqualTo("${COUPON_RELAY_ENABLED:false}"));
        var contextRunner = new ApplicationContextRunner()
                .withBean(CouponReservationRelay.class, () -> relay)
                .withUserConfiguration(CouponReservationRelayRunner.class)
                .withInitializer(context -> {
                    // Make the default case independent of the developer shell environment.
                    context.getEnvironment().getPropertySources().remove("systemEnvironment");
                    context.getEnvironment().getPropertySources().remove("systemProperties");
                    properties.forEach(p -> context.getEnvironment().getPropertySources().addLast(p));
                });
        if (!flag.equals("default")) contextRunner = contextRunner.withPropertyValues("COUPON_RELAY_ENABLED="+flag);
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            if (flag.equals("true")) assertThat(context).hasSingleBean(CouponReservationRelayRunner.class);
            else assertThat(context).doesNotHaveBean(CouponReservationRelayRunner.class);
        });
    }
}

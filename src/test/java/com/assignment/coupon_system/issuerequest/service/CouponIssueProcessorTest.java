package com.assignment.coupon_system.issuerequest.service;

import com.assignment.coupon_system.coupon.entity.Coupon;
import com.assignment.coupon_system.coupon.entity.CouponType;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuedcoupon.repository.IssuedCouponRepository;
import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.MySQLContainer;

import java.time.LocalDateTime;

import static com.assignment.coupon_system.issuerequest.entity.CouponIssueResultStatus.ISSUED;
import static com.assignment.coupon_system.issuerequest.entity.CouponIssueResultStatus.REJECTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest(showSql = false, properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({CouponIssueProcessor.class, CouponIssueProcessorTest.MySqlConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// DataJpaTest의 기본 테스트 트랜잭션을 끄고 서비스 호출마다 실제 커밋/롤백을 확인한다.
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@DisplayName("CouponIssueProcessor MySQL 통합 테스트")
class CouponIssueProcessorTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class MySqlConfig {
        @Bean
        @ServiceConnection
        MySQLContainer<?> mysql() {
            return new MySQLContainer<>("mysql:8.0");
        }
    }

    @Autowired CouponIssueProcessor processor;
    @Autowired CouponRepository couponRepository;
    @Autowired IssuedCouponRepository issuedCouponRepository;
    @Autowired CouponIssueResultRepository resultRepository;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        // Testcontainers 전용 DB이며 로컬 개발 DB에는 접근하지 않는다.
        resultRepository.deleteAll();
        issuedCouponRepository.deleteAll();
        couponRepository.deleteAll();
    }

    @Test
    @DisplayName("정상 신청은 수량, 발급 이력, 성공 결과를 함께 커밋한다")
    void issuesAndCommitsResult() {
        Coupon coupon = createCoupon(1);

        CouponIssueResult result = processor.process("request-001", coupon.getId(), 100L);

        assertThat(result.getStatus()).isEqualTo(ISSUED);
        assertThat(result.getIssuedCouponId()).isNotNull();
        assertThat(result.getProcessedAt()).isNotNull();
        assertThat(issuedCouponRepository.findById(result.getIssuedCouponId()))
                .hasValueSatisfying(issued -> assertThat(issued.getUserId()).isEqualTo(100L));
        assertThat(resultRepository.findByRequestId("request-001"))
                .hasValueSatisfying(saved -> {
                    assertThat(saved.getId()).isEqualTo(result.getId());
                    assertThat(saved.getCouponId()).isEqualTo(coupon.getId());
                    assertThat(saved.getUserId()).isEqualTo(100L);
                    assertThat(saved.getStatus()).isEqualTo(ISSUED);
                    assertThat(saved.getIssuedCouponId()).isEqualTo(result.getIssuedCouponId());
                });
        assertCounts(coupon.getId(), 1, 1, 1);
    }

    @Test
    @DisplayName("커밋된 성공 신청을 재전달하면 같은 결과를 반환하고 추가 발급하지 않는다")
    void replaysSuccessfulRequest() {
        Coupon coupon = createCoupon(2);
        CouponIssueResult first = processor.process("request-001", coupon.getId(), 100L);

        CouponIssueResult replay = processor.process("request-001", coupon.getId(), 100L);

        assertThat(replay.getId()).isEqualTo(first.getId());
        assertThat(replay.getIssuedCouponId()).isEqualTo(first.getIssuedCouponId());
        assertThat(replay.getStatus()).isEqualTo(ISSUED);
        assertCounts(coupon.getId(), 1, 1, 1);
    }

    @Test
    @DisplayName("다른 요청 ID라도 같은 사용자에게는 한 번만 발급한다")
    void rejectsSameUserWithDifferentRequestId() {
        Coupon coupon = createCoupon(2);
        processor.process("request-001", coupon.getId(), 100L);

        CouponIssueResult rejected = processor.process("request-002", coupon.getId(), 100L);

        assertRejected(rejected);
        assertCounts(coupon.getId(), 1, 1, 2);
    }

    @Test
    @DisplayName("재고 소진 시 거절 결과를 저장하고 재전달에도 결과가 늘어나지 않는다")
    void rejectsSoldOutAndReplaysRejection() {
        Coupon coupon = createCoupon(1);
        processor.process("request-001", coupon.getId(), 100L);

        CouponIssueResult rejected = processor.process("request-002", coupon.getId(), 200L);
        CouponIssueResult replay = processor.process("request-002", coupon.getId(), 200L);

        assertRejected(rejected);
        assertRejected(replay);
        assertThat(replay.getId()).isEqualTo(rejected.getId());
        assertCounts(coupon.getId(), 1, 1, 2);
    }

    @Test
    @DisplayName("동일 요청 ID에 다른 사용자나 쿠폰을 전달하면 기존 결과를 보존한다")
    void rejectsRequestIdPayloadConflict() {
        Coupon coupon = createCoupon(2);
        Coupon otherCoupon = createCoupon(2);
        CouponIssueResult first = processor.process("request-001", coupon.getId(), 100L);

        assertThatThrownBy(() -> processor.process("request-001", coupon.getId(), 200L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> processor.process("request-001", otherCoupon.getId(), 100L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(resultRepository.findByRequestId("request-001"))
                .hasValueSatisfying(saved -> {
                    assertThat(saved.getId()).isEqualTo(first.getId());
                    assertThat(saved.getUserId()).isEqualTo(100L);
                    assertThat(saved.getCouponId()).isEqualTo(coupon.getId());
                });
        assertCounts(coupon.getId(), 1, 1, 1);
        assertThat(couponRepository.findById(otherCoupon.getId()).orElseThrow().getIssuedQuantity())
                .isZero();
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰의 신청도 거절 결과로 저장한다")
    void rejectsMissingCoupon() {
        CouponIssueResult result = processor.process("request-missing", Long.MAX_VALUE, 100L);

        assertRejected(result);
        assertThat(resultRepository.findByRequestId("request-missing")).isPresent();
        assertThat(issuedCouponRepository.count()).isZero();
        assertThat(resultRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("발급 기간이 지난 쿠폰은 수량 변경 없이 거절한다")
    void rejectsExpiredCoupon() {
        Coupon coupon = couponRepository.save(Coupon.create(
                "만료 쿠폰", "테스트", CouponType.FIXED_AMOUNT, 0, 1000, 1, 30,
                LocalDateTime.now().minusDays(2), LocalDateTime.now().minusDays(1)));

        assertRejected(processor.process("request-expired", coupon.getId(), 100L));
        assertCounts(coupon.getId(), 0, 0, 1);
    }

    @Test
    @DisplayName("비활성 쿠폰은 수량 변경 없이 거절한다")
    void rejectsInactiveCoupon() {
        Coupon coupon = createCoupon(1);
        jdbc.update("UPDATE coupon SET status = 'INACTIVE' WHERE id = ?", coupon.getId());

        assertRejected(processor.process("request-inactive", coupon.getId(), 100L));
        assertCounts(coupon.getId(), 0, 0, 1);
    }

    @Test
    @DisplayName("결과 저장이 DB에서 실패하면 수량과 발급 이력까지 롤백하고 재시도할 수 있다")
    void rollsBackWhenResultInsertFails() {
        Coupon coupon = createCoupon(1);
        // 발급 이력 INSERT 이후에 실행되는 결과 INSERT만 실제 MySQL 제약으로 실패시킨다.
        jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT test_result_failure "
                + "CHECK (request_id <> 'force-result-failure')");
        try {
            assertThatThrownBy(() -> processor.process("force-result-failure", coupon.getId(), 100L))
                    .isInstanceOf(JpaSystemException.class)
                    .hasMessageContaining("test_result_failure");
            assertCounts(coupon.getId(), 0, 0, 0);
        } finally {
            jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK test_result_failure");
        }

        assertThat(processor.process("force-result-failure", coupon.getId(), 100L).getStatus())
                .isEqualTo(ISSUED);
        assertCounts(coupon.getId(), 1, 1, 1);
    }

    private Coupon createCoupon(int stock) {
        return couponRepository.save(Coupon.create(
                "테스트 쿠폰", "테스트", CouponType.FIXED_AMOUNT, 0, 1000, stock, 30,
                LocalDateTime.now().minusDays(1), LocalDateTime.now().plusDays(1)));
    }

    private void assertRejected(CouponIssueResult result) {
        assertThat(result.getStatus()).isEqualTo(REJECTED);
        assertThat(result.getIssuedCouponId()).isNull();
        assertThat(resultRepository.findByRequestId(result.getRequestId()))
                .hasValueSatisfying(saved -> assertThat(saved.getStatus()).isEqualTo(REJECTED));
    }

    private void assertCounts(Long couponId, int quantity, long issued, long results) {
        assertThat(couponRepository.findById(couponId).orElseThrow().getIssuedQuantity())
                .isEqualTo(quantity);
        assertThat(issuedCouponRepository.count()).isEqualTo(issued);
        assertThat(resultRepository.count()).isEqualTo(results);
    }
}

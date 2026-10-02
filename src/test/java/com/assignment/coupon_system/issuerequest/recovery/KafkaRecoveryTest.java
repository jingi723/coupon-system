package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Kafka 실제 프로세스 장애 복구 테스트")
class KafkaRecoveryTest {
    @Container static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");
    @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.0");
    private static final String GROUP = "recovery-" + UUID.randomUUID();
    private static final TopicPartition PARTITION = new TopicPartition(CouponKafkaConfig.ISSUE_TOPIC, 0);
    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Path evidence = Path.of("build", "recovery-evidence", UUID.randomUUID().toString()).toAbsolutePath();
    private Process app;
    private String base;
    private JdbcTemplate jdbc;
    private Admin admin;

    @BeforeEach
    void prepare() throws Exception {
        Files.createDirectories(evidence);
        jdbc = new JdbcTemplate(new DriverManagerDataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()));
        admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            if (app != null && app.isAlive()) {
                app.destroy();
                if (!app.waitFor(10, TimeUnit.SECONDS)) {
                    app.destroyForcibly();
                    assertThat(app.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
        } finally {
            if (admin != null) admin.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    @DisplayName("API를 유지하고 Consumer만 정지하면 접수가 쌓이고 재개 후 모두 확정된다")
    void acceptsWhileConsumerStoppedAndDrainsAfterRestart() throws Exception {
        launch("stop-resume", null);
        long coupon = createCoupon(2);
        caughtUp();
        long before = endOffset();
        assertThat(post("/__test/consumer/stop", Map.of()).path("running").asBoolean()).isFalse();
        assertThat(app.isAlive()).isTrue();
        List<String> requests = List.of(requestId(), requestId(), requestId());
        for (int i = 0; i < requests.size(); i++) submit(coupon, requests.get(i), 100L + i);
        assertThat(endOffset()).isEqualTo(before + 3);
        assertThat(committedOffset()).isEqualTo(before);
        assertCounts(coupon, 0, 0, 0);
        assertThat(get("/__test/consumer/status").path("running").asBoolean()).isFalse();
        Map<String, Object> stopped = Map.of("accepted", 3, "lag", endOffset() - committedOffset(), "dbResults", 0);

        post("/__test/consumer/start", Map.of());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertCounts(coupon, 2, 2, 3));
        caughtUp();
        assertThat(jdbc.queryForList("SELECT request_id FROM coupon_issue_result WHERE coupon_id=?", String.class, coupon))
                .containsExactlyInAnyOrderElementsOf(requests);
        assertThat(number("SELECT COUNT(*) FROM coupon_issue_result WHERE coupon_id=? AND status='ISSUED'", coupon)).isEqualTo(2);
        assertThat(number("SELECT COUNT(*) FROM coupon_issue_result WHERE coupon_id=? AND status='REJECTED'", coupon)).isEqualTo(1);
        save("stop-resume-result.json", Map.of("couponId", coupon, "stopped", stopped,
                "issued", 2, "rejected", 1, "finalLag", 0, "appPid", app.pid()));
    }

    @Test
    @Order(2)
    @DisplayName("DB 커밋 후 offset 커밋 전 강제 종료하면 같은 Kafka 레코드가 재전달되어도 한 번만 발급된다")
    void survivesCrashAfterDbCommitBeforeOffsetCommit() throws Exception {
        String request = requestId();
        Path firstRun = launch("before-crash", request);
        long coupon = createCoupon(1);
        caughtUp();
        long before = endOffset();
        submit(coupon, request, 200L);
        await().atMost(Duration.ofSeconds(20)).until(() -> Files.exists(firstRun.resolve("after-db-commit.jsonl")));
        JsonNode marker = lines(firstRun.resolve("after-db-commit.jsonl")).get(0);
        assertThat(marker.path("transactionActive").asBoolean()).isFalse();
        // 별도 연결에서 조회해 flush만 된 상태가 아니라 실제 커밋됐음을 확인한다.
        assertCounts(coupon, 1, 1, 1);
        long resultId = number("SELECT id FROM coupon_issue_result WHERE request_id=?", request);
        long issuedId = number("SELECT issued_coupon_id FROM coupon_issue_result WHERE request_id=?", request);
        JsonNode firstDelivery = delivery(firstRun, request);
        assertThat(firstDelivery.path("offset").asLong()).isEqualTo(before);
        assertThat(endOffset()).isEqualTo(before + 1);
        assertThat(committedOffset()).isEqualTo(before);
        long crashedPid = app.pid();
        app.destroyForcibly(); // graceful shutdown 없이 프로세스를 강제 종료
        assertThat(app.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(app.exitValue()).isNotZero();

        Path secondRun = launch("after-crash", null); // 동일 GROUP, 추가 접수/재발행 없음
        caughtUp();
        JsonNode redelivery = delivery(secondRun, request);
        assertThat(redelivery).isEqualTo(firstDelivery);
        assertThat(endOffset()).isEqualTo(before + 1);
        assertThat(committedOffset()).isEqualTo(before + 1);
        assertCounts(coupon, 1, 1, 1);
        assertThat(number("SELECT id FROM coupon_issue_result WHERE request_id=?", request)).isEqualTo(resultId);
        assertThat(number("SELECT issued_coupon_id FROM coupon_issue_result WHERE request_id=?", request)).isEqualTo(issuedId);
        save("crash-recovery-result.json", Map.of("couponId", coupon, "requestId", request,
                "firstDelivery", firstDelivery, "redelivery", redelivery,
                "offsetBeforeCrash", before, "offsetAfterRestart", committedOffset(),
                "resultId", resultId, "issuedCouponId", issuedId, "crashedPid", crashedPid, "restartedPid", app.pid()));
    }

    @Test
    @Order(3)
    @DisplayName("DB 오류로 Consumer가 멈춰도 API는 접수하고 원인 제거 후 재개하면 실패 레코드부터 처리한다")
    void resumesContainerStoppedByRealDbFailure() throws Exception {
        Path run = launch("db-failure", null);
        long coupon = createCoupon(2);
        caughtUp();
        long before = endOffset();
        String failed = requestId();
        String following = requestId();
        // UUID는 테스트에서 생성한 값이다. 결과 INSERT만 실패시키고 서비스 롤백을 확인한다.
        jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT recovery_result_failure CHECK (request_id <> '" + failed + "')");
        try {
            submit(coupon, failed, 300L);
            await().atMost(Duration.ofSeconds(20)).until(() -> !get("/__test/consumer/status").path("running").asBoolean());
            assertCounts(coupon, 0, 0, 0);
            assertThat(committedOffset()).isEqualTo(before);
            submit(coupon, following, 301L);
            assertThat(endOffset()).isEqualTo(before + 2);
            assertCounts(coupon, 0, 0, 0);
        } finally {
            jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK recovery_result_failure");
        }
        assertThat(get("/__test/consumer/status").path("running").asBoolean()).isFalse();
        post("/__test/consumer/start", Map.of());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertCounts(coupon, 2, 2, 2));
        caughtUp();
        List<JsonNode> deliveries = lines(run.resolve("deliveries.jsonl")).stream()
                .filter(n -> n.path("requestId").asText().equals(failed) || n.path("requestId").asText().equals(following)).toList();
        assertThat(deliveries).hasSize(3);
        assertThat(deliveries.get(0)).isEqualTo(deliveries.get(1));
        assertThat(deliveries.get(2).path("requestId").asText()).isEqualTo(following);
        save("db-failure-result.json", Map.of("couponId", coupon, "deliveries", deliveries,
                "accepted", 2, "issued", 2, "finalLag", 0));
    }

    private Path launch(String name, String holdRequest) throws Exception {
        Path run = evidence.resolve(name);
        Files.createDirectories(run);
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx384m", "-Drecovery.dir=" + run));
        if (holdRequest != null) command.add("-Drecovery.hold.request=" + holdRequest);
        command.addAll(List.of("-cp", System.getProperty("recovery.test.classpath"), RecoveryTestApplication.class.getName(),
                "--spring.profiles.active=recovery-probe", "--server.address=127.0.0.1", "--server.port=0",
                "--spring.datasource.url=" + mysql.getJdbcUrl(), "--spring.datasource.username=" + mysql.getUsername(),
                "--spring.datasource.password=" + mysql.getPassword(), "--spring.jpa.hibernate.ddl-auto=update",
                "--spring.jpa.show-sql=false", "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=" + GROUP,
                "--spring.kafka.consumer.properties.session.timeout.ms=6000",
                "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000"));
        app = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(run.resolve("application.log").toFile()).start();
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            assertThat(app.isAlive()).withFailMessage("Child app exited; see %s", run.resolve("application.log")).isTrue();
            assertThat(run.resolve("ready.jsonl")).exists();
        });
        base = "http://127.0.0.1:" + lines(run.resolve("ready.jsonl")).get(0).path("port").asText();
        await().atMost(Duration.ofSeconds(15)).until(() -> admin.describeConsumerGroups(List.of(GROUP))
                .all().get(5, TimeUnit.SECONDS).get(GROUP).members().stream()
                .anyMatch(m -> m.assignment().topicPartitions().contains(PARTITION)));
        return run;
    }

    private void submit(long coupon, String request, long user) throws Exception {
        HttpResponse<String> response = send("POST", "/api/coupons/" + coupon + "/issue-requests",
                Map.of("requestId", request, "userId", user));
        assertThat(response.statusCode()).isEqualTo(202);
        JsonNode data = JSON.readTree(response.body()).path("data");
        assertThat(data.path("requestId").asText()).isEqualTo(request);
        assertThat(data.path("status").asText()).isEqualTo("ACCEPTED");
    }

    private long createCoupon(int stock) throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put("name", "recovery-test"); body.put("description", "isolated test");
        body.put("couponType", "FIXED_AMOUNT"); body.put("discountValue", 1000);
        body.put("minOrderAmount", 0); body.put("maxDiscountAmount", 1000);
        body.put("totalQuantity", stock); body.put("validDays", 30);
        body.put("startDateTime", LocalDateTime.now().minusDays(1).toString());
        body.put("endDateTime", LocalDateTime.now().plusDays(1).toString());
        return post("/api/coupons", body).path("data").path("id").asLong();
    }

    private JsonNode get(String path) throws Exception {
        HttpResponse<String> response = send("GET", path, null);
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private JsonNode post(String path, Object body) throws Exception {
        HttpResponse<String> response = send("POST", path, body);
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private HttpResponse<String> send(String method, String path, Object body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(20));
        if (body == null) builder.GET();
        else builder.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private long endOffset() throws Exception {
        return admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS).get(PARTITION).offset();
    }

    private long committedOffset() throws Exception {
        var offset = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(PARTITION);
        return offset == null ? 0 : offset.offset();
    }

    private void caughtUp() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(endOffset()));
    }

    private long number(String sql, Object... args) {
        return Objects.requireNonNull(jdbc.queryForObject(sql, Long.class, args));
    }

    private void assertCounts(long coupon, long quantity, long issued, long results) {
        assertThat(number("SELECT issued_quantity FROM coupon WHERE id=?", coupon)).isEqualTo(quantity);
        assertThat(number("SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=?", coupon)).isEqualTo(issued);
        assertThat(number("SELECT COUNT(*) FROM coupon_issue_result WHERE coupon_id=?", coupon)).isEqualTo(results);
    }

    private List<JsonNode> lines(Path file) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : Files.readAllLines(file)) if (!line.isBlank()) rows.add(JSON.readTree(line));
        return rows;
    }

    private JsonNode delivery(Path run, String request) throws Exception {
        return lines(run.resolve("deliveries.jsonl")).stream()
                .filter(n -> n.path("requestId").asText().equals(request)).findFirst().orElseThrow();
    }

    private String requestId() { return UUID.randomUUID().toString(); }

    private void save(String file, Map<String, ?> value) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>(value);
        record.put("consumerGroup", GROUP);
        Files.writeString(evidence.resolve(file), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(record));
        System.out.println("Recovery evidence: " + evidence.resolve(file));
    }
}

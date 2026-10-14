package com.assignment.coupon_system.issuerequest.recovery;

import com.assignment.coupon_system.coupon.redis.*;
import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
class ReservationEndToEndTest {
    @Container static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");
    static final String VOLUME = "coupon-reservation-test-" + UUID.randomUUID();
    static final Path EVIDENCE = Path.of("build", "reservation-e2e-evidence", UUID.randomUUID().toString()).toAbsolutePath();
    static final ObjectMapper JSON = new ObjectMapper();
    static final TopicPartition PARTITION = new TopicPartition(CouponKafkaConfig.ISSUE_TOPIC, 0);
    static GenericContainer<?> redisContainer;
    final String group = "reservation-e2e-" + UUID.randomUUID();
    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    LettuceConnectionFactory connection;
    StringRedisTemplate redis;
    CouponIssueRedisService reservations;
    Admin admin;
    JdbcTemplate jdbc;
    Process app;
    String base;
    Path evidence;

    @BeforeAll
    static void startRedis() throws Exception {
        Files.createDirectories(EVIDENCE);
        DockerClientFactory.instance().client().createVolumeCmd().withName(VOLUME).exec();
        try { redisContainer = newRedis(); redisContainer.start(); }
        catch (Exception e) {
            if (redisContainer != null) redisContainer.stop();
            DockerClientFactory.instance().client().removeVolumeCmd(VOLUME).exec();
            throw e;
        }
    }

    static GenericContainer<?> newRedis() {
        return new GenericContainer<>("redis:7-alpine").withExposedPorts(6379)
                .withCommand("redis-server", "--dir", "/data", "--appendonly", "yes",
                        "--appendfsync", "always", "--no-appendfsync-on-rewrite", "no")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withBinds(Bind.parse(VOLUME + ":/data")));
    }

    @AfterAll
    static void stopRedis() throws Exception {
        try { if (redisContainer != null) redisContainer.stop(); }
        finally {
            DockerClientFactory.instance().client().removeVolumeCmd(VOLUME).exec();
            Files.writeString(EVIDENCE.resolve("cleanup.txt"), "Test Redis container stopped and dedicated volume removed: " + VOLUME);
        }
    }

    @BeforeEach
    void prepare(TestInfo info) throws Exception {
        evidence = EVIDENCE.resolve(info.getTestMethod().orElseThrow().getName());
        Files.createDirectories(evidence);
        connectRedis();
        try (var c = connection.getConnection()) { c.serverCommands().flushDb(); }
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
        jdbc = new JdbcTemplate(new DriverManagerDataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword()));
        save("environment.json", Map.of("redisContainer", redisContainer.getContainerId(), "volume", VOLUME,
                "mysqlContainer", MYSQL.getContainerId(), "kafkaContainer", KAFKA.getContainerId(),
                "group", group, "httpTimeoutSeconds", 5, "queryPollMillis", 200,
                "redisCommand", List.of(redisContainer.getCommandParts())));
    }

    void connectRedis() {
        // 재시작 후 바뀔 수 있는 동적 공개 포트를 다시 조회한다.
        int port = Integer.parseInt(DockerClientFactory.instance().client()
                .inspectContainerCmd(redisContainer.getContainerId()).exec()
                .getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(6379))[0].getHostPortSpec());
        connection = new LettuceConnectionFactory(redisContainer.getHost(), port);
        connection.afterPropertiesSet();
        redis = new StringRedisTemplate(connection);
        reservations = new CouponIssueRedisService(redis);
        await().atMost(Duration.ofSeconds(15))
                .ignoreException(org.springframework.data.redis.RedisConnectionFailureException.class).untilAsserted(() -> {
            try (var c = connection.getConnection()) { assertThat(c.ping()).isEqualTo("PONG"); }
        });
    }

    @AfterEach
    void cleanup() throws Exception {
        try { stopApp(); }
        finally {
            if (admin != null) admin.close(Duration.ofSeconds(5));
            if (connection != null) connection.destroy();
        }
    }

    @Test
    void redisKeepsReservationsAcrossRestartAndCrashRecreation() throws Exception {
        Instant now = Instant.now();
        assertThat(reservations.initializeReservation(900L, 10, now.minusSeconds(60), now.plusSeconds(3600))).isTrue();
        String first = UUID.randomUUID().toString(), second = UUID.randomUUID().toString();
        assertThat(reservations.reserve(first, 900L, 1L)).isEqualTo(CouponReservationStatus.RESERVED);
        assertThat(reservations.reserve(second, 900L, 2L)).isEqualTo(CouponReservationStatus.RESERVED);
        assertThat(reservations.claimPendingReservation()).isEqualTo(first);
        var before = snapshot(); save("before.json", before);
        connection.destroy();
        DockerClientFactory.instance().client().restartContainerCmd(redisContainer.getContainerId()).exec();
        connectRedis();
        assertThat(snapshot()).isEqualTo(before); save("after-normal-restart.json", snapshot());
        crashAndRecreateRedis();
        assertThat(snapshot()).isEqualTo(before); save("after-crash-recreation.json", snapshot());
        assertThat(reservations.reserve(first, 900L, 1L)).isEqualTo(CouponReservationStatus.ALREADY_RESERVED);
        assertThat(snapshot()).isEqualTo(before);
        savePersistence();
    }

    void crashAndRecreateRedis() {
        connection.destroy();
        DockerClientFactory.instance().client().killContainerCmd(redisContainer.getContainerId()).withSignal("KILL").exec();
        redisContainer.stop();
        redisContainer = newRedis(); redisContainer.start();
        connectRedis();
    }

    @Test
    void httpReservationsSurviveRedisCrashAndReachDbThroughRealRelayAndKafka() throws Exception {
        launch("accept", false);
        Instant start = Instant.now().minusSeconds(3600), end = Instant.now().plusSeconds(3600);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "reservation-e2e"); body.put("description", "isolated fixture");
        body.put("couponType", "FIXED_AMOUNT"); body.put("minOrderAmount", 0);
        body.put("maxDiscountAmount", 1000); body.put("totalQuantity", 10); body.put("validDays", 30);
        body.put("startDateTime", LocalDateTime.ofInstant(start, ZoneId.of("Asia/Seoul")).toString());
        body.put("endDateTime", LocalDateTime.ofInstant(end, ZoneId.of("Asia/Seoul")).toString());
        long coupon = request("POST", "/api/coupons", body, 200).path("data").path("id").asLong();
        assertThat(coupon).isPositive();
        assertThat(reservations.initializeReservation(coupon, 10, start, end)).isTrue();
        List<String> ids = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String prefix = "/api/coupons/" + coupon;
        List<JsonNode> approvals = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            approvals.add(submit(prefix, ids.get(i), 100 + i));
            assertThat(query(prefix, ids.get(i)).path("status").asText()).isEqualTo("RESERVED");
        }
        assertThat(submit(prefix, ids.get(0), 100)).isEqualTo(approvals.get(0));
        assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1)).containsExactlyElementsOf(ids);
        assertThat(reservations.claimPendingReservation()).isEqualTo(ids.get(0));
        var before = snapshot(); save("approved-before-restart.json", Map.of("approvals", approvals, "redis", before));
        assertCounts(coupon, 0);
        stopApp();
        crashAndRecreateRedis();
        assertThat(snapshot()).isEqualTo(before); save("redis-after-recreation.json", snapshot());
        Path run = launch("deliver", true);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(redis.opsForList().size(CouponRedisKey.pendingReservations())).isZero();
            assertThat(redis.opsForList().size(CouponRedisKey.processingReservations())).isZero();
            assertThat(endOffset()).isEqualTo(3);
        });
        assertCounts(coupon, 0);
        for (String id : ids) assertThat(query(prefix, id).path("status").asText()).isEqualTo("RESERVED");
        request("POST", prefix + "/stock/init", Map.of(), 409);
        request("POST", prefix + "/issue", Map.of("userId", 999), 409);
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("7");
        request("POST", "/__test/consumer/start", Map.of(), 200);
        await().pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            for (String id : ids) assertThat(query(prefix, id).path("status").asText()).isEqualTo("ISSUED");
        });
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(endOffset()));
        assertCounts(coupon, 3);
        List<Map<String, Object>> results = jdbc.queryForList(
                "SELECT request_id, coupon_id, user_id, status, issued_coupon_id FROM coupon_issue_result WHERE coupon_id=?", coupon);
        assertThat(results).extracting(row -> row.get("request_id")).containsExactlyInAnyOrderElementsOf(ids);
        for (int i = 0; i < ids.size(); i++) {
            var result = query(prefix, ids.get(i));
            assertThat(result.path("userId").asLong()).isEqualTo(100 + i);
            assertThat(number("SELECT COUNT(*) FROM issued_coupon WHERE id=? AND coupon_id=? AND user_id=?",
                    result.path("issuedCouponId").asLong(), coupon, 100 + i)).isEqualTo(1);
            assertThat(submit(prefix, ids.get(i), 100 + i)).isEqualTo(approvals.get(i));
            assertThat(reservations.findReservation(ids.get(i)).userId()).isEqualTo(100 + i);
        }
        assertThat(redis.opsForSet().members(CouponRedisKey.issueUsers(coupon))).containsExactlyInAnyOrder("100", "101", "102");
        assertThat(redis.opsForValue().get(CouponRedisKey.stock(coupon))).isEqualTo("7");
        assertThat(redis.opsForList().size(CouponRedisKey.pendingReservations())).isZero();
        assertThat(redis.opsForList().size(CouponRedisKey.processingReservations())).isZero();
        assertThat(endOffset()).isEqualTo(3);
        assertCounts(coupon, 3);
        var deliveries = Files.readAllLines(run.resolve("deliveries.jsonl")).stream().map(line -> {
            try { return JSON.readTree(line); } catch (Exception e) { throw new RuntimeException(e); }
        }).toList();
        assertThat(deliveries).extracting(row -> row.path("requestId").asText()).containsExactlyElementsOf(ids);
        save("final.json", Map.of("couponId", coupon, "requestIds", ids, "results", results,
                "redis", snapshot(), "endOffset", endOffset(), "committedOffset", committedOffset(), "deliveries", deliveries));
        savePersistence();
    }

    Path launch(String name, boolean relay) throws Exception {
        Path run = evidence.resolve(name); Files.createDirectories(run);
        List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx384m", "-Drecovery.dir=" + run, "-cp", System.getProperty("recovery.test.classpath"),
                RecoveryTestApplication.class.getName(), "--spring.profiles.active=recovery-probe",
                "--server.address=127.0.0.1", "--server.port=0", "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(), "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.jpa.hibernate.ddl-auto=update", "--spring.jpa.show-sql=false",
                "--spring.data.redis.host=" + redisContainer.getHost(),
                "--spring.data.redis.port=" + redisContainer.getMappedPort(6379),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--spring.kafka.consumer.group-id=" + group,
                "--spring.kafka.listener.auto-startup=false", "--coupon.relay.enabled=" + relay, "--coupon.legacy.enabled=false"));
        app = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(run.resolve("application.log").toFile()).start();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(app.isAlive()).withFailMessage("App exited; see %s", run).isTrue();
            assertThat(run.resolve("ready.jsonl")).exists();
        });
        base = "http://127.0.0.1:" + JSON.readTree(Files.readAllLines(run.resolve("ready.jsonl")).get(0)).path("port").asText();
        return run;
    }

    void stopApp() throws Exception {
        if (app != null && app.isAlive()) {
            app.destroy();
            if (!app.waitFor(15, TimeUnit.SECONDS)) {
                app.destroyForcibly(); assertThat(app.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    JsonNode submit(String prefix, String id, long user) throws Exception {
        var data = request("POST", prefix + "/issue-requests", Map.of("requestId", id, "userId", user), 202).path("data");
        assertThat(data.path("requestId").asText()).isEqualTo(id);
        assertThat(data.path("status").asText()).isEqualTo("RESERVED");
        return data;
    }
    JsonNode query(String prefix, String id) throws Exception {
        return request("GET", prefix + "/issue-requests/" + id, null, 200).path("data");
    }
    JsonNode request(String method, String path, Object body, int status) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5));
        if (body == null) builder.GET();
        else builder.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        long start = System.nanoTime();
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        Files.writeString(evidence.resolve("http.jsonl"), JSON.writeValueAsString(Map.of("method", method, "path", path,
                "status", response.statusCode(), "elapsedNanos", System.nanoTime() - start, "body", response.body())) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        assertThat(response.statusCode()).withFailMessage("%s %s: %s", method, path, response.body()).isEqualTo(status);
        return JSON.readTree(response.body());
    }
    Map<String, String> snapshot() {
        Map<String, String> map = new TreeMap<>();
        for (String key : redis.keys("*")) map.put(key, Base64.getEncoder().encodeToString(redis.dump(key)));
        return map;
    }
    long number(String sql, Object... args) { return Objects.requireNonNull(jdbc.queryForObject(sql, Long.class, args)); }
    void assertCounts(long coupon, int expected) {
        assertThat(number("SELECT issued_quantity FROM coupon WHERE id=?", coupon)).isEqualTo(expected);
        assertThat(number("SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=?", coupon)).isEqualTo(expected);
        assertThat(number("SELECT COUNT(*) FROM coupon_issue_result WHERE coupon_id=? AND status='ISSUED'", coupon)).isEqualTo(expected);
        assertThat(number("SELECT COUNT(*) FROM coupon_issue_result WHERE coupon_id=?", coupon)).isEqualTo(expected);
    }
    long endOffset() throws Exception {
        return admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS).get(PARTITION).offset();
    }
    long committedOffset() throws Exception {
        var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(PARTITION);
        return offset == null ? 0 : offset.offset();
    }
    void save(String name, Object data) throws Exception {
        Files.writeString(evidence.resolve(name), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(data));
    }
    void savePersistence() throws Exception {
        var config = redisContainer.execInContainer("redis-cli", "CONFIG", "GET", "appendonly", "appendfsync", "dir", "no-appendfsync-on-rewrite");
        assertThat(config.getExitCode()).isZero();
        Files.writeString(evidence.resolve("redis-config.txt"), config.getStdout());
        var info = redisContainer.execInContainer("redis-cli", "INFO", "persistence");
        assertThat(info.getStdout()).contains("aof_enabled:1", "aof_last_write_status:ok");
        Files.writeString(evidence.resolve("redis-persistence.txt"), info.getStdout());
    }
}

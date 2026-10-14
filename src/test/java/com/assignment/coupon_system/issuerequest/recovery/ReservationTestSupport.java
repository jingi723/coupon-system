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
abstract class ReservationTestSupport {
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

    void crashAndRecreateRedis() {
        connection.destroy();
        DockerClientFactory.instance().client().killContainerCmd(redisContainer.getContainerId()).withSignal("KILL").exec();
        redisContainer.stop();
        redisContainer = newRedis(); redisContainer.start();
        connectRedis();
    }

    Path launch(String name, boolean relay) throws Exception {
        return launch(name, relay, null, null);
    }

    Path launch(String name, boolean relay, String boundary, String requestId) throws Exception {
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
        if (this instanceof BatchConsumerRecoveryTest) {
            command.set(command.indexOf(RecoveryTestApplication.class.getName()), com.assignment.coupon_system.issuerequest.profiling.RelayProfileApplication.class.getName());
            command.set(command.indexOf("--spring.profiles.active=recovery-probe"), "--spring.profiles.active=batch-probe");
            command.add(1,"-Dbatch.candidate.size=10");command.add(1,"-Dbatch.candidate.dir="+run);command.add(1,"-Dbatch.candidate.trace=true");
            command.add(1,"-Drelay.profile.enabled=true");command.add(1,"-Drelay.profile.dir="+run);command.add(1,"-Drelay.prepare.lua=true");
            if(boundary!=null)command.add(1,"-Dbatch.candidate.hold="+boundary);
        }
        if (this instanceof ProductionBatchConsumerRecoveryTest) {
            command.set(command.indexOf(RecoveryTestApplication.class.getName()), ProductionBatchTestApplication.class.getName());
            command.set(command.indexOf("--spring.profiles.active=recovery-probe"), "--spring.profiles.active=production-batch-probe");
            command.add("--coupon.consumer.batch-enabled=true");
        }
        if ("true".equals(System.getenv("LOAD_LUA_RECOVERY"))) command.add(1, "-Drelay.prepare.lua=true");
        if (boundary != null) {
            command.add(1, "-Drecovery.hold.request=" + requestId);
            command.add(1, "-Drecovery.hold.boundary=" + boundary);
        }
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

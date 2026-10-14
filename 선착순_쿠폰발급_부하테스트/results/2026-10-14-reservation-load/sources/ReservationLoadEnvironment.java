package com.assignment.coupon_system.issuerequest.profiling;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import java.nio.file.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 성능시험 전용 인프라. 개발 컨테이너와 데이터에 접근하지 않는다. */
public class ReservationLoadEnvironment {
    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]).toAbsolutePath(); Files.createDirectories(out);
        try (var mysql = new MySQLContainer<>("mysql:8.0")
                     .withCreateContainerCmdModifier(c -> c.getHostConfig().withMemory(1073741824L).withNanoCPUs(1000000000L));
             var redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379)
                     .withCommand("redis-server", "--dir", "/data", "--appendonly", "yes", "--appendfsync", "always", "--no-appendfsync-on-rewrite", "no")
                     .withCreateContainerCmdModifier(c -> c.getHostConfig().withMemory(268435456L).withNanoCPUs(500000000L));
             var kafka = new KafkaContainer("apache/kafka:3.7.0")
                     .withCreateContainerCmdModifier(c -> c.getHostConfig().withMemory(1073741824L).withNanoCPUs(1000000000L))) {
            mysql.start(); redis.start(); kafka.start();
            Map<String,String> env = new LinkedHashMap<>();
            env.put("LOAD_DB_URL", mysql.getJdbcUrl()); env.put("LOAD_DB_USER",mysql.getUsername()); env.put("LOAD_DB_PASSWORD",mysql.getPassword());
            env.put("LOAD_REDIS_HOST",redis.getHost()); env.put("LOAD_REDIS_PORT",redis.getMappedPort(6379).toString());
            env.put("LOAD_KAFKA",kafka.getBootstrapServers()); env.put("LOAD_GROUP","reservation-load-"+UUID.randomUUID());
            env.put("LOAD_DB_CONTAINER",mysql.getContainerId()); env.put("LOAD_REDIS_CONTAINER",redis.getContainerId()); env.put("LOAD_KAFKA_CONTAINER",kafka.getContainerId());
            var json = new ObjectMapper();
            Files.writeString(out.resolve("infrastructure.json"),json.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                    "mysql",Map.of("id",mysql.getContainerId(),"cpu",1,"memoryMiB",1024),
                    "redis",Map.of("id",redis.getContainerId(),"cpu",0.5,"memoryMiB",256,"aof","always"),
                    "kafka",Map.of("id",kafka.getContainerId(),"cpu",1,"memoryMiB",1024),"group",env.get("LOAD_GROUP"))));
            for (String id : List.of(mysql.getContainerId(),redis.getContainerId(),kafka.getContainerId())) {
                new ProcessBuilder("docker","inspect",id).redirectOutput(out.resolve("container-"+id.substring(0,12)+".json").toFile()).start().waitFor();
            }
            var pb = new ProcessBuilder("python3", "선착순_쿠폰발급_부하테스트/run-reservation-load.py", out.toString(), System.getProperty("java.class.path"));
            pb.environment().putAll(env); pb.inheritIO();
            var child = pb.start();
            try { if(child.waitFor()!=0)throw new IllegalStateException("Load driver failed"); }
            finally { if(child.isAlive()){child.destroy();if(!child.waitFor(15,java.util.concurrent.TimeUnit.SECONDS))child.destroyForcibly().waitFor();} }
        } finally { Files.writeString(out.resolve("environment-closed.txt"),"Testcontainer close sequence completed; see logs for cleanup errors.\n"); }
    }
}

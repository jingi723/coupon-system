package com.assignment.coupon_system.coupon.redis.profiling;

import com.assignment.coupon_system.coupon.redis.*;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.nio.file.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/** Test-only open arrival-rate driver. Requires a dedicated disposable Redis. */
public class RedisReservationBenchmark {
    static class Row {
        String id = UUID.randomUUID().toString();
        long user, due, start = -1, end = -1;
        String result = "UNSENT", error = "";
    }
    static long origin;
    static LettuceConnectionFactory factory(int port) {
        var f = new LettuceConnectionFactory(new RedisStandaloneConfiguration("127.0.0.1", port),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofSeconds(5)).build());
        f.afterPropertiesSet();
        return f;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !args[2].equals("DEDICATED_REDIS"))
            throw new IllegalArgumentException("port output-dir DEDICATED_REDIS required; this clears Redis");
        int port = Integer.parseInt(args[0]);
        Path out = Path.of(args[1]); Files.createDirectories(out);
        var f = factory(port); var monitorFactory = factory(port);
        var redis = new StringRedisTemplate(f);
        var service = new CouponIssueRedisService(redis);
        var executor = Executors.newFixedThreadPool(64);
        var monitor = Executors.newSingleThreadScheduledExecutor();
        var slots = new Semaphore(64);
        List<Row> rows = new ArrayList<>();
        try {
            try (var c = f.getConnection()) { c.serverCommands().flushDb(); }
            redis.opsForValue().set(CouponRedisKey.stock(1L), "100000");
            for (int i = 1; i <= 1000; i++) {
                if (service.reserve(UUID.randomUUID().toString(), 1L, (long)i) != CouponReservationStatus.RESERVED)
                    throw new IllegalStateException("Warmup failed");
            }
            try (var c = f.getConnection()) { c.serverCommands().flushDb(); }
            redis.opsForValue().set(CouponRedisKey.stock(1L), "100000");
            int[] rates = {100, 1200, 100}, durations = {20, 10, 30};
            long offset = 0;
            for (int stage = 0; stage < 3; stage++) {
                for (int i = 0; i < rates[stage] * durations[stage]; i++) {
                    Row r = new Row(); r.user = rows.size() + 1L;
                    r.due = offset + (long)i * 1_000_000_000L / rates[stage]; rows.add(r);
                }
                offset += durations[stage] * 1_000_000_000L;
            }
            origin = System.nanoTime();
            Files.writeString(out.resolve("origin.txt"), "epochMillis=" + System.currentTimeMillis() + "\noriginNano=" + origin + "\n");
            try (var samples = Files.newBufferedWriter(out.resolve("redis-info.csv"))) {
                samples.write("start_ns,end_ns,cpu_user_s,cpu_sys_s,used_memory,used_memory_rss,used_memory_peak,total_commands,error\n");
                monitor.scheduleAtFixedRate(() -> {
                    long start = System.nanoTime() - origin;
                    try (var c = monitorFactory.getConnection()) {
                        Properties p = c.serverCommands().info();
                        samples.write(start + "," + (System.nanoTime()-origin) + "," +
                                p.getProperty("used_cpu_user") + "," + p.getProperty("used_cpu_sys") + "," +
                                p.getProperty("used_memory") + "," + p.getProperty("used_memory_rss") + "," +
                                p.getProperty("used_memory_peak") + "," + p.getProperty("total_commands_processed") + ",\n");
                        samples.flush();
                    } catch (Exception e) {
                        try { samples.write(start + "," + (System.nanoTime()-origin) + ",,,,,,," + e.getClass().getSimpleName() + "\n"); samples.flush(); }
                        catch (IOException writeError) { throw new UncheckedIOException(writeError); }
                    }
                }, 0, 1, TimeUnit.SECONDS);
                for (Row r : rows) {
                    long wait;
                    while ((wait = origin + r.due - System.nanoTime()) > 0) LockSupport.parkNanos(wait);
                    if (System.nanoTime() - origin - r.due > 100_000_000L) { r.error="SCHEDULER_LATE"; continue; }
                    if (!slots.tryAcquire()) { r.error="NO_WORKER"; continue; }
                    executor.execute(() -> {
                        r.start = System.nanoTime() - origin;
                        try { r.result = service.reserve(r.id, 1L, r.user).name(); }
                        catch (Exception e) { r.result="ERROR"; r.error=e.getClass().getSimpleName(); }
                        finally { r.end = System.nanoTime() - origin; slots.release(); }
                    });
                }
                executor.shutdown();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) throw new IllegalStateException("Calls did not drain");
                // Complete the full 60-second observation window, including final low-rate interval.
                long remaining = origin + 60_000_000_000L - System.nanoTime();
                if (remaining > 0) LockSupport.parkNanos(remaining);
                monitor.shutdown(); monitor.awaitTermination(10, TimeUnit.SECONDS);
            }
            try (var w = Files.newBufferedWriter(out.resolve("requests.csv"))) {
                w.write("request_id,user_id,due_ns,start_ns,end_ns,result,error\n");
                for (Row r : rows) w.write(r.id+","+r.user+","+r.due+","+r.start+","+r.end+","+r.result+","+r.error+"\n");
            }
            List<String> pending = redis.opsForList().range(CouponRedisKey.pendingReservations(), 0, -1);
            Set<String> users = redis.opsForSet().members(CouponRedisKey.issueUsers(1L));
            Files.write(out.resolve("pending.txt"), pending);
            Files.write(out.resolve("users.txt"), users);
            long stock = Long.parseLong(redis.opsForValue().get(CouponRedisKey.stock(1L)));
            Set<String> storedIds = new HashSet<>(), storedUsers = new HashSet<>();
            int missingSuccess=0, unexpectedStored=0, mismatches=0, errorStored=0;
            try (var w = Files.newBufferedWriter(out.resolve("reservations.csv"))) {
                w.write("request_id,user_id,exists,stored_request_id,stored_coupon_id,stored_user_id,reserved_at\n");
                for (Row r : rows) {
                    var h = redis.opsForHash().entries(CouponRedisKey.reservation(r.id));
                    boolean exists = !h.isEmpty();
                    if (exists) {
                        storedIds.add(r.id); storedUsers.add(String.valueOf(r.user));
                        if (!r.id.equals(h.get("requestId")) || !"1".equals(h.get("couponId")) ||
                                !String.valueOf(r.user).equals(h.get("userId")) || h.get("reservedAt") == null) mismatches++;
                        if (r.result.equals("ERROR")) errorStored++;
                        else if (!r.result.equals("RESERVED")) unexpectedStored++;
                    } else if (r.result.equals("RESERVED")) missingSuccess++;
                    w.write(r.id+","+r.user+","+exists+","+h.get("requestId")+","+h.get("couponId")+","+h.get("userId")+","+h.get("reservedAt")+"\n");
                }
            }
            boolean valid = missingSuccess==0 && unexpectedStored==0 && mismatches==0 &&
                    stock==100000-storedIds.size() && users.equals(storedUsers) &&
                    pending.size()==storedIds.size() && new HashSet<>(pending).equals(storedIds);
            Files.writeString(out.resolve("integrity.txt"), "valid="+valid+"\nstock="+stock+"\nreservations="+storedIds.size()+
                    "\nusers="+users.size()+"\npending="+pending.size()+"\nmissing_success="+missingSuccess+
                    "\nunexpected_stored="+unexpectedStored+"\nfield_mismatches="+mismatches+"\nerror_but_stored="+errorStored+"\n");
            if (!valid) throw new AssertionError("Integrity failed");
            System.out.println("Completed: "+out+" reservations="+storedIds.size()+" integrity="+valid);
        } finally {
            executor.shutdownNow(); monitor.shutdownNow();
            f.destroy(); monitorFactory.destroy();
        }
    }
}

package com.assignment.coupon_system.issuerequest.profiling;
import com.assignment.coupon_system.coupon.redis.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class LuaReservationPreparationTest {
 @Container static GenericContainer<?> server=new GenericContainer<>("redis:7-alpine").withExposedPorts(6379).withCommand("redis-server","--appendonly","yes","--appendfsync","always");
 static LettuceConnectionFactory factory;static StringRedisTemplate redis;
 CouponIssueRedisService service;LuaReservationPreparation candidate;
 String a=UUID.randomUUID().toString(),b=UUID.randomUUID().toString();
 @BeforeAll static void connect(){factory=new LettuceConnectionFactory(server.getHost(),server.getMappedPort(6379));factory.afterPropertiesSet();redis=new StringRedisTemplate(factory);}
 @AfterAll static void close(){factory.destroy();}
 @BeforeEach void setup(){try(var c=factory.getConnection()){c.serverCommands().flushDb();}service=new CouponIssueRedisService(redis);candidate=new LuaReservationPreparation(redis);service.initializeReservation(1L,10,Instant.now().minusSeconds(60),Instant.now().plusSeconds(3600));}
 void reserve(){service.reserve(a,1L,1L);service.reserve(b,1L,2L);}
 Map<String,String> snapshot(){Map<String,String> s=new TreeMap<>();for(String k:redis.keys("*"))s.put(k,Base64.getEncoder().encodeToString(redis.dump(k)));return s;}
 @Test void emptyDoesNotMutate(){var before=snapshot();assertThat(candidate.prepare()).isNull();assertThat(snapshot()).isEqualTo(before);}
 @Test void processingWinsAndRetryDoesNotMoveFollowingRequest(){reserve();var expected=service.findReservation(a);assertThat(candidate.prepare()).isEqualTo(expected);assertThat(redis.opsForList().range(CouponRedisKey.pendingReservations(),0,-1)).containsExactly(b);var state=snapshot();assertThat(candidate.prepare()).isEqualTo(expected);assertThat(snapshot()).isEqualTo(state);service.completeDelivery(a);assertThat(candidate.prepare()).isEqualTo(service.findReservation(b));}
 @ParameterizedTest @ValueSource(strings={"missing","wrong-hash-type","missing-field","id-conflict","numeric","overflow","pending-type","processing-type"})
 void badDataLeavesEveryKeyUnchanged(String fault){reserve();String key=CouponRedisKey.reservation(a);
  switch(fault){
   case "missing"->redis.delete(key);
   case "wrong-hash-type"->{redis.delete(key);redis.opsForValue().set(key,"broken");}
   case "missing-field"->redis.opsForHash().delete(key,"userId");
   case "id-conflict"->redis.opsForHash().put(key,"requestId",b);
   case "numeric"->redis.opsForHash().put(key,"couponId","NaN");
   case "overflow"->redis.opsForHash().put(key,"userId","9223372036854775808");
   case "pending-type"->{redis.delete(CouponRedisKey.pendingReservations());redis.opsForValue().set(CouponRedisKey.pendingReservations(),a);}
   case "processing-type"->redis.opsForValue().set(CouponRedisKey.processingReservations(),a);
  }
  var before=snapshot();assertThatThrownBy(()->candidate.prepare()).isInstanceOf(RuntimeException.class);assertThat(snapshot()).isEqualTo(before);
 }
 @Test void corruptProcessingBlocksFollowingPendingWithoutLosingEither(){reserve();service.claimPendingReservation();redis.opsForHash().delete(CouponRedisKey.reservation(a),"reservedAtMillis");var before=snapshot();assertThatThrownBy(()->candidate.prepare()).isInstanceOf(RuntimeException.class);assertThat(snapshot()).isEqualTo(before);}
 @Test void javaLongPrecisionAndSignArePreserved(){reserve();redis.opsForHash().put(CouponRedisKey.reservation(a),"userId","9223372036854775807");redis.opsForHash().put(CouponRedisKey.reservation(a),"reservedAtMillis","-9223372036854775808");assertThat(candidate.prepare()).isEqualTo(service.findReservation(a));}
 @Test void adapterServesPreparedSnapshotAndDoesNotRemoveProcessing(){reserve();var wrapped=candidate.wrap(service);assertThat(wrapped.peekProcessingReservation()).isEqualTo(a);assertThat(wrapped.findReservation(a)).isEqualTo(service.findReservation(a));assertThat(redis.opsForList().range(CouponRedisKey.processingReservations(),0,-1)).containsExactly(a);wrapped.completeDelivery(a);assertThat(wrapped.peekProcessingReservation()).isEqualTo(b);}
}

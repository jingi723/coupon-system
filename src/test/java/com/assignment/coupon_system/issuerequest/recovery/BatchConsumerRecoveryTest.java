package com.assignment.coupon_system.issuerequest.recovery;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.time.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class BatchConsumerRecoveryTest extends ReservationTestSupport {
 long coupon,baseline;String prefix;List<String> ids=new ArrayList<>();
 void setup(String boundary)throws Exception{
  launch("before",true,boundary,null);baseline=endOffset();admin.alterConsumerGroupOffsets(group,Map.of(PARTITION,new OffsetAndMetadata(baseline))).all().get(10,TimeUnit.SECONDS);
  Instant start=Instant.now().minusSeconds(3600),end=Instant.now().plusSeconds(3600);
  coupon=request("POST","/api/coupons",Map.of("name","batch-test","description","fixture","couponType","FIXED_AMOUNT","minOrderAmount",0,"maxDiscountAmount",1000,"totalQuantity",100,"validDays",30,"startDateTime",LocalDateTime.ofInstant(start,ZoneId.of("Asia/Seoul")).toString(),"endDateTime",LocalDateTime.ofInstant(end,ZoneId.of("Asia/Seoul")).toString()),200).path("data").path("id").asLong();
  assertThat(reservations.initializeReservation(coupon,100,start,end)).isTrue();prefix="/api/coupons/"+coupon;
 }
 void add(int n)throws Exception{for(int i=0;i<n;i++){String id=UUID.randomUUID().toString();ids.add(id);submit(prefix,id,100+ids.size());}await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertThat(endOffset()).isEqualTo(baseline+ids.size()));}
 void start()throws Exception{request("POST","/__test/batch/start",Map.of(),200);}
 void finish()throws Exception{
  await().atMost(Duration.ofSeconds(90)).untilAsserted(()->{assertCounts(coupon,ids.size());assertThat(committedOffset()).isEqualTo(baseline+ids.size());});
  for(String id:ids)assertThat(query(prefix,id).path("status").asText()).isEqualTo("ISSUED");
  assertThat(jdbc.queryForList("select request_id from coupon_issue_result where coupon_id=?",String.class,coupon)).containsExactlyInAnyOrderElementsOf(ids);
  assertThat(redis.opsForValue().get(com.assignment.coupon_system.coupon.redis.CouponRedisKey.stock(coupon))).isEqualTo(String.valueOf(100-ids.size()));
  save("final.json",Map.of("ids",ids,"results",results(),"baseline",baseline,"committed",committedOffset(),"end",endOffset()));
 }
 List<Map<String,Object>> results(){return jdbc.queryForList("select id,request_id,user_id,issued_coupon_id from coupon_issue_result where coupon_id=? order by id",coupon);}
 @Test void partialTailAndSingleRequestFlushWithoutFurtherTraffic()throws Exception{
  setup(null);add(13);start();finish();
  var batches=Files.readAllLines(evidence.resolve("before/batches.jsonl"));assertThat(batches).hasSize(2);
  assertThat(JSON.readTree(batches.get(0)).path("requestIds").size()).isEqualTo(10);assertThat(JSON.readTree(batches.get(1)).path("requestIds").size()).isEqualTo(3);
  add(1);finish();
  var tail=JSON.readTree(Files.readAllLines(evidence.resolve("before/batches.jsonl")).get(2));assertThat(tail.path("requestIds").size()).isEqualTo(1);assertThat(tail.path("transactionDuringCollection").asBoolean()).isFalse();assertThat(tail.path("gatherNs").asLong()).isBetween(8_000_000L,250_000_000L);
 }
 @Test void failedBatchRollsBackAndRestartReplaysFailedAndFollowingRecords()throws Exception{
  setup(null);add(13);jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT batch_stream_failure CHECK(coupon_id<>"+coupon+" OR user_id<>105)");
  try{start();await().atMost(Duration.ofSeconds(30)).until(()->Files.exists(evidence.resolve("before/failure.jsonl")));await().atMost(Duration.ofSeconds(10)).until(()->!request("GET","/__test/batch/status",null,200).path("running").asBoolean());assertCounts(coupon,0);assertThat(committedOffset()).isEqualTo(baseline);save("failed.json",Map.of("results",results(),"committed",committedOffset()));}
  finally{jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK batch_stream_failure");}
  stopApp();launch("after",true);start();finish();
  var old=JSON.readTree(Files.readAllLines(evidence.resolve("before/batches.jsonl")).get(0));var replay=JSON.readTree(Files.readAllLines(evidence.resolve("after/batches.jsonl")).get(0));assertThat(replay.path("offsets")).isEqualTo(old.path("offsets"));assertThat(replay.path("requestIds")).isEqualTo(old.path("requestIds"));
 }
 @Test void killAfterBatchCommitBeforeOffsetReplaysWithoutAdditionalIssuance()throws Exception{
  setup("after-db-commit");add(13);start();await().atMost(Duration.ofSeconds(30)).until(()->Files.exists(evidence.resolve("before/after-db-commit.jsonl")));
  assertCounts(coupon,10);assertThat(committedOffset()).isEqualTo(baseline);var before=results();save("committed-before-kill.json",Map.of("results",before,"committed",committedOffset()));
  app.destroyForcibly();assertThat(app.waitFor(10,TimeUnit.SECONDS)).isTrue();launch("after",true);start();finish();assertThat(results().subList(0,10)).isEqualTo(before);
  var old=JSON.readTree(Files.readAllLines(evidence.resolve("before/batches.jsonl")).get(0));var replay=JSON.readTree(Files.readAllLines(evidence.resolve("after/batches.jsonl")).get(0));assertThat(replay.path("offsets")).isEqualTo(old.path("offsets"));
 }
}

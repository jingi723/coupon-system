package com.assignment.coupon_system.issuerequest.recovery;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.time.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ProductionBatchConsumerRecoveryTest extends ReservationTestSupport {
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
  var commits=Files.readAllLines(evidence.resolve("before/commit-attempts.jsonl"));
  assertThat(commits).hasSize(2);
  assertThat(JSON.readTree(commits.get(0)).path("nextOffset").asLong()).isEqualTo(baseline+10);
  assertThat(JSON.readTree(commits.get(1)).path("nextOffset").asLong()).isEqualTo(baseline+13);
  add(1);finish();
  assertThat(Files.readAllLines(evidence.resolve("before/commit-attempts.jsonl"))).hasSize(3);
  assertThat(request("GET","/__test/batch/status",null,200).path("singleRunning").asBoolean()).isFalse();
  request("POST","/__test/batch/stop",Map.of(),200);
  assertThat(request("GET","/__test/batch/status",null,200).path("running").asBoolean()).isFalse();
 }
 @Test void failedBatchRollsBackAndRestartReplaysFailedAndFollowingRecords()throws Exception{
  setup(null);add(13);jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT production_batch_failure CHECK(coupon_id<>"+coupon+" OR user_id<>105)");
  try{
   start();await().atMost(Duration.ofSeconds(30)).until(()->!request("GET","/__test/batch/status",null,200).path("running").asBoolean());
   assertCounts(coupon,0);assertThat(committedOffset()).isEqualTo(baseline);
   assertThat(evidence.resolve("before/commit-attempts.jsonl")).doesNotExist();
   save("failed.json",Map.of("results",results(),"committed",committedOffset()));
  } finally{jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK production_batch_failure");}
  stopApp();launch("after",true);start();finish();
 }
 @Test void killAfterBatchCommitBeforeOffsetReplaysWithoutAdditionalIssuance()throws Exception{
  setup("after-db-commit");add(13);start();await().atMost(Duration.ofSeconds(30)).until(()->Files.exists(evidence.resolve("before/after-db-commit.jsonl")));
  assertCounts(coupon,10);assertThat(committedOffset()).isEqualTo(baseline);var before=results();save("committed-before-kill.json",Map.of("results",before,"committed",committedOffset()));
  app.destroyForcibly();assertThat(app.waitFor(10,TimeUnit.SECONDS)).isTrue();launch("after",true);start();finish();assertThat(results().subList(0,10)).isEqualTo(before);
  assertThat(JSON.readTree(Files.readAllLines(evidence.resolve("after/commit-attempts.jsonl")).get(0)).path("nextOffset").asLong()).isEqualTo(baseline+10);
 }
}

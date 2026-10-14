package com.assignment.coupon_system.issuerequest.profiling;

import com.assignment.coupon_system.coupon.redis.*;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import com.assignment.coupon_system.issuerequest.config.CouponKafkaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;
import org.springframework.web.bind.annotation.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/** Test-only single-partition consumer. Shared implementation for size1 and size10 controls. */
public class BatchConsumerCandidate implements SmartLifecycle {
 final ConsumerFactory<String,CouponIssueMessage> factory;
 final CouponIssueRedisService redis;final CouponIssueProcessor processor;final TransactionTemplate tx;
 final int max=Integer.getInteger("batch.candidate.size",10);final long waitNs=10_000_000L;
 final Path dir=Path.of(System.getProperty("batch.candidate.dir"));final ObjectMapper json=new ObjectMapper();
 volatile boolean running;volatile Consumer<String,CouponIssueMessage> consumer;Thread thread;
 final long anchorNano=System.nanoTime(),anchorWall=System.currentTimeMillis();
 final List<String> spans=new ArrayList<>();long seq;long violations;
 record Pending(ConsumerRecord<String,CouponIssueMessage> record,long received){}
 public BatchConsumerCandidate(ConsumerFactory<String,CouponIssueMessage> factory,CouponIssueRedisService redis,CouponIssueProcessor processor,PlatformTransactionManager manager){this.factory=factory;this.redis=redis;this.processor=processor;tx=new TransactionTemplate(manager);if(max!=1&&max!=10)throw new IllegalArgumentException("size must be1 or10");}
 @TestConfiguration(proxyBeanMethods=false) public static class Config {
  @Bean BatchConsumerCandidate batchWorker(ConsumerFactory<String,CouponIssueMessage> f,CouponIssueRedisService r,CouponIssueProcessor p,PlatformTransactionManager t){return new BatchConsumerCandidate(f,r,p,t);}
  @Bean ApplicationListener<ApplicationReadyEvent> batchReady(BatchConsumerCandidate w){return e->{
   w.marker("ready.jsonl",Map.of("port",e.getApplicationContext().getEnvironment().getProperty("local.server.port"),"pid",ProcessHandle.current().pid(),"batchMax",w.max,"waitMs",10,"anchorNano",w.anchorNano,"anchorWall",w.anchorWall,"autoCommit",false,"commitMode","explicit sync after DB","maxPollRecords",10));
   if(Boolean.getBoolean("batch.candidate.start"))w.start();
  };}
 }
 @RestController @Profile("batch-probe") public static class Controls {
  final BatchConsumerCandidate w;public Controls(BatchConsumerCandidate w){this.w=w;}
  @PostMapping("/__test/batch/start") public Map<String,Boolean> start(){w.start();return status();}
  @GetMapping("/__test/batch/status") public Map<String,Boolean> status(){return Map.of("running",w.isRunning());}
 }
 synchronized void marker(String name,Object data){try{Files.createDirectories(dir);Files.writeString(dir.resolve(name),json.writeValueAsString(data)+"\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);}catch(Exception e){throw new IllegalStateException(e);}}
 void outside(){if(TransactionSynchronizationManager.isActualTransactionActive()){violations++;throw new IllegalStateException("transaction during collection/approval");}}
 @Override public synchronized void start(){if(running)return;running=true;thread=new Thread(this::loop,"batch-candidate");thread.start();}
 @Override public void stop(){running=false;var c=consumer;if(c!=null)c.wakeup();try{if(thread!=null)thread.join(15000);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
 @Override public boolean isRunning(){return running;}
 @Override public boolean isAutoStartup(){return false;}
 @Override public int getPhase(){return Integer.MAX_VALUE;}
 void loop(){
  Properties overrides=new Properties();overrides.put("max.poll.records","10");overrides.put("enable.auto.commit","false");
  try(var c=factory.createConsumer(null,"batch-candidate",null,overrides)){
   consumer=c;Deque<Pending> buffer=new ArrayDeque<>();
   c.subscribe(List.of(CouponKafkaConfig.ISSUE_TOPIC),new ConsumerRebalanceListener(){public void onPartitionsRevoked(Collection<TopicPartition> ps){}public void onPartitionsAssigned(Collection<TopicPartition> ps){if(ps.size()!=1)throw new IllegalStateException("single partition required");System.out.println("partitions assigned: [coupon-issue-requests-0]");}});
   while(running){
    outside();if(buffer.isEmpty()){poll(c,buffer,Duration.ofMillis(100));if(buffer.isEmpty())continue;}
    long collect=System.nanoTime();long deadline=buffer.peekFirst().received()+waitNs;List<Pending> batch=new ArrayList<>();
    while(batch.size()<max){
     outside();while(!buffer.isEmpty()&&batch.size()<max)batch.add(buffer.removeFirst());
     long remaining=deadline-System.nanoTime();if(batch.size()==max||remaining<=0)break;
     poll(c,buffer,Duration.ofNanos(remaining));
    }
    long gathered=System.nanoTime();outside();
    if(Boolean.getBoolean("batch.candidate.trace"))marker("batches.jsonl",Map.of("requestIds",batch.stream().map(p->p.record().value().requestId()).toList(),"offsets",batch.stream().map(p->p.record().offset()).toList(),"transactionDuringCollection",TransactionSynchronizationManager.isActualTransactionActive(),"gatherNs",gathered-collect));
    List<CouponReservation> approved=new ArrayList<>();
    for(var p:batch){var m=p.record().value();if(m==null||m.requestId()==null||m.receivedAt()==null)throw new IllegalArgumentException("invalid message");var r=redis.findReservation(m.requestId());if(!r.couponId().equals(m.couponId())||!r.userId().equals(m.userId())||!r.reservedAt().equals(m.receivedAt()))throw new IllegalArgumentException("reservation mismatch");approved.add(r);}
    long dbStart=System.nanoTime();tx.executeWithoutResult(s->{for(var r:approved)processor.processReserved(r.requestId(),r.couponId(),r.userId(),r.reservedAt());});long dbEnd=System.nanoTime();outside();
    if("after-db-commit".equals(System.getProperty("batch.candidate.hold"))){marker("after-db-commit.jsonl",Map.of("ids",approved.stream().map(CouponReservation::requestId).toList(),"transactionActive",false));new CountDownLatch(1).await();}
    var last=batch.get(batch.size()-1).record();long offset=last.offset()+1;long commitStart=System.nanoTime();c.commitSync(Map.of(new TopicPartition(last.topic(),last.partition()),new OffsetAndMetadata(offset)));long commitEnd=System.nanoTime();
    long batchId=++seq;
    for(var p:batch)spans.add(p.record().value().requestId()+","+batchId+","+batch.size()+","+p.record().offset()+","+p.received()+","+collect+","+gathered+","+dbStart+","+dbEnd+","+commitStart+","+commitEnd);
   }
  }catch(WakeupException e){if(running)marker("failure.jsonl",Map.of("error",e.toString()));}
   catch(Throwable e){marker("failure.jsonl",Map.of("error",e.toString()));e.printStackTrace();}
  finally{running=false;consumer=null;dump();}
 }
 void poll(Consumer<String,CouponIssueMessage> c,Deque<Pending> buffer,Duration duration){outside();var records=c.poll(duration);long received=System.nanoTime();for(var r:records)buffer.addLast(new Pending(r,received));}
 synchronized void dump(){try{Files.createDirectories(dir);Files.writeString(dir.resolve("batch-spans.csv"),"request_id,batch_id,batch_size,offset,received_ns,collect_start_ns,gathered_ns,db_start_ns,db_end_ns,commit_start_ns,commit_end_ns\n"+String.join("\n",spans)+"\n");Files.writeString(dir.resolve("batch-counters.json"),json.writeValueAsString(Map.of("batches",seq,"requests",spans.size(),"violations",violations)));}catch(Exception e){throw new IllegalStateException(e);}}
}

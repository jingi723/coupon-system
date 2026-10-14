package com.assignment.coupon_system.issuerequest.profiling;

import com.assignment.coupon_system.CouponSystemApplication;
import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.issuerequest.relay.CouponReservationRelay;
import com.assignment.coupon_system.issuerequest.producer.CouponIssueProducer;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import java.nio.file.*;
import java.io.*;
import java.util.*;

/** Test-only: single relay thread, bounded memory; write only after process shutdown begins. */
public class RelayProfileApplication {
 static final boolean ON=Boolean.getBoolean("relay.profile.enabled");
 static final ThreadLocal<Attempt> CURRENT=new ThreadLocal<>();
 static final List<Attempt> ROWS=new ArrayList<>(20000);
 static long empty, failed, dropped;
 static class Attempt {long start,end; String id=""; long[] costs=new long[5]; int[] calls=new int[5];}
 public static void main(String[] args) {
  Runtime.getRuntime().addShutdownHook(new Thread(()->{
   try(var w=Files.newBufferedWriter(Path.of(System.getProperty("relay.profile.dir"),"relay-spans.csv"))) {
    w.write("request_id,start_ns,end_ns,peek_ns,claim_ns,lookup_ns,kafka_ack_ns,remove_ns,peek_calls,claim_calls,lookup_calls,kafka_calls,remove_calls\n");
    synchronized(ROWS) { for(var a:ROWS) {
     w.write(a.id+","+a.start+","+a.end);
     for(long v:a.costs)w.write(","+v);
     for(int v:a.calls)w.write(","+v);
     w.write("\n");
    }}
    Files.writeString(Path.of(System.getProperty("relay.profile.dir"),"relay-counters.txt"),"enabled="+ON+"\nempty="+empty+"\nfailed="+failed+"\ndropped="+dropped+"\n");
   }catch(IOException e){throw new UncheckedIOException(e);}
  }));
  Class<?>[] configs=System.getProperty("batch.candidate.size")!=null
    ? new Class<?>[]{CouponSystemApplication.class,Config.class,BatchConsumerCandidate.Config.class}
    : System.getProperty("consumer.profile.dir")!=null
    ? new Class<?>[]{CouponSystemApplication.class,Config.class,ReservedConsumerProfile.Config.class}
    : new Class<?>[]{CouponSystemApplication.class,Config.class};
  SpringApplication.run(configs,args);
 }
 @TestConfiguration(proxyBeanMethods=false)
 public static class Config {
  @Bean static BeanPostProcessor relayProbe(org.springframework.data.redis.core.StringRedisTemplate template){return new BeanPostProcessor(){
   public Object postProcessAfterInitialization(Object bean,String name) {
    if(!ON)return bean;
    if(!(bean instanceof CouponReservationRelay || bean instanceof CouponIssueRedisService || bean instanceof CouponIssueProducer))return bean;
    Object target=Boolean.getBoolean("relay.prepare.lua") && bean instanceof CouponIssueRedisService service ? new LuaReservationPreparation(template).wrap(service) : bean;
    ProxyFactory p=new ProxyFactory(target);p.setProxyTargetClass(true);
    p.addAdvice((MethodInterceptor)i->{
     String m=i.getMethod().getName();
     if(bean instanceof CouponReservationRelay && m.equals("relayOne")) {
      Attempt a=new Attempt();CURRENT.set(a);a.start=System.nanoTime();boolean ok=false;
      try {Object result=i.proceed();ok=Boolean.TRUE.equals(result);return result;}
      catch(Throwable t){failed++;throw t;}
      finally {a.end=System.nanoTime();CURRENT.remove();if(ok){synchronized(ROWS){if(ROWS.size()<100000)ROWS.add(a);else dropped++;}}else empty++;}
     }
     Attempt a=CURRENT.get();
     int slot=bean instanceof CouponIssueProducer && m.equals("send")?3:
       bean instanceof CouponIssueRedisService?switch(m){case "peekProcessingReservation"->0;case "claimPendingReservation"->1;case "findReservation"->2;case "completeDelivery"->4;default->-1;}:-1;
     if(a==null || slot<0)return i.proceed();
     if(slot==2)a.id=(String)i.getArguments()[0];
     long start=System.nanoTime();
     try{return i.proceed();}finally{a.costs[slot]+=System.nanoTime()-start;a.calls[slot]++;}
    });return p.getProxy();
   }
  };}
 }
}

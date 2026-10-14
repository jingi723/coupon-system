package com.assignment.coupon_system.issuerequest.profiling;

import com.assignment.coupon_system.CouponSystemApplication;
import com.assignment.coupon_system.issuerequest.consumer.CouponIssueConsumer;
import com.assignment.coupon_system.issuerequest.dto.CouponIssueMessage;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.transaction.*;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.lang.reflect.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;

/** Only on test classpath; no production bean/config changes. Numeric spans are flushed after drain. */
public class ReservedConsumerProfile {
 static final boolean ENABLED=Boolean.getBoolean("consumer.profile.enabled");
 static final Path OUT=Path.of(System.getProperty("consumer.profile.dir"));
 static final Probe PROBE=new Probe();
 static final ObjectMapper JSON=new ObjectMapper();
 static final ThreadLocal<Boolean> IN_DB=ThreadLocal.withInitial(()->false);
 static final ThreadLocal<Boolean> IN_LISTENER=ThreadLocal.withInitial(()->false);
 static final ThreadLocal<String> REQUEST=new ThreadLocal<>();
 static final ThreadLocal<Long> LISTENER_START=new ThreadLocal<>();
 static final ThreadLocal<Long> COMMIT_START=new ThreadLocal<>();
 static void json(String name,Object data) {
  try {Files.writeString(OUT.resolve(name),JSON.writeValueAsString(data));}
  catch(IOException e){throw new IllegalStateException(e);}
 }
 public static void main(String[] args){SpringApplication.run(new Class<?>[]{CouponSystemApplication.class,Config.class},args);}

 @TestConfiguration(proxyBeanMethods=false)
 public static class Config {
  @Bean static Wrappers profileWrappers(){return new Wrappers();}
  @Bean SmartInitializingSingleton profileTransactions(CouponIssueProcessor processor,PlatformTransactionManager manager) {
   return ()->{
    if(!ENABLED)return;
    if(!(processor instanceof Advised advised))throw new IllegalStateException("Processor must be transaction proxy");
    if(Arrays.stream(advised.getAdvisors()).noneMatch(a->a.getAdvice() instanceof TransactionInterceptor))
     throw new IllegalStateException("No transaction interceptor in processor chain");
    json("transaction-advisors-before.json",Arrays.stream(advised.getAdvisors()).map(a->a.getAdvice().getClass().getName()).toList());
    advised.addAdvice(0,(MethodInterceptor) invocation->{
     if(!invocation.getMethod().getName().equals("processReserved"))return invocation.proceed();
     if(TransactionSynchronizationManager.isActualTransactionActive())PROBE.violation("outer_tx_at_entry");
     long start=System.nanoTime();
     if(!IN_LISTENER.get() || LISTENER_START.get()==null)PROBE.violation("db_outside_listener");
     else PROBE.add("approval",LISTENER_START.get(),start,REQUEST.get(),1);
     IN_DB.set(true);boolean ok=false;
     try {Object value=invocation.proceed();ok=true;return value;}
     finally {
      long end=System.nanoTime();
      if(TransactionSynchronizationManager.isActualTransactionActive())PROBE.violation("outer_tx_at_return");
      IN_DB.set(false);PROBE.add("db_proxy",start,end,(String)invocation.getArguments()[0],ok?1:0);
     }
    });
    ((ConfigurableTransactionManager)manager).addListener(new TransactionExecutionListener(){
     public void afterBegin(TransactionExecution tx,Throwable failure){
      if(IN_DB.get()){
       if(failure!=null || !TransactionSynchronizationManager.isActualTransactionActive())PROBE.violation("begin_boundary");
       PROBE.increment("db_tx_begins");
      }
     }
     public void beforeCommit(TransactionExecution tx){if(IN_DB.get())COMMIT_START.set(System.nanoTime());}
     public void afterCommit(TransactionExecution tx,Throwable failure){
      if(IN_DB.get()){
       Long start=COMMIT_START.get();if(start==null)PROBE.violation("commit_without_start");
       else PROBE.add("db_commit_callbacks",start,System.nanoTime(),REQUEST.get(),failure==null?1:0);
       COMMIT_START.remove();
      }
     }
    });
   };
  }
  @Bean ApplicationListener<ApplicationReadyEvent> profileReady(KafkaListenerEndpointRegistry registry,ConfigurableApplicationContext context){
   return event->{
    Map<String,Object> m=new LinkedHashMap<>();m.put("enabled",ENABLED);m.put("pid",ProcessHandle.current().pid());
    m.put("anchor_wall_ms",PROBE.wall);m.put("anchor_nano",PROBE.nano);
    m.put("containers",registry.getListenerContainers().stream().map(c->Map.of("ackMode",c.getContainerProperties().getAckMode().name(),"syncCommits",c.getContainerProperties().isSyncCommits(),"pollTimeout",c.getContainerProperties().getPollTimeout())).toList());
    json("profile-ready.json",m);
   };
  }
  @Bean ApplicationListener<ContextClosedEvent> dumpProfile(){return event->PROBE.dump();}
 }

 static class Wrappers implements BeanPostProcessor,Ordered {
  public int getOrder(){return Ordered.HIGHEST_PRECEDENCE;}
  @SuppressWarnings({"unchecked","rawtypes"})
  public Object postProcessAfterInitialization(Object bean,String name){
   if(!ENABLED)return bean;
   if(bean instanceof DefaultKafkaConsumerFactory factory){
    factory.addPostProcessor(consumer->(Consumer)Proxy.newProxyInstance(Consumer.class.getClassLoader(),new Class[]{Consumer.class},(proxy,method,args)->{
     String kind=method.getName();if(kind.equals("poll"))REQUEST.remove();boolean timed=kind.equals("commitSync")||kind.equals("commitAsync")||kind.equals("poll");
     long start=timed?System.nanoTime():0;long aux=1;
     if(kind.startsWith("commit") && args!=null && args.length>0 && args[0] instanceof Map<?,?> map && !map.isEmpty())
      aux=((org.apache.kafka.clients.consumer.OffsetAndMetadata)map.values().iterator().next()).offset();
     if(kind.startsWith("commit")&&IN_LISTENER.get())PROBE.violation("offset_inside_listener");
     try {
      Object result=method.invoke(consumer,args);
      if(result instanceof ConsumerRecords records)aux=records.count();
      return result;
     }catch(InvocationTargetException e){aux=-1;throw e.getCause();}
     finally {if(timed)PROBE.add(kind,start,System.nanoTime(),kind.equals("poll")?null:REQUEST.get(),aux);}
    }));
   }
   if(bean instanceof com.assignment.coupon_system.coupon.redis.CouponIssueRedisService){
    ProxyFactory proxy=new ProxyFactory(bean);proxy.setProxyTargetClass(true);
    proxy.addAdvice((MethodInterceptor)i->{
     if(!IN_LISTENER.get() || !i.getMethod().getName().equals("findReservation"))return i.proceed();
     long start=System.nanoTime();boolean ok=false;
     try {Object r=i.proceed();ok=true;return r;}
     finally {PROBE.add("redis_lookup",start,System.nanoTime(),REQUEST.get(),ok?1:0);}
    });return proxy.getProxy();
   }
   if(bean instanceof CouponIssueConsumer){
    ProxyFactory proxy=new ProxyFactory(bean);proxy.setProxyTargetClass(true);
    proxy.addAdvice((MethodInterceptor)invocation->{
     if(!invocation.getMethod().getName().equals("consume"))return invocation.proceed();
     String id=((CouponIssueMessage)invocation.getArguments()[0]).requestId();REQUEST.set(id);IN_LISTENER.set(true);
     long start=System.nanoTime();LISTENER_START.set(start);boolean ok=false;
     try {Object value=invocation.proceed();ok=true;return value;}
     finally {long end=System.nanoTime();IN_LISTENER.set(false);PROBE.add("listener_method",start,end,id,ok?1:0);}
    });return proxy.getProxy();
   }
   return bean;
  }
 }
 record Span(String kind,long start,long end,long thread,String request,long aux){}
 static class Probe {
  final long nano=System.nanoTime(),wall=System.currentTimeMillis();
  final List<Span> spans=new ArrayList<>(100000);
  final Map<String,Long> counters=new LinkedHashMap<>();
  synchronized void increment(String key){counters.merge(key,1L,Long::sum);}
  synchronized void violation(String key){increment("violation_"+key);}
  synchronized void add(String kind,long start,long end,String request,long aux){
   if(spans.size()>=500000){increment("dropped_spans");return;}
   spans.add(new Span(kind,start,end,Thread.currentThread().getId(),request,aux));
  }
  synchronized void dump(){
   try(PrintWriter w=new PrintWriter(Files.newBufferedWriter(OUT.resolve("consumer-spans.csv")))){
    w.println("kind,start_ns,end_ns,thread_id,request_id,aux");
    for(Span s:spans)w.printf("%s,%d,%d,%d,%s,%d%n",s.kind,s.start,s.end,s.thread,s.request==null?"":s.request,s.aux);
   }catch(IOException e){throw new IllegalStateException(e);}
   json("profile-counters.json",Map.of("enabled",ENABLED,"spans",spans.size(),"counters",counters,"dump_nano",System.nanoTime()));
  }
 }
}

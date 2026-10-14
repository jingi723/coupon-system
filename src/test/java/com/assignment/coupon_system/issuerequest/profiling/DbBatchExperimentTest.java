package com.assignment.coupon_system.issuerequest.profiling;

import com.assignment.coupon_system.common.LegacyCouponGuard;
import com.assignment.coupon_system.coupon.entity.*;
import com.assignment.coupon_system.coupon.repository.CouponRepository;
import com.assignment.coupon_system.issuerequest.service.CouponIssueProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.hibernate.BaseSessionEventListener;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.annotation.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.*;
import javax.sql.DataSource;
import java.sql.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@DataJpaTest(showSql=false,properties={"spring.jpa.hibernate.ddl-auto=create-drop","coupon.legacy.enabled=false",
 "spring.datasource.hikari.maximum-pool-size=10","spring.jpa.properties.hibernate.generate_statistics=true",
 "logging.level.org.hibernate.stat=OFF","logging.level.org.hibernate.engine.internal.StatisticalLoggingSessionEventListener=OFF"})
@AutoConfigureTestDatabase(replace=AutoConfigureTestDatabase.Replace.NONE)
@Import({CouponIssueProcessor.class,LegacyCouponGuard.class,DbBatchExperimentTest.Config.class})
@Transactional(propagation=Propagation.NOT_SUPPORTED)
@Testcontainers
@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named="RUN_DB_BATCH_EXPERIMENT",matches="true")
class DbBatchExperimentTest {
 @Container @ServiceConnection static final MySQLContainer<?> MYSQL=new MySQLContainer<>("mysql:8.0")
  .withCommand("--innodb-flush-log-at-trx-commit=1","--sync-binlog=1")
  .withCreateContainerCmdModifier(c->c.getHostConfig().withMemory(1073741824L).withNanoCPUs(1000000000L));
 static final Path OUT=Path.of(System.getenv().getOrDefault("DB_BATCH_OUT","build/db-batch-experiment"));
 static final ObjectMapper JSON=new ObjectMapper();
 static final Instant APPROVED=Instant.parse("2026-10-14T00:00:00Z");
 static boolean recording;
 static Map<String,Long> counts=new TreeMap<>(),sql=new TreeMap<>();
 static long commitNanos;
 static void count(String k){if(recording)counts.merge(k,1L,Long::sum);}
 static void save(String name,Object value)throws Exception{Files.createDirectories(OUT);Files.writeString(OUT.resolve(name),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value));}
 static void reset(){counts.clear();sql.clear();commitNanos=0;}
 public static class Events extends BaseSessionEventListener {
  public void flushEnd(int entities,int collections){count("flushEnd");if(recording)counts.merge("flushEntities",(long)entities,Long::sum);}
  public void partialFlushEnd(int entities,int collections){count("partialFlushEnd");}
  public void jdbcExecuteStatementEnd(){count("jdbcExecuteStatementEnd");}
  public void jdbcExecuteBatchEnd(){count("jdbcExecuteBatchEnd");}
 }
 @TestConfiguration(proxyBeanMethods=false) static class Config {
  @Bean HibernatePropertiesCustomizer counters(){return p->{
   p.put("hibernate.session_factory.statement_inspector",(org.hibernate.resource.jdbc.spi.StatementInspector)s->{if(recording)sql.merge(s,1L,Long::sum);return s;});
   p.put("hibernate.session.events.auto",Events.class.getName());
  };}
  @Bean static BeanPostProcessor jdbcCommits(){return new BeanPostProcessor(){
   public Object postProcessAfterInitialization(Object bean,String name){
    if(!(bean instanceof DataSource ds))return bean;
    return new AbstractDataSource(){
     public Connection getConnection()throws SQLException{return wrap(ds.getConnection());}
     public Connection getConnection(String u,String p)throws SQLException{return wrap(ds.getConnection(u,p));}
     Connection wrap(Connection c){return (Connection)Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class[]{Connection.class},(proxy,m,args)->{
      boolean commit=m.getName().equals("commit");boolean rollback=m.getName().equals("rollback");long start=System.nanoTime();
      if(commit)count("commitCalls");if(rollback)count("rollbackCalls");
      try{Object v=m.invoke(c,args);if(commit)count("commitSuccess");return v;}
      catch(InvocationTargetException e){if(commit)count("commitFailures");throw e.getCause();}
      finally{if(commit&&recording)commitNanos+=System.nanoTime()-start;}
     });}
    };
   }
  };}
 }
 @Autowired CouponIssueProcessor processor;
 @Autowired CouponRepository coupons;
 @Autowired JdbcTemplate jdbc;
 @Autowired PlatformTransactionManager manager;
 @Autowired EntityManagerFactory emf;
 long couponId;
 void fixture(){
  jdbc.update("delete from coupon_issue_result");jdbc.update("delete from issued_coupon");jdbc.update("delete from coupon");
  couponId=coupons.save(Coupon.create("DB batch experiment","fixed approved inputs",CouponType.FIXED_AMOUNT,0,1000,100000,30,
   LocalDateTime.ofInstant(APPROVED.minusSeconds(3600),ZoneOffset.UTC),LocalDateTime.ofInstant(APPROVED.plusSeconds(86400),ZoneOffset.UTC))).getId();
 }
 void clear(){jdbc.update("delete from coupon_issue_result");jdbc.update("delete from issued_coupon");jdbc.update("update coupon set issued_quantity=0 where id=?",couponId);}
 void call(int i){assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();processor.processReserved("db-batch-"+i,couponId,100L+i,APPROVED);}
 void check(int n){
  assertThat(jdbc.queryForObject("select count(*) from coupon_issue_result",Long.class)).isEqualTo(n);
  assertThat(jdbc.queryForObject("select count(*) from issued_coupon",Long.class)).isEqualTo(n);
  assertThat(jdbc.queryForObject("select issued_quantity from coupon where id=?",Integer.class,couponId)).isEqualTo(n);
  assertThat(jdbc.queryForObject("select count(*) from coupon_issue_result r join issued_coupon i on r.issued_coupon_id=i.id where r.status='ISSUED' and r.user_id=i.user_id and r.coupon_id=i.coupon_id",Long.class)).isEqualTo(n);
 }
 @Test @Order(1) void rollbackRetryAndRedelivery()throws Exception{
  fixture();var tx=new TransactionTemplate(manager);var evidence=new ArrayList<Object>();
  for(int batch:List.of(1,10)){
   clear();int bad=batch==1?1:5;
   jdbc.execute("ALTER TABLE coupon_issue_result ADD CONSTRAINT batch_failure CHECK (user_id <> "+(100+bad)+")");
   reset();recording=true;
   try{assertThatThrownBy(()->tx.executeWithoutResult(s->{for(int i=1;i<=batch;i++)call(i);})).hasStackTraceContaining("batch_failure");}
   finally{recording=false;jdbc.execute("ALTER TABLE coupon_issue_result DROP CHECK batch_failure");}
   check(0);assertThat(counts.getOrDefault("commitSuccess",0L)).isZero();assertThat(counts.get("rollbackCalls")).isEqualTo(1);
   var failed=new TreeMap<>(counts);
   reset();recording=true;try{tx.executeWithoutResult(s->{for(int i=1;i<=batch;i++)call(i);});}finally{recording=false;}
   check(batch);assertThat(counts.get("commitSuccess")).isEqualTo(1);
   var before=jdbc.queryForList("select request_id,issued_coupon_id from coupon_issue_result order by request_id");
   tx.executeWithoutResult(s->{for(int i=1;i<=batch;i++)call(i);});check(batch);
   assertThat(jdbc.queryForList("select request_id,issued_coupon_id from coupon_issue_result order by request_id")).isEqualTo(before);
   evidence.add(Map.of("batch",batch,"failureAt",bad,"failedTransaction",failed,"retryAndReplay","passed"));
  }
  save("correctness.json",evidence);
 }
 @Test @Order(2) void compareOneAndTen()throws Exception{
  fixture();var tx=new TransactionTemplate(manager);var statistics=emf.unwrap(SessionFactory.class).getStatistics();
  Files.createDirectories(OUT);
  save("environment.json",Map.of("container",MYSQL.getContainerId(),"mysql",jdbc.queryForMap("select @@version version, @@innodb_flush_log_at_trx_commit durable_commit, @@sync_binlog sync_binlog, @@log_bin log_bin"),"cpu",1,"memoryMiB",1024,"concurrency",1,"hikariMax",10));
  new ProcessBuilder("docker","inspect",MYSQL.getContainerId()).redirectOutput(OUT.resolve("container-inspect.json").toFile()).start().waitFor();
  for(int i=1;i<=500;i++){final int k=i;tx.executeWithoutResult(s->call(k));}
  String[] labels={"one-1","ten-1","ten-2","one-2"};int[] sizes={1,10,10,1};int n=17000;
  for(int run=0;run<labels.length;run++){
   clear();reset();statistics.clear();int batch=sizes[run];long[] starts=new long[n],ends=new long[n],done=new long[n];
   long origin=System.nanoTime();recording=true;
   try{
    for(int offset=0;offset<n;offset+=batch){final int first=offset;
     assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
     tx.executeWithoutResult(s->{for(int j=first;j<first+batch;j++){starts[j]=System.nanoTime();call(j+1);ends[j]=System.nanoTime();}});
     long completed=System.nanoTime();assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
     for(int j=first;j<first+batch;j++)done[j]=completed;
    }
   }finally{recording=false;}
   long elapsed=System.nanoTime()-origin;
   var result=new LinkedHashMap<String,Object>();result.put("batch",batch);result.put("requests",n);result.put("elapsedNs",elapsed);result.put("perSecond",n/(elapsed/1e9));result.put("jdbc",new TreeMap<>(counts));result.put("jdbcCommitNs",commitNanos);result.put("sql",new TreeMap<>(sql));
   result.put("hibernate",Map.of("prepareStatements",statistics.getPrepareStatementCount(),"flushes",statistics.getFlushCount(),"transactions",statistics.getTransactionCount(),"successfulTransactions",statistics.getSuccessfulTransactionCount(),"entityInserts",statistics.getEntityInsertCount()));
   assertThat(counts.get("commitSuccess")).isEqualTo(n/batch);assertThat(counts.getOrDefault("rollbackCalls",0L)).isZero();assertThat(statistics.getSuccessfulTransactionCount()).isEqualTo(n/batch);
   check(n);
   var rows=jdbc.queryForList("select request_id,user_id,coupon_id,status,issued_coupon_id from coupon_issue_result order by user_id");
   for(int i=0;i<n;i++){var row=rows.get(i);assertThat(row.get("request_id")).isEqualTo("db-batch-"+(i+1));assertThat(((Number)row.get("user_id")).longValue()).isEqualTo(101L+i);assertThat(((Number)row.get("coupon_id")).longValue()).isEqualTo(couponId);}
   result.put("consistency","passed: exact request/user/coupon mapping, issued result/history join, count and quantity");
   save(labels[run]+"-summary.json",result);save(labels[run]+"-final-results.json",rows);
   try(var w=Files.newBufferedWriter(OUT.resolve(labels[run]+"-times.csv"))){w.write("request_id,call_start_ns,call_end_ns,committed_return_ns,origin_ns\n");for(int i=0;i<n;i++)w.write("db-batch-"+(i+1)+","+starts[i]+","+ends[i]+","+done[i]+","+origin+"\n");}
   System.out.println("DB_BATCH "+labels[run]+" "+result.get("perSecond")+"/s commits="+counts.get("commitSuccess"));
  }
 }
}

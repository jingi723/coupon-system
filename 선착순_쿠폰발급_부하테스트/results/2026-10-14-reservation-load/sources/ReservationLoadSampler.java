import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.attach.VirtualMachine;
import javax.management.*;
import javax.management.remote.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import java.sql.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.time.Instant;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import com.assignment.coupon_system.coupon.redis.*;

// couponId outputDirectory bootstrap group mode appPid
class ReservationLoadSampler {
 public static void main(String[] args) throws Exception {
  long coupon=Long.parseLong(args[0]); Path out=Path.of(args[1]); boolean kafka=true;
  LettuceConnectionFactory factory=new LettuceConnectionFactory(System.getenv("LOAD_REDIS_HOST"),Integer.parseInt(System.getenv("LOAD_REDIS_PORT")));
  factory.afterPropertiesSet(); StringRedisTemplate redis=new StringRedisTemplate(factory);
  if(args[4].equals("init")) {
   try { if(!new CouponIssueRedisService(redis).initializeReservation(coupon,100000,Instant.parse(args[5]),Instant.parse(args[6])))throw new IllegalStateException("Already initialized"); }
   finally {factory.destroy();} return;
  }
  if(args[4].equals("snapshot")) {
   try(Connection db=database()) {
    dump(db,"SELECT id,user_id FROM issued_coupon WHERE coupon_id="+coupon+" ORDER BY id",out.resolve("final-history.tsv"));
    dump(db,"SELECT request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id="+coupon+" ORDER BY id",out.resolve("final-results.tsv"));
    dump(db,"SELECT total_quantity,issued_quantity FROM coupon WHERE id="+coupon,out.resolve("final-stock.tsv"));
    Files.writeString(out.resolve("redis-stock.txt"),redis.opsForValue().get(CouponRedisKey.stock(coupon)));
    Files.writeString(out.resolve("redis-users.txt"),String.join("\n",redis.opsForSet().members(CouponRedisKey.issueUsers(coupon))));
    try(PrintWriter f=new PrintWriter(Files.newBufferedWriter(out.resolve("redis-reservations.csv")))) {
     f.println("request_id,coupon_id,user_id,reserved_at_millis,policy_version");
     for(String key:redis.keys("coupon:reservation:*")) {
      if(key.equals(CouponRedisKey.pendingReservations())||key.equals(CouponRedisKey.processingReservations()))continue;
      var v=redis.opsForHash().entries(key); if(!String.valueOf(coupon).equals(v.get("couponId")))continue;
      f.printf("%s,%s,%s,%s,%s%n",v.get("requestId"),v.get("couponId"),v.get("userId"),v.get("reservedAtMillis"),v.get("policyVersion"));
     }
    }
    Files.writeString(out.resolve("redis-queues.json"),new ObjectMapper().writeValueAsString(Map.of("pending",redis.opsForList().range(CouponRedisKey.pendingReservations(),0,-1),"processing",redis.opsForList().range(CouponRedisKey.processingReservations(),0,-1))));
   } finally {factory.destroy();} return;
  }
  VirtualMachine vm=VirtualMachine.attach(args[5]); String address;
  try { address=vm.startLocalManagementAgent(); } finally { vm.detach(); }
  Properties props=new Properties(); props.put("bootstrap.servers",args[2]);
  props.put("default.api.timeout.ms","10000"); props.put("request.timeout.ms","5000");
  TopicPartition tp=new TopicPartition("coupon-issue-requests",0);
  ObjectMapper json=new ObjectMapper();
  try (Connection db=database();
       JMXConnector jmx=JMXConnectorFactory.connect(new JMXServiceURL(address));
       Admin admin=Admin.create(props);
       PrintWriter samples=new PrintWriter(Files.newBufferedWriter(out.resolve("progress.jsonl")));
       PrintWriter results=new PrintWriter(Files.newBufferedWriter(out.resolve("observed-results.csv")))) {
   MBeanServerConnection server=jmx.getMBeanServerConnection();
   var pools=server.queryNames(new ObjectName("com.zaxxer.hikari:type=Pool (*)"),null);
   var tomcats=server.queryNames(new ObjectName("*:type=ThreadPool,*"),null);
   if (pools.size()!=1 || tomcats.size()!=1) throw new IllegalStateException("Expected one pool and Tomcat: "+pools+tomcats);
   ObjectName pool=pools.iterator().next(), tomcat=tomcats.iterator().next();
   results.println("id,request_id,user_id,status,issued_coupon_id,query_start_ms,observed_ms");results.flush();
   long lastId=0,nextDb=0,observedCount=0; long history=0,quantity=0;
   while(!Files.exists(out.resolve("monitor.stop"))) {
    long tick=System.currentTimeMillis(), nano=System.nanoTime(); Map<String,Object> s=new LinkedHashMap<>();
    s.put("time_ms",tick);s.put("monotonic_ns",nano);
    s.put("app_cpu_ns",server.getAttribute(new ObjectName("java.lang:type=OperatingSystem"),"ProcessCpuTime"));
    s.put("pending",redis.opsForList().size(CouponRedisKey.pendingReservations()));
    s.put("processing",redis.opsForList().size(CouponRedisKey.processingReservations()));
    s.put("pool_waiting",server.getAttribute(pool,"ThreadsAwaitingConnection"));
    s.put("pool_active",server.getAttribute(pool,"ActiveConnections"));
    s.put("http_busy",server.getAttribute(tomcat,"currentThreadsBusy"));
    if(kafka) {
     long queryStart=System.currentTimeMillis();
     try(PreparedStatement q=db.prepareStatement("SELECT id,request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id=? AND id>? ORDER BY id")) {
      q.setLong(1,coupon);q.setLong(2,lastId);
      try(ResultSet r=q.executeQuery()) {long observed=System.currentTimeMillis();while(r.next()) {
       lastId=r.getLong(1);observedCount++;results.printf("%d,%s,%d,%s,%s,%d,%d%n",lastId,r.getString(2),r.getLong(3),r.getString(4),r.getString(5),queryStart,observed);
      }}
     }results.flush();
     s.put("observed_count",observedCount);s.put("result_query_start_ms",queryStart);s.put("result_query_end_ms",System.currentTimeMillis());
     var offsets=admin.listConsumerGroupOffsets(args[3]).partitionsToOffsetAndMetadata().get();
     long committed=offsets.containsKey(tp)?offsets.get(tp).offset():0;
     long end=admin.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get().get(tp).offset();
     s.put("committed",committed);s.put("end",end);s.put("lag",end-committed);
    }
    if(tick>=nextDb) {
     try(Statement q=db.createStatement();ResultSet r=q.executeQuery("SELECT issued_quantity,(SELECT COUNT(*) FROM issued_coupon WHERE coupon_id="+coupon+") FROM coupon WHERE id="+coupon)) {r.next();quantity=r.getLong(1);history=r.getLong(2);}
     try(Statement q=db.createStatement();ResultSet r=q.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected','Innodb_row_lock_current_waits','Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_os_log_written','Innodb_data_written')")) {while(r.next()) s.put(r.getString(1),r.getLong(2));}
     try(var c=factory.getConnection()) {
      for(String section:List.of("cpu","memory","persistence")) {
       Properties info=c.serverCommands().info(section);
       for(String key:List.of("used_cpu_sys","used_cpu_user","used_memory","used_memory_rss","aof_enabled","aof_current_size","aof_pending_bio_fsync","aof_delayed_fsync","aof_rewrite_in_progress"))
        if(info.getProperty(key)!=null)s.put("redis_"+key,Double.parseDouble(info.getProperty(key)));
      }
     }
     nextDb=tick+1000;s.put("db_sample",true);
    }
    s.put("history",history);s.put("quantity",quantity);s.put("sample_end_ms",System.currentTimeMillis());
    samples.println(json.writeValueAsString(s));samples.flush();
    Thread.sleep(Math.max(1,250-(System.nanoTime()-nano)/1000000));
   }
  } finally {factory.destroy();}
 }
 static Connection database() throws SQLException {return DriverManager.getConnection(System.getenv("LOAD_DB_URL"),System.getenv("LOAD_DB_USER"),System.getenv("LOAD_DB_PASSWORD"));}
 static void dump(Connection db,String sql,Path path) throws Exception {
  try(Statement q=db.createStatement();ResultSet r=q.executeQuery(sql);PrintWriter f=new PrintWriter(Files.newBufferedWriter(path))) {
   int n=r.getMetaData().getColumnCount();while(r.next()){for(int i=1;i<=n;i++){if(i>1)f.print("\t");f.print(r.getString(i));}f.println();}
  }
 }
}

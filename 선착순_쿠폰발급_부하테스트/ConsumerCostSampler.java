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

// couponId outputDirectory bootstrap group mode appPid
class ConsumerCostSampler {
 public static void main(String[] args) throws Exception {
  long coupon=Long.parseLong(args[0]); Path out=Path.of(args[1]); boolean kafka=args[4].equals("kafka");
  VirtualMachine vm=VirtualMachine.attach(args[5]); String address;
  try { address=vm.startLocalManagementAgent(); } finally { vm.detach(); }
  Properties props=new Properties(); props.put("bootstrap.servers",args[2]);
  props.put("default.api.timeout.ms","10000"); props.put("request.timeout.ms","5000");
  TopicPartition tp=new TopicPartition("coupon-issue-requests",0);
  ObjectMapper json=new ObjectMapper();
  try (Connection db=DriverManager.getConnection("jdbc:mysql://localhost:13306/coupon_db","coupon","1234");
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
   long lastId=0,nextDb=0; long history=0,quantity=0;
   while(!Files.exists(out.resolve("monitor.stop"))) {
    long tick=System.currentTimeMillis(), nano=System.nanoTime(); Map<String,Object> s=new LinkedHashMap<>();
    s.put("app_process_cpu_ns",server.getAttribute(new ObjectName("java.lang:type=OperatingSystem"),"ProcessCpuTime"));
    s.put("time_ms",tick);s.put("monotonic_ns",nano);
    s.put("pool_waiting",server.getAttribute(pool,"ThreadsAwaitingConnection"));
    s.put("pool_active",server.getAttribute(pool,"ActiveConnections"));
    s.put("http_busy",server.getAttribute(tomcat,"currentThreadsBusy"));
    if(kafka) {
     try(PreparedStatement q=db.prepareStatement("SELECT id,request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id=? AND id>? ORDER BY id")) {
      q.setLong(1,coupon);q.setLong(2,lastId);
      try(ResultSet r=q.executeQuery()) {long observed=System.currentTimeMillis();while(r.next()) {
       lastId=r.getLong(1);results.printf("%d,%s,%d,%s,%s,%d,%d%n",lastId,r.getString(2),r.getLong(3),r.getString(4),r.getString(5),tick,observed);
      }}
     }results.flush();
     var offsets=admin.listConsumerGroupOffsets(args[3]).partitionsToOffsetAndMetadata().get();
     long committed=offsets.containsKey(tp)?offsets.get(tp).offset():0;
     long end=admin.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get().get(tp).offset();
     s.put("committed",committed);s.put("end",end);s.put("lag",end-committed);
    }
    if(tick>=nextDb) {
     try(Statement q=db.createStatement();ResultSet r=q.executeQuery("SELECT issued_quantity,(SELECT COUNT(*) FROM issued_coupon WHERE coupon_id="+coupon+") FROM coupon WHERE id="+coupon)) {r.next();quantity=r.getLong(1);history=r.getLong(2);}
     try(Statement q=db.createStatement();ResultSet r=q.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected','Innodb_row_lock_current_waits','Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_os_log_written','Innodb_log_writes','Innodb_log_waits','Innodb_data_written','Innodb_data_writes','Com_commit','Com_insert','Com_update','Handler_commit')")) {while(r.next()) s.put(r.getString(1),r.getLong(2));}
     for(ObjectName coordinator:server.queryNames(new ObjectName("kafka.consumer:type=consumer-coordinator-metrics,*"),null)) {
      for(String attr:List.of("commit-total","commit-latency-avg","commit-latency-max")) {
       try {s.put("kafka_"+attr,server.getAttribute(coordinator,attr));} catch(AttributeNotFoundException ignored){}
      }
     }
     nextDb=tick+1000;s.put("db_sample",true);
    }
    s.put("history",history);s.put("quantity",quantity);s.put("sample_end_ms",System.currentTimeMillis());
    samples.println(json.writeValueAsString(s));samples.flush();
    Thread.sleep(Math.max(1,250-(System.nanoTime()-nano)/1000000));
   }
  }
 }
}

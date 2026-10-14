import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import java.sql.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;

// args: couponId outputDirectory bootstrapServers groupId
// Read committed results through an independent auto-commit JDBC connection.
class KafkaBurstSampler {
 public static void main(String[] args) throws Exception {
  long coupon = Long.parseLong(args[0]); Path out = Path.of(args[1]);
  ObjectMapper json = new ObjectMapper();
  Properties props = new Properties(); props.put("bootstrap.servers", args[2]);
  props.put("default.api.timeout.ms", "10000"); props.put("request.timeout.ms", "5000");
  TopicPartition tp = new TopicPartition("coupon-issue-requests", 0);
  try (Connection db = DriverManager.getConnection("jdbc:mysql://localhost:13306/coupon_db", "coupon", "1234");
       Admin admin = Admin.create(props);
       PrintWriter results = new PrintWriter(Files.newBufferedWriter(out.resolve("observed-results.csv")));
       PrintWriter samples = new PrintWriter(Files.newBufferedWriter(out.resolve("progress.jsonl")))) {
   results.println("id,request_id,user_id,status,issued_coupon_id,query_start_ms,observed_ms"); results.flush();
   long lastId = 0, nextStatus = 0;
   while (!Files.exists(out.resolve("monitor.stop"))) {
    long tick = System.currentTimeMillis(); long monotonic = System.nanoTime();
    try (PreparedStatement q = db.prepareStatement("SELECT id,request_id,user_id,status,issued_coupon_id FROM coupon_issue_result WHERE coupon_id=? AND id>? ORDER BY id")) {
     q.setLong(1, coupon); q.setLong(2, lastId);
     try (ResultSet rows = q.executeQuery()) {
      long observed = System.currentTimeMillis();
      while (rows.next()) {
       lastId = rows.getLong(1);
       results.printf("%d,%s,%d,%s,%s,%d,%d%n",lastId,rows.getString(2),rows.getLong(3),rows.getString(4),rows.getString(5),tick,observed);
      }
     }
    }
    results.flush();
    Map<String,Object> sample = new LinkedHashMap<>(); sample.put("query_start_ms",tick); sample.put("monotonic_ns",monotonic);
    sample.put("db_observation_end_ms",System.currentTimeMillis());
    var offsets = admin.listConsumerGroupOffsets(args[3]).partitionsToOffsetAndMetadata().get();
    long committed = offsets.containsKey(tp) ? offsets.get(tp).offset() : 0;
    long end = admin.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get().get(tp).offset();
    sample.put("offset_sample_end_ms",System.currentTimeMillis());
    sample.put("committed",committed); sample.put("end",end); sample.put("lag",end-committed);
    if (tick >= nextStatus) {
     try (Statement s=db.createStatement(); ResultSet rows=s.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected','Innodb_row_lock_current_waits','Innodb_row_lock_waits','Innodb_row_lock_time')")) {
      while(rows.next()) sample.put(rows.getString(1),rows.getLong(2));
     }
     nextStatus = tick+1000;
    }
    samples.println(json.writeValueAsString(sample)); samples.flush();
    Thread.sleep(Math.max(1,250-(System.currentTimeMillis()-tick)));
   }
  }
 }
}

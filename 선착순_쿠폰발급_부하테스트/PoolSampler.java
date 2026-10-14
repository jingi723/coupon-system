import com.sun.tools.attach.VirtualMachine;
import javax.management.*;
import javax.management.remote.*;
import java.io.*;
import java.nio.file.*;

// java --add-modules jdk.attach PoolSampler.java <pid> <output.csv> <stop-file>
// Launch the app with --spring.datasource.hikari.register-mbeans=true.
class PoolSampler {
    public static void main(String[] args) throws Exception {
        VirtualMachine vm = VirtualMachine.attach(args[0]);
        String address;
        try { address = vm.startLocalManagementAgent(); } finally { vm.detach(); }
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(address));
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(Path.of(args[1])))) {
            MBeanServerConnection server = connector.getMBeanServerConnection();
            var pools = server.queryNames(new ObjectName("com.zaxxer.hikari:type=Pool (*)"), null);
            if (pools.size() != 1) throw new IllegalStateException("Expected one pool, found " + pools);
            ObjectName pool = pools.iterator().next();
            out.println("time_ms,waiting,active,idle,total");
            while (!Files.exists(Path.of(args[2]))) {
                out.printf("%d,%s,%s,%s,%s%n", System.currentTimeMillis(),
                    server.getAttribute(pool, "ThreadsAwaitingConnection"),
                    server.getAttribute(pool, "ActiveConnections"),
                    server.getAttribute(pool, "IdleConnections"),
                    server.getAttribute(pool, "TotalConnections"));
                out.flush();
                Thread.sleep(250);
            }
        }
    }
}

package wayangprof;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.LoadEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresFilterOperator;
import org.apache.wayang.postgres.operators.PostgresGlobalReduceOperator;
import org.apache.wayang.postgres.operators.PostgresJoinOperator;
import org.apache.wayang.postgres.operators.PostgresProjectionOperator;
import org.apache.wayang.postgres.operators.PostgresTableSinkOperator;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Measures the real resources that Wayang's PostgreSQL platform uses, as training data for the ML cost model.
 *
 * <p>Same method as the Java and Spark profilers: one JVM run handles one configuration and runs two plans,
 * <pre>
 *   baseline : table source -> (SqlToStream) -> Java sink                SELECT * FROM t
 *   operator : table source -> OPERATOR -> (SqlToStream) -> Java sink     e.g. SELECT * FROM t WHERE key < 500
 * </pre>
 * alternating after warm-up runs; analyze.py subtracts the baseline from the operator run of each pair.
 *
 * <p>How Wayang runs Postgres: the Postgres operators of a stage are not executed one by one. JdbcExecutor turns
 * the whole stage into ONE SQL statement; the statement is executed by SqlToStreamOperator (on the Java side),
 * which opens a new connection, runs the query and streams the rows into Java. So the measured cost of a
 * Postgres operator is what it adds to that statement: database work plus the change in rows sent to Java.
 *
 * <p>What is measured, besides the JVM side (as in the Java profiler):
 * <ul>
 *   <li>Every JDBC connection Wayang opens goes through {@link ProfDriver}, a thin wrapper around the Postgres
 *       driver. It records when each connection was opened, every statement with its start and end, and (with
 *       {@link CountingSocketFactory}) the bytes sent and received on its socket: the network traffic between
 *       Postgres and Java.</li>
 *   <li>The "exec" window starts when Wayang opens the first connection for executing the plan (after its
 *       optimizer, whose {@code SELECT count(*)} cardinality query is recorded separately).</li>
 *   <li>Database CPU (Linux, database on this machine): the CPU of each server process is read from
 *       /proc just before its connection closes; the postmaster's "children CPU" counter adds the parallel query
 *       workers, which are separate processes.</li>
 *   <li>One extra, untimed EXPLAIN (ANALYZE, BUFFERS) of each plan's SQL: server execution and planning time,
 *       buffer hits and reads, temp (spill) blocks, parallel workers, JIT time and the plan shape.</li>
 * </ul>
 *
 * <p>Wayang's own estimates for the same plan are stored next to the measurements in {@code wayang_est_*} columns,
 * in total and split into the Postgres part and the Java part.
 */
public final class PostgresOperatorProfiler {

    // ------------------------------------------------------------------------------------------
    // Operator names
    // ------------------------------------------------------------------------------------------

    /** short name -> {Wayang logical operator, Postgres execution operator}. */
    static final Map<String, String[]> NAMES = new LinkedHashMap<>();

    static {
        NAMES.put("filter", new String[]{"FilterOperator", "PostgresFilterOperator"});
        NAMES.put("projection", new String[]{"MapOperator", "PostgresProjectionOperator"});
        NAMES.put("join", new String[]{"JoinOperator", "PostgresJoinOperator"});
        NAMES.put("globalreduce", new String[]{"GlobalReduceOperator", "PostgresGlobalReduceOperator"});
        NAMES.put("tablesink", new String[]{"TableSink", "PostgresTableSinkOperator"});
        // The Postgres -> Java hand-off: all rows sent to Java vs. only an aggregate (see buildPlans).
        NAMES.put("sqltostream", new String[]{"(channel conversion)", "SqlToStreamOperator"});
    }

    static final Set<String> BINARY = new HashSet<>(Arrays.asList("join"));
    static final String[] COLS = {"id", "key", "value", "text"};

    // ------------------------------------------------------------------------------------------
    // Data
    // ------------------------------------------------------------------------------------------

    /**
     * One input record. Lines in the data file look like {@code id,key,value,word word word}.
     *
     * <p>equals/hashCode use only {@code key}, so the output size of {@code distinct} equals the number
     * of distinct keys and is set with --keys. No other operator here uses equals.
     */
    public static final class Row implements Serializable {
        public final long id;
        public final int key;
        public final double value;
        public final String text;

        public Row(long id, int key, double value, String text) {
            this.id = id;
            this.key = key;
            this.value = value;
            this.text = text;
        }

        public static Row parse(String line) {
            int a = line.indexOf(',');
            int b = line.indexOf(',', a + 1);
            int c = line.indexOf(',', b + 1);
            return new Row(
                    Long.parseLong(line, 0, a, 10),
                    Integer.parseInt(line, a + 1, b, 10),
                    Double.parseDouble(line.substring(b + 1, c)),
                    line.substring(c + 1));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Row && ((Row) o).key == this.key;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(this.key);
        }

        @Override
        public String toString() {
            return this.id + "," + this.key + "," + this.value + "," + this.text;
        }
    }

    /** Makes the same rows every time for the same (rows, keys, words, seed). */
    static final class DataGen {
        private final String[] vocab;

        DataGen() {
            Random r = new Random(7);
            this.vocab = new String[2000];
            for (int i = 0; i < this.vocab.length; i++) {
                int len = 3 + r.nextInt(8);
                StringBuilder sb = new StringBuilder(len);
                for (int j = 0; j < len; j++) sb.append((char) ('a' + r.nextInt(26)));
                this.vocab[i] = sb.toString();
            }
        }

        String line(long id, Random r, int keys, int words) {
            StringBuilder sb = new StringBuilder(16 + words * 8);
            sb.append(id).append(',').append(r.nextInt(keys)).append(',')
                    .append(Math.round(r.nextDouble() * 1_000_000d) / 100d).append(',');
            for (int w = 0; w < words; w++) {
                if (w > 0) sb.append(' ');
                sb.append(this.vocab[r.nextInt(this.vocab.length)]);
            }
            return sb.toString();
        }

        Path ensureFile(Path dir, long rows, int keys, int words, long seed) throws IOException {
            Files.createDirectories(dir);
            Path p = dir.resolve(String.format("rows-%d_keys-%d_words-%d_seed-%d.csv", rows, keys, words, seed));
            if (Files.exists(p)) return p;
            Path tmp = dir.resolve(p.getFileName() + ".tmp." + ProcessHandle.current().pid());
            Random r = new Random(seed);
            try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                for (long i = 0; i < rows; i++) {
                    w.write(this.line(i, r, keys, words));
                    w.newLine();
                }
            }
            try {
                Files.move(tmp, p);
            } catch (IOException e) { // another run made it first
                Files.deleteIfExists(tmp);
            }
            return p;
        }

        List<Row> inMemory(long rows, int keys, int words, long seed) {
            Random r = new Random(seed);
            List<Row> list = new ArrayList<>((int) rows);
            for (long i = 0; i < rows; i++) list.add(Row.parse(this.line(i, r, keys, words)));
            return list;
        }
    }

    // ------------------------------------------------------------------------------------------
    // JVM probes
    // ------------------------------------------------------------------------------------------

    static final com.sun.management.ThreadMXBean TMX =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    static final com.sun.management.OperatingSystemMXBean OSMX =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    static final CompilationMXBean JIT = ManagementFactory.getCompilationMXBean();
    static final ClassLoadingMXBean CLS = ManagementFactory.getClassLoadingMXBean();

    static final List<GarbageCollectorMXBean> GCS = ManagementFactory.getGarbageCollectorMXBeans();

    /** JIT compile time of this JVM so far (all compiler threads), ms. */
    static long jitTotalMs() {
        return JIT != null && JIT.isCompilationTimeMonitoringSupported() ? JIT.getTotalCompilationTime() : 0;
    }

    /** GC time of this JVM so far (all collectors), ms. */
    static long gcTotalMs() {
        long t = 0;
        for (GarbageCollectorMXBean gc : GCS) t += Math.max(0, gc.getCollectionTime());
        return t;
    }

    /** Output records counted by the sink. */
    static long outRows;

    /** Largest live heap right after a GC during the measured run, plus how many GC pauses happened. */
    static final class LiveHeap {
        static volatile boolean measuring;
        static volatile long maxLiveAfterGc;
        static volatile long maxGcPauseMs;
        static final Set<String> heapPools = new HashSet<>();

        static void install() {
            for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
                if (p.getType() == MemoryType.HEAP) heapPools.add(p.getName());
            }
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                if (!(gc instanceof NotificationEmitter)) continue;
                ((NotificationEmitter) gc).addNotificationListener((n, h) -> {
                    if (!measuring) return;
                    if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(n.getType())) return;
                    GarbageCollectionNotificationInfo info =
                            GarbageCollectionNotificationInfo.from((CompositeData) n.getUserData());
                    long live = 0;
                    for (Map.Entry<String, MemoryUsage> e : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
                        if (heapPools.contains(e.getKey())) live += e.getValue().getUsed();
                    }
                    if (live > maxLiveAfterGc) maxLiveAfterGc = live;
                    long pause = info.getGcInfo().getDuration();
                    if (pause > maxGcPauseMs) maxGcPauseMs = pause;
                }, null, null);
            }
        }
    }

    /**
     * GC-independent name of a collector, so every garbage collector fills the same columns:
     * young (G1 Young Generation, Copy, PS Scavenge), old (G1 Old Generation, MarkSweepCompact, PS MarkSweep),
     * other (for example ZGC Cycles/Pauses).
     */
    static String gcKind(String name) {
        String n = name.toLowerCase();
        if (n.contains("young") || n.equals("copy") || n.contains("scavenge") || n.contains("parnew")) return "young";
        if (n.contains("old") || n.contains("marksweep")) return "old";
        return "other";
    }

    /** GC-independent name of a heap pool: eden, survivor, old (G1 Old Gen, Tenured Gen, PS Old Gen) or other_heap. */
    static String heapPoolKind(String name) {
        String n = name.toLowerCase();
        if (n.contains("eden")) return "eden";
        if (n.contains("survivor")) return "survivor";
        if (n.contains("old") || n.contains("tenured")) return "old";
        return "other_heap";
    }

    /** JVM counters that only grow, read before and after a run. */
    static Map<String, Long> jvmCounters() {
        Map<String, Long> m = new TreeMap<>();
        for (String k : List.of("young", "old", "other")) {
            m.put("gc_" + k + "_count", 0L);
            m.put("gc_" + k + "_ms", 0L);
        }
        long n = 0, t = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long cn = Math.max(0, gc.getCollectionCount()), ct = Math.max(0, gc.getCollectionTime());
            m.merge("gc_" + gcKind(gc.getName()) + "_count", cn, Long::sum);
            m.merge("gc_" + gcKind(gc.getName()) + "_ms", ct, Long::sum);
            n += cn;
            t += ct;
        }
        m.put("gc_count", n);
        m.put("gc_time_ms", t);
        if (JIT != null && JIT.isCompilationTimeMonitoringSupported()) m.put("jit_compile_ms", JIT.getTotalCompilationTime());
        m.put("classes_loaded", CLS.getTotalLoadedClassCount());
        m.put("threads_started", TMX.getTotalStartedThreadCount());
        return m;
    }

    static void resetPoolPeaks() {
        for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) p.resetPeakUsage();
        TMX.resetPeakThreadCount();
    }

    /** Peak use of every memory pool since the last reset, plus heap and non-heap totals. */
    static Map<String, Long> poolPeaks() {
        Map<String, Long> m = new TreeMap<>();
        for (String k : List.of("eden", "survivor", "old", "other_heap")) m.put("pool_" + k + "_peak_bytes", 0L);
        long heap = 0, nonHeap = 0;
        for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
            long used = p.getPeakUsage().getUsed();
            if (p.getType() == MemoryType.HEAP) {
                // Heap pools are named after the GC; store them under GC-independent names.
                m.merge("pool_" + heapPoolKind(p.getName()) + "_peak_bytes", used, Long::sum);
                heap += used;
            } else {
                m.put("pool_" + slug(p.getName()) + "_peak_bytes", used); // code cache, metaspace: same for every GC
                nonHeap += used;
            }
        }
        m.put("heap_peak_bytes", heap);
        m.put("nonheap_peak_bytes", nonHeap);
        m.put("heap_committed_bytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted());
        m.put("threads_peak", (long) TMX.getPeakThreadCount());
        return m;
    }

    static String slug(String s) {
        return s.toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_|_$", "");
    }

    // ------------------------------------------------------------------------------------------
    // OS probe
    // ------------------------------------------------------------------------------------------

    /** One reading of the OS counters for this process. */
    static final class OsSnap {
        final Map<String, Long> counters = new TreeMap<>(); // only grow: store after - before
        final Map<String, Long> gauges = new TreeMap<>();   // levels: store value after, and the change
    }

    static final class OsProbe {
        final String helper; // path to pidrusage (macOS), or null
        final long pid = ProcessHandle.current().pid();
        /** Where Linux counters are read (overridable, so the parsing can be tested with sample files). */
        static final String PROC_ROOT = System.getProperty("wayangprof.procRoot", "/proc");
        static final String RAPL_PATH = System.getProperty("wayangprof.raplPath", "/sys/class/powercap/intel-rapl:0/energy_uj");
        /** RAPL's counter wraps around at this value; a negative difference is corrected by adding it. */
        static volatile long raplRangeNj;
        final boolean linux = Files.isReadable(Paths.get(PROC_ROOT, "self", "stat"));

        OsProbe(String helper) {
            this.helper = (helper != null && new File(helper).canExecute()) ? helper : null;
        }

        String kind() {
            return this.helper != null ? "macos-proc_pid_rusage" : this.linux ? "linux-procfs" : "none";
        }

        OsSnap snapshot() {
            OsSnap s = new OsSnap();
            if (this.helper != null) this.readMac(s);
            else if (this.linux) this.readLinux(s);
            return s;
        }

        private void readMac(OsSnap s) {
            try {
                Process p = new ProcessBuilder(this.helper, Long.toString(this.pid)).redirectErrorStream(true).start();
                String line;
                try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    line = r.readLine();
                }
                p.waitFor();
                if (line == null) return;
                for (String item : line.trim().split(" ")) {
                    int colon = item.indexOf(':'), eq = item.indexOf('=');
                    if (colon != 1 || eq < 0) continue;
                    String name = item.substring(2, eq);
                    long v = Long.parseLong(item.substring(eq + 1));
                    (item.charAt(0) == 'g' ? s.gauges : s.counters).put(name, v);
                }
            } catch (Exception e) {
                System.err.println("pidrusage failed: " + e);
            }
        }

        private void readLinux(OsSnap s) {
            Path proc = Paths.get(PROC_ROOT);
            try {
                // Disk and syscall counters of this process. Needs task I/O accounting in the kernel, which some
                // VMs and containers lack: then only these columns stay empty.
                Path io = proc.resolve("self/io");
                if (Files.isReadable(io)) {
                    for (String l : Files.readAllLines(io)) {  // rchar, wchar, syscr, syscw, read_bytes, write_bytes, ...
                        String[] kv = l.split(":\\s*");
                        if (kv.length != 2) continue;
                        String name;
                        switch (kv[0]) {
                            case "read_bytes": name = "disk_read_bytes"; break;
                            case "write_bytes": name = "disk_write_bytes"; break;
                            case "rchar": name = "logical_read_bytes"; break;
                            case "wchar": name = "logical_write_bytes"; break;
                            case "syscr": name = "syscalls_read"; break;
                            case "syscw": name = "syscalls_write"; break;
                            default: name = kv[0]; break;
                        }
                        s.counters.put(name, Long.parseLong(kv[1].trim()));
                    }
                }
                // /proc/self/stat after the ")": index 7 minflt, 9 majflt, 11 utime, 12 stime (clock ticks of 10 ms)
                String stat = new String(Files.readAllBytes(proc.resolve("self/stat")), StandardCharsets.UTF_8);
                String[] f = stat.substring(stat.lastIndexOf(')') + 2).trim().split("\\s+");
                s.counters.put("page_faults", Long.parseLong(f[7]));
                s.counters.put("pageins", Long.parseLong(f[9]));
                s.counters.put("user_ns", Long.parseLong(f[11]) * 10_000_000L);
                s.counters.put("system_ns", Long.parseLong(f[12]) * 10_000_000L);
                for (String l : Files.readAllLines(proc.resolve("self/status"))) {
                    String[] kv = l.split(":\\s*");
                    if (kv.length != 2) continue;
                    String v = kv[1].trim().split("\\s+")[0];
                    switch (kv[0]) {
                        case "voluntary_ctxt_switches": s.counters.put("context_switches_voluntary", Long.parseLong(v)); break;
                        case "nonvoluntary_ctxt_switches": s.counters.put("context_switches_involuntary", Long.parseLong(v)); break;
                        case "VmRSS": s.gauges.put("resident_bytes", Long.parseLong(v) * 1024); break;
                        case "VmHWM": s.gauges.put("lifetime_max_resident_bytes", Long.parseLong(v) * 1024); break;
                        case "VmSize": s.gauges.put("virtual_bytes", Long.parseLong(v) * 1024); break;
                        case "VmData": s.gauges.put("data_bytes", Long.parseLong(v) * 1024); break;
                        case "Threads": s.gauges.put("threads", Long.parseLong(v)); break;
                        default: break;
                    }
                }
                // Whole machine, from the "cpu" line of /proc/stat (clock ticks): time waiting for disk, and steal =
                // time the hypervisor gave this VM's CPUs to other VMs. Steal > 0 means the run was disturbed.
                long machineCpus = 0;
                for (String l : Files.readAllLines(proc.resolve("stat"))) {
                    if (l.startsWith("cpu ")) {
                        String[] c = l.trim().split("\\s+");  // cpu user nice system idle iowait irq softirq steal ...
                        if (c.length > 5) s.counters.put("sys_iowait_ns", Long.parseLong(c[5]) * 10_000_000L);
                        if (c.length > 8) s.counters.put("sys_steal_ns", Long.parseLong(c[8]) * 10_000_000L);
                    } else if (l.startsWith("cpu") && l.length() > 3 && Character.isDigit(l.charAt(3))) {
                        machineCpus++;  // cpu0, cpu1, ...: the whole machine, unlike -XX:ActiveProcessorCount
                    }
                }
                if (machineCpus > 0) s.gauges.put("sys_cpus", machineCpus);
                // RAPL energy of the whole CPU package (not only this process), if readable.
                Path rapl = Paths.get(RAPL_PATH);
                if (Files.isReadable(rapl)) {
                    s.counters.put("energy_pkg_nj", Long.parseLong(Files.readAllLines(rapl).get(0).trim()) * 1000L);
                    Path range = rapl.resolveSibling("max_energy_range_uj");
                    if (raplRangeNj == 0 && Files.isReadable(range)) {
                        raplRangeNj = Long.parseLong(Files.readAllLines(range).get(0).trim()) * 1000L;
                    }
                }
            } catch (Exception e) {
                System.err.println("procfs read failed: " + e);
            }
        }
    }


    // ------------------------------------------------------------------------------------------
    // JDBC tracing: every connection Wayang opens goes through ProfDriver
    // ------------------------------------------------------------------------------------------

    /** Wayang's cardinality estimate of a JdbcTableSource (during optimization): "SELECT count(*) FROM t". */
    static final Pattern COUNT_QUERY = Pattern.compile("SELECT count\\(\\*\\) FROM [A-Za-z0-9_.\"]+");

    /** One statement executed on a traced connection. */
    static final class Stmt {
        final String sql;
        final long startNs;
        volatile long endNs;

        Stmt(String sql) {
            this.sql = sql;
            this.startNs = System.nanoTime();
        }
    }

    /** One JDBC connection opened through ProfDriver. */
    static final class Conn {
        long connectStartNs, connectEndNs;
        // JVM state when the connection was requested: the exec window of a job starts at its first connection.
        long threadId, threadCpuNs, threadAllocBytes, processCpuNs, jitMs, gcMs;
        int backendPid = -1;
        final AtomicLong bytesIn = new AtomicLong(), bytesOut = new AtomicLong();
        final List<Stmt> stmts = Collections.synchronizedList(new ArrayList<>());
        volatile boolean closed;
        // The server process of this connection, read just before it is closed (Linux, local database).
        long backendCpuNs = -1, backendReadBytes = -1, backendWriteBytes = -1;
        Connection real;

        /** A connection of Wayang's optimizer: it ran only the cardinality query. */
        boolean optimizer() {
            synchronized (this.stmts) {
                if (this.stmts.isEmpty()) return false;
                for (Stmt s : this.stmts) if (!COUNT_QUERY.matcher(s.sql.trim()).matches()) return false;
            }
            return true;
        }
    }

    static final class Trace {
        static final List<Conn> conns = Collections.synchronizedList(new ArrayList<>());
        /** The connection being opened on this thread, so its socket can be attributed to it. */
        static final ThreadLocal<Conn> connecting = new ThreadLocal<>();

        static List<Conn> snapshot() {
            synchronized (conns) {
                return new ArrayList<>(conns);
            }
        }
    }

    /**
     * JDBC driver for URLs "jdbc:wayangprof:postgresql://...": opens a normal Postgres connection (with a byte-counting
     * socket and the session options of this run) and returns a proxy that records statements and the close.
     * Wayang is configured with such a URL, so every connection it opens is traced; nothing else changes.
     */
    public static final class ProfDriver implements Driver {
        static final String PREFIX = "jdbc:wayangprof:";
        static volatile String sessionOptions = "";
        final Driver pg = new org.postgresql.Driver();

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(PREFIX);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!this.acceptsURL(url)) return null;
            Conn c = new Conn();
            c.threadId = Thread.currentThread().getId();
            c.connectStartNs = System.nanoTime();
            c.threadCpuNs = TMX.getCurrentThreadCpuTime();
            c.threadAllocBytes = TMX.getThreadAllocatedBytes(c.threadId);
            c.processCpuNs = OSMX.getProcessCpuTime();
            c.jitMs = jitTotalMs();
            c.gcMs = gcTotalMs();
            Properties p = new Properties();
            if (info != null) p.putAll(info);
            p.setProperty("socketFactory", CountingSocketFactory.class.getName());
            if (!sessionOptions.isEmpty()) p.setProperty("options", sessionOptions);
            Trace.connecting.set(c);
            Connection real;
            try {
                real = this.pg.connect("jdbc:" + url.substring(PREFIX.length()), p);
            } finally {
                Trace.connecting.remove();
            }
            c.connectEndNs = System.nanoTime();
            c.real = real;
            try {
                c.backendPid = real.unwrap(PGConnection.class).getBackendPID();
            } catch (SQLException e) {
                c.backendPid = -1;
            }
            Trace.conns.add(c);
            return (Connection) Proxy.newProxyInstance(ProfDriver.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, new ConnHandler(c));
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return this.pg.getPropertyInfo("jdbc:" + url.substring(PREFIX.length()), info);
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }

    static Object call(Object target, Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    static final class ConnHandler implements InvocationHandler {
        final Conn c;

        ConnHandler(Conn c) {
            this.c = c;
        }

        @Override
        public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
            String n = m.getName();
            if (n.equals("close") && (args == null || args.length == 0)) {
                closeConn(this.c);
                return null;
            }
            if (n.equals("isClosed") && (args == null || args.length == 0)) return this.c.closed || this.c.real.isClosed();
            Object r = call(this.c.real, m, args);
            if (r instanceof Statement) {
                String prepared = (n.startsWith("prepare") && args != null && args.length > 0 && args[0] instanceof String)
                        ? (String) args[0] : null;
                return wrapStatement((Statement) r, this.c, prepared);
            }
            return r;
        }
    }

    /** Statement proxy: records every execute* call (SQL, start, end). ResultSets are not wrapped (no per-row cost). */
    static Statement wrapStatement(Statement s, Conn c, String preparedSql) {
        Class<?> iface = s instanceof CallableStatement ? CallableStatement.class
                : s instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
        return (Statement) Proxy.newProxyInstance(ProfDriver.class.getClassLoader(), new Class<?>[]{iface}, (p, m, a) -> {
            if (m.getName().startsWith("execute")) {
                String sql = (a != null && a.length > 0 && a[0] instanceof String) ? (String) a[0] : preparedSql;
                Stmt st = new Stmt(sql == null ? "" : sql);
                c.stmts.add(st);
                try {
                    return call(s, m, a);
                } finally {
                    st.endNs = System.nanoTime();
                }
            }
            return call(s, m, a);
        });
    }

    /** Closes a traced connection, reading its server process's CPU and disk counters first. */
    static void closeConn(Conn c) throws SQLException {
        if (c.closed) return;
        c.closed = true;
        long[] b = Db.procCpuAndIo(c.backendPid);
        if (b != null) {
            c.backendCpuNs = b[0];
            c.backendReadBytes = b[1];
            c.backendWriteBytes = b[2];
        }
        c.real.close();
    }

    /** Counts the bytes on the socket of the connection being opened (Postgres JDBC "socketFactory"). */
    public static final class CountingSocketFactory extends javax.net.SocketFactory {
        public CountingSocketFactory() {
        }

        @Override
        public Socket createSocket() {
            return new CountingSocket(Trace.connecting.get());
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            Socket s = this.createSocket();
            s.connect(new InetSocketAddress(host, port));
            return s;
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            Socket s = this.createSocket();
            s.bind(new InetSocketAddress(localHost, localPort));
            s.connect(new InetSocketAddress(host, port));
            return s;
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            Socket s = this.createSocket();
            s.connect(new InetSocketAddress(host, port));
            return s;
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            Socket s = this.createSocket();
            s.bind(new InetSocketAddress(localAddress, localPort));
            s.connect(new InetSocketAddress(address, port));
            return s;
        }
    }

    static final class CountingSocket extends Socket {
        final Conn c;
        InputStream in;
        OutputStream out;

        CountingSocket(Conn c) {
            this.c = c;
        }

        @Override
        public synchronized InputStream getInputStream() throws IOException {
            if (this.in == null) {
                final Conn conn = this.c;
                this.in = new FilterInputStream(super.getInputStream()) {
                    @Override
                    public int read() throws IOException {
                        int b = super.read();
                        if (b >= 0 && conn != null) conn.bytesIn.incrementAndGet();
                        return b;
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        int n = super.read(b, off, len);
                        if (n > 0 && conn != null) conn.bytesIn.addAndGet(n);
                        return n;
                    }
                };
            }
            return this.in;
        }

        @Override
        public synchronized OutputStream getOutputStream() throws IOException {
            if (this.out == null) {
                final Conn conn = this.c;
                this.out = new FilterOutputStream(super.getOutputStream()) {
                    @Override
                    public void write(int b) throws IOException {
                        this.out.write(b);
                        if (conn != null) conn.bytesOut.incrementAndGet();
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        this.out.write(b, off, len);  // not FilterOutputStream's byte-by-byte default
                        if (conn != null) conn.bytesOut.addAndGet(len);
                    }
                };
            }
            return this.out;
        }
    }

    // ------------------------------------------------------------------------------------------
    // The database: control connection, tables, server process counters, EXPLAIN
    // ------------------------------------------------------------------------------------------

    static final class Db {
        static final String PROC = System.getProperty("wayangprof.procRoot", "/proc");
        final Connection ctl;           // our own connection (not traced), same session options as Wayang's
        final int ctlPid;
        int postmasterPid = -1;         // > 0 only if the database runs on this machine and /proc is readable
        final ObjectMapper json = new ObjectMapper();

        Db(String url, String user, String password, String options) throws SQLException {
            Properties p = new Properties();
            if (user != null) p.setProperty("user", user);
            if (password != null) p.setProperty("password", password);
            if (!options.isEmpty()) p.setProperty("options", options);
            this.ctl = new org.postgresql.Driver().connect("jdbc:" + url, p);
            if (this.ctl == null) throw new SQLException("not a Postgres URL: " + url);
            this.ctlPid = this.ctl.unwrap(PGConnection.class).getBackendPID();
            // Same machine? Then /proc/<backend pid> is our server process, and its parent is the postmaster.
            try {
                Path cmd = Paths.get(PROC, Integer.toString(this.ctlPid), "cmdline");
                if (Files.isReadable(cmd) && new String(Files.readAllBytes(cmd), StandardCharsets.UTF_8).startsWith("postgres")) {
                    String[] f = statFields(this.ctlPid);
                    if (f != null) this.postmasterPid = Integer.parseInt(f[1]);
                }
            } catch (Exception e) {
                this.postmasterPid = -1;
            }
        }

        boolean local() {
            return this.postmasterPid > 0;
        }

        /** Fields of /proc/PID/stat after the command name: [0] state, [1] ppid, ..., [11] utime, [12] stime, [13] cutime, [14] cstime. */
        static String[] statFields(int pid) {
            try {
                String s = new String(Files.readAllBytes(Paths.get(PROC, Integer.toString(pid), "stat")), StandardCharsets.UTF_8);
                return s.substring(s.lastIndexOf(')') + 2).trim().split("\\s+");
            } catch (Exception e) {
                return null;
            }
        }

        /** {cpu ns, disk read bytes, disk write bytes} of a server process, or null (not Linux / not local). io = -1 if unreadable. */
        static long[] procCpuAndIo(int pid) {
            if (pid <= 0 || !LOCAL_DB) return null;
            String[] f = statFields(pid);
            if (f == null) return null;
            long[] r = {(Long.parseLong(f[11]) + Long.parseLong(f[12])) * 10_000_000L, -1, -1};
            try {
                for (String l : Files.readAllLines(Paths.get(PROC, Integer.toString(pid), "io"))) {
                    if (l.startsWith("read_bytes:")) r[1] = Long.parseLong(l.substring(11).trim());
                    if (l.startsWith("write_bytes:")) r[2] = Long.parseLong(l.substring(12).trim());
                }
            } catch (Exception e) {
                // /proc/PID/io is readable only by the process owner: run the profiler as the database's OS user
            }
            return r;
        }

        /** CPU of all exited children of the postmaster (server processes, parallel workers), ns; -1 if not available. */
        long childrenCpuNs() {
            if (!this.local()) return -1;
            String[] f = statFields(this.postmasterPid);
            return f == null ? -1 : (Long.parseLong(f[13]) + Long.parseLong(f[14])) * 10_000_000L;
        }

        /** Waits until these server processes have ended (and were reaped), at most 5 s. */
        void waitGone(Collection<Integer> pids) throws SQLException {
            List<Integer> ps = pids.stream().filter(p -> p > 0).collect(Collectors.toList());
            if (ps.isEmpty()) return;
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                boolean alive;
                if (this.local()) {
                    alive = ps.stream().anyMatch(p -> Files.exists(Paths.get(PROC, Integer.toString(p))));
                } else {
                    try (PreparedStatement st = this.ctl.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(?)")) {
                        st.setArray(1, this.ctl.createArrayOf("int4", ps.toArray()));
                        try (ResultSet rs = st.executeQuery()) {
                            rs.next();
                            alive = rs.getLong(1) > 0;
                        }
                    }
                }
                if (!alive) {
                    sleep(20); // the postmaster adds a child's CPU to its counter when it reaps it
                    return;
                }
                sleep(10);
            }
            System.err.println("[wayangprof] WARNING: server processes still running after 5 s: " + ps);
        }

        void exec(String sql) throws SQLException {
            try (Statement st = this.ctl.createStatement()) {
                st.execute(sql);
            }
        }

        long queryLong(String sql) throws SQLException {
            try (Statement st = this.ctl.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }

        String queryString(String sql) throws SQLException {
            try (Statement st = this.ctl.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? rs.getString(1) : null;
            }
        }

        boolean tableExists(String t) throws SQLException {
            return this.queryString("SELECT to_regclass('" + t + "')::text") != null;
        }

        /**
         * Makes sure table t holds exactly the rows of the generated data file (same generator and seed as the Java
         * and Spark profilers): COPY, optional index on key, VACUUM ANALYZE, autovacuum off for the table, CHECKPOINT.
         */
        void ensureTable(String t, Path csv, long rows, boolean index) throws Exception {
            if (this.tableExists(t) && this.queryLong("SELECT count(*) FROM " + t) == rows
                    && (!index || this.queryLong("SELECT count(*) FROM pg_indexes WHERE tablename = '" + t + "'") > 0)) {
                return;
            }
            System.err.printf("[wayangprof] loading %s (%d rows)%n", t, rows);
            this.exec("DROP TABLE IF EXISTS " + t);
            this.exec("CREATE TABLE " + t + " (id bigint, key int, value double precision, text text)"
                    + " WITH (autovacuum_enabled = false)");
            CopyManager cm = new CopyManager(this.ctl.unwrap(BaseConnection.class));
            try (BufferedReader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
                cm.copyIn("COPY " + t + " FROM STDIN WITH (FORMAT csv)", r);
            }
            if (index) this.exec("CREATE INDEX ON " + t + " (key)");
            this.exec("VACUUM ANALYZE " + t);
            this.checkpoint();
        }

        void checkpoint() {
            try {
                this.exec("CHECKPOINT");
            } catch (SQLException e) {
                if (!checkpointWarned) System.err.println("[wayangprof] WARNING: CHECKPOINT not allowed (needs superuser or pg_checkpoint): " + e.getMessage());
                checkpointWarned = true;
            }
        }

        static boolean checkpointWarned;

        /**
         * EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) of the statements a plan ran, summed over them; runs of it are
         * repeated and the median execution time kept. Write statements run inside a transaction that is rolled back.
         */
        Map<String, Object> explain(List<String> sqls, int runs) {
            Map<String, Object> m = new LinkedHashMap<>();
            if (sqls.isEmpty() || runs <= 0) return m;
            List<Double> exec = new ArrayList<>();
            double plan = 0, jit = 0;
            long hit = 0, read = 0, tempRead = 0, tempWritten = 0, workers = 0, rows = 0;
            List<String> shapes = new ArrayList<>();
            try {
                for (int run = 0; run < runs; run++) {
                    double e = 0;
                    plan = jit = 0;
                    hit = read = tempRead = tempWritten = workers = rows = 0;
                    shapes.clear();
                    for (String sql : sqls) {
                        boolean write = !sql.trim().toUpperCase().startsWith("SELECT");
                        if (write) this.ctl.setAutoCommit(false);
                        try (Statement st = this.ctl.createStatement();
                             ResultSet rs = st.executeQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) " + sql)) {
                            rs.next();
                            JsonNode top = this.json.readTree(rs.getString(1)).get(0);
                            JsonNode p = top.get("Plan");
                            e += top.path("Execution Time").asDouble();
                            plan += top.path("Planning Time").asDouble();
                            jit += top.path("JIT").path("Timing").path("Total").asDouble(0);
                            hit += p.path("Shared Hit Blocks").asLong();
                            read += p.path("Shared Read Blocks").asLong();
                            tempRead += p.path("Temp Read Blocks").asLong();
                            tempWritten += p.path("Temp Written Blocks").asLong();
                            rows += p.path("Actual Rows").asLong();
                            workers += workersLaunched(p);
                            shapes.add(shape(p));
                        } finally {
                            if (write) {
                                this.ctl.rollback();
                                this.ctl.setAutoCommit(true);
                            }
                        }
                    }
                    exec.add(e);
                }
            } catch (Exception e) {
                System.err.println("[wayangprof] WARNING: EXPLAIN failed: " + e);
                return new LinkedHashMap<>();
            }
            Collections.sort(exec);
            m.put("db_explain_exec_ms", exec.get(exec.size() / 2));
            m.put("db_explain_planning_ms", plan);
            m.put("db_explain_jit_ms", jit);
            m.put("db_explain_rows", rows);
            m.put("db_shared_hit_blocks", hit);
            m.put("db_shared_read_blocks", read);
            m.put("db_temp_read_blocks", tempRead);
            m.put("db_temp_written_blocks", tempWritten);
            m.put("db_workers_launched", workers);
            m.put("db_plan", String.join(" ; ", shapes));
            return m;
        }

        static long workersLaunched(JsonNode p) {
            long w = p.path("Workers Launched").asLong(0);
            for (JsonNode child : p.path("Plans")) w += workersLaunched(child);
            return w;
        }

        /** Plan shape, e.g. "Hash Join(Seq Scan,Hash(Seq Scan))". */
        static String shape(JsonNode p) {
            StringBuilder sb = new StringBuilder(p.path("Node Type").asText());
            if (p.has("Plans")) {
                sb.append('(');
                boolean first = true;
                for (JsonNode child : p.get("Plans")) {
                    if (!first) sb.append(',');
                    sb.append(shape(child));
                    first = false;
                }
                sb.append(')');
            }
            return sb.toString();
        }
    }

    /** Set once in main: the database runs on this machine and its processes can be read in /proc. */
    static volatile boolean LOCAL_DB;

    // ------------------------------------------------------------------------------------------
    // Config
    // ------------------------------------------------------------------------------------------

    static final class Cfg {
        String op = "filter";
        long rows = 100_000;
        long rightRows = -1;             // join; -1 = same as rows
        int keys = 1000;
        double sel = 0.5;                // filter selectivity
        int words = 8;                   // words per row (row width)
        int warmup = 2;
        int reps = 5;
        int parallel = 0;                // max_parallel_workers_per_gather of every session
        boolean index = false;           // B-tree index on the key column
        String url = "postgresql://127.0.0.1:5432/wayangprof";
        String user = System.getProperty("user.name");
        String password = null;
        String pgOptions = "";           // more session settings, e.g. "-c work_mem=64MB -c jit=off"
        int explainRuns = 3;
        boolean dropTables = false;
        Path dataDir = Paths.get("data");
        Path out = Paths.get("raw.csv");
        String pidrusage = null;
        String resource = "default";
        String tag = "";
        boolean estimateOnly = false;
        long minWarmupRows = 3_000_000;
        int maxExtraWarmup = 20;

        boolean binary() {
            return BINARY.contains(this.op);
        }

        String configId() {
            return String.format("%s|%s|%s|r%d|rr%d|k%d|s%s|w%d", this.resource, this.op, "table", this.rows,
                    this.binary() ? this.rightRows : 0, this.keys,
                    this.op.equals("filter") ? Double.toString(this.sel) : "-", this.words);
        }

        String sessionOptions() {
            return ("-c max_parallel_workers_per_gather=" + this.parallel + " " + this.pgOptions).trim();
        }

        String table(long n, long seed) {
            return String.format("wp_r%d_k%d_w%d_s%d%s", n, this.keys, this.words, seed, this.index ? "_i" : "");
        }

        String outTable() {
            return "wp_out_" + ProcessHandle.current().pid();
        }
    }

    static Cfg parseArgs(String[] args) {
        Cfg c = new Cfg();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            String v = (i + 1 < args.length) ? args[i + 1] : null;
            switch (a) {
                case "--op": c.op = v; i++; break;
                case "--rows": c.rows = Long.parseLong(v); i++; break;
                case "--right-rows": c.rightRows = Long.parseLong(v); i++; break;
                case "--keys": c.keys = Integer.parseInt(v); i++; break;
                case "--sel": c.sel = Double.parseDouble(v); i++; break;
                case "--words": c.words = Integer.parseInt(v); i++; break;
                case "--warmup": c.warmup = Integer.parseInt(v); i++; break;
                case "--reps": c.reps = Integer.parseInt(v); i++; break;
                case "--parallel": c.parallel = Integer.parseInt(v); i++; break;
                case "--index": c.index = true; break;
                case "--url": c.url = v.startsWith("jdbc:") ? v.substring(5) : v; i++; break;
                case "--user": c.user = v; i++; break;
                case "--password": c.password = v; i++; break;
                case "--pg-options": c.pgOptions = v; i++; break;
                case "--explain-runs": c.explainRuns = Integer.parseInt(v); i++; break;
                case "--drop-tables": c.dropTables = true; break;
                case "--data-dir": c.dataDir = Paths.get(v); i++; break;
                case "--out": c.out = Paths.get(v); i++; break;
                case "--pidrusage": c.pidrusage = v; i++; break;
                case "--resource": c.resource = v; i++; break;
                case "--tag": c.tag = v; i++; break;
                case "--estimate-only": c.estimateOnly = true; break;
                case "--min-warmup-rows": c.minWarmupRows = Long.parseLong(v); i++; break;
                case "--max-extra-warmup": c.maxExtraWarmup = Integer.parseInt(v); i++; break;
                case "-h": case "--help": usage(); System.exit(0); break;
                default: System.err.println("Unknown argument: " + a); usage(); System.exit(2);
            }
        }
        if (!NAMES.containsKey(c.op)) {
            System.err.println("Unknown operator: " + c.op);
            usage();
            System.exit(2);
        }
        if (c.password == null) c.password = System.getenv("PGPASSWORD");
        if (c.rightRows < 0) c.rightRows = c.rows;
        c.keys = Math.max(1, c.keys);
        return c;
    }

    static void usage() {
        System.err.println("Usage: PostgresOperatorProfiler --op <op> [--rows N] [--right-rows N] [--keys K] [--sel S] [--words W]\n"
                + "         [--warmup N] [--reps N] [--parallel N] [--index] [--url postgresql://host:port/db] [--user U]\n"
                + "         [--password P (or PGPASSWORD)] [--pg-options \"-c work_mem=64MB\"] [--explain-runs N] [--drop-tables]\n"
                + "         [--data-dir D] [--out raw.csv] [--pidrusage path] [--resource label] [--tag t]\n"
                + "         [--min-warmup-rows N] [--max-extra-warmup N] [--estimate-only]\n"
                + "Operators: " + NAMES.keySet());
    }

    // ------------------------------------------------------------------------------------------
    // Plans
    // ------------------------------------------------------------------------------------------

    /** Rows that reached the Java sink. */
    static long OUT_ROWS;

    static Operator source(String table) {
        return new PostgresTableSource(table, COLS);
    }

    static Operator javaSink() {
        return javaSink(DataSetType.createDefault(Record.class));
    }

    /** Java sink that counts rows. The type must match the producer's declared type (a join declares Tuple2,
     *  although Postgres delivers flat Records through SqlToStream). */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Operator javaSink(DataSetType<?> type) {
        return new LocalCallbackSink(r -> OUT_ROWS++, type);
    }

    static Operator globalReduce(String sqlAggregate) {
        return new PostgresGlobalReduceOperator(
                new ReduceDescriptor<Record>((a, b) -> a, Record.class).withSqlImplementation(sqlAggregate));
    }

    static Operator chain(Operator... ops) {
        for (int i = 0; i + 1 < ops.length; i++) ops[i].connectTo(0, ops[i + 1], 0);
        return ops[ops.length - 1];
    }

    /** What the baseline of this operator is, written to the CSV. */
    static String baselineKind(Cfg c) {
        switch (c.op) {
            case "join": return "2 jobs: SELECT * FROM left, SELECT * FROM right";
            case "sqltostream": return "SELECT COUNT(id) FROM t (one row to Java)";
            default: return "SELECT * FROM t";
        }
    }

    /**
     * Fresh plans (one per job). The Postgres operators are execution operators placed directly in the plan, so Wayang
     * cannot move them to Java; it only adds the SqlToStream conversion in front of the Java sink.
     * <ul>
     *   <li>filter: WHERE key &lt; sel*keys (keys are uniform, so the output is sel*rows)</li>
     *   <li>projection: SELECT id, value (2 of 4 columns: fewer bytes to Java)</li>
     *   <li>join: SELECT * FROM l JOIN r ON r.key = l.key; baseline = scanning both tables in two jobs</li>
     *   <li>globalreduce: SELECT SUM(value) (one row to Java)</li>
     *   <li>tablesink: CREATE TABLE out AS SELECT * (nothing to Java)</li>
     *   <li>sqltostream: operator = SELECT * (all rows to Java), baseline = SELECT COUNT(id) (one row): the
     *       difference is the cost of handing the rows from Postgres to Java</li>
     * </ul>
     */
    static List<WayangPlan> buildPlans(Cfg c, boolean baseline) {
        String t = c.table(c.rows, 42);
        switch (c.op) {
            case "join": {
                String r = c.table(c.rightRows, 43);
                if (baseline) return List.of(new WayangPlan(chain(source(t), javaSink())),
                        new WayangPlan(chain(source(r), javaSink())));
                Operator join = new PostgresJoinOperator<Integer>(
                        new TransformationDescriptor<Record, Integer>(x -> x.getInt(1), Record.class, Integer.class).withSqlImplementation(t, "key"),
                        new TransformationDescriptor<Record, Integer>(x -> x.getInt(1), Record.class, Integer.class).withSqlImplementation(r, "key"));
                source(t).connectTo(0, join, 0);
                source(r).connectTo(0, join, 1);
                return List.of(new WayangPlan(chain(join, javaSink(join.getOutput(0).getType()))));
            }
            case "sqltostream":
                return List.of(new WayangPlan(baseline ? chain(source(t), globalReduce("COUNT(id)"), javaSink())
                        : chain(source(t), javaSink())));
            default:
                break;
        }
        if (baseline) return List.of(new WayangPlan(chain(source(t), javaSink())));
        switch (c.op) {
            case "filter": {
                final int threshold = (int) Math.round(c.sel * c.keys);
                Operator f = new PostgresFilterOperator(new PredicateDescriptor<Record>(x -> x.getInt(1) < threshold, Record.class)
                        .withSqlImplementation("key < " + threshold));
                return List.of(new WayangPlan(chain(source(t), f, javaSink())));
            }
            case "projection":
                return List.of(new WayangPlan(chain(source(t), new PostgresProjectionOperator("id", "value"), javaSink())));
            case "globalreduce":
                return List.of(new WayangPlan(chain(source(t), globalReduce("SUM(value)"), javaSink())));
            case "tablesink": {
                PostgresTableSinkOperator sink = new PostgresTableSinkOperator(c.outTable(), COLS);
                sink.setMode("overwrite");
                return List.of(new WayangPlan(chain(source(t), sink)));
            }
            default:
                throw new IllegalArgumentException(c.op);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Wayang's own estimates
    // ------------------------------------------------------------------------------------------

    static final class PlanInfo {
        final Map<String, Integer> ops = new TreeMap<>();
        final Map<String, Object> est = new LinkedHashMap<>();
        final List<String> missing = new ArrayList<>();
        /** Operators whose load estimate is Wayang's generic fallback: no wayang.postgres.*.load setting exists for them. */
        final List<String> fallback = new ArrayList<>();
    }

    /**
     * Optimizes every job the way Wayang does before running it and sums the estimates of all execution tasks,
     * like the other profilers, plus the time split by platform (postgres / java). Wayang has no load-estimate setting
     * for Postgres join, global reduce and table sink; it then uses a generic fallback (a fixed range of CPU cycles per
     * row). Those operators are listed in wayang_est_fallback, tasks without any estimate in wayang_est_missing, and
     * wayang_est_complete = 1 only if every task has a real, configured estimate.
     */
    static PlanInfo planInfo(WayangContext ctx, List<WayangPlan> plans) {
        PlanInfo info = new PlanInfo();
        long[] cpu = new long[2], disk = new long[2], net = new long[2], ram = new long[2], sinkRows = new long[2];
        long overheadMs = 0, startupMs = 0;
        int tasks = 0;
        TimeEstimate time = TimeEstimate.ZERO;
        Map<String, TimeEstimate> byPlatform = new TreeMap<>();
        ProbabilisticDoubleInterval cost = new ProbabilisticDoubleInterval(0d, 0d, 1d);
        List<String> breakdown = new ArrayList<>();
        boolean failed = false;
        for (WayangPlan plan : plans) {
            try {
                Job job = ctx.createJob("explain", plan);
                ExecutionPlan ep = job.buildInitialExecutionPlan();
                OptimizationContext oc = job.getOptimizationContext();
                Set<org.apache.wayang.core.platform.Platform> platforms = new HashSet<>();
                for (ExecutionTask t : ep.collectAllTasks()) {
                    ExecutionOperator op = t.getOperator();
                    String name = op.getClass().getSimpleName();
                    info.ops.merge(name, 1, Integer::sum);
                    platforms.add(op.getPlatform());
                    OptimizationContext.OperatorContext opCtx = oc == null ? null : oc.getOperatorContext(op);
                    LoadProfile lp = opCtx == null ? null : opCtx.getLoadProfile();
                    if (op.isSink() && op.getNumInputs() > 0 && opCtx != null && opCtx.getInputCardinality(0) != null) {
                        sinkRows[0] += opCtx.getInputCardinality(0).getLowerEstimate();
                        sinkRows[1] += opCtx.getInputCardinality(0).getUpperEstimate();
                    }
                    if (lp == null) {
                        info.missing.add(name);
                        continue;
                    }
                    for (String key : op.getLoadProfileEstimatorConfigurationKeys()) {
                        if (job.getConfiguration().getOptionalStringProperty(key).isEmpty()) {
                            info.fallback.add(name);
                            break;
                        }
                    }
                    tasks++;
                    long[] c = loadOf(lp, LoadProfile::getCpuUsage), d = loadOf(lp, LoadProfile::getDiskUsage),
                            n = loadOf(lp, LoadProfile::getNetworkUsage), r = loadOf(lp, LoadProfile::getRamUsage);
                    addTo(cpu, c);
                    addTo(disk, d);
                    addTo(net, n);
                    addTo(ram, r);
                    overheadMs += lp.getOverheadMillis();
                    TimeEstimate te = opCtx.getTimeEstimate();
                    time = time.plus(te);
                    byPlatform.merge(op.getPlatform().getConfigurationName(), te, TimeEstimate::plus);
                    cost = cost.plus(opCtx.getCostEstimate());
                    breakdown.add(String.format("%s in=%s out=%s cpu=%s disk=%s net=%s ms=%s", name,
                            cards(opCtx.getInputCardinalities()), cards(opCtx.getOutputCardinalities()),
                            range(c[0], c[1]), range(d[0], d[1]), range(n[0], n[1]),
                            range(te.getLowerEstimate(), te.getUpperEstimate())));
                }
                for (org.apache.wayang.core.platform.Platform p : platforms) {
                    startupMs += job.getConfiguration().getPlatformStartUpTimeProvider().provideFor(p);
                }
            } catch (Exception e) {
                System.err.println("[wayangprof] could not read the execution plan or its estimates: " + e);
                failed = true;
            }
        }
        if (failed) return info;
        Map<String, Object> m = info.est;
        m.put("wayang_est_tasks", tasks);
        m.put("wayang_est_complete", info.missing.isEmpty() && info.fallback.isEmpty() ? 1 : 0);
        m.put("wayang_est_missing", String.join("+", info.missing));
        m.put("wayang_est_fallback", String.join("+", info.fallback));
        m.put("wayang_est_cpu_cycles_lower", cpu[0]);
        m.put("wayang_est_cpu_cycles_upper", cpu[1]);
        m.put("wayang_est_disk_bytes_lower", disk[0]);
        m.put("wayang_est_disk_bytes_upper", disk[1]);
        m.put("wayang_est_network_bytes_lower", net[0]);
        m.put("wayang_est_network_bytes_upper", net[1]);
        m.put("wayang_est_ram_bytes_lower", ram[0]);
        m.put("wayang_est_ram_bytes_upper", ram[1]);
        m.put("wayang_est_overhead_ms", overheadMs);
        m.put("wayang_est_time_ms_lower", time.getLowerEstimate());
        m.put("wayang_est_time_ms_upper", time.getUpperEstimate());
        m.put("wayang_est_time_ms_gmean", time.getGeometricMeanEstimate());
        m.put("wayang_est_time_prob", time.getCorrectnessProbability());
        for (String p : List.of("postgres", "java")) {
            TimeEstimate te = byPlatform.getOrDefault(p, TimeEstimate.ZERO);
            m.put("wayang_est_time_ms_gmean_" + p, te.getGeometricMeanEstimate());
        }
        m.put("wayang_est_cost_lower", cost.getLowerEstimate());
        m.put("wayang_est_cost_upper", cost.getUpperEstimate());
        m.put("wayang_est_startup_ms", startupMs);
        m.put("wayang_est_out_rows_lower", sinkRows[0]);
        m.put("wayang_est_out_rows_upper", sinkRows[1]);
        breakdown.sort(null);
        m.put("wayang_est_breakdown", String.join(" | ", breakdown));
        return info;
    }

    /** Wayang settings that turn load profiles into time (Postgres part and Java part). */
    static Map<String, Object> wayangSettings(Configuration conf) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("wayang_cpu_mhz", conf.getLongProperty("wayang.postgres.cpu.mhz"));
        m.put("wayang_cores", conf.getLongProperty("wayang.postgres.cores"));
        m.put("wayang_disk_ms_per_mb", 0d);     // Wayang's Postgres converter counts only CPU
        m.put("wayang_network_ms_per_mb", 0d);
        m.put("wayang_cost_per_ms", conf.getDoubleProperty("wayang.postgres.costs.per-ms"));
        m.put("wayang_java_cpu_mhz", conf.getLongProperty("wayang.java.cpu.mhz"));
        m.put("wayang_java_cores", conf.getLongProperty("wayang.java.cores"));
        m.put("wayang_cost_model", conf.getOptionalStringProperty("wayang.core.optimizer.cost.model").orElse("default"));
        return m;
    }

    // ------------------------------------------------------------------------------------------
    // Measurement
    // ------------------------------------------------------------------------------------------

    static void quiesce() {
        System.gc();
        System.gc();
        sleep(150);
    }

    /** Before every run: no output table left (so the run's DROP TABLE IF EXISTS is a no-op), dirty pages flushed. */
    static void prepareDb(Cfg c, Db db) throws SQLException {
        if (c.op.equals("tablesink")) {
            db.exec("DROP TABLE IF EXISTS " + c.outTable());
            db.checkpoint();
        }
    }

    /** Closes what Wayang left open (SqlToStream never closes its connection) and waits for the server processes to end. */
    static List<Conn> finishRun(Db db) throws SQLException {
        List<Conn> conns = Trace.snapshot();
        for (Conn c : conns) {
            try {
                closeConn(c);
            } catch (SQLException e) {
                // already broken: nothing to close
            }
        }
        db.waitGone(conns.stream().map(x -> x.backendPid).collect(Collectors.toList()));
        Trace.conns.clear();
        return conns;
    }

    /** Runs both plans of one kind once without measuring (extra warm-up); returns the SQL the plan ran. */
    static List<String> warmRun(WayangContext ctx, Cfg c, Db db, boolean baseline) throws SQLException {
        prepareDb(c, db);
        Trace.conns.clear();
        for (WayangPlan plan : buildPlans(c, baseline)) ctx.execute(c.op + "-warmup", plan);
        List<String> sqls = new ArrayList<>();
        for (Conn x : finishRun(db)) {
            if (x.optimizer()) continue;
            synchronized (x.stmts) {
                for (Stmt s : x.stmts) if (!s.sql.trim().toUpperCase().startsWith("DROP")) sqls.add(s.sql);
            }
        }
        return sqls;
    }

    /** Runs one plan and returns all its measurements. */
    static Map<String, Object> measure(WayangContext ctx, Cfg c, Db db, boolean baseline, OsProbe os) throws Exception {
        List<WayangPlan> plans = buildPlans(c, baseline);
        prepareDb(c, db);
        OUT_ROWS = 0;
        Trace.conns.clear();
        quiesce();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sys_load_avg_1m", OSMX.getSystemLoadAverage());
        m.put("sys_free_memory_bytes", OSMX.getFreeMemorySize());
        resetPoolPeaks();
        LiveHeap.maxLiveAfterGc = 0;
        LiveHeap.maxGcPauseMs = 0;
        Map<String, Long> jvm0 = jvmCounters();
        OsSnap os0 = os.snapshot();
        long children0 = db.childrenCpuNs();

        LiveHeap.measuring = true;
        long me = Thread.currentThread().getId();
        long t0 = System.nanoTime();
        long cpu0 = TMX.getCurrentThreadCpuTime();
        long alloc0 = TMX.getThreadAllocatedBytes(me);
        long pcpu0 = OSMX.getProcessCpuTime();
        long talloc0 = TMX.getTotalThreadAllocatedBytes();

        int nJobs = plans.size();
        long[] jStart = new long[nJobs], jEnd = new long[nJobs], jCpu = new long[nJobs], jAlloc = new long[nJobs],
                jPcpu = new long[nJobs], jJit = new long[nJobs], jGc = new long[nJobs];
        for (int j = 0; j < nJobs; j++) {
            jStart[j] = System.nanoTime();
            ctx.execute(c.op + (baseline ? "-baseline" : ""), plans.get(j));
            jEnd[j] = System.nanoTime();
            jCpu[j] = TMX.getCurrentThreadCpuTime();
            jAlloc[j] = TMX.getThreadAllocatedBytes(me);
            jPcpu[j] = OSMX.getProcessCpuTime();
            jJit[j] = jitTotalMs();
            jGc[j] = gcTotalMs();
        }

        long t1 = System.nanoTime();
        long cpu1 = TMX.getCurrentThreadCpuTime();
        long alloc1 = TMX.getThreadAllocatedBytes(me);
        long pcpu1 = OSMX.getProcessCpuTime();
        long talloc1 = TMX.getTotalThreadAllocatedBytes();
        Map<String, Long> peaks = poolPeaks();
        sleep(50); // let late GC notifications arrive
        LiveHeap.measuring = false;
        OsSnap os1 = os.snapshot();
        Map<String, Long> jvm1 = jvmCounters();
        List<Conn> conns = finishRun(db);
        long children1 = db.childrenCpuNs();

        // Exec window of each job: from the first connection Wayang opened to execute it (not its optimizer's) to the end.
        boolean tapped = true, sameThread = true;
        long wallExec = 0, cpuExec = 0, allocExec = 0, pcpuExec = 0, jitExec = 0, gcExec = 0;
        long connectNs = 0, queryNs = 0, bytesIn = 0, bytesOut = 0, optQueryNs = 0;
        long backendCpu = 0, allBackendCpu = 0, optCpu = 0, readBytes = 0, writeBytes = 0;
        boolean cpuKnown = LOCAL_DB, ioKnown = LOCAL_DB;
        int execConns = 0, execStmts = 0;
        List<String> sqls = new ArrayList<>();
        for (int j = 0; j < nJobs; j++) {
            Conn first = null;
            for (Conn x : conns) {
                if (x.connectStartNs < jStart[j] || x.connectStartNs > jEnd[j] || x.optimizer()) continue;
                if (first == null || x.connectStartNs < first.connectStartNs) first = x;
            }
            if (first == null) {
                tapped = false;
                continue;
            }
            if (first.threadId != me) sameThread = false;
            wallExec += jEnd[j] - first.connectStartNs;
            cpuExec += jCpu[j] - first.threadCpuNs;
            allocExec += jAlloc[j] - first.threadAllocBytes;
            pcpuExec += jPcpu[j] - first.processCpuNs;
            jitExec += jJit[j] - first.jitMs;
            gcExec += jGc[j] - first.gcMs;
        }
        for (Conn x : conns) {
            if (x.backendCpuNs < 0) cpuKnown = false;
            else allBackendCpu += x.backendCpuNs;
            if (x.optimizer()) {
                synchronized (x.stmts) {
                    for (Stmt s : x.stmts) optQueryNs += s.endNs - s.startNs;
                }
                optCpu += Math.max(0, x.backendCpuNs);
                continue;
            }
            execConns++;
            connectNs += x.connectEndNs - x.connectStartNs;
            bytesIn += x.bytesIn.get();
            bytesOut += x.bytesOut.get();
            backendCpu += Math.max(0, x.backendCpuNs);
            if (x.backendReadBytes < 0) ioKnown = false;
            readBytes += Math.max(0, x.backendReadBytes);
            writeBytes += Math.max(0, x.backendWriteBytes);
            synchronized (x.stmts) {
                for (Stmt s : x.stmts) {
                    execStmts++;
                    queryNs += s.endNs - s.startNs;
                    sqls.add(s.sql);
                }
            }
        }

        // Output
        if (c.op.equals("tablesink") && !baseline) {
            m.put("out_rows", db.queryLong("SELECT count(*) FROM " + c.outTable()));
            m.put("out_bytes", db.queryLong("SELECT pg_total_relation_size('" + c.outTable() + "')"));
        } else {
            m.put("out_rows", OUT_ROWS);
            m.put("out_bytes", 0L);
        }
        m.put("same_thread", sameThread ? 1 : 0);
        m.put("jobs", nJobs);
        m.put("sql", String.join(" ; ", sqls));
        // Time. total = whole execute() calls; exec = from the first execution connection to the end
        m.put("wall_total_ns", t1 - t0);
        m.put("wall_exec_ns", tapped ? wallExec : null);
        m.put("wall_connect_ns", connectNs);                     // opening connections (server process start, login)
        m.put("wall_query_ns", queryNs);                         // statement calls: database work + receiving all rows
        m.put("wall_client_ns", tapped ? wallExec - connectNs - queryNs : null);  // the rest: Java turns rows into Records, sink, Wayang
        // JVM CPU and memory (the Java side: driver decoding, Records, sink)
        m.put("thread_cpu_total_ns", cpu1 - cpu0);
        m.put("thread_cpu_exec_ns", tapped && sameThread ? cpuExec : null);
        m.put("process_cpu_ns", pcpu1 - pcpu0);
        m.put("process_cpu_exec_ns", tapped ? pcpuExec : null);
        m.put("jit_compile_exec_ms", tapped ? jitExec : null);
        m.put("gc_time_exec_ms", tapped ? gcExec : null);
        m.put("thread_alloc_total_bytes", alloc1 - alloc0);
        m.put("thread_alloc_exec_bytes", tapped && sameThread ? allocExec : null);
        m.put("total_alloc_bytes", talloc1 - talloc0);
        m.put("live_heap_after_gc_max_bytes", LiveHeap.maxLiveAfterGc);
        m.put("gc_max_pause_ms", LiveHeap.maxGcPauseMs);
        m.putAll(peaks);
        // Database side
        m.put("db_connections", execConns);
        m.put("db_statements", execStmts);
        m.put("network_bytes_in", bytesIn);                      // Postgres -> Java (result rows)
        m.put("network_bytes_out", bytesOut);                    // Java -> Postgres (login, SQL text)
        m.put("network_bytes", bytesIn + bytesOut);
        m.put("db_backend_cpu_ns", cpuKnown ? backendCpu : null);
        long parallel = cpuKnown && children0 >= 0 && children1 >= 0 ? (children1 - children0) - allBackendCpu : Long.MIN_VALUE;
        m.put("db_parallel_cpu_ns", parallel == Long.MIN_VALUE ? null : Math.max(0, parallel));
        m.put("db_cpu_ns", parallel == Long.MIN_VALUE ? null : backendCpu + Math.max(0, parallel));
        m.put("db_disk_read_bytes", ioKnown ? readBytes : null);
        m.put("db_disk_write_bytes", ioKnown ? writeBytes : null);
        m.put("optimizer_db_query_ns", optQueryNs);              // Wayang's SELECT count(*) per table source, while optimizing
        m.put("optimizer_db_cpu_ns", cpuKnown ? optCpu : null);
        // JVM counters
        jvm1.forEach((k, v) -> m.put(k, v - jvm0.getOrDefault(k, 0L)));
        // OS counters of the JVM process (whole run)
        os1.counters.forEach((k, v) -> {
            Long before = os0.counters.get(k);
            Long d = before == null ? null : v - before;
            if (d != null && d < 0 && k.equals("energy_pkg_nj") && OsProbe.raplRangeNj > 0) d += OsProbe.raplRangeNj;
            m.put("os_" + k, d);
        });
        os1.gauges.forEach((k, v) -> {
            Long before = os0.gauges.get(k);
            m.put("os_" + k + "_after", v);
            m.put("os_" + k + "_delta", before == null ? null : v - before);
        });
        return m;
    }

    // ------------------------------------------------------------------------------------------
    // Main
    // ------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Cfg c = parseArgs(args);
        if (!TMX.isThreadCpuTimeSupported() || !TMX.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("This JVM cannot measure per-thread CPU time or allocation.");
        }
        TMX.setThreadCpuTimeEnabled(true);
        TMX.setThreadAllocatedMemoryEnabled(true);
        LiveHeap.install();
        OsProbe os = new OsProbe(c.pidrusage);

        // Database: our own connection, the traced driver for Wayang's connections, and the input tables.
        ProfDriver.sessionOptions = c.sessionOptions();
        DriverManager.registerDriver(new ProfDriver());
        Db db = new Db(c.url, c.user, c.password, c.sessionOptions());
        LOCAL_DB = db.local();
        DataGen gen = new DataGen();
        String leftTable = c.table(c.rows, 42), rightTable = c.table(c.rightRows, 43);
        Path leftCsv = gen.ensureFile(c.dataDir, c.rows, c.keys, c.words, 42);
        long inBytes = Files.size(leftCsv), rightInBytes = 0;
        db.ensureTable(leftTable, leftCsv, c.rows, c.index);
        if (c.binary()) {
            Path rightCsv = gen.ensureFile(c.dataDir, c.rightRows, c.keys, c.words, 43);
            rightInBytes = Files.size(rightCsv);
            db.ensureTable(rightTable, rightCsv, c.rightRows, c.index);
        }

        Configuration conf = new Configuration();
        conf.setProperty("wayang.postgres.jdbc.url", ProfDriver.PREFIX + c.url);
        if (c.user != null) conf.setProperty("wayang.postgres.jdbc.user", c.user);
        if (c.password != null) conf.setProperty("wayang.postgres.jdbc.password", c.password);
        WayangContext ctx = new WayangContext(conf).withPlugin(Java.basicPlugin()).withPlugin(Postgres.plugin());

        PlanInfo baseInfo = planInfo(ctx, buildPlans(c, true));
        PlanInfo opInfo = planInfo(ctx, buildPlans(c, false));
        Trace.conns.clear();
        Map<String, Object> wayangEnv = wayangSettings(conf);
        if (!opInfo.missing.isEmpty() || !baseInfo.missing.isEmpty()) {
            System.err.println("[wayangprof] Wayang has no load estimate for: " + opInfo.missing + " " + baseInfo.missing);
        }
        if (!opInfo.fallback.isEmpty()) {
            System.err.println("[wayangprof] Wayang uses its generic fallback estimate for: " + opInfo.fallback);
        }
        if (c.estimateOnly) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (boolean baseline : new boolean[]{true, false}) {
                PlanInfo pi = baseline ? baseInfo : opInfo;
                if (pi.est.isEmpty()) continue;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("config_id", c.configId());
                row.put("plan", baseline ? "baseline" : "operator");
                row.put("plan_execution_operators", joinCounts(pi.ops));
                row.putAll(pi.est);
                row.putAll(wayangEnv);
                rows.add(row);
            }
            appendCsv(c.out, rows);
            System.exit(rows.size() == 2 ? 0 : 1);
        }
        String added = joinCounts(minus(opInfo.ops, baseInfo.ops));
        String removed = joinCounts(minus(baseInfo.ops, opInfo.ops));
        String measured = ((added.isEmpty() ? "" : "+" + added) + (removed.isEmpty() ? "" : " -" + removed)).trim();

        Map<String, Object> env = new LinkedHashMap<>();
        String host = System.getProperty("wayangprof.host", InetAddress.getLocalHost().getHostName());
        String runId = host + "-" + ProcessHandle.current().pid() + "-" + System.currentTimeMillis();
        env.put("host", host);
        env.put("os_name", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("os_arch", System.getProperty("os.arch"));
        env.put("cpu_model", System.getProperty("wayangprof.cpu", ""));
        env.put("cpus", Runtime.getRuntime().availableProcessors());
        env.put("machine_memory_bytes", OSMX.getTotalMemorySize());
        env.put("java_version", System.getProperty("java.version"));
        env.put("jvm_args", ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(a -> a.startsWith("-X")).collect(Collectors.joining(" ")));
        env.put("gc_name", ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName).collect(Collectors.joining("+")));
        env.put("max_heap_bytes", Runtime.getRuntime().maxMemory());
        env.put("os_probe", os.kind());
        env.put("pg_version", db.queryString("SHOW server_version"));
        env.put("pg_parallel_workers", c.parallel);
        env.put("pg_indexed", c.index ? 1 : 0);
        for (String s : List.of("shared_buffers", "work_mem", "effective_cache_size", "jit", "autovacuum", "random_page_cost")) {
            env.put("pg_" + s, db.queryString("SHOW " + s));
        }
        env.put("pg_options", c.pgOptions);
        env.put("db_local", LOCAL_DB ? 1 : 0);
        env.put("table_bytes", db.queryLong("SELECT pg_total_relation_size('" + leftTable + "')"));
        env.put("right_table_bytes", c.binary() ? db.queryLong("SELECT pg_total_relation_size('" + rightTable + "')") : 0L);

        System.err.printf("[wayangprof] %s  warmup=%d reps=%d  db_local=%s  measured=%s%n",
                c.configId(), c.warmup, c.reps, LOCAL_DB, measured);

        // Warm-up: JIT, Postgres caches (shared buffers, OS page cache), catalog caches. Not recorded.
        for (int i = 0; i < c.warmup; i++) {
            measure(ctx, c, db, true, os);
            measure(ctx, c, db, false, os);
        }
        long perRun = c.rows + (c.binary() ? c.rightRows : 0);
        int want = (int) Math.max(0, Math.min(c.maxExtraWarmup, (c.minWarmupRows + perRun - 1) / Math.max(1, perRun) - c.warmup));
        int extra = 0;
        long warmStart = System.nanoTime();
        List<String> baseSql = List.of(), opSql = List.of();
        for (; extra < want && System.nanoTime() - warmStart < 10_000_000_000L; extra++) {
            baseSql = warmRun(ctx, c, db, true);
            opSql = warmRun(ctx, c, db, false);
        }
        if (baseSql.isEmpty()) baseSql = warmRun(ctx, c, db, true);
        if (opSql.isEmpty()) opSql = warmRun(ctx, c, db, false);
        env.put("warmup_runs", c.warmup + extra);
        // Server-side view of each plan's SQL (untimed, same session settings).
        if (c.op.equals("tablesink")) db.exec("DROP TABLE IF EXISTS " + c.outTable());
        Map<String, Object> baseExplain = db.explain(baseSql, c.explainRuns);
        Map<String, Object> opExplain = db.explain(opSql, c.explainRuns);
        if (c.op.equals("tablesink")) db.exec("DROP TABLE IF EXISTS " + c.outTable());

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int pair = 0; pair < c.reps; pair++) {
            boolean baselineFirst = pair % 2 == 0;
            for (int k = 0; k < 2; k++) {
                boolean baseline = (k == 0) == baselineFirst;
                Map<String, Object> m = measure(ctx, c, db, baseline, os);
                String[] names = NAMES.get(c.op);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("timestamp", Instant.now().toString());
                row.put("run_id", runId);
                row.put("tag", c.tag);
                row.put("resource", c.resource);
                row.put("config_id", c.configId());
                row.put("operator", c.op);
                row.put("wayang_operator", names[0]);
                row.put("postgres_execution_operator", names[1]);
                row.put("plan", baseline ? "baseline" : "operator");
                row.put("baseline_plan", baselineKind(c));
                row.put("plan_execution_operators", joinCounts(baseline ? baseInfo.ops : opInfo.ops));
                row.put("measured_execution_operators", measured);
                row.put("pair", pair);
                row.put("source", "table");
                row.put("rows", c.rows);
                row.put("right_rows", c.binary() ? c.rightRows : 0);
                row.put("keys", c.keys);
                row.put("sel", c.op.equals("filter") ? c.sel : null);
                row.put("words", c.words);
                row.put("in_bytes", inBytes);
                row.put("right_in_bytes", c.binary() ? rightInBytes : 0);
                row.putAll(m);
                row.putAll(baseline ? baseExplain : opExplain);
                row.putAll(baseline ? baseInfo.est : opInfo.est);
                row.putAll(env);
                row.putAll(wayangEnv);
                rows.add(row);
                Object exec = m.get("wall_exec_ns") != null ? m.get("wall_exec_ns") : m.get("wall_total_ns");
                System.err.printf("[wayangprof]   pair %d %-12s exec=%8.1fms query=%8.1fms java_cpu=%8.1fms db_cpu=%s net_in=%8.2fMB out=%d%n",
                        pair, baseline ? "baseline" : c.op, ((Long) exec) / 1e6, ((Long) m.get("wall_query_ns")) / 1e6,
                        ((Long) m.get("thread_cpu_total_ns")) / 1e6,
                        m.get("db_cpu_ns") == null ? "-" : String.format("%.1fms", ((Long) m.get("db_cpu_ns")) / 1e6),
                        ((Long) m.get("network_bytes_in")) / 1e6, (Long) m.get("out_rows"));
            }
        }
        appendCsv(c.out, rows);
        if (c.op.equals("tablesink")) db.exec("DROP TABLE IF EXISTS " + c.outTable());
        if (c.dropTables) {
            db.exec("DROP TABLE IF EXISTS " + leftTable);
            if (c.binary()) db.exec("DROP TABLE IF EXISTS " + rightTable);
        }
        db.ctl.close();
        System.exit(0);
    }


    /** One resource of a load profile plus its nested (UDF) profiles, as {lower, upper}. */
    static long[] loadOf(LoadProfile lp, Function<LoadProfile, LoadEstimate> resource) {
        long[] sum = new long[2];
        List<LoadProfile> all = new ArrayList<>();
        all.add(lp);
        all.addAll(lp.getSubprofiles());
        for (LoadProfile p : all) {
            LoadEstimate e = resource.apply(p);
            if (e == null) continue;
            sum[0] += e.getLowerEstimate();
            sum[1] += e.getUpperEstimate();
        }
        return sum;
    }

    static void addTo(long[] acc, long[] v) {
        acc[0] += v[0];
        acc[1] += v[1];
    }

    static String range(long lo, long hi) {
        return lo == hi ? Long.toString(lo) : lo + ".." + hi;
    }

    static String cards(CardinalityEstimate[] cs) {
        if (cs == null || cs.length == 0) return "-";
        return Arrays.stream(cs).map(e -> e == null ? "?" : range(e.getLowerEstimate(), e.getUpperEstimate()))
                .collect(Collectors.joining("+"));
    }


    static String joinCounts(Map<String, Integer> m) {
        return m.entrySet().stream()
                .map(e -> e.getValue() == 1 ? e.getKey() : e.getKey() + "x" + e.getValue())
                .collect(Collectors.joining("+"));
    }

    /** Operators in `op` that are not in `base` (multiset difference). */
    static Map<String, Integer> minus(Map<String, Integer> op, Map<String, Integer> base) {
        Map<String, Integer> d = new TreeMap<>();
        op.forEach((k, v) -> {
            int left = v - base.getOrDefault(k, 0);
            if (left > 0) d.put(k, left);
        });
        return d;
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------------------------------
    // CSV
    // ------------------------------------------------------------------------------------------

    /**
     * Appends rows. If the rows have columns the file does not have yet (for example per-collector GC
     * columns after switching the GC), the file is rewritten with the new columns added at the end and
     * empty cells in the old rows.
     */
    static void appendCsv(Path out, List<Map<String, Object>> rows) throws IOException {
        if (rows.isEmpty()) return;
        if (out.getParent() != null) Files.createDirectories(out.getParent());
        Set<String> rowKeys = new java.util.LinkedHashSet<>();
        rows.forEach(r -> rowKeys.addAll(r.keySet()));
        List<String> header;
        boolean writeHeader;
        if (Files.exists(out) && Files.size(out) > 0) {
            // Read only the header: raw.csv grows to hundreds of MB on long runs.
            String first;
            try (BufferedReader br = Files.newBufferedReader(out, StandardCharsets.UTF_8)) {
                first = br.readLine();
            }
            header = new ArrayList<>(Arrays.asList(first.split(",", -1)));
            List<String> missing = new ArrayList<>(rowKeys);
            missing.removeAll(header);
            if (!missing.isEmpty()) {  // rare (e.g. a new GC adds columns): rewrite once with the new columns
                String pad = ",".repeat(missing.size());
                header.addAll(missing);
                Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
                try (BufferedReader br = Files.newBufferedReader(out, StandardCharsets.UTF_8);
                     BufferedWriter bw = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                    br.readLine();
                    bw.write(String.join(",", header));
                    bw.newLine();
                    for (String line; (line = br.readLine()) != null; ) {
                        bw.write(line + pad);
                        bw.newLine();
                    }
                }
                Files.move(tmp, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                System.err.println("[wayangprof] added columns to " + out.getFileName() + ": " + missing);
            }
            writeHeader = false;
        } else {
            header = new ArrayList<>(rowKeys);
            writeHeader = true;
        }
        StringBuilder sb = new StringBuilder();
        if (writeHeader) sb.append(String.join(",", header)).append('\n');
        for (Map<String, Object> r : rows) {
            for (int i = 0; i < header.size(); i++) {
                if (i > 0) sb.append(',');
                Object v = r.get(header.get(i));
                if (v != null) {
                    String s = v.toString();
                    sb.append(s.contains(",") || s.contains("\"") ? "\"" + s.replace("\"", "\"\"") + "\"" : s);
                }
            }
            sb.append('\n');
        }
        Files.write(out, sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

}

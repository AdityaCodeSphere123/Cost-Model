package wayangprof;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.basic.operators.CountOperator;
import org.apache.wayang.basic.operators.DistinctOperator;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.MaterializedGroupByOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TextFileSink;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.UnionAllOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
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

import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Measures the real resources that one Wayang Java operator uses.
 *
 * <p>One JVM run handles one configuration (operator + input size + parameters). It runs two plans:
 * <pre>
 *   baseline : source -> parse -> sink
 *   operator : source -> parse -> OPERATOR -> sink
 * </pre>
 * Both plans run in the same JVM, alternating (B,O / O,B / ...), after warm-up runs. Every run is a row
 * in the CSV. The operator's own cost is (operator run - baseline run) of the same pair; analyze.py
 * computes it. Binary operators (join, union, cartesian) use two sources; their baseline has two
 * source -> parse -> sink branches.
 *
 * <p>Every measured number is real, none comes from Wayang's cost estimates. Wayang's own estimates for the
 * same plan (CPU cycles, disk, network, RAM, time, cost) are stored next to them in separate
 * {@code wayang_est_*} columns, see {@link #planInfo}. The measurements:
 * <ul>
 *   <li>JVM MXBeans: thread and process CPU time, bytes allocated, GC count/time per collector,
 *       peak use of every memory pool, live heap after GC, JIT compile time, classes loaded, threads.</li>
 *   <li>OS counters for this process: on macOS from proc_pid_rusage + proc_pidinfo (pidrusage helper):
 *       instructions, cycles, energy, disk bytes, page faults, context switches, syscalls, memory footprint.
 *       On Linux from /proc/self/{io,stat,status} and RAPL energy when readable.</li>
 * </ul>
 */
public final class JavaOperatorProfiler {

    // ------------------------------------------------------------------------------------------
    // Operator names
    // ------------------------------------------------------------------------------------------

    /** short name -> {Wayang logical operator, Java execution operator}. */
    static final Map<String, String[]> NAMES = new LinkedHashMap<>();

    static {
        NAMES.put("map", new String[]{"MapOperator", "JavaMapOperator"});
        // Test operators for checking the profiler itself (not in the training grid), see --work:
        // burn = fixed CPU work per row, sleep = idle wait per row (no CPU).
        NAMES.put("burn", new String[]{"MapOperator", "JavaMapOperator"});
        NAMES.put("sleep", new String[]{"MapOperator", "JavaMapOperator"});
        NAMES.put("filter", new String[]{"FilterOperator", "JavaFilterOperator"});
        NAMES.put("flatmap", new String[]{"FlatMapOperator", "JavaFlatMapOperator"});
        NAMES.put("reduceby", new String[]{"ReduceByOperator", "JavaReduceByOperator"});
        NAMES.put("groupby", new String[]{"MaterializedGroupByOperator", "JavaMaterializedGroupByOperator"});
        NAMES.put("globalreduce", new String[]{"GlobalReduceOperator", "JavaGlobalReduceOperator"});
        NAMES.put("count", new String[]{"CountOperator", "JavaCountOperator"});
        NAMES.put("distinct", new String[]{"DistinctOperator", "JavaDistinctOperator"});
        NAMES.put("sort", new String[]{"SortOperator", "JavaSortOperator"});
        NAMES.put("textsink", new String[]{"TextFileSink", "JavaTextFileSink"});
        NAMES.put("join", new String[]{"JoinOperator", "JavaJoinOperator"});
        NAMES.put("union", new String[]{"UnionAllOperator", "JavaUnionAllOperator"});
        NAMES.put("cartesian", new String[]{"CartesianOperator", "JavaCartesianOperator"});
    }

    static final Set<String> BINARY = new HashSet<>(Arrays.asList("join", "union", "cartesian"));

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

    /**
     * Marks the moment the first record leaves the source. The "exec" metrics start here, so they
     * leave out Wayang's plan optimization and job setup. The Java platform runs on one thread, so the
     * thread that fires this also does the work; this is checked and stored as same_thread.
     */
    static final class Tap {
        static boolean fired;
        static long wallNs, threadCpuNs, threadUserNs, threadAllocBytes, threadId, processCpuNs, jitMs, gcMs;
        // Sums over the jobs of one measured run (the union baseline runs two jobs).
        static long sumWall, sumCpu, sumUser, sumAlloc, sumProcessCpu, sumJit, sumGc;
        static boolean allFired, allSameThread;
        /** OS counters over the same window (first record to job end), summed over the jobs; null = no OS probe. */
        static OsProbe os;
        static OsSnap osAtFire;
        static OsWindow osExec;

        static void resetRun() {
            sumWall = sumCpu = sumUser = sumAlloc = sumProcessCpu = sumJit = sumGc = 0;
            allFired = allSameThread = true;
            osExec = new OsWindow();
        }

        static void reset() {
            fired = false;
            osAtFire = null;
        }

        /** Called right after one job ends, on the thread that ran execute(). */
        static void endJob(long me) {
            if (!fired) {
                allFired = false;
                allSameThread = false;
                return;
            }
            if (threadId != me) allSameThread = false;
            sumWall += System.nanoTime() - wallNs;
            sumCpu += TMX.getCurrentThreadCpuTime() - threadCpuNs;
            sumUser += TMX.getCurrentThreadUserTime() - threadUserNs;
            sumAlloc += TMX.getThreadAllocatedBytes(me) - threadAllocBytes;
            sumProcessCpu += OSMX.getProcessCpuTime() - processCpuNs;
            sumJit += jitTotalMs() - jitMs;
            sumGc += gcTotalMs() - gcMs;
            if (os != null && osAtFire != null) osExec.add(osAtFire, os.snapshot()); // after the clocks above
        }

        static void fire() {
            if (fired) return;
            fired = true;
            // OS counters first, so the reading itself is not inside the timed window.
            if (os != null) osAtFire = os.snapshot();
            threadId = Thread.currentThread().getId();
            wallNs = System.nanoTime();
            threadCpuNs = TMX.getCurrentThreadCpuTime();
            threadUserNs = TMX.getCurrentThreadUserTime();
            threadAllocBytes = TMX.getThreadAllocatedBytes(threadId);
            processCpuNs = OSMX.getProcessCpuTime();
            jitMs = jitTotalMs();
            gcMs = gcTotalMs();
        }
    }

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

    /**
     * OS counters over one measured window. The window can have several pieces (one per job, e.g. the union
     * baseline runs two jobs): counter changes are summed, gauges are taken at the start of the first piece and
     * the end of the last one.
     */
    static final class OsWindow {
        final Map<String, Long> counters = new TreeMap<>();
        Map<String, Long> gaugesBefore, gaugesAfter;
        int pieces;

        void add(OsSnap from, OsSnap to) {
            if (from == null || to == null) return;
            to.counters.forEach((k, v) -> {
                Long before = from.counters.get(k);
                Long d = before == null ? null : v - before;
                if (d != null && d < 0 && k.equals("energy_pkg_nj") && OsProbe.raplRangeNj > 0) d += OsProbe.raplRangeNj;
                if (this.pieces == 0) this.counters.put(k, d);
                else {
                    Long acc = this.counters.get(k);
                    this.counters.put(k, acc == null || d == null ? null : acc + d);
                }
            });
            if (this.gaugesBefore == null) this.gaugesBefore = from.gauges;
            this.gaugesAfter = to.gauges;
            this.pieces++;
        }

        /** Writes os_window, os_<counter> and os_<gauge>_after / _delta. */
        void putInto(Map<String, Object> m, String window) {
            m.put("os_window", window);
            this.counters.forEach((k, v) -> m.put("os_" + k, v));
            if (this.gaugesAfter == null) return;
            this.gaugesAfter.forEach((k, v) -> {
                Long before = this.gaugesBefore.get(k);
                m.put("os_" + k + "_after", v);
                m.put("os_" + k + "_delta", before == null ? null : v - before);
            });
        }
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
        /**
         * The helper kept running in --serve mode: a reading is a pipe round trip (~20 us) instead of a new process
         * (several ms), so the counters can be read right where the measured work starts and ends. Null: start one
         * helper per reading (older helper without --serve).
         */
        private Process server;
        private BufferedWriter toServer;
        private BufferedReader fromServer;

        OsProbe(String helper) {
            this.helper = (helper != null && new File(helper).canExecute()) ? helper : null;
            if (this.helper != null) this.startServer();
        }

        private void startServer() {
            try {
                Process p = new ProcessBuilder(this.helper, "--serve").redirectError(ProcessBuilder.Redirect.DISCARD).start();
                this.toServer = new BufferedWriter(new java.io.OutputStreamWriter(p.getOutputStream(), StandardCharsets.US_ASCII));
                this.fromServer = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.US_ASCII));
                this.server = p;
                String first = this.ask(this.pid);
                if (first == null || !first.contains("c:user_ns=")) throw new IOException("no answer");
            } catch (IOException e) {
                System.err.println("[wayangprof] pidrusage --serve not available (rebuild native/pidrusage); starting it per reading");
                if (this.server != null) this.server.destroy();
                this.server = null;
            }
        }

        String kind() {
            return this.helper != null ? "macos-proc_pid_rusage" : this.linux ? "linux-procfs" : "none";
        }

        /** One reading of this JVM's own counters. */
        OsSnap snapshot() {
            OsSnap s = new OsSnap();
            if (this.helper != null) this.readMac(this.pid, s);
            else if (this.linux) this.readLinux(s);
            return s;
        }

        /** One reading of another process of the same user (macOS helper only), or null. */
        OsSnap snapshotOf(long otherPid) {
            if (this.helper == null || otherPid <= 0) return null;
            OsSnap s = new OsSnap();
            return this.readMac(otherPid, s) ? s : null;
        }

        private synchronized String ask(long p) throws IOException {
            this.toServer.write(Long.toString(p));
            this.toServer.newLine();
            this.toServer.flush();
            return this.fromServer.readLine();
        }

        private boolean readMac(long p, OsSnap s) {
            try {
                String line = null;
                if (this.server != null) {
                    try {
                        line = this.ask(p);
                    } catch (IOException e) {
                        System.err.println("[wayangprof] pidrusage --serve stopped (" + e + "); starting it per reading");
                        this.server = null;
                    }
                }
                if (this.server == null) {
                    Process proc = new ProcessBuilder(this.helper, Long.toString(p)).redirectErrorStream(true).start();
                    try (BufferedReader r = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                        line = r.readLine();
                    }
                    proc.waitFor();
                }
                if (line == null || line.startsWith("error")) return false;
                for (String item : line.trim().split(" ")) {
                    int colon = item.indexOf(':'), eq = item.indexOf('=');
                    if (colon != 1 || eq < 0) continue;
                    String name = item.substring(2, eq);
                    long v = Long.parseLong(item.substring(eq + 1));
                    (item.charAt(0) == 'g' ? s.gauges : s.counters).put(name, v);
                }
                return true;
            } catch (Exception e) {
                System.err.println("pidrusage failed: " + e);
                return false;
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
    // Config
    // ------------------------------------------------------------------------------------------

    static final class Cfg {
        String op = "filter";
        String source = "text";          // text | collection
        long rows = 100_000;
        long rightRows = -1;             // binary operators; -1 = same as rows
        int keys = 1000;
        double sel = 0.5;                // filter selectivity
        int words = 8;                   // words per row (flatmap fan-out and row width)
        int warmup = 3;
        int reps = 7;
        Path dataDir = Paths.get("data");
        Path out = Paths.get("raw.csv");
        Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String pidrusage = null;
        String coldCmd = null;           // e.g. "sudo -n purge" to empty the file cache before each measured run
        String resource = "default";     // label of the JVM resource setting (heap, GC, cores), set by the runner
        String tag = "";
        boolean estimateOnly = false;    // only write Wayang's estimates for both plans to --out, run nothing
        long work = 1000;                // test operators: burn = loop iterations per row, sleep = microseconds per row
        // Extra warm-up runs (no measurements) until each plan has read this many input rows, so small
        // configs are measured with JIT-compiled code too. At most maxExtraWarmup runs or 10 s.
        long minWarmupRows = 3_000_000;
        int maxExtraWarmup = 20;

        boolean binary() {
            return BINARY.contains(this.op);
        }

        String configId() {
            return String.format("%s|%s|%s|r%d|rr%d|k%d|s%s|w%d", this.resource, this.op, this.source, this.rows,
                    this.binary() ? this.rightRows : 0, this.keys,
                    this.op.equals("filter") ? Double.toString(this.sel) : "-", this.words);
        }
    }

    static Cfg parseArgs(String[] args) {
        Cfg c = new Cfg();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            String v = (i + 1 < args.length) ? args[i + 1] : null;
            switch (a) {
                case "--op": c.op = v; i++; break;
                case "--source": c.source = v; i++; break;
                case "--rows": c.rows = Long.parseLong(v); i++; break;
                case "--right-rows": c.rightRows = Long.parseLong(v); i++; break;
                case "--keys": c.keys = Integer.parseInt(v); i++; break;
                case "--sel": c.sel = Double.parseDouble(v); i++; break;
                case "--words": c.words = Integer.parseInt(v); i++; break;
                case "--warmup": c.warmup = Integer.parseInt(v); i++; break;
                case "--reps": c.reps = Integer.parseInt(v); i++; break;
                case "--data-dir": c.dataDir = Paths.get(v); i++; break;
                case "--out": c.out = Paths.get(v); i++; break;
                case "--tmp-dir": c.tmpDir = Paths.get(v); i++; break;
                case "--pidrusage": c.pidrusage = v; i++; break;
                case "--cold-cmd": c.coldCmd = v; i++; break;
                case "--resource": c.resource = v; i++; break;
                case "--tag": c.tag = v; i++; break;
                case "--estimate-only": c.estimateOnly = true; break;
                case "--work": c.work = Long.parseLong(v); i++; break;
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
        if (!c.source.equals("text") && !c.source.equals("collection")) {
            System.err.println("--source must be text or collection");
            System.exit(2);
        }
        if (c.rightRows < 0) c.rightRows = c.rows;
        c.keys = Math.max(1, c.keys);
        return c;
    }

    static void usage() {
        System.err.println("Usage: JavaOperatorProfiler --op <op> [--source text|collection] [--rows N] [--right-rows N]\n"
                + "         [--keys K] [--sel S] [--words W] [--warmup N] [--reps N] [--data-dir D] [--out raw.csv]\n"
                + "         [--tmp-dir D] [--pidrusage path] [--cold-cmd \"sudo -n purge\"] [--resource label] [--tag t]\n"
                + "         [--min-warmup-rows N] [--max-extra-warmup N]\n"
                + "         [--estimate-only]   (write only Wayang's estimates for the baseline and operator plan to --out)\n"
                + "Operators: " + NAMES.keySet());
    }

    // ------------------------------------------------------------------------------------------
    // Plans
    // ------------------------------------------------------------------------------------------

    static final class Inputs {
        String leftUrl, rightUrl;            // text mode
        List<Row> leftRows, rightRows;       // collection mode
        long leftBytes, rightBytes;          // size of the input on disk (text) or as text (collection)
    }

    /** source -> parse/tap. Returns the tap operator; its output 0 carries Rows. */
    static Operator sourceBranch(Cfg c, Inputs in, boolean left) {
        if (c.source.equals("text")) {
            TextFileSource src = new TextFileSource(left ? in.leftUrl : in.rightUrl);
            MapOperator<String, Row> parse = new MapOperator<>(line -> {
                if (!Tap.fired) Tap.fire();
                return Row.parse(line);
            }, String.class, Row.class);
            src.connectTo(0, parse, 0);
            return parse;
        }
        CollectionSource<Row> src = new CollectionSource<>(left ? in.leftRows : in.rightRows, Row.class);
        MapOperator<Row, Row> tap = new MapOperator<>(r -> {
            if (!Tap.fired) Tap.fire();
            return r;
        }, Row.class, Row.class);
        src.connectTo(0, tap, 0);
        return tap;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Operator countingSink(DataSetType<?> type) {
        Consumer<Object> count = x -> {
            if (MemProbe.armed) MemProbe.take();
            outRows++;
        };
        return new LocalCallbackSink(count, type);
    }

    /**
     * Memory an operator holds: in an extra, untimed run, the first row that reaches the sink triggers a full
     * GC and reads the heap still in use. Operators that collect their whole input before emitting (sort,
     * groupby, distinct, reduceby, the join's table) hold it at that moment; streaming ones (map, filter) hold
     * ~nothing. Operator run - baseline run = the operator's held memory (mem_held_bytes).
     */
    static final class MemProbe {
        static volatile boolean armed;
        static volatile long liveBytes = -1;

        static void take() {
            armed = false;
            System.gc();
            System.gc();
            liveBytes = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        }
    }

    /** Runs the plan once, untimed, and returns the live heap when its first row reached the sink (-1 if none). */
    static long memProbe(WayangContext ctx, Cfg c, Inputs in, boolean baseline, Path sinkFile) throws IOException {
        Files.deleteIfExists(sinkFile);
        List<WayangPlan> plans = buildPlans(c, in, baseline, sinkFile);
        quiesce();
        long max = -1;
        for (WayangPlan plan : plans) {  // the union baseline has two jobs: keep the larger one
            MemProbe.liveBytes = -1;
            MemProbe.armed = true;
            ctx.execute(c.op + "-memprobe", plan);
            MemProbe.armed = false;
            max = Math.max(max, MemProbe.liveBytes);
        }
        Files.deleteIfExists(sinkFile);
        return max;
    }

    static Operator unaryOperator(Cfg c, Path sinkFile) {
        switch (c.op) {
            case "map":
                return new MapOperator<Row, Row>(r -> new Row(r.id, r.key, r.value * 1.0001 + 1.0, r.text), Row.class, Row.class);
            case "burn": {
                final long n = c.work;
                return new MapOperator<Row, Row>(r -> {
                    burn(n);
                    return r;
                }, Row.class, Row.class);
            }
            case "sleep": {
                final long us = c.work;
                return new MapOperator<Row, Row>(r -> {
                    java.util.concurrent.locks.LockSupport.parkNanos(us * 1000);
                    return r;
                }, Row.class, Row.class);
            }
            case "filter": {
                final int threshold = (int) Math.round(c.sel * c.keys); // keys are uniform in [0, keys)
                return new FilterOperator<Row>(r -> r.key < threshold, Row.class);
            }
            case "flatmap":
                return new FlatMapOperator<Row, String>(r -> Arrays.asList(r.text.split(" ")), Row.class, String.class);
            case "reduceby":
                return new ReduceByOperator<Row, Integer>(r -> r.key,
                        (a, b) -> new Row(a.id, a.key, a.value + b.value, a.text), Integer.class, Row.class);
            case "groupby":
                return new MaterializedGroupByOperator<Row, Integer>(r -> r.key, Row.class, Integer.class);
            case "globalreduce":
                return new GlobalReduceOperator<Row>((a, b) -> new Row(a.id, a.key, a.value + b.value, a.text), Row.class);
            case "count":
                return new CountOperator<>(Row.class);
            case "distinct":
                return new DistinctOperator<>(Row.class);
            case "sort":
                return new SortOperator<Row, Double>(r -> r.value, Row.class, Double.class);
            case "textsink":
                return new TextFileSink<Row>(sinkFile.toUri().toString(), r -> {
                    if (MemProbe.armed) MemProbe.take();
                    outRows++;
                    return r.toString();
                }, Row.class);
            default:
                throw new IllegalArgumentException(c.op);
        }
    }

    static volatile long BURN_SINK;

    /** Pure CPU work: n rounds of xorshift, no allocation, no memory traffic. */
    static void burn(long n) {
        long x = 88172645463325252L;
        for (long i = 0; i < n; i++) {
            x ^= x << 13;
            x ^= x >>> 7;
            x ^= x << 17;
        }
        BURN_SINK = x;
    }

    /** CPU nanoseconds of one burn(n), measured on this thread after warm-up: the expected cost per row. */
    static double burnCalibration(long n) {
        for (int i = 0; i < 20_000; i++) burn(n);
        long c0 = TMX.getCurrentThreadCpuTime();
        for (int i = 0; i < 20_000; i++) burn(n);
        return (TMX.getCurrentThreadCpuTime() - c0) / 20_000.0;
    }

    static Operator binaryOperator(Cfg c) {
        switch (c.op) {
            case "join":
                return new JoinOperator<Row, Row, Integer>(r -> r.key, r -> r.key, Row.class, Row.class, Integer.class);
            case "union":
                return new UnionAllOperator<>(Row.class);
            case "cartesian":
                return new CartesianOperator<>(Row.class, Row.class);
            default:
                throw new IllegalArgumentException(c.op);
        }
    }

    /** What the baseline of this operator is, written to the CSV. */
    static String baselineKind(Cfg c) {
        if (!c.binary()) return "source>parse>sink";
        if (c.op.equals("union")) return "2 jobs: left>parse>sink, right>parse>sink";
        return "left>parse,right>parse>UnionAll>sink";
    }

    /**
     * Builds fresh plans (one per job). Plans are rebuilt for every run because Wayang changes them while
     * executing. Wayang only optimizes connected plans, so a baseline with two inputs is either joined by
     * UnionAll (the cheapest binary operator, used for join and cartesian) or run as two jobs (for union).
     */
    static List<WayangPlan> buildPlans(Cfg c, Inputs in, boolean baseline, Path sinkFile) {
        if (!c.binary()) {
            Operator left = sourceBranch(c, in, true);
            if (baseline) {
                Operator sink = countingSink(left.getOutput(0).getType());
                left.connectTo(0, sink, 0);
                return List.of(new WayangPlan(sink));
            }
            Operator op = unaryOperator(c, sinkFile);
            left.connectTo(0, op, 0);
            if (c.op.equals("textsink")) return List.of(new WayangPlan(op));
            Operator sink = countingSink(op.getOutput(0).getType());
            op.connectTo(0, sink, 0);
            return List.of(new WayangPlan(sink));
        }
        if (baseline && c.op.equals("union")) {
            Operator left = sourceBranch(c, in, true);
            Operator right = sourceBranch(c, in, false);
            Operator s0 = countingSink(left.getOutput(0).getType());
            Operator s1 = countingSink(right.getOutput(0).getType());
            left.connectTo(0, s0, 0);
            right.connectTo(0, s1, 0);
            return List.of(new WayangPlan(s0), new WayangPlan(s1));
        }
        Operator left = sourceBranch(c, in, true);
        Operator right = sourceBranch(c, in, false);
        Operator op = baseline ? new UnionAllOperator<>(Row.class) : binaryOperator(c);
        left.connectTo(0, op, 0);
        right.connectTo(0, op, 1);
        Operator sink = countingSink(op.getOutput(0).getType());
        op.connectTo(0, sink, 0);
        return List.of(new WayangPlan(sink));
    }

    // ------------------------------------------------------------------------------------------
    // Wayang's own estimates
    // ------------------------------------------------------------------------------------------

    /** What Wayang's optimizer picks for a plan and what it predicts the plan will cost. */
    static final class PlanInfo {
        /** Execution operators Wayang picks (including channel conversions), as a count map. */
        final Map<String, Integer> ops = new TreeMap<>();
        /** wayang_est_* columns; empty if Wayang could not estimate every task. */
        final Map<String, Object> est = new LinkedHashMap<>();
    }

    /**
     * Optimizes every job of the plan the way Wayang does before running it, and reads the estimates the
     * optimizer stored for each execution task (regular operators and channel conversions alike):
     * <ul>
     *   <li>load profile: CPU cycles, disk bytes, network bytes, RAM bytes (each a lower..upper interval;
     *       a UDF's nested profile is added to its operator, as Wayang's time converter does), fixed overhead;</li>
     *   <li>the time (ms) and cost Wayang derives from it with the platform's converters;</li>
     *   <li>the estimated number of rows that reach the sink(s).</li>
     * </ul>
     * Values are summed over all tasks and jobs, like the measured columns cover the whole run, so
     * (operator plan - baseline plan) is Wayang's estimate of the operator. Platform start-up time is kept
     * apart in wayang_est_startup_ms. Each profiler plan runs on one platform, so there is only one possible
     * execution plan, and the pick (scalar or vector cost model) cannot change these numbers.
     */
    static PlanInfo planInfo(WayangContext ctx, List<WayangPlan> plans) {
        PlanInfo info = new PlanInfo();
        long[] cpu = new long[2], disk = new long[2], net = new long[2], ram = new long[2], sinkRows = new long[2];
        long overheadMs = 0, startupMs = 0;
        int tasks = 0;
        TimeEstimate time = TimeEstimate.ZERO;
        ProbabilisticDoubleInterval cost = new ProbabilisticDoubleInterval(0d, 0d, 1d);
        List<String> breakdown = new ArrayList<>();
        boolean ok = true;
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
                    OptimizationContext.OperatorContext opCtx = oc == null ? null : oc.getOperatorContext(op);
                    LoadProfile lp = opCtx == null ? null : opCtx.getLoadProfile();
                    if (lp == null) {
                        System.err.println("[wayangprof] Wayang has no estimate for " + name);
                        ok = false;
                        continue;
                    }
                    platforms.add(op.getPlatform());
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
                    cost = cost.plus(opCtx.getCostEstimate());
                    if (op.isSink() && op.getNumInputs() > 0 && opCtx.getInputCardinality(0) != null) {
                        sinkRows[0] += opCtx.getInputCardinality(0).getLowerEstimate();
                        sinkRows[1] += opCtx.getInputCardinality(0).getUpperEstimate();
                    }
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
                ok = false;
            }
        }
        if (!ok) return info; // no partial sums: they would look like a cheaper plan
        Map<String, Object> m = info.est;
        m.put("wayang_est_tasks", tasks);
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
        m.put("wayang_est_cost_lower", cost.getLowerEstimate());
        m.put("wayang_est_cost_upper", cost.getUpperEstimate());
        m.put("wayang_est_startup_ms", startupMs);
        m.put("wayang_est_out_rows_lower", sinkRows[0]);
        m.put("wayang_est_out_rows_upper", sinkRows[1]);
        breakdown.sort(null); // tasks come from a set; sort for a stable text
        m.put("wayang_est_breakdown", String.join(" | ", breakdown));
        return info;
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

    /** Wayang settings that turn a load profile into time, so the estimates can be reproduced. */
    static Map<String, Object> wayangSettings(Configuration conf) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("wayang_cpu_mhz", conf.getLongProperty("wayang.java.cpu.mhz"));
        m.put("wayang_cores", conf.getLongProperty("wayang.java.cores"));
        m.put("wayang_disk_ms_per_mb", conf.getDoubleProperty("wayang.java.hdfs.ms-per-mb"));
        m.put("wayang_network_ms_per_mb", 0d); // the Java platform has no network cost
        m.put("wayang_cost_per_ms", conf.getDoubleProperty("wayang.java.costs.per-ms"));
        m.put("wayang_cost_model", conf.getOptionalStringProperty("wayang.core.optimizer.cost.model").orElse("default"));
        return m;
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

    // ------------------------------------------------------------------------------------------
    // Measurement
    // ------------------------------------------------------------------------------------------

    static void quiesce() {
        System.gc();
        System.gc();
        sleep(150);
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void runCold(Cfg c) {
        if (c.coldCmd == null || !c.source.equals("text")) return;
        try {
            new ProcessBuilder("/bin/sh", "-c", c.coldCmd).inheritIO().start().waitFor();
        } catch (Exception e) {
            System.err.println("cold command failed: " + e);
        }
    }

    /** Runs one plan and returns all its measurements. */
    static Map<String, Object> measure(WayangContext ctx, Cfg c, Inputs in, boolean baseline, OsProbe os,
                                       Path sinkFile) throws IOException {
        Files.deleteIfExists(sinkFile);
        List<WayangPlan> plans = buildPlans(c, in, baseline, sinkFile);
        Tap.resetRun();
        outRows = 0;
        runCold(c);
        quiesce();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sys_load_avg_1m", OSMX.getSystemLoadAverage());
        m.put("sys_free_memory_bytes", OSMX.getFreeMemorySize());
        resetPoolPeaks();
        LiveHeap.maxLiveAfterGc = 0;
        LiveHeap.maxGcPauseMs = 0;
        Map<String, Long> jvm0 = jvmCounters();
        OsSnap os0 = os.snapshot();

        LiveHeap.measuring = true;
        long me = Thread.currentThread().getId();
        long t0 = System.nanoTime();
        long cpu0 = TMX.getCurrentThreadCpuTime();
        long user0 = TMX.getCurrentThreadUserTime();
        long alloc0 = TMX.getThreadAllocatedBytes(me);
        long pcpu0 = OSMX.getProcessCpuTime();
        long talloc0 = TMX.getTotalThreadAllocatedBytes();

        for (WayangPlan plan : plans) {
            Tap.reset();
            ctx.execute(c.op + (baseline ? "-baseline" : ""), plan);
            Tap.endJob(me);
        }

        long t1 = System.nanoTime();
        long cpu1 = TMX.getCurrentThreadCpuTime();
        long user1 = TMX.getCurrentThreadUserTime();
        long alloc1 = TMX.getThreadAllocatedBytes(me);
        long pcpu1 = OSMX.getProcessCpuTime();
        long talloc1 = TMX.getTotalThreadAllocatedBytes();
        OsSnap os1 = os.snapshot(); // right at the end, before the pause below
        Map<String, Long> peaks = poolPeaks();
        sleep(50); // let late GC notifications arrive
        LiveHeap.measuring = false;
        Map<String, Long> jvm1 = jvmCounters();

        boolean tapped = Tap.allFired;
        boolean sameThread = tapped && Tap.allSameThread;

        m.put("out_rows", outRows);
        m.put("out_bytes", Files.exists(sinkFile) ? sizeOf(sinkFile) : 0L);
        m.put("same_thread", sameThread ? 1 : 0);
        // Time and CPU. "total" = whole execute() call; "exec" = from the first record to the end.
        m.put("wall_total_ns", t1 - t0);
        m.put("jobs", plans.size());
        m.put("wall_exec_ns", tapped ? Tap.sumWall : null);
        m.put("thread_cpu_total_ns", cpu1 - cpu0);
        m.put("thread_cpu_exec_ns", sameThread ? Tap.sumCpu : null);
        m.put("thread_user_total_ns", user1 - user0);
        m.put("thread_user_exec_ns", sameThread ? Tap.sumUser : null);
        m.put("process_cpu_ns", pcpu1 - pcpu0);
        // All threads of the JVM from the first record to the end: the operator thread plus the GC and JIT work
        // it causes (thread_cpu_exec_ns leaves those out).
        m.put("process_cpu_exec_ns", tapped ? Tap.sumProcessCpu : null);
        // JIT and GC between the first record and the end. JIT here means the code was not fully compiled yet.
        m.put("jit_compile_exec_ms", tapped ? Tap.sumJit : null);
        m.put("gc_time_exec_ms", tapped ? Tap.sumGc : null);
        // Memory
        m.put("thread_alloc_total_bytes", alloc1 - alloc0);
        m.put("thread_alloc_exec_bytes", sameThread ? Tap.sumAlloc : null);
        m.put("total_alloc_bytes", talloc1 - talloc0);
        m.put("live_heap_after_gc_max_bytes", LiveHeap.maxLiveAfterGc);
        m.put("gc_max_pause_ms", LiveHeap.maxGcPauseMs);
        m.putAll(peaks);
        // JVM counters
        jvm1.forEach((k, v) -> m.put(k, v - jvm0.getOrDefault(k, 0L)));
        // OS counters over the exec window (first record to job end, like wall_exec_ns), so they leave out Wayang's
        // optimizer and the JIT/GC work after the run. Window "run" (whole execute() calls) only if the tap did not fire.
        if (tapped && Tap.osExec.pieces == plans.size()) {
            Tap.osExec.putInto(m, "exec");
        } else {
            OsWindow run = new OsWindow();
            run.add(os0, os1);
            run.putInto(m, "run");
        }
        // Network: the Java platform runs in one JVM and sends nothing over the network.
        m.put("network_bytes", 0L);
        Files.deleteIfExists(sinkFile);
        return m;
    }

    static long sizeOf(Path p) throws IOException {
        if (Files.isDirectory(p)) {
            try (var s = Files.walk(p)) {
                return s.filter(Files::isRegularFile).mapToLong(f -> f.toFile().length()).sum();
            }
        }
        return Files.size(p);
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
        Tap.os = os.kind().equals("none") ? null : os;

        // Inputs. The right input uses another seed so it is not a copy of the left one.
        DataGen gen = new DataGen();
        Inputs in = new Inputs();
        if (c.source.equals("text")) {
            Path l = gen.ensureFile(c.dataDir, c.rows, c.keys, c.words, 42);
            in.leftUrl = l.toUri().toString();
            in.leftBytes = Files.size(l);
            if (c.binary()) {
                Path r = gen.ensureFile(c.dataDir, c.rightRows, c.keys, c.words, 43);
                in.rightUrl = r.toUri().toString();
                in.rightBytes = Files.size(r);
            }
        } else {
            in.leftRows = gen.inMemory(c.rows, c.keys, c.words, 42);
            in.leftBytes = in.leftRows.stream().mapToLong(x -> x.toString().length() + 1).sum();
            if (c.binary()) {
                in.rightRows = gen.inMemory(c.rightRows, c.keys, c.words, 43);
                in.rightBytes = in.rightRows.stream().mapToLong(x -> x.toString().length() + 1).sum();
            }
        }

        Configuration conf = new Configuration();
        WayangContext ctx = new WayangContext(conf).withPlugin(Java.basicPlugin());
        Files.createDirectories(c.tmpDir);
        Path sinkFile = c.tmpDir.resolve("wayangprof-sink-" + ProcessHandle.current().pid() + ".txt");

        // Which Java operators each plan really runs, and what Wayang estimates each plan costs.
        PlanInfo baseInfo = planInfo(ctx, buildPlans(c, in, true, sinkFile));
        PlanInfo opInfo = planInfo(ctx, buildPlans(c, in, false, sinkFile));
        Map<String, Integer> baseOps = baseInfo.ops;
        Map<String, Integer> opOps = opInfo.ops;
        Map<String, Object> wayangEnv = wayangSettings(conf);
        if (baseInfo.est.isEmpty() || opInfo.est.isEmpty()) {
            System.err.println("[wayangprof] WARNING: no Wayang estimates for " + c.configId() + "; wayang_est_* stay empty");
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
            System.err.printf("[wayangprof] %s  Wayang estimate: baseline %s ms, operator %s ms%n", c.configId(),
                    baseInfo.est.get("wayang_est_time_ms_gmean"), opInfo.est.get("wayang_est_time_ms_gmean"));
            System.exit(rows.size() == 2 ? 0 : 1);
        }
        // What (operator run - baseline run) measures: operators only in the operator plan, minus those only in the baseline.
        String added = joinCounts(minus(opOps, baseOps));
        String removed = joinCounts(minus(baseOps, opOps));
        String measured = "+" + added + (removed.isEmpty() ? "" : " -" + removed);

        // Fixed facts about this JVM and machine.
        Map<String, Object> env = new LinkedHashMap<>();
        String host = InetAddress.getLocalHost().getHostName();
        String runId = host + "-" + ProcessHandle.current().pid() + "-" + System.currentTimeMillis();
        env.put("host", host);
        env.put("os_name", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("os_arch", System.getProperty("os.arch"));
        env.put("cpu_model", System.getProperty("wayangprof.cpu", ""));
        env.put("cpus", Runtime.getRuntime().availableProcessors());
        env.put("machine_memory_bytes", OSMX.getTotalMemorySize());
        env.put("java_version", System.getProperty("java.version"));
        env.put("jvm_args", String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments()
                .stream().filter(a -> a.startsWith("-X")).collect(Collectors.toList())));
        env.put("gc_name", ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName).collect(Collectors.joining("+")));
        env.put("max_heap_bytes", Runtime.getRuntime().maxMemory());
        env.put("os_probe", os.kind());
        if (c.op.equals("burn")) env.put("burn_calibrated_ns_per_row", burnCalibration(c.work));
        if (c.op.equals("burn") || c.op.equals("sleep")) env.put("work", c.work);

        System.err.printf("[wayangprof] %s  warmup=%d reps=%d  os_probe=%s  measured=%s%n",
                c.configId(), c.warmup, c.reps, os.kind(), measured);

        // Warm-up: let the JIT compile both plans' code. Not recorded.
        for (int i = 0; i < c.warmup; i++) {
            measure(ctx, c, in, true, os, sinkFile);
            measure(ctx, c, in, false, os, sinkFile);
        }
        // Small inputs: a few warm-up runs are too few rows for the JIT to compile the per-row code, so the
        // measured runs would partly run interpreted. Run both plans more often (cheap: no measuring).
        long perRun = c.rows + (c.binary() ? c.rightRows : 0);
        int want = (int) Math.max(0, Math.min(c.maxExtraWarmup, (c.minWarmupRows + perRun - 1) / Math.max(1, perRun) - c.warmup));
        int extra = 0;
        long warmStart = System.nanoTime();
        for (; extra < want && System.nanoTime() - warmStart < 10_000_000_000L; extra++) {
            for (boolean baseline : new boolean[]{true, false}) {
                Files.deleteIfExists(sinkFile);
                for (WayangPlan plan : buildPlans(c, in, baseline, sinkFile)) {
                    Tap.reset();
                    ctx.execute(c.op + "-warmup", plan);
                }
            }
        }
        Files.deleteIfExists(sinkFile);
        env.put("warmup_runs", c.warmup + extra);

        // Held memory of each plan, from one extra untimed run each (see MemProbe).
        long memBase = memProbe(ctx, c, in, true, sinkFile);
        long memOp = memProbe(ctx, c, in, false, sinkFile);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int pair = 0; pair < c.reps; pair++) {
            // Swap the order every other pair so slow drift (heat, background load) hits both plans equally.
            boolean baselineFirst = pair % 2 == 0;
            for (int k = 0; k < 2; k++) {
                boolean baseline = (k == 0) == baselineFirst;
                Map<String, Object> m = measure(ctx, c, in, baseline, os, sinkFile);
                String[] names = NAMES.get(c.op);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("timestamp", Instant.now().toString());
                row.put("run_id", runId);
                row.put("tag", c.tag);
                row.put("resource", c.resource);
                row.put("config_id", c.configId());
                row.put("operator", c.op);
                row.put("wayang_operator", names[0]);
                row.put("java_execution_operator", names[1]);
                row.put("plan", baseline ? "baseline" : "operator");
                row.put("baseline_plan", baselineKind(c));
                row.put("plan_execution_operators", joinCounts(baseline ? baseOps : opOps));
                row.put("measured_execution_operators", measured);
                row.put("pair", pair);
                row.put("source", c.source);
                row.put("rows", c.rows);
                row.put("right_rows", c.binary() ? c.rightRows : 0);
                row.put("keys", c.keys);
                row.put("sel", c.op.equals("filter") ? c.sel : null);
                row.put("words", c.words);
                row.put("in_bytes", in.leftBytes);
                row.put("right_in_bytes", c.binary() ? in.rightBytes : 0);
                row.putAll(m);
                long held = baseline ? memBase : memOp;
                row.put("mem_held_bytes", held >= 0 ? held : null);  // same value in every pair of this plan
                row.putAll(baseline ? baseInfo.est : opInfo.est); // Wayang's estimate for this plan
                row.putAll(env);
                row.putAll(wayangEnv);
                rows.add(row);
                Object exec = m.get("wall_exec_ns") != null ? m.get("wall_exec_ns") : m.get("wall_total_ns");
                System.err.printf("[wayangprof]   pair %d %-9s exec=%8.1fms cpu=%8.1fms alloc=%9.1fMB energy=%s out=%d%n",
                        pair, baseline ? "baseline" : c.op, ((Long) exec) / 1e6,
                        ((Long) m.get("thread_cpu_total_ns")) / 1e6,
                        ((Long) m.get("thread_alloc_total_bytes")) / 1e6,
                        m.get("os_energy_nj") == null ? "-" : String.format("%.3fJ", ((Long) m.get("os_energy_nj")) / 1e9),
                        (Long) m.get("out_rows"));
            }
        }
        appendCsv(c.out, rows);
        System.exit(0); // Wayang can leave non-daemon threads behind
    }
}

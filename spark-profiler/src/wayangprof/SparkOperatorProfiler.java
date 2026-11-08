package wayangprof;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
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
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkContextReference;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Measures the real resources that one Wayang Spark operator uses, with Spark in local mode
 * (a single-node cluster: driver and executor share one JVM, one task thread per core).
 *
 * <p>Same method as the Java profiler. One JVM run handles one configuration and runs two plans:
 * <pre>
 *   baseline : source -> parse -> sink
 *   operator : source -> parse -> OPERATOR -> sink
 * </pre>
 * alternating (B,O / O,B / ...) after warm-up runs. analyze.py subtracts the baseline from the operator
 * run of each pair.
 *
 * <p>Spark-specific choices:
 * <ul>
 *   <li>One SparkContext is kept for the whole JVM. Wayang closes it after every job by default, which would
 *       put ~2 s of Spark start-up into every run.</li>
 *   <li>The sink is {@link CountingSink}: it runs {@code rdd.count()}, which reads every row on all cores
 *       and sends only a number to the driver. Wayang's LocalCallbackSink pulls every row to the driver one
 *       partition at a time, which would make the sink the bottleneck and serialize the plan.</li>
 *   <li>Besides JVM and OS counters, a SparkListener sums Spark's own task metrics (executor CPU, shuffle
 *       bytes, spill, peak execution memory, ...) over all tasks of the run.</li>
 * </ul>
 *
 * <p>Wayang's own estimates for the same plan (CPU cycles, disk, network, RAM, time, cost) are stored next
 * to the measurements in separate {@code wayang_est_*} columns, see {@link #planInfo}.
 */
public final class SparkOperatorProfiler {

    // ------------------------------------------------------------------------------------------
    // Operator names
    // ------------------------------------------------------------------------------------------

    /** short name -> {Wayang logical operator, Spark execution operator}. */
    static final Map<String, String[]> NAMES = new LinkedHashMap<>();

    static {
        NAMES.put("map", new String[]{"MapOperator", "SparkMapOperator"});
        // Test operators for checking the profiler itself (not in the training grid), see --work:
        // burn = fixed CPU work per row, sleep = idle wait per row (no CPU).
        NAMES.put("burn", new String[]{"MapOperator", "SparkMapOperator"});
        NAMES.put("sleep", new String[]{"MapOperator", "SparkMapOperator"});
        NAMES.put("filter", new String[]{"FilterOperator", "SparkFilterOperator"});
        NAMES.put("flatmap", new String[]{"FlatMapOperator", "SparkFlatMapOperator"});
        NAMES.put("reduceby", new String[]{"ReduceByOperator", "SparkReduceByOperator"});
        NAMES.put("groupby", new String[]{"MaterializedGroupByOperator", "SparkMaterializedGroupByOperator"});
        NAMES.put("globalreduce", new String[]{"GlobalReduceOperator", "SparkGlobalReduceOperator"});
        NAMES.put("count", new String[]{"CountOperator", "SparkCountOperator"});
        NAMES.put("distinct", new String[]{"DistinctOperator", "SparkDistinctOperator"});
        NAMES.put("sort", new String[]{"SortOperator", "SparkSortOperator"});
        NAMES.put("textsink", new String[]{"TextFileSink", "SparkTextFileSink"});
        NAMES.put("join", new String[]{"JoinOperator", "SparkJoinOperator"});
        NAMES.put("union", new String[]{"UnionAllOperator", "SparkUnionAllOperator"});
        NAMES.put("cartesian", new String[]{"CartesianOperator", "SparkCartesianOperator"});
    }

    static final Set<String> BINARY = new HashSet<>(Arrays.asList("join", "union", "cartesian"));

    // ------------------------------------------------------------------------------------------
    // Data (identical to the Java profiler, so both read the same files)
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
     * Marks the moment the first record leaves the source, on whichever Spark task thread gets there
     * first. The "exec" metrics start here, so they leave out Wayang's plan optimization and Spark's job
     * set-up. In local mode the tasks run in this JVM, so a static field is visible to them.
     */
    static final class Tap {
        static final AtomicBoolean fired = new AtomicBoolean();
        static volatile long wallNs, processCpuNs, totalAllocBytes;
        // Sums over the jobs of one measured run (the union baseline runs two jobs).
        static long sumWall, sumCpu, sumAlloc;
        static boolean allFired;

        static void resetRun() {
            sumWall = sumCpu = sumAlloc = 0;
            allFired = true;
        }

        static void reset() {
            fired.set(false);
        }

        static void fire() {
            if (!fired.compareAndSet(false, true)) return;
            wallNs = System.nanoTime();
            processCpuNs = OSMX.getProcessCpuTime();
            totalAllocBytes = TMX.getTotalThreadAllocatedBytes();
        }

        /** Called on the driver right after one job ends. */
        static void endJob() {
            if (!fired.get()) {
                allFired = false;
                return;
            }
            sumWall += System.nanoTime() - wallNs;
            sumCpu += OSMX.getProcessCpuTime() - processCpuNs;
            sumAlloc += TMX.getTotalThreadAllocatedBytes() - totalAllocBytes;
        }
    }

    /** Output records counted by the sink (or by the text sink's formatter). */
    static final AtomicLong OUT_ROWS = new AtomicLong();

    /** Largest live heap right after a GC during the measured run, plus the longest GC pause. */
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
    // Spark task metrics
    // ------------------------------------------------------------------------------------------

    /**
     * Sums Spark's own metrics over every task, stage and job of one measured run. Spark reports them
     * on its listener bus, which is asynchronous; {@link #drain} waits until all events have arrived.
     */
    static final class SparkStats extends SparkListener {
        private final Map<String, Long> m = new TreeMap<>();

        synchronized void reset() {
            this.m.clear();
            for (String k : List.of("spark_jobs", "spark_stages", "spark_tasks", "spark_stage_tasks_max",
                    "task_run_ms", "task_run_ms_max", "task_cpu_ns", "task_deserialize_ms", "task_deserialize_cpu_ns",
                    "task_gc_ms", "task_result_bytes", "task_result_serialize_ms", "task_peak_execution_memory_bytes",
                    "task_peak_execution_memory_bytes_max", "spill_memory_bytes", "spill_disk_bytes",
                    "input_bytes", "input_records", "output_bytes", "output_records",
                    "shuffle_write_bytes", "shuffle_write_records", "shuffle_write_time_ns",
                    "shuffle_read_local_bytes", "shuffle_read_remote_bytes", "shuffle_read_remote_to_disk_bytes",
                    "shuffle_read_records", "shuffle_fetch_wait_ms")) {
                this.m.put(k, 0L);
            }
        }

        synchronized Map<String, Long> snapshot() {
            return new TreeMap<>(this.m);
        }

        private void add(String k, long v) {
            this.m.merge(k, v, Long::sum);
        }

        private void max(String k, long v) {
            this.m.merge(k, v, Math::max);
        }

        @Override
        public synchronized void onJobStart(SparkListenerJobStart e) {
            this.add("spark_jobs", 1);
        }

        @Override
        public synchronized void onStageCompleted(SparkListenerStageCompleted e) {
            this.add("spark_stages", 1);
            this.max("spark_stage_tasks_max", e.stageInfo().numTasks());
        }

        @Override
        public synchronized void onTaskEnd(SparkListenerTaskEnd e) {
            this.add("spark_tasks", 1);
            TaskMetrics t = e.taskMetrics();
            if (t == null) return;
            this.add("task_run_ms", t.executorRunTime());
            this.max("task_run_ms_max", t.executorRunTime());
            this.add("task_cpu_ns", t.executorCpuTime());
            this.add("task_deserialize_ms", t.executorDeserializeTime());
            this.add("task_deserialize_cpu_ns", t.executorDeserializeCpuTime());
            this.add("task_gc_ms", t.jvmGCTime());
            this.add("task_result_bytes", t.resultSize());
            this.add("task_result_serialize_ms", t.resultSerializationTime());
            this.add("task_peak_execution_memory_bytes", t.peakExecutionMemory());
            this.max("task_peak_execution_memory_bytes_max", t.peakExecutionMemory());
            this.add("spill_memory_bytes", t.memoryBytesSpilled());
            this.add("spill_disk_bytes", t.diskBytesSpilled());
            this.add("input_bytes", t.inputMetrics().bytesRead());
            this.add("input_records", t.inputMetrics().recordsRead());
            this.add("output_bytes", t.outputMetrics().bytesWritten());
            this.add("output_records", t.outputMetrics().recordsWritten());
            this.add("shuffle_write_bytes", t.shuffleWriteMetrics().bytesWritten());
            this.add("shuffle_write_records", t.shuffleWriteMetrics().recordsWritten());
            this.add("shuffle_write_time_ns", t.shuffleWriteMetrics().writeTime());
            this.add("shuffle_read_local_bytes", t.shuffleReadMetrics().localBytesRead());
            this.add("shuffle_read_remote_bytes", t.shuffleReadMetrics().remoteBytesRead());
            this.add("shuffle_read_remote_to_disk_bytes", t.shuffleReadMetrics().remoteBytesReadToDisk());
            this.add("shuffle_read_records", t.shuffleReadMetrics().recordsRead());
            this.add("shuffle_fetch_wait_ms", t.shuffleReadMetrics().fetchWaitTime());
        }

        /** Waits until Spark has delivered every listener event of the finished jobs. */
        static void drain(JavaSparkContext jsc) {
            try {
                jsc.sc().listenerBus().waitUntilEmpty(10_000);
            } catch (Exception e) {
                sleep(300);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Sink
    // ------------------------------------------------------------------------------------------

    /**
     * Spark sink that counts its input with {@code rdd.count()}: every partition is read in parallel on
     * the executor threads and only the count goes to the driver. It also accepts a driver-side collection
     * (the output of count and globalreduce) so no extra conversion job is added for those.
     *
     * <p>It is an execution operator placed directly in the plan, so Wayang uses it as it is.
     */
    public static final class CountingSink<T> extends LocalCallbackSink<T> implements SparkExecutionOperator {

        public CountingSink(DataSetType<T> type) {
            super(x -> { }, type);
        }

        @Override
        public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
                ChannelInstance[] inputs, ChannelInstance[] outputs, SparkExecutor executor,
                OptimizationContext.OperatorContext operatorContext) {
            ChannelInstance in = inputs[0];
            long n = (in instanceof RddChannel.Instance)
                    ? ((RddChannel.Instance) in).provideRdd().count()
                    : ((CollectionChannel.Instance) in).provideCollection().size();
            OUT_ROWS.addAndGet(n);
            return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
        }

        @Override
        protected ExecutionOperator createCopy() {
            return new CountingSink<>(this.getType());
        }

        @Override
        public String getLoadProfileEstimatorConfigurationKey() {
            return "wayang.spark.localcallbacksink.load";
        }

        @Override
        public List<ChannelDescriptor> getSupportedInputChannels(int index) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR, CollectionChannel.DESCRIPTOR);
        }

        @Override
        public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
            throw new UnsupportedOperationException("CountingSink has no outputs.");
        }

        @Override
        public boolean containsAction() {
            return true;
        }
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
        final boolean linux = new File("/proc/self/io").canRead();

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
            try {
                for (String l : Files.readAllLines(Paths.get("/proc/self/io"))) {
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
                String stat = new String(Files.readAllBytes(Paths.get("/proc/self/stat")), StandardCharsets.UTF_8);
                String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
                s.counters.put("page_faults", Long.parseLong(f[7]));
                s.counters.put("pageins", Long.parseLong(f[9]));
                s.counters.put("user_ns", Long.parseLong(f[11]) * 10_000_000L);
                s.counters.put("system_ns", Long.parseLong(f[12]) * 10_000_000L);
                for (String l : Files.readAllLines(Paths.get("/proc/self/status"))) {
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
                Path rapl = Paths.get("/sys/class/powercap/intel-rapl:0/energy_uj");
                if (Files.isReadable(rapl)) {
                    s.counters.put("energy_pkg_nj", Long.parseLong(Files.readAllLines(rapl).get(0).trim()) * 1000L);
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
        String cores = "all";            // Spark task threads: local[N] or local[*]
        Path dataDir = Paths.get("data");
        Path out = Paths.get("raw.csv");
        Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String pidrusage = null;
        String coldCmd = null;
        String resource = "default";
        String tag = "";
        boolean estimateOnly = false;    // only write Wayang's estimates for both plans to --out, run nothing
        long work = 1000;                // test operators: burn = loop iterations per row, sleep = microseconds per row

        boolean binary() {
            return BINARY.contains(this.op);
        }

        int coreCount() {
            return this.cores.equals("all") ? Runtime.getRuntime().availableProcessors() : Integer.parseInt(this.cores);
        }

        String master() {
            return this.cores.equals("all") ? "local[*]" : "local[" + this.cores + "]";
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
                case "--cores": c.cores = v; i++; break;
                case "--data-dir": c.dataDir = Paths.get(v); i++; break;
                case "--out": c.out = Paths.get(v); i++; break;
                case "--tmp-dir": c.tmpDir = Paths.get(v); i++; break;
                case "--pidrusage": c.pidrusage = v; i++; break;
                case "--cold-cmd": c.coldCmd = v; i++; break;
                case "--resource": c.resource = v; i++; break;
                case "--tag": c.tag = v; i++; break;
                case "--estimate-only": c.estimateOnly = true; break;
                case "--work": c.work = Long.parseLong(v); i++; break;
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
        if (!c.cores.equals("all") && !c.cores.matches("[1-9][0-9]*")) {
            System.err.println("--cores must be a positive number or all");
            System.exit(2);
        }
        if (c.rightRows < 0) c.rightRows = c.rows;
        c.keys = Math.max(1, c.keys);
        return c;
    }

    static void usage() {
        System.err.println("Usage: SparkOperatorProfiler --op <op> [--source text|collection] [--rows N] [--right-rows N]\n"
                + "         [--keys K] [--sel S] [--words W] [--warmup N] [--reps N] [--cores N|all] [--data-dir D]\n"
                + "         [--out raw.csv] [--tmp-dir D] [--pidrusage path] [--cold-cmd \"sudo -n purge\"]\n"
                + "         [--resource label] [--tag t]\n"
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
                if (!Tap.fired.get()) Tap.fire();
                return Row.parse(line);
            }, String.class, Row.class);
            src.connectTo(0, parse, 0);
            return parse;
        }
        CollectionSource<Row> src = new CollectionSource<>(left ? in.leftRows : in.rightRows, Row.class);
        MapOperator<Row, Row> tap = new MapOperator<>(r -> {
            if (!Tap.fired.get()) Tap.fire();
            return r;
        }, Row.class, Row.class);
        src.connectTo(0, tap, 0);
        return tap;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Operator countingSink(DataSetType<?> type) {
        return new CountingSink(type);
    }

    static Operator unaryOperator(Cfg c, Path sinkDir) {
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
                return new TextFileSink<Row>(sinkDir.toUri().toString(), r -> {
                    OUT_ROWS.incrementAndGet();
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
     * Builds fresh plans (one per job), exactly like the Java profiler. A baseline with two inputs is joined
     * by UnionAll (for join and cartesian) or run as two jobs (for union), because Wayang only optimizes
     * connected plans.
     */
    static List<WayangPlan> buildPlans(Cfg c, Inputs in, boolean baseline, Path sinkDir) {
        if (!c.binary()) {
            Operator left = sourceBranch(c, in, true);
            if (baseline) {
                Operator sink = countingSink(left.getOutput(0).getType());
                left.connectTo(0, sink, 0);
                return List.of(new WayangPlan(sink));
            }
            Operator op = unaryOperator(c, sinkDir);
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
        m.put("wayang_cpu_mhz", conf.getLongProperty("wayang.spark.cpu.mhz"));
        // Wayang divides CPU cycles by machines x cores-per-machine (set to the task threads in main).
        m.put("wayang_cores", conf.getLongProperty("wayang.spark.machines") * conf.getLongProperty("wayang.spark.cores-per-machine"));
        m.put("wayang_disk_ms_per_mb", conf.getDoubleProperty("wayang.spark.hdfs.ms-per-mb"));
        m.put("wayang_network_ms_per_mb", conf.getDoubleProperty("wayang.spark.network.ms-per-mb"));
        m.put("wayang_cost_per_ms", conf.getDoubleProperty("wayang.spark.costs.per-ms"));
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

    static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> s = Files.walk(p)) {
            for (Path x : s.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) Files.deleteIfExists(x);
        }
    }

    /** Runs one plan and returns all its measurements. */
    static Map<String, Object> measure(WayangContext ctx, JavaSparkContext jsc, SparkStats stats, Cfg c, Inputs in,
                                       boolean baseline, OsProbe os, Path sinkDir) throws IOException {
        deleteTree(sinkDir);
        List<WayangPlan> plans = buildPlans(c, in, baseline, sinkDir);
        Tap.resetRun();
        OUT_ROWS.set(0);
        runCold(c);
        quiesce();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sys_load_avg_1m", OSMX.getSystemLoadAverage());
        m.put("sys_free_memory_bytes", OSMX.getFreeMemorySize());
        resetPoolPeaks();
        LiveHeap.maxLiveAfterGc = 0;
        LiveHeap.maxGcPauseMs = 0;
        SparkStats.drain(jsc);
        stats.reset();
        Map<String, Long> jvm0 = jvmCounters();
        OsSnap os0 = os.snapshot();

        LiveHeap.measuring = true;
        long t0 = System.nanoTime();
        long cpu0 = TMX.getCurrentThreadCpuTime();
        long pcpu0 = OSMX.getProcessCpuTime();
        long talloc0 = TMX.getTotalThreadAllocatedBytes();

        for (WayangPlan plan : plans) {
            Tap.reset();
            ctx.execute(c.op + (baseline ? "-baseline" : ""), plan);
            Tap.endJob();
        }

        long t1 = System.nanoTime();
        long cpu1 = TMX.getCurrentThreadCpuTime();
        long pcpu1 = OSMX.getProcessCpuTime();
        long talloc1 = TMX.getTotalThreadAllocatedBytes();
        Map<String, Long> peaks = poolPeaks();
        SparkStats.drain(jsc);
        Map<String, Long> spark = stats.snapshot();
        sleep(50); // let late GC notifications arrive
        LiveHeap.measuring = false;
        OsSnap os1 = os.snapshot();
        Map<String, Long> jvm1 = jvmCounters();

        boolean tapped = Tap.allFired;
        m.put("out_rows", OUT_ROWS.get());
        m.put("out_bytes", Files.exists(sinkDir) ? sizeOf(sinkDir) : 0L);
        // Time and CPU. "total" = whole execute() call; "exec" = from the first record to the end.
        m.put("wall_total_ns", t1 - t0);
        m.put("jobs", plans.size());
        m.put("wall_exec_ns", tapped ? Tap.sumWall : null);
        m.put("process_cpu_ns", pcpu1 - pcpu0);
        m.put("process_cpu_exec_ns", tapped ? Tap.sumCpu : null);
        m.put("driver_thread_cpu_ns", cpu1 - cpu0);
        // Memory
        m.put("total_alloc_bytes", talloc1 - talloc0);
        m.put("total_alloc_exec_bytes", tapped ? Tap.sumAlloc : null);
        m.put("live_heap_after_gc_max_bytes", LiveHeap.maxLiveAfterGc);
        m.put("gc_max_pause_ms", LiveHeap.maxGcPauseMs);
        m.putAll(peaks);
        // Spark's own task metrics, summed over all tasks of this run
        m.putAll(spark);
        // JVM counters
        jvm1.forEach((k, v) -> m.put(k, v - jvm0.getOrDefault(k, 0L)));
        // OS counters
        os1.counters.forEach((k, v) -> {
            Long before = os0.counters.get(k);
            m.put("os_" + k, before == null ? null : v - before);
        });
        os1.gauges.forEach((k, v) -> {
            Long before = os0.gauges.get(k);
            m.put("os_" + k + "_after", v);
            m.put("os_" + k + "_delta", before == null ? null : v - before);
        });
        // Network: in local mode all shuffle data is read from the local block manager, so this is 0.
        // On a real cluster most of shuffle_write_bytes would cross the network.
        m.put("network_bytes", spark.getOrDefault("shuffle_read_remote_bytes", 0L));
        deleteTree(sinkDir);
        return m;
    }

    static long sizeOf(Path p) throws IOException {
        if (Files.isDirectory(p)) {
            try (Stream<Path> s = Files.walk(p)) {
                // Spark writes part-* files plus _SUCCESS and .crc checksums; count only the data.
                return s.filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().startsWith("part-"))
                        .mapToLong(f -> f.toFile().length()).sum();
            }
        }
        return Files.size(p);
    }

    // ------------------------------------------------------------------------------------------
    // CSV
    // ------------------------------------------------------------------------------------------

    /**
     * Appends rows. If the rows have columns the file does not have yet, the file is rewritten with the
     * new columns added at the end and empty cells in the old rows.
     */
    static void appendCsv(Path out, List<Map<String, Object>> rows) throws IOException {
        if (rows.isEmpty()) return;
        if (out.getParent() != null) Files.createDirectories(out.getParent());
        Set<String> rowKeys = new java.util.LinkedHashSet<>();
        rows.forEach(r -> rowKeys.addAll(r.keySet()));
        List<String> header;
        boolean writeHeader;
        if (Files.exists(out) && Files.size(out) > 0) {
            List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
            header = new ArrayList<>(Arrays.asList(lines.get(0).split(",", -1)));
            List<String> missing = new ArrayList<>(rowKeys);
            missing.removeAll(header);
            if (!missing.isEmpty()) {
                String pad = ",".repeat(missing.size());
                header.addAll(missing);
                List<String> widened = new ArrayList<>(lines.size());
                widened.add(String.join(",", header));
                for (int i = 1; i < lines.size(); i++) widened.add(lines.get(i) + pad);
                Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
                Files.write(tmp, widened, StandardCharsets.UTF_8);
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
            throw new IllegalStateException("This JVM cannot measure thread CPU time or allocation.");
        }
        TMX.setThreadCpuTimeEnabled(true);
        TMX.setThreadAllocatedMemoryEnabled(true);
        LiveHeap.install();
        OsProbe os = new OsProbe(c.pidrusage);

        // Inputs: the same files as the Java profiler. The right input uses another seed.
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

        // Spark in local mode: one machine with `cores` task threads. Wayang uses
        // 2 x machines x cores-per-machine partitions for shuffles (reduceby, groupby, join, sort, distinct).
        int cores = c.coreCount();
        Configuration conf = new Configuration();
        conf.setProperty("spark.master", c.master());
        conf.setProperty("spark.app.name", "wayangprof");
        conf.setProperty("spark.ui.showConsoleProgress", "false");
        conf.setProperty("wayang.spark.machines", "1");
        conf.setProperty("wayang.spark.cores-per-machine", Integer.toString(cores));
        if (System.getProperty("spark.ui.enabled") == null) System.setProperty("spark.ui.enabled", "false");
        WayangContext ctx = new WayangContext(conf).withPlugin(Spark.basicPlugin());
        Files.createDirectories(c.tmpDir);
        Path sinkDir = c.tmpDir.resolve("wayangprof-sink-" + ProcessHandle.current().pid());

        // Which Spark operators each plan really runs, and what Wayang estimates each plan costs.
        // Only the optimizer runs here; it does not need the SparkContext.
        PlanInfo baseInfo = planInfo(ctx, buildPlans(c, in, true, sinkDir));
        PlanInfo opInfo = planInfo(ctx, buildPlans(c, in, false, sinkDir));
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

        // Start Spark once and hold a reference, so Wayang does not close it after every job.
        Job pin = ctx.createJob("wayangprof-start", buildPlans(c, in, true, sinkDir).get(0));
        SparkContextReference sparkRef = SparkPlatform.getInstance().getSparkContext(pin);
        sparkRef.noteObtainedReference();
        JavaSparkContext jsc = sparkRef.get();
        SparkStats stats = new SparkStats();
        jsc.sc().addSparkListener(stats);
        int shufflePartitions = 2 * cores;
        int inPartitions, rightPartitions = 0;
        if (c.source.equals("text")) {
            inPartitions = jsc.textFile(in.leftUrl).getNumPartitions();
            if (c.binary()) rightPartitions = jsc.textFile(in.rightUrl).getNumPartitions();
        } else {
            inPartitions = shufflePartitions; // Wayang parallelizes collections into this many partitions
            if (c.binary()) rightPartitions = shufflePartitions;
        }

        String added = joinCounts(minus(opOps, baseOps));
        String removed = joinCounts(minus(baseOps, opOps));
        String measured = "+" + added + (removed.isEmpty() ? "" : " -" + removed);

        // Fixed facts about this JVM, Spark and machine.
        Map<String, Object> env = new LinkedHashMap<>();
        // A machine label, not the computer's name (set by the runner; falls back to the hostname).
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
        env.put("spark_version", jsc.version());
        env.put("spark_master", jsc.master());
        env.put("deploy_mode", "local-single-node");
        env.put("executors", 1);
        env.put("task_slots", jsc.sc().defaultParallelism());
        env.put("shuffle_partitions", shufflePartitions);
        env.put("in_partitions", inPartitions);
        env.put("right_in_partitions", rightPartitions);
        env.put("serializer", jsc.getConf().get("spark.serializer", "org.apache.spark.serializer.JavaSerializer"));
        env.put("os_probe", os.kind());
        if (c.op.equals("burn")) env.put("burn_calibrated_ns_per_row", burnCalibration(c.work));
        if (c.op.equals("burn") || c.op.equals("sleep")) env.put("work", c.work);

        System.err.printf("[wayangprof] %s  master=%s  spark=%s  warmup=%d reps=%d  os_probe=%s  measured=%s%n",
                c.configId(), jsc.master(), jsc.version(), c.warmup, c.reps, os.kind(), measured);

        // Warm-up: let the JIT compile both plans' code and Spark load its classes. Not recorded.
        for (int i = 0; i < c.warmup; i++) {
            measure(ctx, jsc, stats, c, in, true, os, sinkDir);
            measure(ctx, jsc, stats, c, in, false, os, sinkDir);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int pair = 0; pair < c.reps; pair++) {
            // Swap the order every other pair so slow drift (heat, background load) hits both plans equally.
            boolean baselineFirst = pair % 2 == 0;
            for (int k = 0; k < 2; k++) {
                boolean baseline = (k == 0) == baselineFirst;
                Map<String, Object> m = measure(ctx, jsc, stats, c, in, baseline, os, sinkDir);
                String[] names = NAMES.get(c.op);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("timestamp", Instant.now().toString());
                row.put("run_id", runId);
                row.put("tag", c.tag);
                row.put("resource", c.resource);
                row.put("config_id", c.configId());
                row.put("operator", c.op);
                row.put("wayang_operator", names[0]);
                row.put("spark_execution_operator", names[1]);
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
                row.putAll(baseline ? baseInfo.est : opInfo.est); // Wayang's estimate for this plan
                row.putAll(env);
                row.putAll(wayangEnv);
                rows.add(row);
                Object exec = m.get("wall_exec_ns") != null ? m.get("wall_exec_ns") : m.get("wall_total_ns");
                System.err.printf("[wayangprof]   pair %d %-9s exec=%8.1fms cpu=%8.1fms task_cpu=%8.1fms shuffle=%7.1fMB tasks=%d energy=%s out=%d%n",
                        pair, baseline ? "baseline" : c.op, ((Long) exec) / 1e6,
                        ((Long) m.get("process_cpu_ns")) / 1e6,
                        ((Long) m.get("task_cpu_ns")) / 1e6,
                        ((Long) m.get("shuffle_write_bytes")) / 1e6,
                        (Long) m.get("spark_tasks"),
                        m.get("os_energy_nj") == null ? "-" : String.format("%.3fJ", ((Long) m.get("os_energy_nj")) / 1e9),
                        (Long) m.get("out_rows"));
            }
        }
        appendCsv(c.out, rows);
        System.exit(0); // Spark and Wayang leave non-daemon threads behind
    }
}

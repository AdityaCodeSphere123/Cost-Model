package wayangprof;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CountOperator;
import org.apache.wayang.basic.operators.DistinctOperator;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.LoadEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkContextReference;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Runs multi-operator Wayang jobs on Java only, Spark only, and mixed (scan + first operator on Spark, the rest
 * on Java), and records for each job:
 * <ul>
 *   <li>the measured runtime (from the first record to the end, like the profilers' wall_exec_ns);</li>
 *   <li>Wayang's estimate for every execution task (load profile, time, cardinalities);</li>
 *   <li>one row per operator with the features the cost model needs (true and Wayang-estimated row counts).</li>
 * </ul>
 * evaluate_jobs.py then compares measured vs Wayang vs the cost model.
 *
 * Outputs (in --out): job_runs.csv, job_tasks.csv, job_operators.csv.
 */
public final class JobBench {

    // ------------------------------------------------------------------------------------------
    // Data (same generator and files as the profilers)
    // ------------------------------------------------------------------------------------------

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
            return new Row(Long.parseLong(line, 0, a, 10), Integer.parseInt(line, a + 1, b, 10),
                    Double.parseDouble(line.substring(b + 1, c)), line.substring(c + 1));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Row && ((Row) o).key == this.key;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(this.key);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Measurement hooks
    // ------------------------------------------------------------------------------------------

    static final AtomicBoolean FIRED = new AtomicBoolean();
    static final AtomicLong FIRST_NS = new AtomicLong();
    static final AtomicLong OUT_ROWS = new AtomicLong();

    static void tap() {
        if (!FIRED.get() && FIRED.compareAndSet(false, true)) FIRST_NS.set(System.nanoTime());
    }

    /** Spark sink that counts with rdd.count() (the same sink the Spark profiler measured). */
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
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAction() {
            return true;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Jobs
    // ------------------------------------------------------------------------------------------

    enum Placement { java, spark, mixed }

    /** One job at one size: how to build it, and its true row counts per operator. */
    static final class JobSpec {
        String name, size, leftFile, rightFile;
        double sel;
    }

    static <T extends Operator> T on(T op, boolean spark) {
        op.addTargetPlatform(spark ? Spark.platform() : Java.platform());
        return op;
    }

    static Operator scan(String url, boolean spark) {
        TextFileSource src = on(new TextFileSource(url), spark);
        MapOperator<String, Row> parse = on(new MapOperator<>(line -> {
            tap();
            return Row.parse(line);
        }, String.class, Row.class), spark);
        src.connectTo(0, parse, 0);
        return parse;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Operator sink(Operator last, boolean spark) {
        DataSetType<?> type = last.getOutput(0).getType();
        Operator s = spark ? new CountingSink(type) : on(new LocalCallbackSink(x -> OUT_ROWS.incrementAndGet(), type), false);
        last.connectTo(0, s, 0);
        return s;
    }

    /**
     * Builds the job. Mixed: the scan and the first operator run on Spark, everything after it on Java
     * (a big parallel scan, then a smaller result handled locally).
     */
    static WayangPlan build(JobSpec j, Placement p, Path dir) {
        boolean first = p != Placement.java;     // scan + first operator
        boolean rest = p == Placement.spark;     // everything after the first operator
        String left = dir.resolve(j.leftFile).toUri().toString();
        Operator a = scan(left, first);
        Operator last;
        switch (j.name) {
            case "filter-reduceby": {
                final int thr = threshold(j);
                FilterOperator<Row> f = on(new FilterOperator<Row>(r -> r.key < thr, Row.class), first);
                ReduceByOperator<Row, Integer> rb = on(new ReduceByOperator<Row, Integer>(r -> r.key,
                        (x, y) -> new Row(x.id, x.key, x.value + y.value, x.text), Integer.class, Row.class), rest);
                a.connectTo(0, f, 0);
                f.connectTo(0, rb, 0);
                last = rb;
                break;
            }
            case "map-sort": {
                MapOperator<Row, Row> m = on(new MapOperator<Row, Row>(r -> new Row(r.id, r.key, r.value * 1.0001 + 1.0, r.text),
                        Row.class, Row.class), first);
                SortOperator<Row, Double> s = on(new SortOperator<Row, Double>(r -> r.value, Row.class, Double.class), rest);
                a.connectTo(0, m, 0);
                m.connectTo(0, s, 0);
                last = s;
                break;
            }
            case "filter-distinct": {
                final int thr = threshold(j);
                FilterOperator<Row> f = on(new FilterOperator<Row>(r -> r.key < thr, Row.class), first);
                DistinctOperator<Row> d = on(new DistinctOperator<>(Row.class), rest);
                a.connectTo(0, f, 0);
                f.connectTo(0, d, 0);
                last = d;
                break;
            }
            case "flatmap-count": {
                FlatMapOperator<Row, String> fm = on(new FlatMapOperator<Row, String>(r -> Arrays.asList(r.text.split(" ")),
                        Row.class, String.class), first);
                CountOperator<String> c = on(new CountOperator<>(String.class), rest);
                a.connectTo(0, fm, 0);
                fm.connectTo(0, c, 0);
                last = c;
                break;
            }
            case "join-map-reduceby": {
                Operator b = scan(dir.resolve(j.rightFile).toUri().toString(), first);
                JoinOperator<Row, Row, Integer> jn = on(new JoinOperator<Row, Row, Integer>(r -> r.key, r -> r.key,
                        Row.class, Row.class, Integer.class), first);
                @SuppressWarnings("unchecked")
                MapOperator<Tuple2<Row, Row>, Row> m = on(new MapOperator<>(
                        (Tuple2<Row, Row> t) -> new Row(t.field0.id, t.field0.key, t.field0.value + t.field1.value, t.field0.text),
                        (Class<Tuple2<Row, Row>>) (Class<?>) Tuple2.class, Row.class), rest);
                ReduceByOperator<Row, Integer> rb = on(new ReduceByOperator<Row, Integer>(r -> r.key,
                        (x, y) -> new Row(x.id, x.key, x.value + y.value, x.text), Integer.class, Row.class), rest);
                a.connectTo(0, jn, 0);
                b.connectTo(0, jn, 1);
                jn.connectTo(0, m, 0);
                m.connectTo(0, rb, 0);
                last = rb;
                break;
            }
            default:
                throw new IllegalArgumentException(j.name);
        }
        return new WayangPlan(sink(last, rest));
    }

    static int fileKeys(String file) {
        String s = file.substring(file.indexOf("_keys-") + 6);
        return Integer.parseInt(s.substring(0, s.indexOf('_')));
    }

    static int threshold(JobSpec j) {
        return (int) Math.round(j.sel * fileKeys(j.leftFile));
    }

    // ------------------------------------------------------------------------------------------
    // True row counts per operator (computed directly from the input files)
    // ------------------------------------------------------------------------------------------

    static final class Stage {
        String op;                 // profiler name: filter, reduceby, ...
        long inRows, rightRows, outRows, keys;
        double inBytes, rightBytes;
        Double sel;
    }

    static List<Row> read(Path p) throws IOException {
        try (var lines = Files.lines(p, StandardCharsets.UTF_8)) {
            return lines.map(Row::parse).collect(Collectors.toList());
        }
    }

    static long distinctKeys(List<Row> rows) {
        return rows.stream().mapToInt(r -> r.key).distinct().count();
    }

    static Stage stage(String op, long in, double inBytes, long out, long keys) {
        Stage s = new Stage();
        s.op = op;
        s.inRows = in;
        s.inBytes = inBytes;
        s.outRows = out;
        s.keys = keys;
        return s;
    }

    static List<Stage> trueStages(JobSpec j, Path dir) throws IOException {
        Path lp = dir.resolve(j.leftFile);
        List<Row> rows = read(lp);
        double bpr = (double) Files.size(lp) / rows.size();
        long fk = fileKeys(j.leftFile);
        List<Stage> st = new ArrayList<>();
        switch (j.name) {
            case "filter-reduceby":
            case "filter-distinct": {
                int thr = threshold(j);
                List<Row> f = rows.stream().filter(r -> r.key < thr).collect(Collectors.toList());
                Stage s1 = stage("filter", rows.size(), rows.size() * bpr, f.size(), Math.min(fk, rows.size()));
                s1.sel = j.sel;
                st.add(s1);
                long k = distinctKeys(f);
                st.add(stage(j.name.endsWith("reduceby") ? "reduceby" : "distinct", f.size(), f.size() * bpr, k, k));
                break;
            }
            case "map-sort":
                st.add(stage("map", rows.size(), rows.size() * bpr, rows.size(), fk));
                st.add(stage("sort", rows.size(), rows.size() * bpr, rows.size(), fk));
                break;
            case "flatmap-count": {
                long words = rows.stream().mapToLong(r -> r.text.split(" ").length).sum();
                double wordBytes = rows.stream().mapToLong(r -> r.text.length() + 1).sum() / (double) words;
                st.add(stage("flatmap", rows.size(), rows.size() * bpr, words, fk));
                st.add(stage("count", words, words * wordBytes, 1, fk));
                break;
            }
            case "join-map-reduceby": {
                Path rp = dir.resolve(j.rightFile);
                List<Row> right = read(rp);
                Map<Integer, Long> cl = new HashMap<>(), cr = new HashMap<>();
                rows.forEach(r -> cl.merge(r.key, 1L, Long::sum));
                right.forEach(r -> cr.merge(r.key, 1L, Long::sum));
                long out = 0;
                Set<Integer> outKeys = new HashSet<>();
                for (Map.Entry<Integer, Long> e : cl.entrySet()) {
                    Long n = cr.get(e.getKey());
                    if (n != null) {
                        out += e.getValue() * n;
                        outKeys.add(e.getKey());
                    }
                }
                Stage js = stage("join", rows.size(), rows.size() * bpr, out, fk);
                js.rightRows = right.size();
                js.rightBytes = Files.size(rp);
                st.add(js);
                st.add(stage("map", out, out * bpr, out, outKeys.size()));
                st.add(stage("reduceby", out, out * bpr, outKeys.size(), outKeys.size()));
                break;
            }
            default:
                throw new IllegalArgumentException(j.name);
        }
        return st;
    }

    // ------------------------------------------------------------------------------------------
    // Wayang's estimate per task
    // ------------------------------------------------------------------------------------------

    static final class TaskInfo {
        String label, cls, platform;
        long[] cpu, disk, net, ram, inCard = new long[2], outCard = new long[2], in0 = new long[2], in1 = new long[2];
        long timeLo, timeHi, timeGmean;
        ExecutionTask task;
    }

    static long[] loadOf(LoadProfile lp, java.util.function.Function<LoadProfile, LoadEstimate> f) {
        long[] s = new long[2];
        List<LoadProfile> all = new ArrayList<>();
        all.add(lp);
        all.addAll(lp.getSubprofiles());
        for (LoadProfile p : all) {
            LoadEstimate e = f.apply(p);
            if (e == null) continue;
            s[0] += e.getLowerEstimate();
            s[1] += e.getUpperEstimate();
        }
        return s;
    }

    static String label(ExecutionTask t) {
        String c = t.getOperator().getClass().getSimpleName();
        if (c.contains("TextFileSource")) return "source";
        if (c.contains("FlatMapOperator")) return "flatmap";
        if (c.contains("MapOperator")) {
            Channel in = t.getNumInputChannels() > 0 ? t.getInputChannel(0) : null;
            boolean afterSource = in != null && in.getProducer() != null
                    && in.getProducer().getOperator().getClass().getSimpleName().contains("TextFileSource");
            return afterSource ? "parse" : "map";
        }
        if (c.contains("FilterOperator")) return "filter";
        if (c.contains("ReduceByOperator")) return "reduceby";
        if (c.contains("SortOperator")) return "sort";
        if (c.contains("DistinctOperator")) return "distinct";
        if (c.contains("JoinOperator")) return "join";
        if (c.contains("CountOperator")) return "count";
        if (c.contains("LocalCallbackSink") || c.contains("CountingSink")) return "sink";
        return "conversion";
    }

    static List<TaskInfo> wayangTasks(WayangContext ctx, WayangPlan plan) {
        Job job = ctx.createJob("estimate", plan);
        ExecutionPlan ep = job.buildInitialExecutionPlan();
        OptimizationContext oc = job.getOptimizationContext();
        List<TaskInfo> res = new ArrayList<>();
        for (ExecutionTask t : ep.collectAllTasks()) {
            ExecutionOperator op = t.getOperator();
            OptimizationContext.OperatorContext c = oc.getOperatorContext(op);
            TaskInfo ti = new TaskInfo();
            ti.task = t;
            ti.cls = op.getClass().getSimpleName();
            ti.platform = op.getPlatform().getName().toLowerCase().contains("spark") ? "spark" : "java";
            ti.label = label(t);
            LoadProfile lp = c.getLoadProfile();
            ti.cpu = loadOf(lp, LoadProfile::getCpuUsage);
            ti.disk = loadOf(lp, LoadProfile::getDiskUsage);
            ti.net = loadOf(lp, LoadProfile::getNetworkUsage);
            ti.ram = loadOf(lp, LoadProfile::getRamUsage);
            TimeEstimate te = c.getTimeEstimate();
            ti.timeLo = te.getLowerEstimate();
            ti.timeHi = te.getUpperEstimate();
            ti.timeGmean = te.getGeometricMeanEstimate();
            CardinalityEstimate[] ins = c.getInputCardinalities();
            for (int k = 0; k < ins.length; k++) {
                CardinalityEstimate e = ins[k];
                if (e == null) continue;
                ti.inCard[0] += e.getLowerEstimate();
                ti.inCard[1] += e.getUpperEstimate();
                long[] slot = k == 0 ? ti.in0 : ti.in1;
                slot[0] += e.getLowerEstimate();
                slot[1] += e.getUpperEstimate();
            }
            for (CardinalityEstimate e : c.getOutputCardinalities()) {
                if (e == null) continue;
                ti.outCard[0] += e.getLowerEstimate();
                ti.outCard[1] += e.getUpperEstimate();
            }
            res.add(ti);
        }
        return res;
    }

    // ------------------------------------------------------------------------------------------
    // Main
    // ------------------------------------------------------------------------------------------

    static JobSpec spec(String name, String size, String left, String right, double sel) {
        JobSpec j = new JobSpec();
        j.name = name;
        j.size = size;
        j.leftFile = left;
        j.rightFile = right;
        j.sel = sel;
        return j;
    }

    static String csv(Object v) {
        if (v == null) return "";
        String s = v.toString();
        return s.contains(",") || s.contains("\"") ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }

    static void write(Path p, List<Map<String, Object>> rows) throws IOException {
        List<String> head = new ArrayList<>();
        for (Map<String, Object> r : rows) for (String k : r.keySet()) if (!head.contains(k)) head.add(k);
        StringBuilder sb = new StringBuilder(String.join(",", head)).append('\n');
        for (Map<String, Object> r : rows) {
            sb.append(head.stream().map(k -> csv(r.get(k))).collect(Collectors.joining(","))).append('\n');
        }
        Files.write(p, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args.length > 0 ? args[0] : "../java-profiler/data");
        Path out = Paths.get(args.length > 1 ? args[1] : "results");
        int warmup = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        int reps = args.length > 3 ? Integer.parseInt(args[3]) : 3;
        Files.createDirectories(out);

        List<JobSpec> jobs = List.of(
                spec("filter-reduceby", "100k", "rows-100000_keys-1000_words-8_seed-42.csv", null, 0.5),
                spec("filter-reduceby", "1M", "rows-1000000_keys-1000_words-8_seed-42.csv", null, 0.5),
                spec("map-sort", "100k", "rows-100000_keys-1000_words-8_seed-42.csv", null, 0),
                spec("map-sort", "1M", "rows-1000000_keys-1000_words-8_seed-42.csv", null, 0),
                spec("filter-distinct", "100k", "rows-100000_keys-1000_words-8_seed-42.csv", null, 0.3),
                spec("filter-distinct", "1M", "rows-1000000_keys-1000_words-8_seed-42.csv", null, 0.3),
                spec("flatmap-count", "100k", "rows-100000_keys-1000_words-8_seed-42.csv", null, 0),
                spec("flatmap-count", "1M", "rows-1000000_keys-1000_words-8_seed-42.csv", null, 0),
                spec("join-map-reduceby", "100k-k100k", "rows-100000_keys-100000_words-8_seed-42.csv",
                        "rows-100000_keys-100000_words-8_seed-43.csv", 0),
                spec("join-map-reduceby", "100k-k10k", "rows-100000_keys-10000_words-8_seed-42.csv",
                        "rows-100000_keys-10000_words-8_seed-43.csv", 0));

        int cores = Runtime.getRuntime().availableProcessors();
        Configuration conf = new Configuration();
        conf.setProperty("spark.master", "local[*]");
        conf.setProperty("spark.app.name", "jobbench");
        conf.setProperty("spark.ui.showConsoleProgress", "false");
        conf.setProperty("wayang.spark.machines", "1");
        conf.setProperty("wayang.spark.cores-per-machine", Integer.toString(cores));
        if (System.getProperty("spark.ui.enabled") == null) System.setProperty("spark.ui.enabled", "false");
        WayangContext ctx = new WayangContext(conf).withPlugin(Java.basicPlugin()).withPlugin(Spark.basicPlugin());

        // Keep one SparkContext for all jobs, as the Spark profiler does.
        Job pin = ctx.createJob("pin", build(jobs.get(0), Placement.spark, dir));
        SparkContextReference ref = SparkPlatform.getInstance().getSparkContext(pin);
        ref.noteObtainedReference();
        JavaSparkContext jsc = ref.get();

        String gc = ManagementFactory.getGarbageCollectorMXBeans().stream().map(GarbageCollectorMXBean::getName)
                .collect(Collectors.joining("+"));
        long heap = Runtime.getRuntime().maxMemory();
        double mhz = conf.getLongProperty("wayang.java.cpu.mhz");
        double sparkMhz = conf.getLongProperty("wayang.spark.cpu.mhz");

        List<Map<String, Object>> runRows = new ArrayList<>(), taskRows = new ArrayList<>(), opRows = new ArrayList<>();
        String only = args.length > 4 ? args[4] : null;  // e.g. "map-sort|100k" to run one job
        for (JobSpec j : jobs) {
            if (only != null && !(j.name + "|" + j.size).contains(only)) continue;
            List<Stage> stages = trueStages(j, dir);
            for (Placement p : Placement.values()) {
                String id = j.name + "|" + j.size + "|" + p;
                List<TaskInfo> tasks = wayangTasks(ctx, build(j, p, dir));
                System.err.printf("[jobbench] %s  tasks: %s%n", id,
                        tasks.stream().map(t -> t.label + ":" + t.cls).sorted().collect(Collectors.joining(" ")));

                // Measured runtime
                for (int i = 0; i < warmup + reps; i++) {
                    WayangPlan plan = build(j, p, dir);
                    FIRED.set(false);
                    OUT_ROWS.set(0);
                    System.gc();
                    Thread.sleep(150);
                    long t0 = System.nanoTime();
                    ctx.execute(id, plan);
                    long t1 = System.nanoTime();
                    if (i < warmup) continue;
                    Map<String, Object> r = new LinkedHashMap<>();
                    r.put("job", j.name);
                    r.put("size", j.size);
                    r.put("placement", p.name());
                    r.put("run", i - warmup);
                    r.put("exec_ns", FIRED.get() ? t1 - FIRST_NS.get() : null);
                    r.put("total_ns", t1 - t0);
                    r.put("out_rows", OUT_ROWS.get());
                    runRows.add(r);
                    System.err.printf("[jobbench]   run %d exec=%.1fms out=%d%n", i - warmup,
                            (t1 - FIRST_NS.get()) / 1e6, OUT_ROWS.get());
                }

                // Wayang per task
                for (TaskInfo t : tasks) {
                    Map<String, Object> r = new LinkedHashMap<>();
                    r.put("job", j.name);
                    r.put("size", j.size);
                    r.put("placement", p.name());
                    r.put("label", t.label);
                    r.put("class", t.cls);
                    r.put("platform", t.platform);
                    r.put("wayang_time_ms_lower", t.timeLo);
                    r.put("wayang_time_ms_upper", t.timeHi);
                    r.put("wayang_time_ms_gmean", t.timeGmean);
                    r.put("wayang_cpu_cycles_lower", t.cpu[0]);
                    r.put("wayang_cpu_cycles_upper", t.cpu[1]);
                    r.put("wayang_in_rows_lower", t.inCard[0]);
                    r.put("wayang_in_rows_upper", t.inCard[1]);
                    r.put("wayang_out_rows_lower", t.outCard[0]);
                    r.put("wayang_out_rows_upper", t.outCard[1]);
                    taskRows.add(r);
                }

                // Model input rows: one per operator, one per scan (source + parse; the sink is added to the first)
                Map<String, Object> common = new LinkedHashMap<>();
                common.put("job", j.name);
                common.put("size", j.size);
                common.put("placement", p.name());
                common.put("source", "text");
                common.put("words", 8);
                common.put("gc_name", gc);
                common.put("max_heap_bytes", heap);
                common.put("cpus", cores);
                common.put("pair", 0);
                List<TaskInfo> parses = tasks.stream().filter(t -> t.label.equals("parse")).collect(Collectors.toList());
                TaskInfo sinkTask = tasks.stream().filter(t -> t.label.equals("sink")).findFirst().orElse(null);
                for (int s = 0; s < parses.size(); s++) {
                    TaskInfo parse = parses.get(s);
                    ExecutionTask producer = parse.task.getInputChannel(0).getProducer();
                    TaskInfo src = tasks.stream().filter(t -> t.task == producer).findFirst().orElse(null);
                    List<TaskInfo> parts = new ArrayList<>(List.of(parse));
                    if (src != null) parts.add(src);
                    if (s == 0 && sinkTask != null) parts.add(sinkTask);
                    boolean binaryRight = s == 1;
                    String file = binaryRight ? j.rightFile : j.leftFile;
                    long n = Files.lines(dir.resolve(file)).count();
                    Map<String, Object> r = new LinkedHashMap<>(common);
                    r.put("config_id", id + "|scan" + s);
                    r.put("operator", "scan");
                    r.put(parse.platform + "_execution_operator", parse.platform.equals("spark") ? "SparkScan" : "JavaScan");
                    r.put("rows", n);
                    r.put("right_rows", 0);
                    r.put("keys", fileKeys(file));
                    r.put("in_bytes", Files.size(dir.resolve(file)));
                    r.put("out_rows", n);
                    putPlatform(r, parse.platform, cores, jsc, dir.resolve(file));
                    putWayang(r, parts, parse.outCard, parse.platform.equals("spark") ? sparkMhz : mhz, conf, parse.platform);
                    r.put("wayang_est_rows", (parse.in0[0] + parse.in0[1]) / 2);
                    r.put("wayang_est_right_rows", 0);
                    opRows.add(r);
                }
                // Operators, in pipeline order; match each true stage with its task
                for (Stage st : stages) {
                    TaskInfo t = tasks.stream().filter(x -> x.label.equals(st.op)).findFirst().orElse(null);
                    if (t == null) {
                        System.err.println("[jobbench] WARNING: no task for " + st.op + " in " + id);
                        continue;
                    }
                    Map<String, Object> r = new LinkedHashMap<>(common);
                    r.put("config_id", id + "|" + st.op);
                    r.put("operator", st.op);
                    r.put(t.platform + "_execution_operator", t.cls);
                    r.put("rows", st.inRows);
                    r.put("right_rows", st.rightRows);
                    r.put("keys", st.keys);
                    r.put("sel", st.sel);
                    r.put("in_bytes", (long) st.inBytes);
                    r.put("right_in_bytes", (long) st.rightBytes);
                    r.put("out_rows", st.outRows);
                    putPlatform(r, t.platform, cores, jsc, dir.resolve(j.leftFile));
                    putWayang(r, List.of(t), t.outCard, t.platform.equals("spark") ? sparkMhz : mhz, conf, t.platform);
                    r.put("wayang_est_rows", (t.in0[0] + t.in0[1]) / 2);
                    r.put("wayang_est_right_rows", (t.in1[0] + t.in1[1]) / 2);
                    opRows.add(r);
                }
            }
        }
        write(out.resolve("job_runs.csv"), runRows);
        write(out.resolve("job_tasks.csv"), taskRows);
        write(out.resolve("job_operators.csv"), opRows);
        System.err.println("[jobbench] wrote " + out);
        System.exit(0);
    }

    static void putPlatform(Map<String, Object> r, String platform, int cores, JavaSparkContext jsc, Path file) {
        if (platform.equals("spark")) {  // Java rows must not have task_slots: the model reads it as threads
            r.put("task_slots", cores);
            r.put("shuffle_partitions", 2 * cores);
            r.put("in_partitions", jsc.textFile(file.toUri().toString()).getNumPartitions());
        }
    }

    static void putWayang(Map<String, Object> r, List<TaskInfo> parts, long[] outCard, double mhz, Configuration conf,
                          String platform) {
        long[] cpu = new long[2], disk = new long[2], net = new long[2], ram = new long[2];
        long tLo = 0, tHi = 0;
        for (TaskInfo t : parts) {
            for (int i = 0; i < 2; i++) {
                cpu[i] += t.cpu[i];
                disk[i] += t.disk[i];
                net[i] += t.net[i];
                ram[i] += t.ram[i];
            }
            tLo += t.timeLo;
            tHi += t.timeHi;
        }
        r.put("wayang_est_cpu_cycles_lower", cpu[0]);
        r.put("wayang_est_cpu_cycles_upper", cpu[1]);
        r.put("wayang_est_disk_bytes_lower", disk[0]);
        r.put("wayang_est_disk_bytes_upper", disk[1]);
        r.put("wayang_est_network_bytes_lower", net[0]);
        r.put("wayang_est_network_bytes_upper", net[1]);
        r.put("wayang_est_ram_bytes_lower", ram[0]);
        r.put("wayang_est_ram_bytes_upper", ram[1]);
        r.put("wayang_est_time_ms_gmean", Math.round(Math.sqrt((double) Math.max(tLo, 1) * Math.max(tHi, 1))));
        r.put("wayang_est_out_rows_lower", outCard[0]);
        r.put("wayang_est_out_rows_upper", outCard[1]);
        r.put("wayang_cpu_mhz", (long) mhz);
        r.put("wayang_cost_per_ms", conf.getDoubleProperty("wayang." + platform + ".costs.per-ms"));
    }
}

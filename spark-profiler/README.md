# Spark operator profiler

Measures the **real** resources each Wayang Spark operator uses, as training data for the ML cost model.
It is the Spark version of [`../java-profiler`](../java-profiler/README.md): same method, same input data,
same plan shapes, same CSV layout. No measured column comes from Wayang's cost estimates. Wayang's own
estimates for the same plans are stored next to them in separate `wayang_*` columns, for comparison (see
[Wayang's own estimates](#wayangs-own-estimates)).

## Cluster setup: a single-node Spark cluster

Spark runs in **local mode**, which works as a **single-node cluster** on this machine:

```
one machine
└── one JVM
    ├── Spark driver      (Wayang plans the job, Spark schedules it)
    └── one executor      (runs the tasks: CORES task threads, one per core)
```

- `spark.master = local[CORES]` (or `local[*]` for all cores). No Spark install or cluster is needed.
- Driver and executor share one heap (`-Xmx`).
- Wayang is told it has 1 machine with CORES cores, so it uses `2 × CORES` partitions for shuffles
  (reduceby, groupby, join, sort, distinct). This is Wayang's own rule.
- Shuffles are real Spark shuffles: data is serialized, written to local shuffle files, and read back.
  The only difference from a multi-machine cluster is that nothing crosses the network, so `network_bytes`
  is 0. On a real cluster most of `shuffle_write_bytes` would go over the network.

## Method

For each config the profiler starts one JVM (one Spark context) and runs two plans in alternating pairs,
after warm-up runs:

```
baseline : source -> parse -> sink
operator : source -> parse -> OPERATOR -> sink
operator cost = operator run - baseline run   (per pair, then median)
```

This is the same as the Java profiler. Binary operators need a connected plan: `join` and `cartesian` use
`left, right -> UnionAll -> sink` as baseline, and `union` uses two separate jobs. The column
`measured_execution_operators` says exactly what each difference covers
(for example `+SparkJoinOperator -SparkUnionAllOperator`).

Operators: map, filter, flatmap, reduceby, groupby, globalreduce, count, distinct, sort, textsink, join, union, cartesian.

### Spark-specific details

- **One Spark context per JVM.** Wayang normally shuts Spark down after every job, which would add ~2 s
  of start-up to every run. The profiler keeps one Spark context alive for all runs of a config.
- **The sink counts rows in parallel.** Wayang's usual sink (`LocalCallbackSink`) pulls every row back to
  the driver one partition at a time. That would make the sink the slowest part and run the whole plan on
  one thread. The profiler uses its own `CountingSink` instead: it runs `rdd.count()`, so all partitions
  are read on all cores and only one number goes back to the driver. The same sink is in both plans.
- **"exec" metrics start at the first record.** `wall_exec_ns`, `process_cpu_exec_ns` and
  `total_alloc_exec_bytes` start when the first row leaves the source, so they leave out Wayang's plan
  optimization and Spark's job setup. `*_total_*` and `process_cpu_ns` cover the whole `execute()` call.
- **Same input data as the Java profiler.** Inputs are read from `../java-profiler/data`. The zipped files
  there are unzipped when a config needs them; missing files are generated with the same seed, so the
  contents are identical.

## Run

```bash
./run-profiling.sh --dry-run            # list the configs
./run-profiling.sh --profile quick      # a few minutes
./run-profiling.sh                      # default grid (78 configs, same as the Java profiler)
./run-profiling.sh --profile full       # large grid with 6 resource settings, many hours
./run-profiling.sh --ops "sort join" --rows "100000 1000000" --resources "4g:G1:all 4g:G1:2"
./run-profiling.sh --add-estimates      # add Wayang's estimates to rows already in raw.csv (measures nothing)
./run-profiling.sh --help
```

Resource settings are `HEAP:GC:CORES`: heap size, collector (G1/Parallel/Serial/Z), and the number of
Spark task threads (`local[CORES]`) or `all`. Stopping and restarting continues where it left off.

**Requirements:**
- The built Wayang distribution in `../../wayang`, the same one the Java profiler uses.
- Java 17.
- Spark itself is not in the Wayang distribution. The first run asks Maven for the exact Spark version
  Wayang was built against (3.5.7) and saves the list in `build/spark-classpath.txt`. The jars come from the
  local Maven cache. To use other jars, set `SPARK_JARS` to a classpath.

## Output (in `results/`)

| File | Contents |
|---|---|
| `raw.csv` | every measured run, baseline and operator |
| `pairs.csv` | one row per pair: features + (operator − baseline) for every metric |
| `operator_costs.csv` | one row per config: median difference and its IQR; `cpu_signal_ok` = task CPU cost larger than noise |
| `baseline_costs.csv` | the baseline itself, which is the cost of source + parse + sink |
| `run.log` | full JVM output |

## What is measured

### Features (describe the config)

Same as the Java profiler (`operator`, `rows`, `right_rows`, `keys`, `sel`, `words`, `in_bytes`, heap, GC,
machine), plus:

| Column | Meaning |
|---|---|
| `spark_execution_operator` | e.g. `SparkJoinOperator` |
| `spark_master`, `deploy_mode`, `executors` | `local[*]`, `local-single-node`, `1` |
| `task_slots` | task threads = cores Spark uses |
| `shuffle_partitions` | partitions Wayang uses for shuffles (`2 × cores`) |
| `in_partitions`, `right_in_partitions` | partitions of the input (Spark splits files in 32 MB pieces) |
| `spark_version`, `serializer` | `3.5.7`, Java serializer |

### Metrics (operator − baseline in `pairs.csv` and `operator_costs.csv`)

| Group | Columns | Source |
|---|---|---|
| Time | `wall_exec_ns`, `wall_total_ns`, `task_run_ms` (sum over tasks), `task_run_ms_max` (slowest task) | `System.nanoTime`, Spark |
| CPU | `task_cpu_ns` (all Spark tasks), `process_cpu_exec_ns`, `process_cpu_ns` (whole JVM), `driver_thread_cpu_ns`, `os_user_ns`, `os_system_ns`, `task_deserialize_cpu_ns` | Spark, JVM, OS |
| Hardware | `os_instructions`, `os_cycles`, `os_*_pcore` | OS (Apple Silicon counters) |
| Energy | `os_energy_nj`, `os_energy_pcore_nj` | OS (macOS); Linux: `os_energy_pkg_nj` from RAPL |
| Memory | `total_alloc_exec_bytes`, `task_peak_execution_memory_bytes` (sum and `_max`), `live_heap_after_gc_max_bytes`, `heap_peak_bytes`, `pool_eden/survivor/old_peak_bytes`, `os_phys_footprint_bytes_delta` | Spark, MXBeans, OS |
| Spill | `spill_memory_bytes`, `spill_disk_bytes` | Spark |
| Shuffle | `shuffle_write_bytes`, `shuffle_write_records`, `shuffle_write_time_ns`, `shuffle_read_local_bytes`, `shuffle_read_records`, `shuffle_fetch_wait_ms` | Spark |
| Spark work | `spark_jobs`, `spark_stages`, `spark_tasks`, `spark_stage_tasks_max`, `task_result_bytes` | Spark |
| I/O | `input_bytes`, `input_records`, `output_bytes`, `output_records`, `os_disk_read_bytes`, `os_disk_write_bytes`, `out_bytes` | Spark, OS |
| GC / JIT | `gc_count`, `gc_time_ms`, `gc_young_*`, `gc_old_*`, `task_gc_ms`, `gc_max_pause_ms`, `jit_compile_ms` | MXBeans, Spark |
| OS activity | `os_page_faults`, `os_context_switches`, `os_syscalls_unix`, `os_threads_*` | OS |
| Network | `network_bytes` = remote shuffle bytes read | always 0 on a single node |

GC columns have the same names for every garbage collector: `gc_young_*` (G1 young, Serial Copy, Parallel Scavenge), `gc_old_*` (G1 old, MarkSweepCompact, PS MarkSweep), `gc_other_*` (e.g. ZGC), and heap pools `pool_eden_peak_bytes`, `pool_survivor_peak_bytes`, `pool_old_peak_bytes`, `pool_other_heap_peak_bytes`. So every row fills every column, whichever GC the resource setting uses.

All `os_*` and `process_*` columns count the whole JVM, which here includes the driver, the executor, and
the GC and JIT threads.

## Wayang's own estimates

Every row also holds what Wayang's optimizer **predicts** for the same plan, in the same `wayang_*` columns
as the Java profiler (full list and caveats in
[the Java README](../java-profiler/README.md#wayangs-own-estimates)): CPU cycles, disk bytes, network bytes,
RAM bytes, overhead, time (ms), cost, estimated sink rows, and a per-task breakdown in `raw.csv`. The
profiler optimizes each plan the way Wayang does before running it and adds up the estimates of all
execution tasks. In `pairs.csv` and `operator_costs.csv` they are operator plan − baseline plan, like the
measured columns.

Spark-specific points:

| Wayang estimate | Compare with |
|---|---|
| `wayang_est_cpu_cycles_*` | `os_cycles`, `task_cpu_ns` |
| `wayang_est_disk_bytes_*` | `shuffle_write_bytes`, `spill_disk_bytes`, `os_disk_*` (Wayang charges disk for shuffles, e.g. in join) |
| `wayang_est_network_bytes_*` | `network_bytes` (0 on a single node) |
| `wayang_est_time_ms_*` | `wall_exec_ns` / 1e6 |

- Wayang divides CPU cycles by `wayang.spark.machines × cores-per-machine`. The profiler sets this to the
  task threads of the run, so `wayang_cores` = `task_slots`. Clock speed is Wayang's default
  (`wayang_cpu_mhz` = 2700), not the measured machine's.
- `wayang_est_startup_ms` (Spark start-up, 4.5 s by default) is not included in `wayang_est_time_ms_*`. The
  profiler keeps one SparkContext for all runs, so the measurements do not include start-up either. Union's
  baseline runs two jobs, so it counts start-up twice.
- `./run-profiling.sh --add-estimates` uses `shuffle_partitions / 2` as the cores of each config, so old rows
  get exactly the estimate a new run would write. It runs only Wayang's optimizer, not Spark.

## Checking the profiler itself

Two test operators (not part of the training grid) check that the CPU counters only count work, not waiting:

- `burn`: a fixed amount of pure CPU work per row (`--work` = loop rounds). Its true cost is calibrated on the
  driver thread and stored as `burn_calibrated_ns_per_row`.
- `sleep`: waits `--work` microseconds per row without using the CPU.

```bash
# same JVM flags and classpath as run-profiling.sh uses
java ... wayangprof.SparkOperatorProfiler --op burn  --rows 100000 --work 10000 ...
java ... wayangprof.SparkOperatorProfiler --op sleep --rows 10000  --work 100   ...
```

Result on the M4 Pro (8 Nov 2026, 2 warm-up + 5 pairs, operator − baseline):

| | burn (expected 1,354 ms CPU) | sleep (1,000 ms of waiting) |
|---|---|---|
| `task_cpu_ns` | 1,393 ms (103%) | 59 ms (the park/unpark calls themselves) |
| `task_run_ms` | 1,394 ms | 1,325 ms (waiting counts as run time) |
| `process_cpu_exec_ns` | 1,406 ms | 78 ms |
| `wall_exec_ns` | 698 ms (2 input partitions, so 2 tasks in parallel) | 662 ms |
| `os_cycles` | 6.10 G | 0.15 G |
| `os_energy_nj` | 3.56 J | 0.05 J |

So CPU, cycles and energy follow the work, and waiting shows up only in run time and wall time.

## Notes

- **Cheap operators are within noise.** Filter, map, union and count on small inputs cost less than the
  run-to-run spread. Use 1M+ rows for them and check `cpu_signal_ok`.
- **Count costs about 0.** On Spark, `count` runs the same `rdd.count()` that the baseline's sink already
  runs, so operator − baseline is about 0. Its real cost is the counting part of the baseline.
- **Union's baseline runs two jobs.** That adds one extra Wayang/Spark job overhead to the baseline, so
  union's cost can be slightly negative.
- **Sort reads its input twice.** Spark first samples the input to choose range partitions, then sorts.
  Both passes are part of sort's cost.
- **Input is usually cached.** Files are usually in the OS file cache. Use `--cold` (needs passwordless
  sudo) to empty the cache before each run.
- **Machine state matters.** Plug the laptop in and close other apps: CPU speed and energy change on
  battery and under load.
- **Host label.** The `host` column holds the machine model (e.g. `Mac16,8`), not the computer's name.

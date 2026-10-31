# Plan: training data for the Wayang ML cost model

Goal: a large, real (measured, not synthetic) dataset of operator costs that a neural network can learn from,
on three platforms:

1. **Java**: Wayang's Java platform (one thread, one JVM)
2. **Spark, single node**: Spark local mode (one machine, driver and executor in one JVM)
3. **Spark, clusters**: Spark with several executors and machines, so shuffles cross a network

Every row in every phase is produced the same way: run `source → parse → sink` and
`source → parse → OPERATOR → sink` back to back, and store the difference. Nothing is generated or interpolated.

---

## 0. Where we are now

| | Java | Spark (single node) |
|---|---|---|
| Profiler | done (`java-profiler/`) | done (`spark-profiler/`) |
| Data collected | 9 configs, 63 pairs | the same 9 configs, 63 pairs |
| Config selection | fixed grid (78 configs by default) | fixed grid (78 configs by default) |
| Runs on | MacBook (Apple M4 Pro) | MacBook |

Already in place: the baseline/operator pair method, warm-up runs, alternating order, JVM/OS/Spark metrics,
GC-independent column names, resume after a stop, and analysis into `pairs.csv` / `operator_costs.csv`.

Missing for large-scale collection: a sweep over the full config space, faster runs per config, and an HPC job
setup. These are listed per phase below.

---

## 1. How big the config space is

Cost only changes noticeably when a value changes by about 25% (run-to-run noise is about 5–10%), so values are
taken in log steps:

| Setting | Values used |
|---|---|
| rows, right_rows | 1,000 → 3,000,000 in ×1.25 steps (36 levels) |
| keys | 1, 2, 4, … up to rows (up to 22 levels) |
| sel (filter) | 0.05, 0.10, … 1.00 (20 levels) |
| words per row | 1, 2, 4, 8, 16, 32, 64 (7 levels) |
| output limit | join and cartesian configs with more than 20M output rows are skipped |

Number of meaningfully different data configs per operator:

| Operator | Configs |
|---|---|
| map, count, globalreduce, sort, textsink | 1,260 (252 each) |
| flatmap | 252 |
| filter | 5,040 |
| reduceby, groupby, distinct | 12,159 |
| join | 59,948 |
| union | 9,072 |
| cartesian | 735 |
| **Total (one JVM setting)** | **88,466** |

JVM settings multiply this: heap {1g, 2g, 4g, 8g} × GC {G1, Parallel, Serial, Z} × cpus {1, 2, 4, all} × source
{text, collection} = 128 settings, so 11.3 million in total (about 4 years). That is too many, so each phase runs
the **full data grid at one main setting**, plus a **random sample** across the other settings.

Each config gives 5 rows in `pairs.csv` (5 measured pairs), so 88,466 configs ≈ **440,000 training rows** per
platform.

---

## 2. Phase 1: Java (one HPC node, ~10–12 days)

### What runs

| Part | Configs | Rows in pairs.csv | Time (≈10 s/config) |
|---|---|---|---|
| A. Full data grid at `4g:G1:all`, text source | 88,466 | ~442,000 | ~10 days |
| B. Random sample: random data config + random heap/GC/cpus/source | 10,000 | ~50,000 | ~1–2 days |

**Order matters.** The grid runs in a **shuffled order** (fixed seed). If the run stops early, whatever has finished
is still an even random sample of the whole space, not "all of map and nothing of join". Part B can run before or
after A.

### Changes to build in `java-profiler`

1. **Sweep mode**: `./run-profiling.sh --sweep grid|random --seed 1 --hours N`
   - `grid`: builds the 88,466 configs from the log-step levels above and shuffles them.
   - `random`: draws configs at random (log-uniform rows/keys, uniform sel, random JVM setting).
   - Resume works as now: configs already in `raw.csv` are skipped.
2. **Faster configs**
   - Run many configs in one JVM (one JVM per JVM setting, not per config). Saves ~1 s JVM start per config.
   - 2 warm-up pairs and 5 measured pairs (now 3 and 7).
3. **Temporary input files**: create each input file just before its config and delete it after, so tens of
   thousands of configs don't fill the disk. Same generator and seeds, so the files are identical to today's.
4. **Output in chunks**: one `raw-<job>-<n>.csv` per job/chunk, plus a `merge.py` that combines them and runs the
   analysis. Avoids one huge file being rewritten and keeps each job's output separate.
5. **HPC job script** (`hpc/java-sweep.slurm`) that copies the inputs to node-local disk, runs the sweep until the
   time limit, and restarts cleanly in the next job.
6. **Offline bundle**: all jars (Wayang distribution + dependencies) packed into one folder, so compute nodes need no
   internet or Maven.

### On the HPC node

- Ask for the node **exclusively** (`--exclusive`), so no other job shares it and distorts timings.
- Run **one profiler at a time** for clean data. Optional: 2 in parallel, one pinned to each CPU socket
  (`numactl`), for ~2× speed with slightly more noise. Record which was used in the `tag` column.
- Linux gives fewer OS counters than macOS: no instruction/cycle counts; energy only if RAPL
  (`/sys/class/powercap`) is readable. CPU time, wall time, memory, GC, disk and page faults all work.
- Wall-time limits (e.g. 24–48 h per job) are fine: each job resumes where the previous one stopped.

---

## 3. Phase 2: Spark single node (same HPC node, ~10–15 days)

Spark in local mode on one machine (see `spark-profiler/README.md`). The data grid is the same as Java, so every
Java config has a Spark counterpart. Rows can be compared directly, and "platform" becomes a feature.

### What runs

| Part | Configs | Rows in pairs.csv | Time (≈12 s/config) |
|---|---|---|---|
| A. Full data grid at `8g:G1:all` (all cores of the node) | 88,466 | ~442,000 | ~12 days |
| B. Random sample over cores {1, 2, 4, 8, 16, all} × heap × GC | 10,000 | ~50,000 | ~1–2 days |

Cores matter much more on Spark than on Java (they set the task threads and Wayang's shuffle partitions,
`2 × cores`), so part B samples them widely. An HPC node with 32–64 cores also covers far more parallelism than the
laptop.

### Changes to build in `spark-profiler`

Same as Java (sweep mode, many configs per JVM with one Spark context, 2+5 pairs, temporary inputs, chunked
output, SLURM script, offline bundle), plus:

- Put `spark.local.dir` (shuffle files) on node-local disk, not the shared filesystem.
- Optionally record Spark's per-executor memory peaks (`spark.executor.processTreeMetrics.enabled`, Linux only).

---

## 4. Phase 3: Spark with different clusters

Here shuffles go between separate executors, so the data finally includes **network/transfer cost**
(`shuffle_read_remote_bytes`, `network_bytes` > 0), which local mode cannot measure.

### 4a. Pseudo-clusters on the single HPC node (possible with 1 node)

Start a Spark **standalone cluster inside the node**: one master and several workers, each worker its own JVM.
Executors then exchange shuffle data over the local network interface (loopback TCP) like separate machines would,
just without a physical network.

Cluster shapes to cover (these become features):

| Feature | Values |
|---|---|
| executors | 1, 2, 4, 8 |
| cores per executor | 1, 2, 4, 8 |
| memory per executor | 2g, 4g, 8g |
| total cores | executors × cores per executor (limited to the node's cores) |

About 30–40 valid shapes. Running the full 88k grid for each shape is not possible, so this phase uses a
**reduced data grid** (rows in ×2 steps, 12 levels; ~5,000 data configs) **sampled at random** across shapes.
Target: **20,000–30,000 configs (~100,000–150,000 rows)** in about 5–7 days.

### 4b. Real multi-node clusters (needs more than one node; optional)

The same, but workers on 2, 4 or 8 separate HPC nodes, so shuffles cross the real cluster network. Adds the
features `nodes` and network type (e.g. Ethernet / InfiniBand). Even a short allocation (e.g. 4 nodes for 1–2 days)
gives a valuable set of real-network rows to compare with 4a.

### Changes to build for Phase 3

The current Spark profiler assumes everything runs in one JVM. For separate executors:

1. **Cluster start-up in the job script**: start master and workers (`start-master.sh` / `start-worker.sh` from a
   Spark 3.5.7 distribution), set `spark.master=spark://<host>:7077`, `spark.executor.cores`,
   `spark.executor.memory`, `spark.executor.instances`. Wayang already passes these settings to Spark.
2. **Ship the profiler's code to executors**: register the profiler jar with the job (Wayang's UDF jars /
   `spark.jars`), since executors no longer share the driver's classpath.
3. **"exec" start time**: the first-record marker uses a static field, which only works in one JVM. Replace it with
   the start time of the first task, from Spark's listener events.
4. **Output counts**: count text-sink output with Spark's `output_records` metric instead of a static counter.
5. **CPU and memory of executors**: `process_cpu_ns` and the OS counters only cover the driver JVM. Use Spark's
   task metrics (already collected: `task_cpu_ns`, shuffle, spill, peak execution memory, GC) and executor metric
   peaks (heap, off-heap, process memory).
6. **Note on partitions**: when `spark.executor.cores` is set, Wayang uses `2 × executor cores` shuffle
   partitions, not `2 × total cores`. Record `shuffle_partitions` so the model sees the real value.

---

## 5. Using the data for the neural network

- **Input features**: operator, platform (java / spark-local / spark-cluster), rows, right_rows, keys, sel, words,
  in_bytes, heap, GC, cores, and for Spark: task slots, shuffle partitions, input partitions, executors, cores per
  executor, nodes.
- **Targets** (one model per target, or one model with several outputs): `cpu` (Java: `thread_cpu_exec_ns`, Spark:
  `task_cpu_ns`), `wall_exec_ns`, memory allocated, peak memory, GC time, shuffle bytes, network bytes, energy.
- **Use `pairs.csv`** (5 rows per config). The spread between pairs shows the model the natural noise.
- **Log-transform** sizes and targets (`log(1 + x)`): values range from microseconds to minutes.
- **Split by config**: all pairs of one config go to the same side (train or test). Better still, also hold out whole
  ranges (e.g. all configs with rows > 1M) to test whether the model extrapolates.
- **Noise**: tiny operators (filter or map on small inputs) are mostly noise. Keep them (the model should learn they
  cost ~0), but judge accuracy on configs with `cpu_signal_ok = 1`.
- **Negative differences**: operators that shrink data (count, filter with low sel) can come out slightly negative
  on Spark. Clip at 0 or keep them as noise, but do the same thing in training and testing.

---

## 6. Timeline (one HPC node)

| Step | What | Time |
|---|---|---|
| 1 | Build sweep mode, chunked output, merge script, SLURM script, offline bundle (Java) | 1–2 days of work |
| 2 | Test for ~1 hour on the laptop, then ~2 hours on the HPC node; check data and per-config time | 1 day |
| 3 | **Phase 1 Java**: grid (shuffled) + random sample | ~10–12 days of running |
| 4 | Port the same changes to the Spark profiler | 1 day (can be done while step 3 runs) |
| 5 | **Phase 2 Spark single node**: grid + random sample | ~10–15 days |
| 6 | Build cluster support (Phase 3 changes) and test on the node | 2–3 days |
| 7 | **Phase 3a Spark pseudo-clusters** on the node | ~5–7 days |
| 8 | Optional **Phase 3b** on several nodes | 1–2 days per allocation |

Total running time on one node: about **4–5 weeks** for all three phases. Results after each phase are usable on
their own, so training can start after Phase 1.

Expected dataset size:

| Phase | Configs | Rows in pairs.csv |
|---|---|---|
| 1. Java | ~98,000 | ~490,000 |
| 2. Spark single node | ~98,000 | ~490,000 |
| 3a. Spark pseudo-clusters | ~20,000–30,000 | ~100,000–150,000 |
| **Total** | **~220,000** | **~1,100,000** |

---

## 7. What to ask the HPC admins for

> One compute node, **exclusive**, for batch jobs over about 4–5 weeks (jobs of 24–48 h each, restarted
> automatically). Java 17. Node-local scratch disk (~50 GB). Read access to RAPL energy counters
> (`/sys/class/powercap/intel-rapl*`) if possible. Permission to run a Spark standalone cluster (several JVMs
> talking over localhost) inside a job. Optional later: 2–8 nodes for 1–2 days for multi-node Spark runs.

---

## 8. Risks and how they are handled

| Risk | Handling |
|---|---|
| Run stops early (time limit, node failure) | resume skips finished configs; shuffled order means partial data is still an even sample |
| Other jobs on the node distort timings | exclusive node; `sys_load_avg_1m` is recorded per run to spot bad periods |
| Too many input files | inputs are created per config and deleted after |
| Very large joins/cartesians run out of memory or time | 20M output-row limit, heap check, per-run timeout; failures are logged and skipped |
| macOS and Linux columns differ | models use the columns both have; OS-specific columns are extra |
| Laptop data (battery, Apple CPU) differs from HPC data | `host`, `cpu_model`, `cpus` are features; keep laptop data as a separate machine |

# Cost-Model

Measured resource usage of Apache Wayang operators on three platforms, **Java**, **Spark** and **PostgreSQL**, collected as
training data for an ML cost model. All measured numbers are real measurements from running the operators.
Next to them, every row also holds **Wayang's own estimate** for the same plan (`wayang_est_*` columns: CPU
cycles, disk, network, RAM, time, cost), so the measured cost can be compared with what Wayang predicts.

Both profilers use the same method, the same input data and the same CSV layout:

```
baseline : source -> parse -> sink
operator : source -> parse -> OPERATOR -> sink
operator cost = operator run - baseline run   (per pair, then median over pairs)
```

| Profiler | Platform | Details |
|---|---|---|
| [java-profiler](java-profiler/README.md) | Wayang Java platform: one thread in one JVM | |
| [spark-profiler](spark-profiler/README.md) | Wayang Spark platform (Spark 3.5.7) in local mode: a **single-node cluster**, driver and executor in one JVM, one task thread per core; or a standalone cluster of N executors | adds Spark task metrics: executor CPU, shuffle bytes, spill, peak execution memory |
| [postgres-profiler](postgres-profiler/README.md) | Wayang PostgreSQL platform: filter, projection, join, global reduce, table sink, and the Postgres -> Java hand-off (`SqlToStream`) | Postgres operators become one SQL statement: measures connections, query, bytes on the wire, database CPU (Linux and macOS, local server), EXPLAIN ANALYZE |

## File structure

```
Cost-Model/
├── README.md                      this file
├── java-profiler/
│   ├── README.md                  method, how to run, full list of measured columns
│   ├── run-profiling.sh           runs the profiler over a grid of configs (start here)
│   ├── analyze.py                 turns raw.csv into pairs.csv, operator_costs.csv, baseline_costs.csv
│   ├── wayang_estimates.py        adds Wayang's estimates to rows already in raw.csv (--add-estimates)
│   ├── log4j2-quiet.xml           logging config that silences Wayang's output during runs
│   ├── src/wayangprof/
│   │   └── JavaOperatorProfiler.java   the profiler: generates data, runs plans, records metrics
│   ├── native/
│   │   └── pidrusage.c            small macOS helper that reads OS counters for the JVM process
│   ├── data/                      input files, zipped (shared by both profilers; inputs, not results)
│   └── results/                   Java measurements (the training data)
│       ├── raw.csv
│       ├── pairs.csv
│       ├── operator_costs.csv
│       ├── baseline_costs.csv
│       └── run.log
├── model/                       the cost model trained on this data (see model/README.md)
│   └── results/                   reports of the trained models (held-out q-errors vs Wayang)
├── job-eval/                      multi-operator jobs: measured vs Wayang vs model (see job-eval/README.md)
│   └── results/                   job_comparison.md / .csv, raw job runs, charts/*.png
├── postgres-profiler/
│   ├── README.md                  how Wayang runs Postgres, plans, measured columns
│   ├── start-local-postgres.sh    starts a PostgreSQL server used only for profiling (127.0.0.1:5433)
│   ├── run-profiling.sh           runs the profiler over a grid of configs (start here)
│   ├── analyze.py                 same analysis, with Postgres columns
│   └── src/wayangprof/
│       └── PostgresOperatorProfiler.java  the profiler: tables, traced JDBC driver, /proc and EXPLAIN probes
└── spark-profiler/
    ├── README.md                  single-node cluster setup, method, Spark-specific columns
    ├── run-profiling.sh           runs the profiler over a grid of configs (start here)
    ├── analyze.py                 same analysis as the Java one, with Spark columns
    ├── wayang_estimates.py        same as the Java one
    ├── log4j2-quiet.xml
    ├── src/wayangprof/
    │   └── SparkOperatorProfiler.java  the profiler: Spark plans, counting sink, Spark task metrics
    ├── native/
    │   └── pidrusage.c
    └── results/                   Spark measurements (same files as the Java results)
```

`build/` (compiled classes and the Spark classpath) and `native/pidrusage` (compiled helper) are created by
`run-profiling.sh` and are not committed. The input CSVs are committed as one `.zip` per file to stay
under GitHub's file size limit; the unzipped `.csv` files are git-ignored.

## What the data holds

### `*/results/` (measurements)

Both profilers write the same files:

| File | Rows (each profiler) | One row is | Use it for |
|---|---|---|---|
| `operator_costs.csv` | 9 | one config: the operator's own cost (median of operator run − baseline run over 7 pairs) and its IQR for every metric | **training the cost model** |
| `pairs.csv` | 63 | one operator/baseline pair: the difference for every metric | checking spread and outliers |
| `raw.csv` | 126 | one measured run (`plan` = `baseline` or `operator`), absolute values | full detail, re-running the analysis |
| `baseline_costs.csv` | 9 | one config: cost of the baseline plan alone (source → parse → sink) | separating operator cost from read/parse cost |
| `run.log` | – | full JVM output from the run | debugging |

Every CSV row starts with the **features** that describe the config:

| Feature | Meaning |
|---|---|
| `operator`, `wayang_operator` | which operator was measured (e.g. `join`, `JoinOperator`) |
| `java_execution_operator` / `spark_execution_operator` | the platform operator, e.g. `JavaJoinOperator` / `SparkJoinOperator` |
| `resource` | JVM setup as `heap-GC-cpus`, e.g. `heap4g-G1-cpuall`, `heap1g-Serial-cpu2`. On Spark, cpus = task threads |
| `rows`, `right_rows` | input size (`right_rows` only for two-input operators like join) |
| `keys` | number of distinct keys in the input |
| `in_bytes`, `right_in_bytes` | input size in bytes |
| `out_rows`, `out_bytes` | output size |
| `cpu_model`, `cpus`, `machine_memory_bytes`, `java_version`, `jvm_args`, `gc_name`, `max_heap_bytes` | machine and JVM description |
| Spark only: `spark_master`, `deploy_mode`, `task_slots`, `shuffle_partitions`, `in_partitions` | Spark setup: `local[*]`, `local-single-node`, cores, shuffle partitions (2 × cores), input partitions |
| `cpu_signal_ok`, `<metric>_signal_ok` | 1 if that cost (CPU; `wall_exec_ns`, `os_cycles`, `os_energy_nj`, ...) is larger than the measurement noise; drop rows with 0 for that target. In `operator_costs.csv` and `pairs.csv` |
| `os_window` | `exec` = OS counters cover first record → job end (like `wall_exec_ns`); `run` = whole `execute()` calls |

followed by the **measured resource usage**:

| Resource | Java columns | Spark columns | Unit |
|---|---|---|---|
| Time | `wall_exec_ns`, `wall_total_ns` | same, plus `task_run_ms`, `task_run_ms_max` | ns, ms |
| CPU | `thread_cpu_exec_ns`, `thread_user_exec_ns`, `process_cpu_ns` | `task_cpu_ns` (all Spark tasks), `process_cpu_exec_ns`, `process_cpu_ns` | ns |
| Hardware counters | `os_instructions`, `os_cycles` | same | count |
| Energy | `os_energy_nj` | same | nJ |
| Memory | `thread_alloc_exec_bytes`, `total_alloc_bytes`, `heap_peak_bytes`, `live_heap_after_gc_max_bytes` | `total_alloc_exec_bytes`, `task_peak_execution_memory_bytes`, `heap_peak_bytes`, `live_heap_after_gc_max_bytes` | bytes |
| Shuffle / spill | – | `shuffle_write_bytes`, `shuffle_read_local_bytes`, `spill_memory_bytes`, `spill_disk_bytes` | bytes |
| Spark work | – | `spark_jobs`, `spark_stages`, `spark_tasks` | count |
| GC / JIT | `gc_count`, `gc_time_ms`, `gc_young_*`, `gc_old_*`, `gc_max_pause_ms`, `jit_compile_ms` | same, plus `task_gc_ms` | count, ms |
| Disk | `os_disk_read_bytes`, `os_disk_write_bytes` | same, plus `input_bytes`, `output_bytes` | bytes |
| OS activity | `os_page_faults`, `os_context_switches`, `os_syscalls_unix` | same | count |
| Network | `network_bytes` (always 0) | `network_bytes` (0 on a single node) | bytes |

and **Wayang's estimates** for the same plan (read from Wayang's optimizer, not measured):

| Estimate | Columns | Unit | Compare with |
|---|---|---|---|
| CPU | `wayang_est_cpu_cycles_lower`, `_upper` | cycles | `os_cycles`; Java `thread_cpu_exec_ns`, Spark `task_cpu_ns` |
| Disk | `wayang_est_disk_bytes_lower`, `_upper` | bytes | `os_disk_*`; Spark `shuffle_write_bytes`, `spill_disk_bytes` |
| Network | `wayang_est_network_bytes_lower`, `_upper` | bytes | `network_bytes` |
| RAM | `wayang_est_ram_bytes_lower`, `_upper` | bytes | `heap_peak_bytes`, `live_heap_after_gc_max_bytes` |
| Time | `wayang_est_time_ms_lower`, `_upper`, `_gmean`, plus `wayang_est_overhead_ms`, `wayang_est_startup_ms` | ms | `wall_exec_ns` / 1e6 |
| Cost | `wayang_est_cost_lower`, `_upper` | Wayang cost units | |
| Rows | `wayang_est_out_rows_lower`, `_upper` (Wayang's cardinality estimate) | rows | `out_rows` |
| Settings | `wayang_cpu_mhz`, `wayang_cores`, `wayang_disk_ms_per_mb`, `wayang_network_ms_per_mb`, `wayang_cost_per_ms`, `wayang_cost_model` | | |

Like the measured columns, they are operator plan − baseline plan in `pairs.csv` and `operator_costs.csv`.
Disk cannot be compared on the current data: Wayang's Java estimates never contain disk bytes, and the input
files are read from the warm OS file cache, so measured `os_disk_read_bytes` is ~0 (written bytes are real).
Measuring cold reads needs `./run-profiling.sh --cold` (empties the file cache before every run, needs
passwordless sudo).
Wayang works from its own row-count guesses and default machine settings, so the gap to the measured cost
mixes cardinality and cost-formula errors. Details:
[java-profiler/README.md](java-profiler/README.md#wayangs-own-estimates). Rows measured before these columns
existed get them with `./run-profiling.sh --add-estimates` (measures nothing, changes no measured value).

Each numeric column in `operator_costs.csv` has a matching `<column>_iqr` with its spread. The full column
lists are in [java-profiler/README.md](java-profiler/README.md#what-is-measured) and
[spark-profiler/README.md](spark-profiler/README.md#what-is-measured).

**Current coverage** (Apple M4 Pro, 12 cores, 24 GB RAM, macOS, Java 17), all 13 operators, inputs from 1k
to 1M rows (10k, 30k, 100k, 300k, 1M; cartesian 1k–10k × 10–1,000), every row with Wayang's estimates:

| Platform | Settings (configs) | Configs | Rows in `pairs.csv` |
|---|---|---|---|
| Java | `heap4g-G1-cpuall` (125), `heap1g-Serial-cpu2` (118) | 243 | 765 |
| Spark | `heap4g-G1-cpuall` (125), `heap4g-G1-cpu4` (125), `heap1g-Serial-cpu2` (3) | 253 | 795 |

Most configs were measured with 2 warm-up + 3 pairs (`--tag demo`, 2 Nov 2026); the first 9–18 configs with
3 + 7. Running `./run-profiling.sh` in either folder adds more and continues where it left off.

## Results: the cost model vs Wayang

The cost model in [`model/`](model/README.md) is trained on this data and compared with Wayang's own
estimates. q-error = max(predicted / measured, measured / predicted): how many times off, 1 is perfect
(median over all cases).

**Single operators** on configs the model never saw
([model/results/](model/results/A-random-split/report.md)):

| | Wayang | Cost model |
|---|---|---|
| Runtime (320 held-out rows) | 5.70 | **1.31** |
| CPU | 5.48 | **1.41** |
| Memory | 7.84 | **1.01** |
| Runtime on 1M-row inputs, trained only on ≤ 300k rows | 4.27 | **1.44** |

**Whole multi-operator jobs** (5 jobs × 2 sizes × Java / Spark / mixed = 30 jobs, measured runtime vs
predicted; [job-eval/](job-eval/README.md)). Training only had single-operator plans, and for this test the
model was trained **without any 100k or 1M-row configs** (the jobs' input sizes), so every operator in every
job is at an input size it has never seen ([D-no-job-sizes](model/results/D-no-job-sizes/report.md)):

| Placement | Wayang | Cost model | Model closer |
|---|---|---|---|
| Java only | 4.51 | **1.15** | 10 of 10 |
| Spark only | 2.08 | **1.30** | 8 of 10 |
| Mixed (Spark scan, Java rest) | **1.61** | 2.24 | 4 of 10 |
| All 30 jobs | 2.69 | **1.31** | 22 of 30 |

With Wayang's own row-count estimates instead of the real ones (what the model gets inside Wayang), the model
scores 1.39 over all jobs. A model trained on all data (including the job sizes) scores 1.29, so seeing the
job sizes barely matters. Only the runtime of jobs is compared; per-resource comparisons with measured CPU and
memory are for single operators (table above). Mixed jobs are its weak spot because the Spark → Java conversion (`SparkCollect`)
is not profiled yet: Wayang estimates it at ~10 ms at any size, while the measured gap is 0.1–1.1 s at 1M rows.

### `java-profiler/data/` (inputs, shared by both profilers)

Synthetic, randomly generated input files. The name gives the settings:
`rows-<rows>_keys-<distinct keys>_words-<words per line>_seed-<random seed>.csv`.
Seed 43 files are the right-hand input for joins.

Each line is `id,key,value,words`, for example:

```
0,130,546.65,hgwz sjhhr pyffogbt chbx uhalzcqz awquka umdzggaurv pvea
```

| Column | Meaning |
|---|---|
| `id` | row number |
| `key` | integer key, from 0 to `keys` − 1 (used by join, reduceby, groupby, distinct) |
| `value` | random decimal number |
| `words` | random words (used by flatmap and text size) |

The 100k-row files are about 8 MB (3.5 MB zipped) and the 1M-row files about 80 MB (35 MB zipped).
The Spark profiler unzips the files it needs by itself. For the Java profiler, unzip them first:

```bash
cd java-profiler/data && for f in *.zip; do unzip -o "$f"; done
```

Both profilers also regenerate any missing file by themselves, with identical contents (same seed, same rows).

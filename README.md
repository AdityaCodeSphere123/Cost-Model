# Training-Data

Measured resource usage of Apache Wayang operators on two platforms, **Java** and **Spark**, collected as
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
| [spark-profiler](spark-profiler/README.md) | Wayang Spark platform (Spark 3.5.7) in local mode: a **single-node cluster**, driver and executor in one JVM, one task thread per core | adds Spark task metrics: executor CPU, shuffle bytes, spill, peak execution memory |

## File structure

```
Training-Data/
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
| `cpu_signal_ok` (operator_costs only) | 1 if the CPU cost is larger than the measurement noise; drop rows with 0 |

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
Wayang works from its own row-count guesses and default machine settings, so the gap to the measured cost
mixes cardinality and cost-formula errors. Details:
[java-profiler/README.md](java-profiler/README.md#wayangs-own-estimates). Rows measured before these columns
existed get them with `./run-profiling.sh --add-estimates` (measures nothing, changes no measured value).

Each numeric column in `operator_costs.csv` has a matching `<column>_iqr` with its spread. The full column
lists are in [java-profiler/README.md](java-profiler/README.md#what-is-measured) and
[spark-profiler/README.md](spark-profiler/README.md#what-is-measured).

**Current coverage**: the same 9 configs on both platforms (Apple M4 Pro, 24 GB RAM, macOS, Java 17):

| Operator | Resource | rows × right_rows | keys |
|---|---|---|---|
| sort | heap4g-G1-cpuall | 100k, 1M | 1,000 |
| sort | heap1g-Serial-cpu2 | 100k | 1,000 |
| join | heap4g-G1-cpuall | 100k × 100k | 10,000 / 100,000 |
| join | heap4g-G1-cpuall | 1M × 1M | 100,000 / 1,000,000 |
| join | heap1g-Serial-cpu2 | 100k × 100k | 10,000 / 100,000 |

This is a first, small run. Running `./run-profiling.sh` in either folder adds the rest of the default grid
(78 configs, 13 operators) and continues where it left off.

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

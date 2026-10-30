# Training-Data

Measured resource usage of Apache Wayang's Java operators, collected as training data for an ML cost model.
All numbers are real measurements from running the operators, not Wayang's own cost estimates.
For how the profiler works, see [java-profiler/README.md](java-profiler/README.md).

## File structure

```
Training-Data/
├── README.md                      this file
└── java-profiler/
    ├── README.md                  method, how to run, full list of measured columns
    ├── run-profiling.sh           runs the profiler over a grid of configs (start here)
    ├── analyze.py                 turns raw.csv into pairs.csv, operator_costs.csv, baseline_costs.csv
    ├── log4j2-quiet.xml           logging config that silences Wayang's output during runs
    ├── src/wayangprof/
    │   └── JavaOperatorProfiler.java   the profiler: generates data, runs plans, records metrics
    ├── native/
    │   └── pidrusage.c            small macOS helper that reads OS counters for the JVM process
    ├── data/                      generated input files, zipped (operator inputs, not results)
    └── results/                   measured resource usage (the training data)
        ├── raw.csv
        ├── pairs.csv
        ├── operator_costs.csv
        ├── baseline_costs.csv
        └── run.log
```

`build/` (compiled classes) and `native/pidrusage` (compiled helper) are created by `run-profiling.sh`
and are not committed. The input CSVs in `data/` are committed as one `.zip` per file to stay under
GitHub's file size limit; the unzipped `.csv` files are git-ignored.

## What the data holds

### `java-profiler/results/` (measurements)

| File | Rows | One row is | Use it for |
|---|---|---|---|
| `operator_costs.csv` | 9 | one config: the operator's own cost (median of operator run − baseline run over 7 pairs) and its IQR for every metric | **training the cost model** |
| `pairs.csv` | 63 | one operator/baseline pair: the difference for every metric | checking spread and outliers |
| `raw.csv` | 126 | one measured run (`plan` = `baseline` or `operator`), absolute values | full detail, re-running the analysis |
| `baseline_costs.csv` | 9 | one config: cost of the baseline plan alone (source → parse → sink) | separating operator cost from read/parse cost |
| `run.log` | – | full JVM output from the run | debugging |

Every CSV row starts with the **features** that describe the config:

| Feature | Meaning |
|---|---|
| `operator`, `wayang_operator`, `java_execution_operator` | which operator was measured (e.g. `join`, `JoinOperator`, `JavaJoinOperator`) |
| `resource` | JVM setup as `heap-GC-cpus`, e.g. `heap4g-G1-cpuall`, `heap1g-Serial-cpu2` |
| `rows`, `right_rows` | input size (`right_rows` only for two-input operators like join) |
| `keys` | number of distinct keys in the input |
| `in_bytes`, `right_in_bytes` | input size in bytes |
| `out_rows`, `out_bytes` | output size |
| `cpu_model`, `cpus`, `machine_memory_bytes`, `java_version`, `jvm_args`, `gc_name`, `max_heap_bytes` | machine and JVM description |
| `cpu_signal_ok` (operator_costs only) | 1 if the CPU cost is larger than the measurement noise; drop rows with 0 |

followed by the **measured resource usage**:

| Resource | Main columns | Unit |
|---|---|---|
| Time | `wall_exec_ns`, `wall_total_ns` | ns |
| CPU | `thread_cpu_exec_ns`, `thread_user_exec_ns`, `process_cpu_ns`, `os_user_ns`, `os_system_ns` | ns |
| Hardware counters | `os_instructions`, `os_cycles` | count |
| Energy | `os_energy_nj` | nJ |
| Memory | `thread_alloc_exec_bytes`, `total_alloc_bytes`, `heap_peak_bytes`, `live_heap_after_gc_max_bytes`, `os_phys_footprint_bytes_delta` | bytes |
| GC / JIT | `gc_count`, `gc_time_ms`, `gc_max_pause_ms`, `jit_compile_ms` | count, ms |
| Disk | `os_disk_read_bytes`, `os_disk_write_bytes` | bytes |
| OS activity | `os_page_faults`, `os_context_switches`, `os_syscalls_unix` | count |

Each numeric column in `operator_costs.csv` has a matching `<column>_iqr` with its spread.
The full column list is in [java-profiler/README.md](java-profiler/README.md#what-is-measured).

**Current coverage** (Apple M4 Pro, 24 GB RAM, macOS, Java 17):

| Operator | Resource | rows × right_rows | keys |
|---|---|---|---|
| sort | heap4g-G1-cpuall | 100k, 1M | 1,000 |
| sort | heap1g-Serial-cpu2 | 100k | 1,000 |
| join | heap4g-G1-cpuall | 100k × 100k | 10,000 / 100,000 |
| join | heap4g-G1-cpuall | 1M × 1M | 100,000 / 1,000,000 |
| join | heap1g-Serial-cpu2 | 100k × 100k | 10,000 / 100,000 |

This is a first, small run. Running `./run-profiling.sh` adds the rest of the default grid (78 configs, 13 operators)
and continues where it left off.

### `java-profiler/data/` (inputs)

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
Unzip them before use:

```bash
cd java-profiler/data && for f in *.zip; do unzip -o "$f"; done
```

The profiler also regenerates any missing file by itself, with identical contents (same seed, same rows).

# Java operator profiler

Measures the **real** resources each Wayang Java operator uses, as training data for the ML cost model.
No measured column comes from Wayang's cost estimates. Wayang's own estimates for the same plans are stored
next to them in separate `wayang_*` columns, for comparison (see [Wayang's own estimates](#wayangs-own-estimates)).

## Method

For each config the profiler starts one JVM and runs two plans in alternating pairs, after warm-up runs:

```
baseline : source -> parse -> sink
operator : source -> parse -> OPERATOR -> sink
operator cost = operator run - baseline run   (per pair, then median)
```

Each plan runs one operator at a time, so the additive cost assumption holds (no parallel branches).
Binary operators need a connected plan: `join` and `cartesian` use `left, right -> UnionAll -> sink` as
baseline, and `union` uses two separate jobs. The column `measured_execution_operators` says exactly what
each difference covers (for example `+JavaJoinOperator -JavaUnionAllOperator`).

Operators: map, filter, flatmap, reduceby, groupby, globalreduce, count, distinct, sort, textsink, join, union, cartesian.

## Run

```bash
./run-profiling.sh --dry-run            # list the configs
./run-profiling.sh --profile quick      # a few minutes
./run-profiling.sh                      # default grid (78 configs)
./run-profiling.sh --profile full       # large grid, many hours
./run-profiling.sh --ops "sort join" --rows "100000 1000000" --resources "4g:G1:all 1g:Serial:2"
./run-profiling.sh --add-estimates      # add Wayang's estimates to rows already in raw.csv (measures nothing)
./run-profiling.sh --help
```

Resource settings are `HEAP:GC:CPUS` (heap size, G1/Parallel/Serial/Z collector, CPU count or `all`).
Stopping and restarting continues where it left off.

## Output (in `results/`)

| File | Contents |
|---|---|
| `raw.csv` | every measured run, baseline and operator |
| `pairs.csv` | one row per pair: features + (operator − baseline) for every metric |
| `operator_costs.csv` | one row per config: median difference and its IQR; `cpu_signal_ok` = cost larger than noise |
| `baseline_costs.csv` | the baseline itself, which is the cost of source + parse + sink |
| `run.log` | full JVM output |

## What is measured

| Group | Columns | Source |
|---|---|---|
| Time | `wall_exec_ns`, `wall_total_ns` | `System.nanoTime` |
| CPU | `thread_cpu_exec_ns`, `thread_user_exec_ns`, `process_cpu_ns`, `os_user_ns`, `os_system_ns`, `os_user_pcore_ns`, `os_runnable_ns`, `os_cpu_qos_*` | ThreadMXBean, OS |
| Hardware | `os_instructions`, `os_cycles`, `os_*_pcore` | OS (Apple Silicon counters) |
| Energy | `os_energy_nj`, `os_energy_pcore_nj` | OS (macOS); Linux: `os_energy_pkg_nj` from RAPL, whole CPU package |
| Memory | `thread_alloc_exec_bytes`, `total_alloc_bytes`, `live_heap_after_gc_max_bytes`, `heap_peak_bytes`, `pool_eden/survivor/old_peak_bytes`, `os_phys_footprint_bytes_delta`, `os_resident_bytes_delta` | MXBeans, OS |
| GC / JIT | `gc_count`, `gc_time_ms`, `gc_young_*`, `gc_old_*`, `gc_max_pause_ms`, `jit_compile_ms`, `classes_loaded` | MXBeans |
| Disk | `os_disk_read_bytes`, `os_disk_write_bytes`, `os_logical_write_bytes`, `os_pageins`, `out_bytes` | OS |
| OS activity | `os_page_faults`, `os_cow_faults`, `os_context_switches`, `os_syscalls_unix`, `os_syscalls_mach`, `os_threads_*` | OS |
| Network | `network_bytes` | always 0: the Java platform runs inside one JVM |

GC columns have the same names for every garbage collector: `gc_young_*` (G1 young, Serial Copy, Parallel Scavenge), `gc_old_*` (G1 old, MarkSweepCompact, PS MarkSweep), `gc_other_*` (e.g. ZGC), and heap pools `pool_eden_peak_bytes`, `pool_survivor_peak_bytes`, `pool_old_peak_bytes`, `pool_other_heap_peak_bytes`. So every row fills every column, whichever GC the resource setting uses.

`*_exec_*` columns start at the first record, so they leave out Wayang's plan optimization; `*_total_*`
cover the whole `execute()` call. `thread_*` columns count only the thread that runs the operator.
`os_*` columns count the whole JVM process, including JIT and GC threads, so they are noisier.

## Wayang's own estimates

Every row also holds what Wayang's optimizer **predicts** for the same plan. The profiler optimizes each plan
the way Wayang does before running it, then reads the estimate Wayang stored for every execution task
(including channel conversions) and adds them up. Nothing is measured for these columns.

| Column | Meaning | Compare with |
|---|---|---|
| `wayang_est_cpu_cycles_lower/upper` | CPU cycles (Wayang's load profile, UDF part included) | `os_cycles`, `thread_cpu_exec_ns` |
| `wayang_est_disk_bytes_lower/upper` | disk bytes | `os_disk_read_bytes`, `os_disk_write_bytes` |
| `wayang_est_network_bytes_lower/upper` | network bytes | `network_bytes` |
| `wayang_est_ram_bytes_lower/upper` | RAM bytes | `live_heap_after_gc_max_bytes`, `heap_peak_bytes` |
| `wayang_est_overhead_ms` | fixed overhead in the load profiles | |
| `wayang_est_time_ms_lower/upper/gmean` | time Wayang derives from the load (gmean = geometric mean, what the optimizer uses) | `wall_exec_ns` / 1e6 |
| `wayang_est_cost_lower/upper` | cost = time × `wayang.java.costs.per-ms` (the vector model's money axis) | |
| `wayang_est_startup_ms` | platform start-up, not included in `wayang_est_time_ms_*` (0 for Java) | |
| `wayang_est_out_rows_lower/upper` | rows Wayang expects at the sink (its cardinality estimate) | `out_rows` |
| `wayang_est_tasks` | execution tasks in the plan | |
| `wayang_est_breakdown` (raw.csv only) | per task: estimated input/output rows, cycles, disk, network, ms | |
| `wayang_cpu_mhz`, `wayang_cores`, `wayang_disk_ms_per_mb`, `wayang_network_ms_per_mb`, `wayang_cost_per_ms`, `wayang_cost_model` | Wayang settings used to turn load into time and cost (features, not subtracted) | |

The estimate columns are handled like the measured ones: `pairs.csv` and `operator_costs.csv` hold
operator plan − baseline plan, which is Wayang's estimate of the operator itself; `baseline_costs.csv` holds
the baseline plan's estimate. Estimates do not change between runs of a config, so their `_iqr` is 0.

Things to know when comparing:

- Wayang uses its **own cardinality estimates**, not the real row counts (e.g. a default selectivity for
  filters and a key-independent output size for joins). Compare `wayang_est_out_rows_*` with `out_rows` to
  separate cardinality errors from cost-formula errors.
- Wayang's time assumes its default machine (`wayang.java.cpu.mhz = 2700`, 1 core), not the measured one.
- Each profiler plan runs on one platform, so there is only one possible execution plan; the plan pick (the
  scalar or the multi-objective vector cost model) cannot change these numbers. The load estimates come from
  the per-operator estimators, which the vector change does not touch.
- Rows measured before these columns existed get them with `./run-profiling.sh --add-estimates`. It runs only
  the optimizer for each config in `raw.csv`, checks that Wayang still picks the same execution operators as
  in the measured run, and then adds the columns and redoes the analysis. Measured values are not changed.

## Notes

- Very cheap operators (filter, map, union, count on small inputs) are within noise. Use 1M+ rows for them
  and check `cpu_signal_ok`.
- Input files are usually in the OS file cache, so `os_disk_read_bytes` is near 0. Use `--cold` (needs
  passwordless sudo) to empty the cache before each run.
- Plug the laptop in and close other apps: CPU speed and energy change on battery and under load.
- `distinct` treats rows with the same key as equal, so its output size is set with `--keys`.

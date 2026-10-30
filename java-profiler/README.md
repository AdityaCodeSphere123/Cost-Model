# Java operator profiler

Measures the **real** resources each Wayang Java operator uses, as training data for the ML cost model.
Nothing comes from Wayang's cost estimates.

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
| Memory | `thread_alloc_exec_bytes`, `total_alloc_bytes`, `live_heap_after_gc_max_bytes`, `heap_peak_bytes`, `pool_*_peak_bytes`, `os_phys_footprint_bytes_delta`, `os_resident_bytes_delta` | MXBeans, OS |
| GC / JIT | `gc_count`, `gc_time_ms`, `gc_<collector>_*`, `gc_max_pause_ms`, `jit_compile_ms`, `classes_loaded` | MXBeans |
| Disk | `os_disk_read_bytes`, `os_disk_write_bytes`, `os_logical_write_bytes`, `os_pageins`, `out_bytes` | OS |
| OS activity | `os_page_faults`, `os_cow_faults`, `os_context_switches`, `os_syscalls_unix`, `os_syscalls_mach`, `os_threads_*` | OS |
| Network | `network_bytes` | always 0: the Java platform runs inside one JVM |

`*_exec_*` columns start at the first record, so they leave out Wayang's plan optimization; `*_total_*`
cover the whole `execute()` call. `thread_*` columns count only the thread that runs the operator.
`os_*` columns count the whole JVM process, including JIT and GC threads, so they are noisier.

## Notes

- Very cheap operators (filter, map, union, count on small inputs) are within noise. Use 1M+ rows for them
  and check `cpu_signal_ok`.
- Input files are usually in the OS file cache, so `os_disk_read_bytes` is near 0. Use `--cold` (needs
  passwordless sudo) to empty the cache before each run.
- Plug the laptop in and close other apps: CPU speed and energy change on battery and under load.
- `distinct` treats rows with the same key as equal, so its output size is set with `--keys`.

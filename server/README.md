# Running the profilers on a server / VM (3–4 days, unattended)

Collects the large training dataset for the cost model on a dedicated machine: Wayang's Java platform and
Spark single-node, alternating, with Wayang's estimate stored next to every measurement.

## Files

| File | What it does |
|---|---|
| `make-bundle.sh` | **on the Mac**: packs Wayang, Spark 3.5, Java 17 for Linux (x64 + ARM, checksum-verified), both profilers and the model code into `dist/cost-model-server.tar.gz` (~1 GB) |
| `run-server.sh` | **on the server**: `--check` tests everything; without it, runs Java and Spark in turns until the time limit |
| `sweep.py` | random config lists (13 operators, rows 1k–3M, keys, selectivity, words, heap / GC / cores), balanced across operators |
| `next_chunk.py` | the next unfinished configs of a list (used by `run-server.sh`) |
| `validate_data.py` | checks the data for impossible or suspicious values (runs automatically) |
| `run-server.slurm` | the same as a SLURM batch job, if the server uses SLURM |
| `run-cluster.sh` | **on the server, after run-server.sh**: Spark standalone clusters of 1-10 executors, one at a time (see [below](#spark-clusters-run-clustersh)) |

## Steps

**1. On the Mac** (once, and again after any profiler change):
```bash
cd Cost-Model/server && ./make-bundle.sh          # --jdk x64 if the VM is Intel/AMD: ~190 MB smaller
scp ../dist/cost-model-server.tar.gz user@vm:
```

**2. On the VM.** Needs only Linux, `python3` and ~30 GB free disk. Java is in the bundle; no internet, Maven or
Wayang source needed.
```bash
tar xzf cost-model-server.tar.gz
cd cost-model-server/Cost-Model/server
./run-server.sh --check
```
The check takes about a minute and prints:
- a **machine report**: virtualization type, CPU frequency policy, energy counter, and steal time over 5 idle
  seconds (should be ~0%);
- one small config per platform, and the columns it produced;
- the **self-test**: a fixed amount of CPU work must be measured as 90–125% of its known cost, and pure waiting
  must cost < 15% CPU, on Java and on Spark;
- the data validation. It must end with **CHECK PASSED** and **DATA OK**. If not, don't start the run; send
  the output.

**3. Start the run** in the background, so it keeps going after you log out:
```bash
nohup ./run-server.sh --hours 84 >/dev/null 2>&1 &     # or inside tmux / screen
```
With SLURM instead: `sbatch run-server.slurm` (resubmit it when it ends; it continues).
To restart automatically after a VM reboot: `crontab -e` and add
`@reboot cd ~/cost-model-server/Cost-Model/server && ./run-server.sh --hours 84 >/dev/null 2>&1`
(the time limit counts from each start).

**4. Watch it:**
```bash
tail -f ../results-server/server.log                   # one line per chunk: configs done, failed, hours left
cat ../results-server/validation.txt                   # data checks, refreshed every 10 rounds
```

**5. Stop / restart.** `pkill -f run-server.sh; pkill -f OperatorProfiler` stops it. Starting it again with the
same options continues where it stopped (finished configs are skipped, failed ones are not retried).

**6. Bring the data back** and train:
```bash
rsync -av user@vm:cost-model-server/Cost-Model/results-server/ Cost-Model/results-server/
cd Cost-Model/model
.venv/bin/python train.py --data ../results-server/java/pairs.csv ../results-server/java/baseline_costs.csv \
                                 ../results-server/spark/pairs.csv ../results-server/spark/baseline_costs.csv
```

## How the data is kept clean

| Risk | What protects against it |
|---|---|
| Different Java on the VM | Java 17 (Temurin) ships in the bundle and is used automatically on Linux (`USE_SYSTEM_JAVA=1` to opt out) |
| Other VMs on the same host take CPU ("steal") | every run records the machine's steal time; `analyze.py` drops pairs where > 2% of the machine's CPU time was stolen, and the validation reports how many |
| Measurement broken on this machine | the self-test in `--check` compares measured CPU with a known amount of work and with pure waiting |
| Impossible values (bugs, broken runs) | `validate_data.py`: first-row marker, sink row counts, filter output, Spark task CPU ≤ run time, shuffle records, one-thread check, Wayang estimate present |
| Spark task metrics incomplete | `spark_listener_drained = 0` marks the run; `analyze.py` drops the pair |
| JIT warm-up, drift, GC leftovers | 2 warm-up pairs, alternating order, forced GC before each run (as on the laptop) |
| Disk filling up | inputs generated per config and deleted after it |
| A config that hangs | 30-minute limit per config (`perl`, or `timeout` if perl is missing) |
| Run stopped (time limit, crash, reboot) | restart continues; finished configs skipped, failed ones not retried |
| Uneven data if it stops early | random order with operators taking turns: any finished part is an even sample |
| Energy counter wrapping around | corrected with RAPL's range (`max_energy_range_uj`) |

## What the run does

- **Config lists** (`results-server/java.list`, `spark.list`): 80,000 random configs per platform, made once with
  fixed seeds for this machine's cores and RAM (heaps at most a third of RAM; 1 GB heaps on small VMs).
- **Settings**: main setting 8 GB heap, G1, all cores (~40% of configs); the rest mix heaps 2/4/8/16 GB,
  G1/Parallel/Serial and 1, 2, 4, … cores (Spark task threads).
- **Per config**: 2 warm-up + 5 measured pairs (baseline / operator, alternating), plus on Java one untimed
  memory run per plan (`mem_held_bytes`). A fresh JVM per config.
- **Analysis and validation**: every 10 rounds and at the end.

**How much data to expect**: plan for ~8–12 s per config, so 84 hours give roughly **10,000–15,000 configs
per platform = 50,000–75,000 rows in `pairs.csv` each**.

## Best results on a VM

- Ask for **dedicated vCPUs** (not shared or "burstable" CPUs). The check's steal time shows whether the host is
  busy.
- Run nothing else heavy during the run, and keep the options the same between restarts.
- Linux gives fewer counters than macOS: CPU time, wall time, memory, GC, disk, page faults and steal work; CPU
  cycles and instructions do not (CPU time is what the model uses). Energy only if RAPL is readable (rare
  in VMs): `sudo chmod a+r /sys/class/powercap/intel-rapl:0/energy_uj`.

## Spark clusters (`run-cluster.sh`)

The third phase of [`../plan.md`](../plan.md): Spark with several executors, so shuffles cross from one executor
to another and the data includes network cost. On one big VM (e.g. 64 cores) it starts **standalone Spark clusters
of 1 to 10 executors**, one cluster at a time:

```bash
./run-cluster.sh --check                                  # ~1 min: a 2-executor cluster, one config, self-test
nohup ./run-cluster.sh --hours 72 >/dev/null 2>&1 &       # the real run; restart continues
tail -f ../results-cluster/cluster.log
```

**Run it after `run-server.sh` has finished, never at the same time**: both scripts refuse to start while the other
runs, because two runs on one machine distort each other's timings.

How a cluster is built (64 cores, defaults):

```
cores 0-3    master + driver (the profiler JVM, 8 GB heap)          <- --reserve 4, --driver-heap
cores 4-9    worker 1 -> executor 1 (6 task threads, own JVM)        \
cores 10-15  worker 2 -> executor 2                                   } N workers for shape Nx6, N = 1..10
...                                                                   } each pinned with taskset, heap = --executor-memory-gb
cores 58-63  worker 10 -> executor 10                                /
```

- **Shapes** `1x6, 2x6, ..., 10x6` (N executors x 6 cores = 6 to 60 task threads). Other shapes with `--shapes`, e.g.
  the same 24 cores split differently, to separate "more parallelism" from "more network":
  `--shapes "1x24 2x12 3x8 4x6 6x4 8x3 12x2"`.
- **Same configs on every shape**: `sweep.py --platform spark-cluster` draws each data config once and lists it for
  every shape, so each shape is measured on the same inputs. Shapes take turns in chunks of 20 configs (start
  rotates every round), so a run that stops early still covers all shapes evenly.
- **Executor memory** is the same for every shape (by default as much as the 10-executor shape fits into 75% of
  RAM, at most 8 GB). Restarting with other shapes, heap or memory is refused (the rows would not be comparable).
- **Everything on 127.0.0.1**: master, workers, executors and driver, so nothing is reachable from outside the VM.
- **Network**: `spark.shuffle.readHostLocalDisk=false`, so shuffle blocks of other executors are fetched over TCP
  and counted in `network_bytes` (with Spark's default they would be read from disk and counted as local). This is
  loopback TCP: serialization, Netty and kernel copies are real, a physical network's bandwidth limit and
  latency are not. Real multi-node runs are plan.md's phase 3b.
- **Checks per config**: the profiler waits until exactly N executors with N x C cores are registered; otherwise
  the config is marked `RETRY` and runs again later, never recorded with the wrong shape.
- **Hyperthreads**: on a VM with 2 threads per core, cores 33-64 are hyperthreads of cores 1-32, so shapes above
  ~5 executors share physical cores. The machine report in `cluster.log` says so; it is a property of the machine.

Output in `../results-cluster/spark-cluster/` (same files as the other runs) plus `cluster.list`, `lists/<shape>.list`,
`logs/` (master and worker logs) and `validation.txt`. Train with it like the Spark data; add `executors` and
`executor_cores` as model features (the current features know only `task_slots`).

Stop: `pkill -f run-cluster.sh; pkill -f OperatorProfiler` (stops the cluster too). Each config takes ~10-20 s
(a fresh driver and executors per config), so 72 hours give roughly 1,300-2,500 configs per shape.

## Changes in the profilers for this run

- 8 Nov 2026:
  - `raw.csv` appends read only the header, and resume uses a small list of finished ids, so long runs stay fast.
  - Java: `mem_held_bytes` from an untimed memory run.
  - Spark: `spark_listener_drained`.
  - Runners: `--config-list`, `--hours`, `--ephemeral-inputs`, `--no-analyze`.
- 11 Nov 2026:
  - Linux counters work without `/proc/self/io`.
  - New columns `os_sys_steal_ns`, `os_sys_iowait_ns`, `os_sys_cpus_after`; steal filter in `analyze.py`.
  - RAPL wrap-around corrected.
  - `burn` / `sleep` test operators in both profilers.
  - `timeout` fallback; CPU model on ARM; small-RAM guard in `sweep.py`.
  - Bundled JDK; `--check` self-test and validation.
- 12 Nov 2026:
  - Spark cluster mode (`--master`, `HEAP:GC:NxC` resources) and `run-cluster.sh`; `sweep.py --platform spark-cluster`.
  - New columns: `process_cpu_exec_ns` (Java), `jit_compile_exec_ms`, `gc_time_exec_ms`, `warmup_runs`, `wall_first_task_ns`,
    `executors_used`, `executors_process_cpu_ns`, `executor_cores`, `executor_memory_bytes`, `shuffle_host_local_disk`.
  - Extra warm-up for small configs (until 3M input rows per plan, at most 20 runs / 10 s).
  - `next_chunk.py` prints nothing when a list is finished (before, both runners kept looping until the time limit).
  - `validate_data.py`: cluster network and executor checks, JIT-during-measurement warning.

# Running the profilers on a server / VM (3–4 days, unattended)

Collects the large training dataset for the cost model on a dedicated machine: Wayang's Java platform and
Spark single-node, alternating, with Wayang's estimate stored next to every measurement.

## The big run on the server: prepare, test, collect (recommended)

Three commands, in this order. Only the first needs the internet; none needs root.

```bash
# on the Mac: pack everything (code + Wayang jars of this repo + Spark jars + Java 17), copy it over
cd Cost-Model/server && ./make-bundle.sh --jdk x64
scp ../dist/cost-model-server.tar.gz USER@SERVER:

# on the server
tar xzf cost-model-server.tar.gz && cd cost-model-server/Cost-Model/server
./prepare.sh        # ONLINE, ~5-20 min: whatever is missing (portable PostgreSQL, model packages), compiles
                    #   -> ends with "prepare DONE"; from here on the internet can be off
./test.sh           # offline, ~15 min: every part on small single-operator jobs -> must end with TEST PASSED
tmux new -s wayang  # so the run survives closing VS Code / the laptop
./collect.sh start  # Ctrl+B then D to detach; the dataset is complete by Sunday 2026-11-15 16:00
```

Back later: `ssh USER@SERVER`, then `tmux attach -t wayang` (live), `./collect.sh status`, or
`tail -f ../collect/collect.log`. The dataset: `scp USER@SERVER:cost-model-server/Cost-Model/collect/export/wayang-cost-data.tar.gz .`
(refreshed every 3 hours and at the end, so there is always a recent one).

**Wayang on the server.** The bundle carries the Wayang jars of this repository (4.7 MB of Wayang code plus its
libraries): the profilers use Wayang internals and were compiled and tested against exactly these. To use a Wayang
already on the server instead, copy only `Cost-Model/` and set `WAYANG_HOME=<its built distribution, with jars/ and
libs/>` (or put `Cost-Model/` next to the Wayang source: `prepare.sh` then builds it with Maven, online). `prepare.sh`
compiles the profilers against it and stops if they do not fit.

### What `collect.sh` does

It splits the machine into **lanes** that run at the same time, each pinned to its own CPUs (`taskset`; both
hyperthreads of a core go to the same lane), each with its own memory budget, data folder and ports:

| Lane | What it measures | Example on 64 CPUs |
|---|---|---|
| `java-N` | Wayang's Java platform, 13 operators, one JVM at a time | 4 lanes x 2 CPUs |
| `spark-N` | Spark local mode, 13 operators, local[1..4] | 3 lanes x 4 CPUs |
| `postgres-N` | Wayang's PostgreSQL platform, 6 operators; **every lane has its own PostgreSQL server**, so the database CPU read from `/proc` belongs to that lane | 2 lanes x 6 CPUs |
| `cluster` | Spark standalone clusters with **1, 2, 3, ..., 10 executors**, one cluster at a time; the sizes take turns every 20 configs and all sizes run the same data configs | 2 + 10 x 3 CPUs |

`./collect.sh plan` prints the plan for the machine without starting. On a shared server, leave CPUs to others:
`--cpus 0-47`; or set lanes by hand: `--plan "cluster=10x3 java=4x2 spark=3x4 postgres=2x6"`. Memory: 85% of what
is free at the start (`--mem-gb` to change), 40% for the cluster lane, the rest by CPUs (Java lanes count double:
their large sorts and joins need 4 GB+ heaps).

Every lane works through its share of one fixed, random config list per platform (100,000 Java / Spark configs,
40,000 PostgreSQL configs, 5,000 data configs x 10 cluster sizes): all operators in turn; input sizes 1k..3M rows in
x1.25 steps; keys, selectivity, row width; heap / GC / cores; PostgreSQL parallel workers and indexes. The lists are
far longer than 3 days can finish, so the run is limited only by time, and the finished part is always an even
sample of the whole space (no corner left out). Each config: 2 warm-up + 5 measured pairs of operator plan vs
baseline plan, Wayang's cost estimate stored next to each.

**What lanes do and do not isolate.** Each lane has its own CPUs (`taskset`), so lanes never take CPU *time slots*
from each other. They still share the last-level cache, memory bandwidth, the disk and the file cache, and the CPU
package's power budget (the all-core clock). Cache and memory stalls count as CPU time, so lanes can raise CPU time as
well as wall time, most for memory-heavy operators. This is handled, not assumed away:

- **Calibration** (`data/calibration.txt` / `.csv`): before the lanes start, the first configs of the first Java and
  Spark lane are measured *alone* on that lane's CPUs; the lane then measures the same configs first, on the same CPUs,
  with all lanes running. The ratio (under load / alone) per metric is the number to report next to results from
  lane data (`calibration_report.py`).
- **Intruders on a lane's CPUs**: every row records the busy time of its lane's CPUs (`os_lane_busy_ns`); analyze.py
  turns it into `lane_foreign_cpu_cores` and flags runs where anything else used > 0.25 cores there (`quiet_ok = 0`;
  such pairs are left out of `operator_costs.csv` and of training).
- **Housekeeping CPUs** (one physical core by default, `plan_lanes.py --housekeeping`): `collect.sh` itself, the merges
  every 3 h and the status run there, never on a lane's CPUs.
- **Energy** is not recorded while lanes run: RAPL counts the whole package, i.e. every lane at once.
- Not covered: the disk and file cache. `--ephemeral-inputs` regenerates each input file per config, which can push
  another lane's input out of the file cache (watch `os_disk_read_bytes`).

Every row records its lane (`tag` = `lane-java-2` etc.). On a server that other people use too, their jobs show up
as `quiet_ok = 0` on the lanes they touch; `collect.log` also lists other users' busy processes every 10 minutes.
Outside `collect.sh`, the runners measure **one config at a time per machine** (a lock in
`../common/measure-gate.sh`), so two runs started by hand take turns instead of disturbing each other.

**The time limit** is when the final dataset must be ready: by default Sunday 2026-11-15 16:00 (server clock; another
time with `--until "YYYY-MM-DD HH:MM"`). New configs stop 50 min before it, configs still running 30 min before it are
stopped, then the final merge and export run. Everything finished until then is kept: each config is written to disk
the moment it ends, and the dataset is exported every 3 h along the way. A lane that finishes its list early gets a
fresh random batch (new seed; configs it already ran are skipped), so no lane sits idle.

**For days without attention:** a lane whose runner crashes is restarted after 1 minute (it gives up after 10
failures in a row; the other lanes go on); a config that hangs is killed after 30 minutes; a cut CSV line is
repaired; `./collect.sh stop` / a crash / a reboot loses only the configs that were running, and
`./collect.sh start` (no options needed) continues the same run with the same plan, lists and deadline.

### Files to look at

| File | |
|---|---|
| `collect/collect.log` | **the log**: environment, plan, every lane start / end / restart (with the last lines of a failure), status every 10 min, merges, validations |
| `collect/STATUS.txt` | latest status: configs done / failed per lane and platform, configs per cluster size, rates, load |
| `collect/data/<platform>/` | merged dataset: `raw.csv` (every run), `pairs.csv` (operator - baseline per pair), `operator_costs.csv` (per config, with Wayang's estimates), `baseline_costs.csv` |
| `collect/data/validation.txt` | checks of the merged data (impossible values, missing estimates, noise) |
| `collect/export/wayang-cost-data.tar.gz` | all of the above + every lane's run.log, in one file |
| `collect/lanes/<lane>/run.log` | every config of that lane with times, and why one failed |
| `test-run/test.log`, `prepare.log` | output of test.sh / prepare.sh |


## Files

| File | What it does |
|---|---|
| `prepare.sh` | **server step 1** (the only online step, no root): Java 17, Wayang, Spark 3.5.7 jars, portable PostgreSQL, model packages, compiles the profilers |
| `test.sh` | **server step 2** (offline, ~15 min): every part of the pipeline on small single-operator jobs, incl. Wayang's estimates |
| `collect.sh` | **server step 3** (offline, days): all profilers at once in CPU-pinned lanes, clusters of 1..10 executors, merged dataset every 3 h |
| `env.sh`, `plan_lanes.py`, `merge_lanes.py` | used by the three above: finds the tools / splits the machine into lanes / merges the lanes' data |
| `make-bundle.sh` | **on the Mac**: packs Wayang, Spark 3.5, Java 17 for Linux (x64 + ARM, checksum-verified), both profilers and the model code into `dist/cost-model-server.tar.gz` (~1 GB) |
| `setup-vm.sh` | **on the VM, once** (sudo): installs PostgreSQL and tools, starts the profiling database |
| `run-server.sh` | **on the server**: `--check` tests everything; without it, runs Java, Spark (and with `--postgres local` PostgreSQL) in turns until the time limit |
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

**2. On the VM** (Linux with sudo; ~30 GB free disk). Java 17, Wayang and Spark are in the bundle.
```bash
tar xzf cost-model-server.tar.gz
cd cost-model-server/Cost-Model/server
./setup-vm.sh                                   # once, as your normal user: installs Java 17, Spark 3.5.7,
                                                # PostgreSQL, python3, perl, taskset; starts the profiling database
./run-server.sh --check --postgres local
```
`setup-vm.sh` (internet needed for this step only):
- installs Java 17 (`openjdk-17-jdk-headless` / `java-17-openjdk-devel`), PostgreSQL and the small tools with
  `apt-get` or `dnf`;
- downloads **Apache Spark 3.5.7** (the version Wayang is built against, Scala 2.12, hadoop3 build) from apache.org,
  checks its SHA-512 and installs it in `/opt/spark-3.5.7`, linked as `/opt/spark`;
- writes `JAVA_HOME`, `SPARK_HOME` and `PATH` for new logins to `/etc/profile.d/wayang-profiling.sh`
  (`spark-shell`, `spark-submit` then work);
- disables the distribution's default PostgreSQL service it just installed (profiling uses its own server), makes the
  energy counter readable and sets the CPU frequency policy to performance where the VM exposes them;
- starts the profiling database (127.0.0.1:5433). Safe to run again; `--no-spark` / `--no-java` skip those parts.

Which Java and Spark the runs use: the bundle's Java 17 and Spark jars first (the same jars the profilers were
tested with); the installed ones when the bundle has none (`make-bundle.sh --jdk none`, or a bundle without
`spark-jars/`). The `java_version` and `spark_version` columns record what was used.

The check takes about three minutes and prints:
- a **machine report**: virtualization type, CPU frequency policy, energy counter, and steal time over 5 idle
  seconds (should be ~0%);
- one small config per platform, and the columns it produced;
- the **self-test**: a fixed amount of CPU work must be measured as 90–125% of its known cost, and pure waiting
  must cost < 15% CPU, on Java and on Spark;
- the **Postgres check**: bytes on the socket, EXPLAIN, filter output, and the measured database CPU against the
  server's own execution time;
- the data validation. It must end with **CHECK PASSED**, **POSTGRES CHECK PASSED** and **DATA OK**. If not, don't
  start the run; send the output.

Without PostgreSQL, leave out `--postgres local` (Java and Spark only). Another server can be used with
`--postgres postgresql://HOST:PORT/DB --pg-user U` (database CPU is then measured only if it runs on this machine).

**3. Start the run** in the background, so it keeps going after you log out:
```bash
nohup ./run-server.sh --hours 84 --postgres local >/dev/null 2>&1 &     # or inside tmux / screen
```
Java, Spark and Postgres take turns in chunks. `--postgres local` starts the profiling database if it is not
running, so restarts need nothing else. With SLURM instead: `sbatch run-server.slurm` (resubmit it when it ends).
To restart automatically after a VM reboot: `crontab -e` and add
`@reboot cd ~/cost-model-server/Cost-Model/server && ./run-server.sh --hours 84 --postgres local >/dev/null 2>&1`
(the time limit counts from each start; energy counter and CPU policy reset at reboot: run `./setup-vm.sh` again
when you are logged in, it is safe to repeat).

**4. Watch it:**
```bash
tail -f ../results-server/server.log                   # one line per chunk: configs done, failed, hours left
cat ../results-server/validation.txt                   # data checks, refreshed every 10 rounds
```

**5. Stop / restart.** `pkill -f run-server.sh; pkill -f OperatorProfiler` stops it. Starting it again with the
same options continues where it stopped (finished configs are skipped, failed ones are not retried).

**6. Bring the data back** and train:
```bash
rsync -av user@vm:cost-model-server/Cost-Model/results-server/ Cost-Model/results-server/   # java/ spark/ postgres/
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
  - PostgreSQL profiler (`../postgres-profiler`), `sweep.py --platform postgres`, `run-server.sh --postgres URL`.
  - `validate_data.py`: Postgres checks; fixed: the Java checks (same thread, thread CPU <= wall) were skipped for
    data with the new JIT columns.

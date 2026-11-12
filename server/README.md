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

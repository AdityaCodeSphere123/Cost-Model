#!/usr/bin/env bash
# Spark cluster profiling on one large machine (e.g. a 64-core VM): standalone Spark clusters of 1 to 10
# executors, one cluster shape at a time, until a time limit.
#
#   ./run-cluster.sh --check                                  # ~5 min: 2-executor cluster, one config, self-test
#   nohup ./run-cluster.sh --hours 72 >/dev/null 2>&1 &       # the real run; safe to stop and restart
#
# One cluster = 1 master + N workers, all on this machine and bound to 127.0.0.1. Every worker has C cores and
# runs exactly one executor (its own JVM, pinned to its own C cores with taskset), so executors exchange shuffle
# data over TCP like separate machines do (over loopback, so without a physical network's bandwidth limit).
# The profiler JVM is the driver, pinned to the first --reserve cores together with the master.
#
# Correctness rules this script keeps:
#   - one cluster at a time (two clusters at once would share CPUs, caches and memory bandwidth);
#   - it refuses to start while run-server.sh or another profiler runs on this machine;
#   - every shape uses the same data configs and the same executor memory, so shapes can be compared directly;
#   - shapes take turns in chunks, so a run that stops early still covers every shape evenly;
#   - every config checks that it really got N executors x C cores (else it is retried, not recorded);
#   - spark.shuffle.readHostLocalDisk=false, so shuffle blocks of other executors count as network bytes.
#
# Default shapes on a machine with K cores: N = 1..10 executors with C = (K - reserve) / 10 cores each
# (64 cores, reserve 4: 1x6, 2x6, ..., 10x6). Choose others with --shapes, e.g. a fixed total of 24 cores
# split differently: --shapes "1x24 2x12 3x8 4x6 6x4 8x3 12x2".
#
# Output (default ../results-cluster/): spark-cluster/ (raw.csv, pairs.csv, operator_costs.csv, ...), lists/,
# cluster.log, logs/ (master and worker logs).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BUNDLE="$(cd "$REPO/.." && pwd)"

HOURS=72
CHUNK=20
WARMUP=2
REPS=5
SEED=1
COUNT=5000
OUT="$REPO/results-cluster"
CHECK=0
SHAPES=""
MAX_EXECUTORS=10
RESERVE=4
EXEC_MEM_GB=""
DRIVER_HEAP=8g
PORT=7077
PIN=1
ANALYZE_EVERY=3
CORES=""
MEM_GB=""
CPU_LIST=""     # --cpus: the logical CPUs this run may use (default 0..CORES-1)
SHARED=0        # --shared: other profiling lanes run on other CPUs of this machine (collect.sh)
GIVEN_LIST=""   # --list: a fixed config list instead of the random one (test.sh)
PYTHON="${PYTHON:-python3}"

usage() { sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; cat <<EOF

Options
  --hours H              time limit (default $HOURS)
  --shapes "NxC ..."     cluster shapes (default: 1..$MAX_EXECUTORS executors, equal cores each, see above)
  --max-executors N      largest cluster for the default shapes (default $MAX_EXECUTORS)
  --reserve N            cores kept for the driver, master and OS (default $RESERVE)
  --executor-memory-gb G heap of every executor, the same for all shapes (default: fits the largest shape in RAM, max 8)
  --driver-heap H        heap of the driver (profiler JVM) (default $DRIVER_HEAP)
  --chunk N              configs per shape before the next shape (default $CHUNK)
  --count N              data configs in the list (each runs on every shape) (default $COUNT)
  --warmup N / --reps N  warm-up / measured pairs per config (default $WARMUP / $REPS)
  --seed N               seed of the config list; keep it the same when restarting (default $SEED)
  --port P               master port (default $PORT; the web UI uses P+1000, both on 127.0.0.1 only)
  --no-pin               do not pin workers and driver to cores (taskset)
  --cores N / --mem-gb N plan for this many cores / GB RAM (default: this machine)
  --out DIR              output folder (default $OUT)
  --cpus LIST            logical CPUs to use, e.g. "0-31" or "0,32,1,33" (driver gets the first --reserve of them,
                         executor i the next C); default: 0 .. cores-1
  --shared               allow other profiling runs on the other CPUs of this machine (used by collect.sh)
  --list FILE            run this config list (lines DRIVERHEAP:G1:NxC|op|...) instead of a random one
  --check                only test that everything works
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hours) HOURS="$2"; shift 2 ;;
    --shapes) SHAPES="$2"; shift 2 ;;
    --max-executors) MAX_EXECUTORS="$2"; shift 2 ;;
    --reserve) RESERVE="$2"; shift 2 ;;
    --executor-memory-gb) EXEC_MEM_GB="$2"; shift 2 ;;
    --driver-heap) DRIVER_HEAP="$2"; shift 2 ;;
    --chunk) CHUNK="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --no-pin) PIN=0; shift ;;
    --cores) CORES="$2"; shift 2 ;;
    --mem-gb) MEM_GB="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --cpus) CPU_LIST="$2"; shift 2 ;;
    --shared) SHARED=1; shift ;;
    --list) GIVEN_LIST="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2 ;;
    --check) CHECK=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 2 ;;
  esac
done
UI_PORT=$((PORT + 1000))
MASTER_URL="spark://127.0.0.1:$PORT"

mkdir -p "$OUT/logs"
LOG="$OUT/cluster.log"
say() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }
fail() { say "ERROR: $*"; exit 1; }

# ---- one profiling job per machine ---------------------------------------------------------------
others=$(pgrep -f "run-server.sh|OperatorProfiler" 2>/dev/null | grep -v "^$$\$" || true)
(( SHARED )) || [[ -z "$others" ]] || fail "another profiling run is active on this machine (pids: $(echo $others)). Two runs at once
       share CPUs and disturb each other's timings: stop it first (pkill -f run-server.sh; pkill -f OperatorProfiler)."

# ---- machine and tools ---------------------------------------------------------------------------
# The CPUs, expanded to one number each ("0-3,8" -> 0 1 2 3 8).
if [[ -n "$CPU_LIST" ]]; then
  read -r -a CPUS_ARR <<<"$("$PYTHON" -c '
import sys
out = []
for part in sys.argv[1].split(","):
    a, _, b = part.partition("-")
    out += list(range(int(a), int(b or a) + 1))
print(" ".join(map(str, out)))' "$CPU_LIST")"
  CORES=${#CPUS_ARR[@]}
fi
if [[ -z "$CORES" ]]; then CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu); fi
[[ -n "$CPU_LIST" ]] || read -r -a CPUS_ARR <<<"$(seq -s ' ' 0 $((CORES - 1)))"
if [[ -z "$MEM_GB" ]]; then
  if [[ -r /proc/meminfo ]]; then MEM_GB=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
  else MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 )); fi
fi
if [[ -d "$BUNDLE/wayang/jars" ]]; then export WAYANG_HOME="$BUNDLE/wayang"; fi
case "$(uname -m)" in x86_64|amd64) ARCH=x64 ;; aarch64|arm64) ARCH=aarch64 ;; *) ARCH=other ;; esac
if [[ "$(uname -s)" == Linux && -x "$BUNDLE/jdk/linux-$ARCH/bin/java" && -z "${USE_SYSTEM_JAVA:-}" ]]; then
  export JAVA_HOME="$BUNDLE/jdk/linux-$ARCH"
fi
if [[ -z "${JAVA_HOME:-}" ]]; then  # no bundled JDK: the Java 17 that setup-vm.sh installed
  for d in /usr/lib/jvm/java-17-openjdk* /usr/lib/jvm/java-17 /usr/lib/jvm/temurin-17*; do
    [[ -x "$d/bin/javac" ]] && { export JAVA_HOME="$d"; break; }
  done
fi
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
command -v "$JAVA" >/dev/null || fail "java not found (install Java 17 or set JAVA_HOME)"
command -v "$PYTHON" >/dev/null || fail "python3 not found"
if (( PIN )) && ! command -v taskset >/dev/null; then
  say "WARNING: taskset not found (util-linux): executors are not pinned to their own cores. Each executor still"
  say "         sees only its C cores (-XX:ActiveProcessorCount), but the OS may move them around."
  PIN=0
fi

# The Spark jars: from the bundle, or the list the Spark profiler saved. Workers need a SPARK_HOME with jars/ in it
# to launch executors; one is made from links, so master, workers, executors and driver use the very same jars.
SPARK_HOME_DIR="$OUT/spark-home"
make_spark_home() {
  local jars=()
  if [[ -d "$BUNDLE/spark-jars" ]]; then
    export SPARK_JARS="$BUNDLE/spark-jars/*"
    jars=("$BUNDLE"/spark-jars/*.jar)
  elif [[ -s "$REPO/spark-profiler/build/spark-classpath.txt" ]]; then
    IFS=: read -r -a jars <"$REPO/spark-profiler/build/spark-classpath.txt"
  else
    # The Spark that setup-vm.sh installed: driver, master, workers and executors all use its jars.
    local sh
    for sh in "${SPARK_HOME:-}" /opt/spark; do
      if [[ -n "$sh" && -d "$sh/jars" ]]; then
        export SPARK_JARS="$sh/jars/*"
        jars=("$sh"/jars/*.jar)
        break
      fi
    done
    (( ${#jars[@]} > 0 )) || fail "Spark jars not found: run ./setup-vm.sh (installs Spark), or use the bundle"
  fi
  rm -rf "$SPARK_HOME_DIR"
  mkdir -p "$SPARK_HOME_DIR/jars" "$SPARK_HOME_DIR/conf"
  for j in "${jars[@]}"; do [[ -f "$j" ]] && ln -s "$j" "$SPARK_HOME_DIR/jars/"; done
  cat >"$SPARK_HOME_DIR/conf/log4j2.properties" <<'EOF'
rootLogger.level = warn
rootLogger.appenderRef.stderr.ref = console
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
EOF
}
make_spark_home
export SPARK_HOME="$SPARK_HOME_DIR" SPARK_SCALA_VERSION=2.12 SPARK_LOCAL_IP=127.0.0.1
OPENS=""
for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.util java.util.concurrent \
         java.util.concurrent.atomic sun.nio.ch sun.nio.cs sun.security.action sun.util.calendar; do
  OPENS="$OPENS --add-opens=java.base/$p=ALL-UNNAMED"
done
DAEMON_CP="$SPARK_HOME_DIR/conf:$SPARK_HOME_DIR/jars/*"

# ---- shapes and memory ---------------------------------------------------------------------------
if [[ -n "$GIVEN_LIST" ]]; then
  [[ -s "$GIVEN_LIST" ]] || fail "--list $GIVEN_LIST is empty or missing"
  SHAPES=$(cut -d'|' -f1 "$GIVEN_LIST" | cut -d: -f3 | awk '!seen[$0]++' | xargs)
  DRIVER_HEAP=$(head -1 "$GIVEN_LIST" | cut -d: -f1)
fi
USABLE=$(( CORES - RESERVE ))
(( USABLE >= 2 )) || fail "only $CORES cores: nothing left after --reserve $RESERVE"
if [[ -z "$SHAPES" ]]; then
  C=$(( USABLE / MAX_EXECUTORS ))
  if (( C < 1 )); then C=1; MAX_EXECUTORS=$USABLE; fi
  for n in $(seq 1 "$MAX_EXECUTORS"); do SHAPES="$SHAPES ${n}x$C"; done
  SHAPES="${SHAPES# }"
fi
NMAX=0
for s in $SHAPES; do
  [[ "$s" =~ ^([1-9][0-9]*)x([1-9][0-9]*)$ ]] || fail "bad shape '$s' (use NxC, e.g. 4x6)"
  n=${BASH_REMATCH[1]}; c=${BASH_REMATCH[2]}
  (( n * c <= USABLE )) || fail "shape $s needs $((n * c)) cores, but only $USABLE are free after --reserve $RESERVE"
  (( n > NMAX )) && NMAX=$n
done
(( CHECK )) && { NMAX=2; DRIVER_HEAP=2g; }   # the check runs a 2-executor cluster with a small driver
DRIVER_GB=${DRIVER_HEAP%[gG]}
if [[ -z "$EXEC_MEM_GB" ]]; then
  # 75% of RAM for JVMs: the driver, ~0.5 GB per worker daemon and the master, and per executor its heap plus
  # ~20% JVM overhead (metaspace, threads, Netty buffers). The rest stays for the OS and the page cache.
  EXEC_MEM_GB=$("$PYTHON" -c "print(max(0, min(8, int(($MEM_GB * 0.75 - $DRIVER_GB - 0.5 * ($NMAX + 1) - 2) / ($NMAX * 1.2)))))")
fi
(( EXEC_MEM_GB >= 1 )) || fail "not enough RAM ($MEM_GB GB) for $NMAX executors next to a ${DRIVER_HEAP} driver: use --driver-heap 4g or fewer --max-executors"

# ---- cluster control -----------------------------------------------------------------------------
pin() { (( PIN )) && echo "taskset -c $1"; }
cpu_slice() {  # first count -> "a,b,c": the logical CPUs number first .. first+count-1 of this run
  local i out=""
  for (( i = $1; i < $1 + $2; i++ )); do out="$out,${CPUS_ARR[$i]}"; done
  echo "${out#,}"
}
DRIVER_CPUS=$(cpu_slice 0 "$RESERVE")

master_status() {  # prints "<alive workers> <status> <active apps>", or nothing if the master is not reachable
  "$PYTHON" - "$UI_PORT" 2>/dev/null <<'PY'
import json, sys, urllib.request
d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/json/", timeout=5))
print(d.get("aliveworkers", 0), d.get("status", ""), len(d.get("activeapps", [])))
PY
}

start_master() {
  if [[ -n "$(master_status)" ]]; then fail "something already answers on 127.0.0.1:$UI_PORT (an old master?): stop it or use --port"; fi
  # shellcheck disable=SC2046
  nohup $(pin "$DRIVER_CPUS") "$JAVA" -Xmx512m $OPENS -cp "$DAEMON_CP" org.apache.spark.deploy.master.Master \
    --host 127.0.0.1 --port "$PORT" --webui-port "$UI_PORT" >"$OUT/logs/master.log" 2>&1 &
  echo $! >"$OUT/master.pid"
  for _ in $(seq 1 60); do
    [[ "$(master_status | awk '{print $2}')" == ALIVE ]] && return 0
    sleep 1
  done
  fail "the Spark master did not start (see $OUT/logs/master.log)"
}

WORKER_PIDS=()
start_workers() {  # N C
  local n="$1" c="$2" i lo
  WORKER_PIDS=()
  for i in $(seq 0 $((n - 1))); do
    lo=$(( RESERVE + i * c ))
    local cpus; cpus=$(cpu_slice "$lo" "$c")
    rm -rf "$OUT/work/w$i" "$OUT/local/w$i"; mkdir -p "$OUT/work/w$i" "$OUT/local/w$i"
    # shellcheck disable=SC2046
    SPARK_LOCAL_DIRS="$OUT/local/w$i" nohup $(pin "$cpus") "$JAVA" -Xmx256m $OPENS -cp "$DAEMON_CP" \
      org.apache.spark.deploy.worker.Worker --host 127.0.0.1 --cores "$c" --memory "${EXEC_MEM_GB}g" \
      --work-dir "$OUT/work/w$i" --webui-port 0 "$MASTER_URL" >"$OUT/logs/worker-$i.log" 2>&1 &
    WORKER_PIDS+=($!)
  done
  for _ in $(seq 1 90); do
    [[ "$(master_status | awk '{print $1}')" == "$n" ]] && return 0
    sleep 1
  done
  say "workers did not all register (wanted $n, master reports '$(master_status)'); see $OUT/logs/worker-*.log"
  return 1
}

stop_workers() {
  local p
  for p in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do kill "$p" 2>/dev/null; done   # a worker stops its executors
  for _ in $(seq 1 20); do
    local alive=0
    for p in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do kill -0 "$p" 2>/dev/null && alive=1; done
    (( alive )) || break
    sleep 0.5
  done
  for p in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do kill -9 "$p" 2>/dev/null; done
  # Orphan executors of this run only (their class path is this run's SPARK_HOME; other lanes have their own).
  pkill -f "$SPARK_HOME_DIR/.*CoarseGrainedExecutorBackend|CoarseGrainedExecutorBackend.*$SPARK_HOME_DIR/" 2>/dev/null
  WORKER_PIDS=()
  for _ in $(seq 1 30); do [[ "$(master_status | awk '{print $1}')" == 0 ]] && break; sleep 1; done
  rm -rf "$OUT/work" "$OUT/local"
}

cleanup() {
  stop_workers
  if [[ -s "$OUT/master.pid" ]]; then
    local mp; mp=$(cat "$OUT/master.pid")
    kill "$mp" 2>/dev/null
    for _ in $(seq 1 20); do kill -0 "$mp" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$mp" 2>/dev/null
  fi
  rm -f "$OUT/master.pid"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

run_chunk() {  # list-file out-dir hours tag [extra run-profiling args...]
  local list="$1" out="$2" hours="$3" tag="$4"; shift 4
  # shellcheck disable=SC2046
  JAVA_PREFIX="$(pin "$DRIVER_CPUS")" "$REPO/spark-profiler/run-profiling.sh" --config-list "$list" --hours "$hours" \
    --warmup "$WARMUP" --reps "$REPS" --out "$out" --data-dir "$OUT/data" --ephemeral-inputs --no-analyze \
    --tag "$tag" --timeout 1800 --master "$MASTER_URL" --executor-memory "${EXEC_MEM_GB}g" "$@" >>"$out-runner.log" 2>&1
}

machine_report() {
  local tpc numa steal
  tpc=$(lscpu 2>/dev/null | awk -F: '/Thread\(s\) per core/ {gsub(/ /, "", $2); print $2}')
  numa=$(lscpu 2>/dev/null | awk -F: '/NUMA node\(s\)/ {gsub(/ /, "", $2); print $2}')
  steal=$("$PYTHON" - 2>/dev/null <<'PY'
import time
def read():
    f = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    return sum(f), (f[7] if len(f) > 7 else 0)
t0, s0 = read(); time.sleep(5); t1, s1 = read()
print(f"{(s1 - s0) / max(1, t1 - t0):.2%}")
PY
)
  say "machine: $(uname -sr), $CORES cores (threads per core: ${tpc:-?}, NUMA nodes: ${numa:-?}), $MEM_GB GB RAM, steal over 5 s idle: ${steal:-n/a}"
  if [[ "${tpc:-1}" -gt 1 ]]; then
    say "NOTE: $tpc hardware threads per core: cores beyond $((CORES / tpc)) are hyperthreads of the first ones, so large"
    say "      shapes share physical cores. That is real for this machine; the data records it (cpu_model, cpus)."
  fi
}

say "shapes: $SHAPES | executor heap ${EXEC_MEM_GB}g | driver heap $DRIVER_HEAP | driver+master on cores $DRIVER_CPUS | pinning: $([[ $PIN == 1 ]] && echo taskset || echo off)"

# ---- --check -----------------------------------------------------------------------------------
if (( CHECK )); then
  C="$OUT/check"; rm -rf "$C"; mkdir -p "$C"
  machine_report
  cc=$(( USABLE >= 4 ? 2 : 1 ))
  shape="2x$cc"
  start_master
  start_workers 2 "$cc" || fail "could not start a 2-worker cluster"
  say "check: cluster $shape up; one shuffle config, then the CPU self-test (burn = fixed work, sleep = waiting)"
  echo "2g:G1:$shape|reduceby|text|200000|0|1000|0.5|8" >"$C/one.list"
  echo "2g:G1:$shape|burn|text|20000|0|1000|0.5|8" >"$C/burn.list"
  echo "2g:G1:$shape|sleep|text|2000|0|1000|0.5|8" >"$C/sleep.list"
  ok=1
  run_chunk "$C/one.list" "$C/spark" 1 check || ok=0
  PROFILER_ARGS="--work 20000" run_chunk "$C/burn.list" "$C/selftest" 1 selftest || ok=0
  PROFILER_ARGS="--work 200" run_chunk "$C/sleep.list" "$C/selftest" 1 selftest || ok=0
  (( ok )) || { tail -30 "$C"/*.log "$C"/spark/run.log 2>/dev/null; fail "a check run failed (logs in $C)"; }
  "$PYTHON" - "$C" <<'PY' | tee -a "$LOG"
import csv, statistics, sys
c = sys.argv[1]
ok = True
def flag(good, text):
    global ok
    ok &= bool(good)
    print(f"  {'ok  ' if good else 'FAIL'}  {text}")
rows = list(csv.DictReader(open(f"{c}/spark/raw.csv")))
op = [r for r in rows if r["plan"] == "operator"]
f = lambda r, k: float(r.get(k) or "nan")
flag(all(r["deploy_mode"] == "standalone-cluster" and r["executors"] == "2" for r in rows), "ran on the standalone cluster with 2 executors")
flag(all(r.get("wall_exec_ns") for r in rows), "wall_exec_ns measured (from the first task launch)")
flag(all(f(r, "executors_used") == 2 for r in op), f"both executors ran tasks (executors_used = {sorted({r['executors_used'] for r in op})})")
flag(all(f(r, "shuffle_read_remote_bytes") > 0 for r in op),
     f"shuffle between executors is counted as network: network_bytes = {[r['network_bytes'] for r in op]}")
flag(all(r.get("shuffle_host_local_disk") == "false" for r in rows), "spark.shuffle.readHostLocalDisk = false")
if rows[0].get("os_probe") == "linux-procfs":
    flag(all(r.get("executors_process_cpu_ns") not in ("", None) for r in rows), "executor CPU read from /proc (executors_process_cpu_ns)")
else:
    print("  skip  executors_process_cpu_ns (needs Linux: macOS does not let one process read another's CPU time)")
flag(all(f(r, "out_rows") == 1000 for r in op), "reduceby output = number of keys (1000)")
raw = list(csv.DictReader(open(f"{c}/selftest/raw.csv")))
def cost(name):
    by = {}
    for r in raw:
        if r["operator"] == name:
            by.setdefault(r["pair"], {})[r["plan"]] = float(r["task_cpu_ns"])
    return statistics.median(d["operator"] - d["baseline"] for d in by.values()), next(r for r in raw if r["operator"] == name)
burn, r = cost("burn")
expected = float(r["burn_calibrated_ns_per_row"]) * float(r["rows"])
flag(0.9 <= burn / expected <= 1.25, f"burn: task CPU {burn/1e6:.0f} ms vs expected {expected/1e6:.0f} ms = {burn/expected:.0%} (90-125%)")
sleep, r = cost("sleep")
waited = float(r["rows"]) * float(r["work"]) * 1000
flag(sleep / waited < 0.15, f"sleep: task CPU {sleep/1e6:.0f} ms during {waited/1e6:.0f} ms of waiting = {sleep/waited:.0%} (< 15%)")
print("CHECK PASSED" if ok else "CHECK FAILED")
PY
  "$PYTHON" "$HERE/validate_data.py" "$C/spark/raw.csv" | tee -a "$LOG"
  rm -rf "$C/data"
  exit 0
fi

# ---- the long run --------------------------------------------------------------------------------
machine_report
mkdir -p "$OUT/lists" "$OUT/spark-cluster"
if [[ -n "$GIVEN_LIST" && ! -s "$OUT/cluster.list" ]]; then
  cp "$GIVEN_LIST" "$OUT/cluster.list"
  for s in $SHAPES; do grep -F ":$s|" "$OUT/cluster.list" >"$OUT/lists/$s.list"; done
elif [[ ! -s "$OUT/cluster.list" ]]; then
  "$PYTHON" "$HERE/sweep.py" --platform spark-cluster --shapes "$SHAPES" --driver-heap "$DRIVER_HEAP" \
    --executor-memory-gb "$EXEC_MEM_GB" --count "$COUNT" --seed "$SEED" >"$OUT/cluster.list" 2>>"$LOG" || fail "sweep.py failed"
  for s in $SHAPES; do grep -F ":$s|" "$OUT/cluster.list" >"$OUT/lists/$s.list"; done
else
  # Restart: the shapes and memory must match the list, or old and new rows would not be comparable.
  for s in $SHAPES; do [[ -s "$OUT/lists/$s.list" ]] || fail "shape $s is not in the existing $OUT/cluster.list (use the same --shapes, or a new --out)"; done
  grep -q "^$DRIVER_HEAP:" "$OUT/cluster.list" || fail "--driver-heap differs from the existing list (use the same options, or a new --out)"
fi
EXEC_MEM_FILE="$OUT/.executor_memory_gb"
if [[ -s "$EXEC_MEM_FILE" && "$(cat "$EXEC_MEM_FILE")" != "$EXEC_MEM_GB" ]]; then
  fail "executor heap is ${EXEC_MEM_GB}g, but this run started with $(cat "$EXEC_MEM_FILE")g: pass --executor-memory-gb $(cat "$EXEC_MEM_FILE")"
fi
echo "$EXEC_MEM_GB" >"$EXEC_MEM_FILE"

start_master
say "start: $HOURS h, chunks of $CHUNK configs per shape, $WARMUP warm-up + $REPS pairs per config"
START=$(date +%s)
DEADLINE=$(( START + $("$PYTHON" -c "print(int(float('$HOURS') * 3600))") ))
ROUND=0
BAD_STARTS=0
read -r -a SHAPE_ARR <<<"$SHAPES"
while (( $(date +%s) < DEADLINE )); do
  ROUND=$((ROUND + 1))
  ran=0
  # Rotate the starting shape every round, so no shape always runs at the same point of a round.
  k=${#SHAPE_ARR[@]}
  for j in $(seq 0 $((k - 1))); do
    s=${SHAPE_ARR[$(( (j + ROUND - 1) % k ))]}
    left=$(( DEADLINE - $(date +%s) )); (( left > 0 )) || break
    chunk="$OUT/lists/$s.chunk"
    "$PYTHON" "$HERE/next_chunk.py" "$OUT/lists/$s.list" "$OUT/spark-cluster/.done_ids" "$OUT/spark-cluster/run.log" "$CHUNK" >"$chunk"
    [[ -s "$chunk" ]] || continue
    ran=1
    if ! start_workers "${s%x*}" "${s#*x}"; then
      stop_workers
      BAD_STARTS=$((BAD_STARTS + 1))
      (( BAD_STARTS < 5 )) || fail "workers failed to start 5 times; see $OUT/logs/"
      continue
    fi
    before=$( [[ -s "$OUT/spark-cluster/.done_ids" ]] && wc -l <"$OUT/spark-cluster/.done_ids" || echo 0 )
    run_chunk "$chunk" "$OUT/spark-cluster" "$("$PYTHON" -c "print(round($left / 3600, 4))")" cluster
    stop_workers
    after=$( [[ -s "$OUT/spark-cluster/.done_ids" ]] && wc -l <"$OUT/spark-cluster/.done_ids" || echo 0 )
    failed=$(grep -c "^FAILED " "$OUT/spark-cluster/run.log" 2>/dev/null); failed=${failed:-0}
    say "round $ROUND shape $s: +$((after - before)) configs (total $after done, $failed failed), $(( left / 3600 )) h left"
  done
  (( ran )) || { say "all configs in the lists are done"; break; }
  if (( ROUND % ANALYZE_EVERY == 0 )); then
    "$PYTHON" "$REPO/spark-profiler/analyze.py" "$OUT/spark-cluster/raw.csv" --out "$OUT/spark-cluster" --quiet >>"$LOG" 2>&1
    "$PYTHON" "$HERE/validate_data.py" "$OUT/spark-cluster/raw.csv" >"$OUT/validation.txt" 2>&1 \
      || say "WARNING: data validation found errors, see $OUT/validation.txt"
  fi
done

if [[ -s "$OUT/spark-cluster/raw.csv" ]]; then
  "$PYTHON" "$REPO/spark-profiler/analyze.py" "$OUT/spark-cluster/raw.csv" --out "$OUT/spark-cluster" --quiet 2>&1 | tee -a "$LOG"
  "$PYTHON" "$HERE/validate_data.py" "$OUT/spark-cluster/raw.csv" 2>&1 | tee "$OUT/validation.txt" | tee -a "$LOG"
fi
say "finished after $(( ($(date +%s) - START) / 3600 )) h. Data: $OUT/spark-cluster"

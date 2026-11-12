#!/usr/bin/env bash
# Long, unattended profiling run on a server: Java and Spark single-node, alternating, until a time limit.
#
#   ./run-server.sh --check                 # 2-minute test: tools, one tiny config per platform, columns
#   nohup ./run-server.sh --hours 84 >/dev/null 2>&1 &     # the real run (3.5 days); safe to restart
#
# What it does
#   1. makes one random config list per platform (sweep.py, fixed seeds, sized for this machine's cores/RAM)
#   2. repeatedly runs the next CHUNK unfinished configs of Java, then of Spark (so both grow evenly)
#   3. stops starting new configs at the time limit; analyzes the data every few rounds and at the end
#   Restarting continues where it stopped: finished configs are skipped, failed ones are not retried.
#
# Output (default ../results-server/): java/ and spark/ with raw.csv, pairs.csv, operator_costs.csv,
# baseline_costs.csv, run.log; java.list, spark.list; server.log.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BUNDLE="$(cd "$REPO/.." && pwd)"

HOURS=84
CHUNK=40
WARMUP=2
REPS=5
SEED=1
OUT="$REPO/results-server"
CHECK=0
ANALYZE_EVERY=10
CORES=""
MEM_GB=""
PYTHON="${PYTHON:-python3}"

usage() { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; cat <<EOF

Options
  --hours H        time limit (default $HOURS)
  --chunk N        configs per platform before switching (default $CHUNK)
  --warmup N       warm-up pairs per config (default $WARMUP)
  --reps N         measured pairs per config (default $REPS)
  --seed N         seed of the config lists (default $SEED); keep it the same when restarting
  --cores N        cores to plan for (default: all of this machine)
  --mem-gb N       RAM to plan for (default: all of this machine)
  --out DIR        output folder (default $OUT)
  --check          only test that everything works
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hours) HOURS="$2"; shift 2 ;;
    --chunk) CHUNK="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --cores) CORES="$2"; shift 2 ;;
    --mem-gb) MEM_GB="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --check) CHECK=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 2 ;;
  esac
done

mkdir -p "$OUT"
LOG="$OUT/server.log"
say() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

# ---- machine and tools -------------------------------------------------------------------------
if [[ -z "$CORES" ]]; then CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu); fi
if [[ -z "$MEM_GB" ]]; then
  if [[ -r /proc/meminfo ]]; then MEM_GB=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
  else MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 )); fi
fi
# The bundle (make-bundle.sh) ships Wayang and Spark next to Cost-Model/; in the repo the usual paths are used.
if [[ -d "$BUNDLE/wayang/jars" ]]; then export WAYANG_HOME="$BUNDLE/wayang"; fi
if [[ -d "$BUNDLE/spark-jars" ]]; then export SPARK_JARS="$BUNDLE/spark-jars/*"; fi

fail() { say "ERROR: $*"; exit 1; }
# Prefer the JDK shipped in the bundle (make-bundle.sh --with-jdk): the same Java everywhere, nothing to install.
case "$(uname -m)" in x86_64|amd64) ARCH=x64 ;; aarch64|arm64) ARCH=aarch64 ;; *) ARCH=other ;; esac
if [[ "$(uname -s)" == Linux && -x "$BUNDLE/jdk/linux-$ARCH/bin/java" && -z "${USE_SYSTEM_JAVA:-}" ]]; then
  export JAVA_HOME="$BUNDLE/jdk/linux-$ARCH"
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
command -v "$JAVA" >/dev/null || fail "java not found (install Java 17 or set JAVA_HOME)"
JV=$("$JAVA" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')
(( JV >= 17 )) || say "WARNING: Java $JV found; Java 17 is what the data so far was measured with"
command -v "${JAVA_HOME:+$JAVA_HOME/bin/}javac" >/dev/null || fail "javac not found (install a JDK, not only a JRE)"
command -v "$PYTHON" >/dev/null || fail "python3 not found"
command -v perl >/dev/null || command -v timeout >/dev/null || fail "neither perl nor timeout found (needed for per-config time limits)"
FREE_GB=$(df -Pk "$OUT" | awk 'NR==2 {print int($4/1024/1024)}')
(( FREE_GB >= 30 )) || say "WARNING: only ${FREE_GB} GB free in $OUT (30+ GB recommended: inputs up to 3M rows, Spark shuffle files)"
say "machine: $(uname -sr), $CORES cores, $MEM_GB GB RAM, ${FREE_GB} GB free disk, Java $JV"
say "Java: $("$JAVA" -version 2>&1 | head -2 | tail -1)  (JAVA_HOME=${JAVA_HOME:-system})"
say "WAYANG_HOME=${WAYANG_HOME:-<repo default>}  SPARK_JARS=${SPARK_JARS:-<from spark-profiler/build>}"

machine_report() {  # what kind of machine this is; things that disturb timings
  [[ "$(uname -s)" == Linux ]] || { say "machine report: not Linux, skipped"; return; }
  local virt gov rapl steal
  virt=$(systemd-detect-virt 2>/dev/null || echo unknown)
  gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "not exposed (usual in VMs)")
  if [[ -r /sys/class/powercap/intel-rapl:0/energy_uj ]]; then rapl="readable"; else rapl="NOT readable (no energy data; sudo chmod a+r /sys/class/powercap/intel-rapl:0/energy_uj)"; fi
  # Steal: share of this machine's CPU time the hypervisor gave to other VMs over 5 idle seconds.
  steal=$("$PYTHON" - <<'PY'
import time
def read():
    f = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    return sum(f), (f[7] if len(f) > 7 else 0)
t0, s0 = read(); time.sleep(5); t1, s1 = read()
print(f"{(s1 - s0) / max(1, t1 - t0):.2%}")
PY
)
  say "virtualization: $virt | CPU frequency policy: $gov | energy (RAPL): $rapl | steal over 5 s idle: $steal"
  say "CPU: $(lscpu 2>/dev/null | grep -m1 'Model name' | cut -d: -f2 | sed 's/^ *//')"
}

run_platform() {  # platform config-list hours extra-args...
  local p="$1" list="$2" hours="$3"; shift 3
  "$REPO/$p-profiler/run-profiling.sh" --config-list "$list" --hours "$hours" --warmup "$WARMUP" --reps "$REPS" \
    --out "$OUT/$p" --data-dir "$OUT/data" --ephemeral-inputs --no-analyze --tag server --timeout 1800 "$@" \
    >>"$OUT/$p-runner.log" 2>&1
}

# ---- --check: machine report, one small config, measurement self-test, data validation ------------
if (( CHECK )); then
  C="$OUT/check"; rm -rf "$C"; mkdir -p "$C"
  machine_report
  echo "2g:G1:2|sort|text|10000|0|1000|0.5|8" >"$C/one.list"
  echo "2g:G1:2|burn|text|20000|0|1000|0.5|8" >"$C/burn.list"
  echo "2g:G1:2|sleep|text|2000|0|1000|0.5|8" >"$C/sleep.list"
  for p in java spark; do
    say "check: $p (compiles the profiler, then one small config)"
    "$REPO/$p-profiler/run-profiling.sh" --config-list "$C/one.list" --warmup 0 --reps 1 --out "$C/$p" \
      --data-dir "$C/data" --ephemeral-inputs --tag check >"$C/$p.log" 2>&1 || { tail -20 "$C/$p.log"; fail "$p check failed (log: $C/$p.log)"; }
    say "check: $p self-test (fixed CPU work, then pure waiting)"
    for t in burn:20000 sleep:200; do  # burn: 20k rounds per row; sleep: 200 us per row
      PROFILER_ARGS="--work ${t#*:}" "$REPO/$p-profiler/run-profiling.sh" --config-list "$C/${t%%:*}.list" --warmup 1 --reps 3 \
        --out "$C/$p-selftest" --data-dir "$C/data" --ephemeral-inputs --tag selftest >>"$C/$p.log" 2>&1 \
        || { tail -20 "$C/$p.log"; fail "$p self-test failed (log: $C/$p.log)"; }
    done
  done
  "$PYTHON" - "$C" <<'PY' | tee -a "$LOG"
import csv, statistics, sys
c = sys.argv[1]
ok = True
for p, need in (("java", ["thread_cpu_exec_ns", "mem_held_bytes", "wayang_est_time_ms_gmean", "os_probe"]),
                ("spark", ["task_cpu_ns", "spark_listener_drained", "shuffle_write_bytes", "wayang_est_time_ms_gmean", "os_probe"])):
    rows = list(csv.DictReader(open(f"{c}/{p}/raw.csv")))
    for col in need:
        vals = {r.get(col, "") for r in rows}
        good = all(v not in ("", None) for v in vals) and not (col == "spark_listener_drained" and "0" in vals)
        ok &= good
        print(f"  {p:5} column {col:28} {'ok' if good else 'MISSING'}  {sorted(vals)[:2]}")
    # Self-test: operator - baseline CPU of each pair, against the known truth.
    cpu = "task_cpu_ns" if p == "spark" else "thread_cpu_exec_ns"
    raw = list(csv.DictReader(open(f"{c}/{p}-selftest/raw.csv")))
    def cost(op):
        runs = [r for r in raw if r["operator"] == op]
        by = {}
        for r in runs:
            by.setdefault(r["pair"], {})[r["plan"]] = float(r[cpu])
        return statistics.median(d["operator"] - d["baseline"] for d in by.values()), runs[0]
    burn, r = cost("burn")
    expected = float(r["burn_calibrated_ns_per_row"]) * float(r["rows"])
    ratio = burn / expected
    good = 0.9 <= ratio <= 1.25
    ok &= good
    print(f"  {p:5} self-test burn: CPU {burn/1e6:.0f} ms vs expected {expected/1e6:.0f} ms = {ratio:.0%}  {'ok' if good else 'FAIL (should be 90-125%)'}")
    sleep, r = cost("sleep")
    waited = float(r["rows"]) * float(r["work"]) * 1000
    share = sleep / waited
    good = share < 0.15
    ok &= good
    print(f"  {p:5} self-test sleep: CPU {sleep/1e6:.0f} ms during {waited/1e6:.0f} ms of waiting = {share:.0%}  {'ok' if good else 'FAIL (should be < 15%)'}")
print("CHECK PASSED" if ok else "CHECK FAILED")
PY
  "$PYTHON" "$HERE/validate_data.py" "$C/java/raw.csv" "$C/spark/raw.csv" | tee -a "$LOG"
  rm -rf "$C/data"
  exit 0
fi

# ---- the long run ------------------------------------------------------------------------------
for p in java spark; do
  if [[ ! -s "$OUT/$p.list" ]]; then
    seed=$(( SEED * 2 )); [[ $p == spark ]] && seed=$(( SEED * 2 + 1 ))
    "$PYTHON" "$HERE/sweep.py" --platform "$p" --cores "$CORES" --mem-gb "$MEM_GB" --count 80000 --seed "$seed" \
      >"$OUT/$p.list" 2>>"$LOG"
  fi
  mkdir -p "$OUT/$p"
done
say "start: $HOURS h, chunks of $CHUNK configs per platform, $WARMUP warm-up + $REPS pairs per config"

START=$(date +%s)
DEADLINE=$(( START + $("$PYTHON" -c "print(int(float('$HOURS') * 3600))") ))
ROUND=0
while (( $(date +%s) < DEADLINE )); do
  ROUND=$((ROUND + 1))
  ran=0
  for p in java spark; do
    left=$(( DEADLINE - $(date +%s) )); (( left > 0 )) || break
    chunk="$OUT/$p.chunk"
    "$PYTHON" "$HERE/next_chunk.py" "$OUT/$p.list" "$OUT/$p/.done_ids" "$OUT/$p/run.log" "$CHUNK" >"$chunk"
    [[ -s "$chunk" ]] || continue
    hours=$("$PYTHON" -c "print(round($left / 3600, 4))")
    before=$( [[ -s "$OUT/$p/.done_ids" ]] && wc -l <"$OUT/$p/.done_ids" || echo 0 )
    run_platform "$p" "$chunk" "$hours"
    after=$( [[ -s "$OUT/$p/.done_ids" ]] && wc -l <"$OUT/$p/.done_ids" || echo 0 )
    failed=$(grep -c "^FAILED " "$OUT/$p/run.log" 2>/dev/null); failed=${failed:-0}
    say "round $ROUND $p: +$((after - before)) configs (total $after done, $failed failed), $(( left / 3600 )) h left"
    ran=1
  done
  (( ran )) || { say "all configs in the lists are done"; break; }
  if (( ROUND % ANALYZE_EVERY == 0 )); then
    for p in java spark; do "$PYTHON" "$REPO/$p-profiler/analyze.py" "$OUT/$p/raw.csv" --out "$OUT/$p" --quiet >>"$LOG" 2>&1; done
    "$PYTHON" "$HERE/validate_data.py" "$OUT/java/raw.csv" "$OUT/spark/raw.csv" >"$OUT/validation.txt" 2>&1 \
      || say "WARNING: data validation found errors, see $OUT/validation.txt"
  fi
done

for p in java spark; do
  [[ -s "$OUT/$p/raw.csv" ]] && "$PYTHON" "$REPO/$p-profiler/analyze.py" "$OUT/$p/raw.csv" --out "$OUT/$p" --quiet 2>&1 | tee -a "$LOG"
done
"$PYTHON" "$HERE/validate_data.py" "$OUT/java/raw.csv" "$OUT/spark/raw.csv" 2>&1 | tee "$OUT/validation.txt" | tee -a "$LOG"
say "finished after $(( ($(date +%s) - START) / 3600 )) h. Data: $OUT/java and $OUT/spark"

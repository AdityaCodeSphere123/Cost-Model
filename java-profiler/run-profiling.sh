#!/usr/bin/env bash
# Collects real resource usage of Wayang Java operators, for training the ML cost model.
#
# For every (resource setting x operator x input size x parameter) it starts one JVM that runs
#   baseline : source -> parse -> sink
#   operator : source -> parse -> OPERATOR -> sink
# in alternating pairs after warm-up runs and appends every run to raw.csv. analyze.py then subtracts
# the baseline from the operator run of each pair to get the operator's own cost.
#
# Examples
#   ./run-profiling.sh --dry-run                       # show what would run
#   ./run-profiling.sh --profile quick                 # ~10 min smoke test
#   ./run-profiling.sh                                 # default grid
#   ./run-profiling.sh --profile full                  # large grid, several hours
#   ./run-profiling.sh --ops "filter sort join" --rows "100000 1000000"
#   ./run-profiling.sh --resources "4g:G1:all 1g:Parallel:2 8g:Serial:1"
#
# A resource setting is HEAP:GC:CPUS
#   HEAP  JVM heap (-Xms and -Xmx), e.g. 512m, 2g, 8g
#   GC    G1 | Parallel | Serial | Z   (garbage collector)
#   CPUS  number of CPUs the JVM may use (-XX:ActiveProcessorCount), or "all"
# The Java platform runs operators on one thread, so CPUS mostly changes how many GC and JIT threads run.
#
# The run can be stopped and restarted: configs already in raw.csv are skipped (use --no-resume to redo).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

# ---------------------------------------------------------------------------------------------
# Defaults (override with flags or environment variables)
# ---------------------------------------------------------------------------------------------
DEFAULT_DIST="$ROOT/wayang/wayang-assembly/target/apache-wayang-assembly-1.1.2-SNAPSHOT-dist/wayang-1.1.2-SNAPSHOT"
WAYANG_HOME="${WAYANG_HOME:-$DEFAULT_DIST}"
# WAYANG_HOME must be the built distribution (it has jars/ and libs/), not the source folder.
[[ -d "$WAYANG_HOME/jars" ]] || WAYANG_HOME="$DEFAULT_DIST"
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
  JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
PYTHON="${PYTHON:-python3}"

PROFILE="default"
OUT_DIR="$HERE/results"
DATA_DIR="$HERE/data"
OPS=""
SOURCES=""
ROWS=""
RESOURCES=""
WARMUP=""
REPS=""
TIMEOUT_S=1800          # per JVM run
MAX_OUT_ROWS=20000000   # skip join/cartesian configs that would produce more output rows than this
DRY_RUN=0
RESUME=1
COLD=0
TAG=""
JAVA_EXTRA="${JAVA_EXTRA:-}"

usage() {
  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
  cat <<EOF

Options
  --profile quick|default|full   size of the grid (default: default)
  --ops "a b c"                  operators to run (default: all)
                                 map filter flatmap reduceby groupby globalreduce count distinct sort textsink join union cartesian
  --sources "text collection"    text = read a CSV file (includes disk I/O); collection = in-memory input
  --rows "N N N"                 input sizes
  --resources "HEAP:GC:CPUS ..." JVM resource settings
  --warmup N / --reps N          warm-up runs (not recorded) / measured pairs per config
  --out DIR                      where raw.csv, run.log and the analysis go (default: results/)
  --data-dir DIR                 where generated input files go (default: data/)
  --timeout SEC                  stop one JVM run after SEC seconds (default: $TIMEOUT_S)
  --cold                         empty the OS file cache before each measured run (runs "sudo -n purge"
                                 on macOS or drops caches on Linux; needs passwordless sudo)
  --tag TEXT                     free text stored with every row
  --no-resume                    run configs again even if raw.csv already has them
  --dry-run                      only print the configs
Environment: WAYANG_HOME, JAVA_HOME, JAVA_EXTRA (more JVM flags), PYTHON
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --ops) OPS="$2"; shift 2 ;;
    --sources) SOURCES="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --resources) RESOURCES="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --timeout) TIMEOUT_S="$2"; shift 2 ;;
    --cold) COLD=1; shift ;;
    --tag) TAG="$2"; shift 2 ;;
    --no-resume) RESUME=0; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------------------------
# Grids. Selectivities are written the way Java prints doubles (1.0, not 1) so resume can match them.
# ---------------------------------------------------------------------------------------------
ALL_OPS="map filter flatmap reduceby groupby globalreduce count distinct sort textsink join union cartesian"
case "$PROFILE" in
  quick)
    D_ROWS="10000 100000"
    D_RES="2g:G1:all"
    D_SOURCES="text"
    D_WARMUP=2; D_REPS=3
    SELS="0.1 0.5 1.0"
    KEYS="10 1000"
    WORDS="8"
    FLATMAP_WORDS="2 8"
    CART_LEFT="1000"; CART_RIGHT="10 100"
    JOIN_FANOUT="1"
    ;;
  default)
    D_ROWS="10000 100000 1000000"
    D_RES="4g:G1:all"
    D_SOURCES="text"
    D_WARMUP=3; D_REPS=7
    SELS="0.01 0.1 0.5 0.9 1.0"
    KEYS="10 1000 100000"
    WORDS="8"
    FLATMAP_WORDS="2 8 32"
    CART_LEFT="1000 10000"; CART_RIGHT="10 100 1000"
    JOIN_FANOUT="1 10"
    ;;
  full)
    D_ROWS="10000 50000 100000 500000 1000000 3000000"
    D_RES="4g:G1:all 1g:G1:all 8g:Parallel:all 4g:Serial:2 4g:G1:1"
    D_SOURCES="text collection"
    D_WARMUP=3; D_REPS=9
    SELS="0.01 0.05 0.1 0.25 0.5 0.75 0.9 1.0"
    KEYS="1 10 100 1000 10000 100000 1000000"
    WORDS="2 8 32"
    FLATMAP_WORDS="1 2 4 8 16 32 64"
    CART_LEFT="1000 10000 100000"; CART_RIGHT="10 100 1000"
    JOIN_FANOUT="1 2 10 100"
    ;;
  *) echo "Unknown profile: $PROFILE (use quick, default or full)"; exit 2 ;;
esac
OPS="${OPS:-$ALL_OPS}"
ROWS="${ROWS:-$D_ROWS}"
RESOURCES="${RESOURCES:-$D_RES}"
SOURCES="${SOURCES:-$D_SOURCES}"
WARMUP="${WARMUP:-$D_WARMUP}"
REPS="${REPS:-$D_REPS}"

RAW="$OUT_DIR/raw.csv"
LOG="$OUT_DIR/run.log"
BUILD="$HERE/build"
OS="$(uname -s)"

# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------
say() { echo "[$(date +%H:%M:%S)] $*"; }

heap_bytes() { # 512m / 4g -> bytes
  local v="${1%[gGmMkK]}" u="${1: -1}"
  case "$u" in g|G) echo $((v * 1024 * 1024 * 1024)) ;; m|M) echo $((v * 1024 * 1024)) ;;
               k|K) echo $((v * 1024)) ;; *) echo "$1" ;; esac
}

resource_label() { # 4g:G1:all -> heap4g-G1-cpuall
  IFS=: read -r h g c <<<"$1"
  echo "heap${h}-${g}-cpu${c}"
}

resource_flags() { # 4g:G1:all -> JVM flags
  IFS=: read -r h g c <<<"$1"
  local f="-Xms$h -Xmx$h"
  case "$g" in
    G1) f="$f -XX:+UseG1GC" ;;
    Parallel) f="$f -XX:+UseParallelGC" ;;
    Serial) f="$f -XX:+UseSerialGC" ;;
    Z) f="$f -XX:+UseZGC" ;;
    *) echo "Unknown GC '$g' in resource '$1'" >&2; return 1 ;;
  esac
  [[ "$c" != "all" ]] && f="$f -XX:ActiveProcessorCount=$c"
  echo "$f"
}

# Must match Cfg.configId() in JavaOperatorProfiler.java.
config_id() { # resource op source rows right_rows keys sel words
  local s="-"; [[ "$2" == "filter" ]] && s="$7"
  local rr=0; [[ "$2" == "join" || "$2" == "union" || "$2" == "cartesian" ]] && rr="$5"
  echo "$1|$2|$3|r$4|rr$rr|k$6|s$s|w$8"
}

# Rough memory need of operators that hold their whole input, used to skip runs that cannot fit the heap.
fits_heap() { # op rows right_rows words heap_bytes
  local per=$((120 + $4 * 14)) need=0
  case "$1" in
    sort|groupby|globalreduce) need=$(( $2 * per * 3 )) ;;
    join|cartesian) need=$(( $3 * per * 3 )) ;;
    *) need=0 ;;
  esac
  (( need < $5 * 6 / 10 ))
}

cpu_model() {
  if [[ "$OS" == "Darwin" ]]; then sysctl -n machdep.cpu.brand_string 2>/dev/null
  else grep -m1 "model name" /proc/cpuinfo 2>/dev/null | cut -d: -f2 | sed 's/^ *//'; fi
}

cold_cmd() {
  if [[ "$OS" == "Darwin" ]]; then echo "sudo -n purge"
  else echo "sync && echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null"; fi
}

# Runs a command with a time limit (macOS has no `timeout`).
with_timeout() {
  local secs="$1"; shift
  perl -e 'my $s = shift; my $pid = fork(); if ($pid == 0) { exec @ARGV or exit 127 }
           local $SIG{ALRM} = sub { kill "TERM", $pid; sleep 2; kill "KILL", $pid; exit 124 };
           alarm $s; waitpid($pid, 0); exit($? >> 8)' "$secs" "$@"
}

# ---------------------------------------------------------------------------------------------
# Build the config list
# ---------------------------------------------------------------------------------------------
# Each entry: resource|op|source|rows|right_rows|keys|sel|words
CONFIGS=()
SKIPPED=0
add() { CONFIGS+=("$*"); }

for res in $RESOURCES; do
  hb=$(heap_bytes "${res%%:*}")
  for src in $SOURCES; do
    for op in $OPS; do
      for n in $ROWS; do
        case "$op" in
          filter)
            for s in $SELS; do add "$res|$op|$src|$n|0|1000|$s|8"; done ;;
          flatmap)
            for w in $FLATMAP_WORDS; do add "$res|$op|$src|$n|0|1000|0.5|$w"; done ;;
          reduceby|groupby|distinct)
            for k in $KEYS; do
              (( k > n )) && continue
              if fits_heap "$op" "$n" 0 8 "$hb"; then add "$res|$op|$src|$n|0|$k|0.5|8"; else SKIPPED=$((SKIPPED+1)); fi
            done ;;
          join)
            # keys = rows / fanout, so each left row matches about `fanout` right rows.
            for f in $JOIN_FANOUT; do
              k=$(( n / f )); (( k < 1 )) && k=1
              out=$(( n * n / k ))
              if (( out > MAX_OUT_ROWS )) || ! fits_heap join "$n" "$n" 8 "$hb"; then SKIPPED=$((SKIPPED+1)); continue; fi
              add "$res|$op|$src|$n|$n|$k|0.5|8"
            done ;;
          union)
            add "$res|$op|$src|$n|$n|1000|0.5|8" ;;
          cartesian)
            ;; # own grid below
          *)
            for w in $WORDS; do
              if fits_heap "$op" "$n" 0 "$w" "$hb"; then add "$res|$op|$src|$n|0|1000|0.5|$w"; else SKIPPED=$((SKIPPED+1)); fi
            done ;;
        esac
      done
      if [[ "$op" == "cartesian" ]]; then
        for l in $CART_LEFT; do for r in $CART_RIGHT; do
          if (( l * r > MAX_OUT_ROWS )); then SKIPPED=$((SKIPPED+1)); continue; fi
          add "$res|$op|$src|$l|$r|1000|0.5|8"
        done; done
      fi
    done
  done
done

TOTAL=${#CONFIGS[@]}
(( TOTAL > 0 )) || { echo "No configs to run (check --ops / --rows)."; exit 1; }
say "profile=$PROFILE  configs=$TOTAL  skipped(too big)=$SKIPPED  warmup=$WARMUP reps=$REPS"
say "resources: $RESOURCES"
say "sources:   $SOURCES"
say "rows:      $ROWS"
say "operators: $OPS"

if (( DRY_RUN )); then
  printf '%s\n' "${CONFIGS[@]}" | column -t -s '|' -N "RESOURCE,OP,SOURCE,ROWS,RIGHT_ROWS,KEYS,SEL,WORDS" 2>/dev/null \
    || printf '%s\n' "${CONFIGS[@]}"
  exit 0
fi

# ---------------------------------------------------------------------------------------------
# Checks and build
# ---------------------------------------------------------------------------------------------
[[ -d "$WAYANG_HOME/jars" ]] || { echo "Wayang distribution not found at $WAYANG_HOME"
  echo "Build it: cd $ROOT/wayang && ./mvnw clean package -pl :wayang-assembly -Pdistribution -DskipTests"; exit 1; }
command -v "$JAVA" >/dev/null || { echo "java not found (set JAVA_HOME)"; exit 1; }
JV=$("$JAVA" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')
(( JV >= 14 )) || { echo "Java 14 or newer is needed (found $JV)"; exit 1; }

mkdir -p "$OUT_DIR" "$DATA_DIR" "$BUILD"
CP="$BUILD:$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*"

if [[ ! -f "$BUILD/wayangprof/JavaOperatorProfiler.class" || "$HERE/src/wayangprof/JavaOperatorProfiler.java" -nt "$BUILD/wayangprof/JavaOperatorProfiler.class" ]]; then
  say "compiling the profiler"
  "$JAVAC" -nowarn -d "$BUILD" -cp "$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*" "$HERE/src/wayangprof/JavaOperatorProfiler.java" || exit 1
fi

PIDRUSAGE=""
if [[ "$OS" == "Darwin" ]]; then
  if [[ ! -x "$HERE/native/pidrusage" || "$HERE/native/pidrusage.c" -nt "$HERE/native/pidrusage" ]]; then
    say "compiling the macOS counter helper"
    cc -O2 -o "$HERE/native/pidrusage" "$HERE/native/pidrusage.c" || exit 1
  fi
  PIDRUSAGE="$HERE/native/pidrusage"
  if pmset -g batt 2>/dev/null | grep -q "Battery Power"; then
    say "WARNING: running on battery. CPU speed and energy differ from mains power; plug in for consistent data."
  fi
fi

COLD_ARGS=()
if (( COLD )); then
  if sudo -n true 2>/dev/null; then COLD_ARGS=(--cold-cmd "$(cold_cmd)")
  else say "WARNING: --cold needs passwordless sudo; running with a warm file cache instead."; fi
fi

CPU_MODEL="$(cpu_model)"
{
  echo "==== $(date)  profile=$PROFILE  configs=$TOTAL"
  echo "host=$(hostname)  os=$(uname -sr)  cpu=$CPU_MODEL"
  "$JAVA" -version 2>&1
} >>"$LOG"

# ---------------------------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------------------------
START=$(date +%s)
DONE=0; FAILED=0; RESUMED=0
for entry in "${CONFIGS[@]}"; do
  IFS='|' read -r res op src n rr k s w <<<"$entry"
  DONE=$((DONE + 1))
  label=$(resource_label "$res")
  id=$(config_id "$label" "$op" "$src" "$n" "$rr" "$k" "$s" "$w")

  if (( RESUME )) && [[ -f "$RAW" ]] && grep -qF ",$id," "$RAW"; then
    RESUMED=$((RESUMED + 1))
    continue
  fi

  elapsed=$(( $(date +%s) - START ))
  ran=$(( DONE - RESUMED - 1 ))
  eta="?"; (( ran > 0 )) && eta="$(( elapsed * (TOTAL - DONE + 1) / ran / 60 ))min"
  say "[$DONE/$TOTAL] $id   (eta $eta)"

  flags=$(resource_flags "$res") || exit 2
  right=$n; (( rr > 0 )) && right=$rr
  # shellcheck disable=SC2086
  with_timeout "$TIMEOUT_S" "$JAVA" $flags $JAVA_EXTRA \
    -Dlog4j.configurationFile="file://$HERE/log4j2-quiet.xml" \
    -Dwayangprof.cpu="$CPU_MODEL" \
    -cp "$CP" wayangprof.JavaOperatorProfiler \
    --op "$op" --source "$src" --rows "$n" --right-rows "$right" --keys "$k" --sel "$s" --words "$w" \
    --warmup "$WARMUP" --reps "$REPS" \
    --data-dir "$DATA_DIR" --out "$RAW" --tmp-dir "$OUT_DIR/tmp" \
    ${PIDRUSAGE:+--pidrusage "$PIDRUSAGE"} ${COLD_ARGS[@]+"${COLD_ARGS[@]}"} \
    --resource "$label" --tag "$TAG" \
    >>"$LOG" 2>&1
  rc=$?
  if (( rc != 0 )); then
    FAILED=$((FAILED + 1))
    why="exit $rc"; (( rc == 124 )) && why="timeout after ${TIMEOUT_S}s"
    grep -q "OutOfMemoryError" <(tail -50 "$LOG") && why="out of memory"
    say "   FAILED ($why), see $LOG"
    echo "FAILED $id ($why)" >>"$LOG"
  fi
done

say "finished in $(( ($(date +%s) - START) / 60 )) min: $((DONE - RESUMED - FAILED)) ok, $FAILED failed, $RESUMED already done"
rm -rf "$OUT_DIR/tmp"

# ---------------------------------------------------------------------------------------------
# Subtract baselines
# ---------------------------------------------------------------------------------------------
if [[ -f "$RAW" ]]; then
  "$PYTHON" "$HERE/analyze.py" "$RAW" --out "$OUT_DIR"
fi

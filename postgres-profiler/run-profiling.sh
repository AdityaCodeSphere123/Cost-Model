#!/usr/bin/env bash
# Collects real resource usage of Wayang's PostgreSQL operators, for training the ML cost model.
#
# For every (resource setting x operator x input size x parameter) it starts one JVM that runs
#   baseline : table source -> (SqlToStream) -> Java sink               SELECT * FROM t
#   operator : table source -> OPERATOR -> (SqlToStream) -> Java sink    e.g. SELECT * FROM t WHERE key < 500
# in alternating pairs after warm-up runs and appends every run to raw.csv. analyze.py then subtracts the
# baseline from the operator run of each pair. Input tables are loaded once from the same generated data as the
# Java and Spark profilers (same rows), and reused.
#
# Needs a PostgreSQL server (ideally on this machine, used only for profiling: then the database CPU is measured
# too). ./start-local-postgres.sh starts such a server. Connection: --url / --user, or PGURL / PGUSER / PGPASSWORD.
#
# Examples
#   ./start-local-postgres.sh                          # dedicated server on 127.0.0.1:5433 (once)
#   ./run-profiling.sh --dry-run                       # show what would run
#   ./run-profiling.sh --profile quick                 # smoke test
#   ./run-profiling.sh                                 # default grid
#   ./run-profiling.sh --ops "filter join" --rows "100000 1000000" --resources "4g:G1:p0 4g:G1:p2"
#
# A resource setting is HEAP:GC:pN[i]
#   HEAP   JVM heap of the Java side (-Xms and -Xmx), e.g. 4g
#   GC     G1 | Parallel | Serial | Z
#   pN     max_parallel_workers_per_gather = N for every Postgres session (0 = no parallel query)
#   i      (optional) a B-tree index on the key column of the input tables (filter and join can use it)
#
# The run can be stopped and restarted: configs already in raw.csv are skipped (use --no-resume to redo).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/measure-gate.sh
source "$HERE/../common/measure-gate.sh"  # waits for mains power and for other profilers before each config
ROOT="$(cd "$HERE/../.." && pwd)"

WAYANG_SRC="${WAYANG_SRC:-$ROOT/wayang}"
DEFAULT_DIST="$WAYANG_SRC/wayang-assembly/target/apache-wayang-assembly-1.1.2-SNAPSHOT-dist/wayang-1.1.2-SNAPSHOT"
WAYANG_HOME="${WAYANG_HOME:-$DEFAULT_DIST}"
[[ -d "$WAYANG_HOME/jars" ]] || WAYANG_HOME="$DEFAULT_DIST"
# In the server bundle (make-bundle.sh) Wayang and a Java 17 JDK sit next to Cost-Model/.
[[ ! -d "$WAYANG_HOME/jars" && -d "$ROOT/wayang/jars" ]] && WAYANG_HOME="$ROOT/wayang"
case "$(uname -m)" in x86_64|amd64) _ARCH=x64 ;; aarch64|arm64) _ARCH=aarch64 ;; *) _ARCH=other ;; esac
if [[ -z "${JAVA_HOME:-}" && "$(uname -s)" == Linux && -x "$ROOT/jdk/linux-$_ARCH/bin/java" ]]; then
  JAVA_HOME="$ROOT/jdk/linux-$_ARCH"
fi
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
  JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
fi
export JAVA_HOME
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
PYTHON="${PYTHON:-python3}"

PROFILE="default"
OUT_DIR="$HERE/results"
DATA_DIR="$HERE/../java-profiler/data"   # same input files as the Java and Spark profilers
OPS=""
ROWS=""
RESOURCES=""
WARMUP=""
REPS=""
TIMEOUT_S=1800
MAX_OUT_ROWS=20000000
DRY_RUN=0
RESUME=1
CONFIG_LIST=""
HOURS=""
ANALYZE=1
EPHEMERAL=0
TAG=""
PG_URL="${PGURL:-postgresql://127.0.0.1:5433/wayangprof}"
PG_USER="${PGUSER:-$(id -un)}"
PG_OPTIONS=""
JAVA_EXTRA="${JAVA_EXTRA:-}"

usage() {
  sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
  cat <<EOF

Options
  --profile quick|default|full   size of the grid (default: default)
  --ops "a b c"                  operators (default: all): filter projection join globalreduce tablesink sqltostream
  --rows "N N N"                 input sizes
  --resources "HEAP:GC:pN[i] ..." resource settings
  --warmup N / --reps N          warm-up pairs (not recorded) / measured pairs per config
  --url postgresql://H:P/DB      database (default: \$PGURL or $PG_URL)
  --user U                       database user (default: \$PGUSER or the OS user); password from \$PGPASSWORD
  --pg-options "-c k=v ..."      more session settings for every run, e.g. "-c work_mem=64MB -c jit=off"
  --out DIR                      where raw.csv, run.log and the analysis go (default: results/)
  --data-dir DIR                 generated input files (default: ../java-profiler/data)
  --timeout SEC                  stop one JVM run after SEC seconds (default: $TIMEOUT_S)
  --tag TEXT                     free text stored with every row
  --no-resume                    run configs again even if raw.csv already has them
  --dry-run                      only print the configs
  --config-list FILE             run the configs in FILE (one per line: HEAP:GC:pN[i]|op|table|rows|right_rows|keys|sel|words,
                                 e.g. from ../server/sweep.py --platform postgres) instead of the built-in grid
  --hours H                      stop starting new configs after H hours (the running one finishes)
  --ephemeral-inputs             after each config, delete its input files and drop its tables (long sweeps: saves disk)
  --no-analyze                   do not run analyze.py at the end
Environment: PROFILER_ARGS (extra profiler arguments), WAYANG_HOME, WAYANG_SRC, JAVA_HOME, HOST_LABEL, JAVA_EXTRA, PYTHON
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --ops) OPS="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --resources) RESOURCES="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --url) PG_URL="$2"; shift 2 ;;
    --user) PG_USER="$2"; shift 2 ;;
    --pg-options) PG_OPTIONS="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --timeout) TIMEOUT_S="$2"; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    --no-resume) RESUME=0; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --config-list) CONFIG_LIST="$2"; shift 2 ;;
    --hours) HOURS="$2"; shift 2 ;;
    --ephemeral-inputs) EPHEMERAL=1; shift ;;
    --no-analyze) ANALYZE=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 2 ;;
  esac
done
PG_URL="${PG_URL#jdbc:}"

# ---------------------------------------------------------------------------------------------
# Grids. Selectivities are written the way Java prints doubles (1.0, not 1) so resume can match them.
# ---------------------------------------------------------------------------------------------
ALL_OPS="filter projection join globalreduce tablesink sqltostream"
case "$PROFILE" in
  quick)
    D_ROWS="10000 100000"; D_RES="2g:G1:p0"; D_WARMUP=2; D_REPS=3
    SELS="0.1 0.5"; WORDS="8"; JOIN_FANOUT="1"
    ;;
  default)
    D_ROWS="10000 100000 1000000"; D_RES="4g:G1:p0"; D_WARMUP=2; D_REPS=5
    SELS="0.01 0.1 0.5 0.9 1.0"; WORDS="2 8 32"; JOIN_FANOUT="1 10"
    ;;
  full)
    D_ROWS="10000 30000 100000 300000 1000000 3000000"; D_RES="4g:G1:p0 4g:G1:p2 4g:G1:p0i 4g:G1:p2i"; D_WARMUP=2; D_REPS=5
    SELS="0.001 0.01 0.05 0.1 0.25 0.5 0.75 0.9 1.0"; WORDS="2 8 32"; JOIN_FANOUT="1 2 10 100"
    ;;
  *) echo "Unknown profile: $PROFILE (use quick, default or full)"; exit 2 ;;
esac
OPS="${OPS:-$ALL_OPS}"
ROWS="${ROWS:-$D_ROWS}"
RESOURCES="${RESOURCES:-$D_RES}"
WARMUP="${WARMUP:-$D_WARMUP}"
REPS="${REPS:-$D_REPS}"

RAW="$OUT_DIR/raw.csv"
LOG="$OUT_DIR/run.log"
BUILD="$HERE/build"
OS="$(uname -s)"

say() { echo "[$(date +%H:%M:%S)] $*"; }

resource_label() { # 4g:G1:p2i -> heap4g-G1-p2i
  IFS=: read -r h g p <<<"$1"
  echo "heap${h}-${g}-${p}"
}

resource_flags() { # 4g:G1:p2 -> JVM flags
  IFS=: read -r h g p <<<"$1"
  local f="-Xms$h -Xmx$h"
  case "$g" in
    G1) f="$f -XX:+UseG1GC" ;; Parallel) f="$f -XX:+UseParallelGC" ;;
    Serial) f="$f -XX:+UseSerialGC" ;; Z) f="$f -XX:+UseZGC" ;;
    *) echo "Unknown GC '$g' in resource '$1'" >&2; return 1 ;;
  esac
  [[ "$p" =~ ^p[0-9]+i?$ ]] || { echo "third field must be pN or pNi (parallel workers, index) in resource '$1'" >&2; return 1; }
  echo "$f"
}

resource_pg_args() { # 4g:G1:p2i -> --parallel 2 --index
  IFS=: read -r _ _ p <<<"$1"
  local n="${p#p}"; n="${n%i}"
  echo "--parallel $n"
  [[ "$p" == *i ]] && echo "--index"
}

config_id() { # resource op source rows right_rows keys sel words  (must match Cfg.configId())
  local s="-"; [[ "$2" == "filter" ]] && s="$7"
  local rr=0; [[ "$2" == "join" ]] && rr="$5"
  echo "$1|$2|table|r$4|rr$rr|k$6|s$s|w$8"
}

cpu_model() {
  if [[ "$OS" == "Darwin" ]]; then sysctl -n machdep.cpu.brand_string 2>/dev/null
  else
    m=$(grep -m1 "model name" /proc/cpuinfo 2>/dev/null | cut -d: -f2 | sed 's/^ *//')
    [[ -z "$m" ]] && m=$(lscpu 2>/dev/null | grep -m1 "Model name" | cut -d: -f2 | sed 's/^ *//')
    echo "$m"
  fi
}

with_timeout() {
  local secs="$1"; shift
  if ! command -v perl >/dev/null && command -v timeout >/dev/null; then
    timeout -k 5 "$secs" "$@"; return $?
  fi
  perl -e 'my $s = shift; my $pid = fork(); if ($pid == 0) { exec @ARGV or exit 127 }
           local $SIG{ALRM} = sub { kill "TERM", $pid; sleep 2; kill "KILL", $pid; exit 124 };
           alarm $s; waitpid($pid, 0); exit($? >> 8)' "$secs" "$@"
}

ensure_input() { # rows keys words seed: unzip the committed file if there is one (else the profiler generates it)
  local name="rows-$1_keys-$2_words-$3_seed-$4"
  if [[ ! -f "$DATA_DIR/$name.csv" && -f "$DATA_DIR/$name.zip" ]]; then
    unzip -q -o "$DATA_DIR/$name.zip" -d "$DATA_DIR"
  fi
}

# ---------------------------------------------------------------------------------------------
# Config list. Each entry: resource|op|table|rows|right_rows|keys|sel|words
# ---------------------------------------------------------------------------------------------
CONFIGS=()
SKIPPED=0
add() { CONFIGS+=("$*"); }
for res in $RESOURCES; do
  for op in $OPS; do
    for n in $ROWS; do
      case "$op" in
        filter) for s in $SELS; do add "$res|$op|table|$n|0|1000|$s|8"; done ;;
        projection|sqltostream) for w in $WORDS; do add "$res|$op|table|$n|0|1000|0.5|$w"; done ;;
        join)
          for f in $JOIN_FANOUT; do
            k=$(( n / f )); (( k < 1 )) && k=1
            if (( n * n / k > MAX_OUT_ROWS )); then SKIPPED=$((SKIPPED + 1)); continue; fi
            add "$res|$op|table|$n|$n|$k|0.5|8"
          done ;;
        *) add "$res|$op|table|$n|0|1000|0.5|8" ;;
      esac
    done
  done
done
if [[ -n "$CONFIG_LIST" ]]; then
  [[ -f "$CONFIG_LIST" ]] || { echo "Config list not found: $CONFIG_LIST"; exit 1; }
  CONFIGS=(); SKIPPED=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    CONFIGS+=("$line")
  done <"$CONFIG_LIST"
fi
TOTAL=${#CONFIGS[@]}
(( TOTAL > 0 )) || { echo "No configs to run (check --ops / --rows)."; exit 1; }
say "profile=$PROFILE  configs=$TOTAL  skipped(too big)=$SKIPPED  warmup=$WARMUP reps=$REPS  database=$PG_URL user=$PG_USER"
if (( DRY_RUN )); then
  printf '%s\n' "${CONFIGS[@]}" | column -t -s '|' -N "RESOURCE,OP,SOURCE,ROWS,RIGHT_ROWS,KEYS,SEL,WORDS" 2>/dev/null \
    || printf '%s\n' "${CONFIGS[@]}"
  exit 0
fi

# ---------------------------------------------------------------------------------------------
# Checks and build
# ---------------------------------------------------------------------------------------------
[[ -d "$WAYANG_HOME/jars" ]] || { echo "Wayang distribution not found at $WAYANG_HOME"; exit 1; }
command -v "$JAVA" >/dev/null || { echo "java not found (set JAVA_HOME)"; exit 1; }
mkdir -p "$OUT_DIR" "$DATA_DIR" "$BUILD"
CP="$BUILD:$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*"
SRC="$HERE/src/wayangprof/PostgresOperatorProfiler.java"
if [[ ! -f "$BUILD/wayangprof/PostgresOperatorProfiler.class" || "$SRC" -nt "$BUILD/wayangprof/PostgresOperatorProfiler.class" ]]; then
  say "compiling the profiler"
  "$JAVAC" -nowarn -d "$BUILD" -cp "$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*" "$SRC" || exit 1
fi
PIDRUSAGE=""
if [[ "$OS" == "Darwin" ]]; then
  NATIVE="$HERE/../java-profiler/native"
  if [[ ! -x "$NATIVE/pidrusage" || "$NATIVE/pidrusage.c" -nt "$NATIVE/pidrusage" ]]; then
    cc -O2 -o "$NATIVE/pidrusage" "$NATIVE/pidrusage.c" || exit 1
  fi
  PIDRUSAGE="$NATIVE/pidrusage"
fi
CPU_MODEL="$(cpu_model)"
if [[ -z "${HOST_LABEL:-}" ]]; then
  if [[ "$OS" == "Darwin" ]]; then HOST_LABEL="$(sysctl -n hw.model 2>/dev/null)"; else HOST_LABEL="$(uname -m)-$(nproc 2>/dev/null)cpu"; fi
fi
{
  echo "==== $(date)  profile=$PROFILE  configs=$TOTAL  database=$PG_URL"
  echo "os=$(uname -sr)  cpu=$CPU_MODEL"
  "$JAVA" -version 2>&1
} >>"$LOG"

# ---------------------------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------------------------
START=$(date +%s)
DEADLINE=""
[[ -n "$HOURS" ]] && DEADLINE=$(( START + $("$PYTHON" -c "print(int(float('$HOURS') * 3600))") ))
DONE_IDS="$OUT_DIR/.done_ids"
if (( RESUME )) && [[ -s "$RAW" ]]; then
  "$PYTHON" - "$RAW" >"$DONE_IDS" <<'PY'
import csv, sys
with open(sys.argv[1], newline="") as f:
    print("\n".join(sorted({r["config_id"] for r in csv.DictReader(f)})))
PY
else
  : >"$DONE_IDS"
fi
DONE=0; FAILED=0; RESUMED=0
for entry in "${CONFIGS[@]}"; do
  IFS='|' read -r res op src n rr k s w <<<"$entry"
  DONE=$((DONE + 1))
  if [[ -n "$DEADLINE" ]] && (( $(date +%s) >= DEADLINE )); then
    say "time limit of ${HOURS} h reached; stopping (rerun to continue)"
    break
  fi
  label=$(resource_label "$res")
  id=$(config_id "$label" "$op" "$src" "$n" "$rr" "$k" "$s" "$w")
  if (( RESUME )) && grep -qxF "$id" "$DONE_IDS"; then RESUMED=$((RESUMED + 1)); continue; fi
  elapsed=$(( $(date +%s) - START )); ran=$(( DONE - RESUMED - 1 ))
  eta="?"; (( ran > 0 )) && eta="$(( elapsed * (TOTAL - DONE + 1) / ran / 60 ))min"
  say "[$DONE/$TOTAL] $id   (eta $eta)"
  gate_enter  # mains power, no other profiler measuring (../common/measure-gate.sh)
  flags=$(resource_flags "$res") || exit 2
  # shellcheck disable=SC2207
  pg_args=($(resource_pg_args "$res"))
  right=$n; (( rr > 0 )) && right=$rr
  ensure_input "$n" "$k" "$w" 42
  [[ "$op" == "join" ]] && ensure_input "$right" "$k" "$w" 43
  drop=(); (( EPHEMERAL )) && drop=(--drop-tables)
  # shellcheck disable=SC2086
  with_timeout "$TIMEOUT_S" "$JAVA" $flags $JAVA_EXTRA \
    -Dlog4j.configurationFile="file://$HERE/log4j2-quiet.xml" \
    -Dwayangprof.cpu="$CPU_MODEL" -Dwayangprof.host="$HOST_LABEL" \
    -cp "$CP" wayangprof.PostgresOperatorProfiler \
    --op "$op" --rows "$n" --right-rows "$right" --keys "$k" --sel "$s" --words "$w" \
    "${pg_args[@]}" --url "$PG_URL" --user "$PG_USER" ${PG_OPTIONS:+--pg-options "$PG_OPTIONS"} \
    --warmup "$WARMUP" --reps "$REPS" --data-dir "$DATA_DIR" --out "$RAW" \
    ${PIDRUSAGE:+--pidrusage "$PIDRUSAGE"} ${drop[@]+"${drop[@]}"} \
    --resource "$label" --tag "$TAG" ${PROFILER_ARGS:-} \
    </dev/null >>"$LOG" 2>&1
  rc=$?
  gate_leave
  (( rc == 0 )) && echo "$id" >>"$DONE_IDS"
  if (( EPHEMERAL )); then
    rm -f "$DATA_DIR/rows-${n}_keys-${k}_words-${w}_seed-42.csv" "$DATA_DIR/rows-${right}_keys-${k}_words-${w}_seed-43.csv"
  fi
  if (( rc != 0 )); then
    FAILED=$((FAILED + 1))
    why="exit $rc"; (( rc == 124 )) && why="timeout after ${TIMEOUT_S}s"
    grep -q "OutOfMemoryError" <(tail -50 "$LOG") && why="out of memory"
    if grep -q "Connection refused\|Could not connect to database\|password authentication failed\|does not exist" <(tail -50 "$LOG"); then
      # Not the config's fault: do not mark it failed (failed configs are never retried); stop instead.
      say "stopping: cannot use the database $PG_URL as $PG_USER (see $LOG)"
      exit 1
    fi
    say "   FAILED ($why), see $LOG"
    echo "FAILED $id ($why)" >>"$LOG"
  fi
done
say "finished in $(( ($(date +%s) - START) / 60 )) min: $((DONE - RESUMED - FAILED)) ok, $FAILED failed, $RESUMED already done"

if (( ANALYZE )) && [[ -f "$RAW" ]]; then
  "$PYTHON" "$HERE/analyze.py" "$RAW" --out "$OUT_DIR"
fi

#!/usr/bin/env bash
# Step 2 on the server (after ./prepare.sh; offline): tests every part of the pipeline on small single-operator jobs
# before the big run. ~15 minutes. Everything goes to ../test-run/ and ../test-run/test.log.
#
#   ./test.sh
#
#   1. environment      Java 17, Wayang, Spark 3.5 jars, PostgreSQL, Python found locally (nothing to download)
#   2. single node      run-server.sh --check: Java, Spark and PostgreSQL profilers on one small operator each,
#                       the measurement self-test (fixed CPU work must measure 90-125%, pure waiting < 15% CPU),
#                       PostgreSQL socket bytes / EXPLAIN / database CPU, and the data validation
#   3. Spark cluster    run-cluster.sh --check: a 2-executor standalone cluster, shuffle counted as network bytes,
#                       executor CPU, the same self-test
#   4. lanes            collect.sh --test: all lanes at once (Java, Spark, PostgreSQL with its own server, a 1- and
#                       2-executor cluster) on tiny fixed lists, then merge, analysis, validation and export
#   5. Wayang estimates every measured row has Wayang's cost estimate; prints Wayang's estimate next to the measured
#                       cost (operator plan - baseline plan) for each tested operator
# Ends with TEST PASSED (then: ./collect.sh start) or TEST FAILED and the step that failed.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CM="$(cd "$HERE/.." && pwd)"
[[ "${1:-}" == -h || "${1:-}" == --help ]] && { sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 0; }
T="$CM/test-run"
for p in $(pgrep -f "collect.sh start|run-server.sh|run-cluster.sh" 2>/dev/null); do
  [[ "$(ps -o args= -p "$p")" == *"$T"* ]] || { echo "A profiling run is active on this machine (pid $p): stop it first (./collect.sh stop)."; exit 1; }
done
rm -rf "$T"; mkdir -p "$T"
LOG="$T/test.log"
exec > >(tee -a "$LOG") 2>&1
say() { echo "[$(date '+%F %T')] $*"; }
FAILED=""
step_fail() { FAILED="$FAILED\n  - $1"; say "FAIL: $1"; }
rm -f "$CM/.deps/TEST_PASSED"

# ---- 1 ----------------------------------------------------------------------------------------
say "1/5 environment"
# shellcheck source=env.sh
source "$HERE/env.sh"
env_report
env_ok || { say "TEST FAILED: something is missing (run ./prepare.sh with internet)"; exit 1; }
say "machine: $(uname -srm), $(nproc 2>/dev/null || sysctl -n hw.ncpu) CPUs, $(awk '/MemTotal/ {printf "%.0f GB", $2/1048576}' /proc/meminfo 2>/dev/null) RAM, load $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"

# ---- 2 ----------------------------------------------------------------------------------------
say "2/5 single node: Java, Spark, PostgreSQL profilers + self-test (~5 min)"
"$HERE/run-server.sh" --check --postgres local --out "$T/server" >"$T/server.txt" 2>&1
grep -E "column|self-test|ok  |FAIL|PASSED|FAILED|DATA|ERROR " "$T/server.txt" | grep -v "ok       0 of" | sed 's/^/    /'
grep -q "^CHECK PASSED" "$T/server.txt" || step_fail "single-node check (Java/Spark): see $T/server.txt"
grep -q "POSTGRES CHECK PASSED" "$T/server.txt" || step_fail "PostgreSQL check: see $T/server.txt"
grep -q "DATA OK" "$T/server.txt" || step_fail "data validation of the single-node check: see $T/server.txt"

# ---- 3 ----------------------------------------------------------------------------------------
say "3/5 Spark cluster: 2 executors (~3 min)"
"$HERE/run-cluster.sh" --check --out "$T/cluster" >"$T/cluster.txt" 2>&1
grep -E "ok  |FAIL|skip|PASSED|FAILED|DATA" "$T/cluster.txt" | grep -v "ok       0 of" | sed 's/^/    /'
grep -q "^CHECK PASSED" "$T/cluster.txt" || step_fail "cluster check: see $T/cluster.txt"

# ---- 4 ----------------------------------------------------------------------------------------
say "4/5 lanes: every profiler at once on tiny lists, merge, analysis, export (~5 min)"
"$HERE/collect.sh" start --test --out "$T/collect" --hours 0.4 >"$T/collect.txt" 2>&1
grep -E "lane .*(ended|finished|GIVES UP)|merge|validation|export|platform" "$T/collect/collect.log" | sed 's/^\[[^]]*\] /    /'
"$PYTHON" - "$T/collect" <<'PY' || step_fail "lanes / merge: see $T/collect/collect.log"
import csv, os, sys
run = sys.argv[1]
ok = True
for p, want in (("java", 2), ("spark", 2), ("postgres", 2), ("spark-cluster", 2)):
    d = os.path.join(run, "data", p)
    try:
        rows = list(csv.DictReader(open(os.path.join(d, "raw.csv"))))
        configs = {r["config_id"] for r in rows}
        costs = list(csv.DictReader(open(os.path.join(d, "operator_costs.csv"))))
    except OSError as e:
        print(f"    FAIL  {p}: {e}")
        ok = False
        continue
    good = len(configs) >= want and len(costs) >= 1
    ok &= good
    print(f"    {'ok  ' if good else 'FAIL'}  {p:14} {len(configs)} configs merged ({len(rows)} runs), {len(costs)} operator cost rows")
exp = os.path.join(run, "export", "wayang-cost-data.tar.gz")
good = os.path.exists(exp) and os.path.getsize(exp) > 0
ok &= good
print(f"    {'ok  ' if good else 'FAIL'}  dataset export {exp}")
sys.exit(0 if ok else 1)
PY
grep -q "DATA OK" "$T/collect/data/validation.txt" 2>/dev/null || step_fail "validation of the merged lane data: see $T/collect/data/validation.txt"

# ---- 5 ----------------------------------------------------------------------------------------
say "5/5 Wayang's estimates next to the measurements (operator plan - baseline plan)"
"$PYTHON" - "$T/server" "$T/cluster" "$T/collect/data" <<'PY' || step_fail "Wayang estimates missing"
import csv, glob, os, sys
ok = True
files = []
for base in sys.argv[1:]:
    files += glob.glob(os.path.join(base, "*", "operator_costs.csv")) + glob.glob(os.path.join(base, "operator_costs.csv"))
raws = []
for base in sys.argv[1:]:
    raws += [f for f in glob.glob(os.path.join(base, "*", "raw.csv")) if "selftest" not in f]
for f in raws:
    rows = [r for r in csv.DictReader(open(f)) if r.get("plan") == "operator" and r.get("operator") not in ("burn", "sleep")]
    if not rows:
        continue
    have = sum(1 for r in rows if r.get("wayang_est_time_ms_gmean") not in ("", None))
    good = have == len(rows)
    ok &= good
    print(f"    {'ok  ' if good else 'FAIL'}  {have}/{len(rows)} measured operator runs carry Wayang's estimate  ({os.path.relpath(f)})")
print(f"\n    {'platform / config':58} {'Wayang est. ms':>14} {'measured wall ms':>17} {'measured CPU ms':>16}")
seen = set()
for f in sorted(files):
    if "selftest" in f:
        continue
    for r in csv.DictReader(open(f)):
        if r.get("operator") in ("burn", "sleep") or r["config_id"] in seen:
            continue
        seen.add(r["config_id"])
        def ms(k):
            v = r.get(k, "")
            try:
                return f"{float(v) / 1e6:.1f}"
            except ValueError:
                return "-"
        cpu = next((k for k in ("thread_cpu_exec_ns", "task_cpu_ns", "db_cpu_ns") if r.get(k) not in ("", None)), "")
        est = r.get("wayang_est_time_ms_gmean", "")
        try:
            est = f"{float(est):.1f}"
        except ValueError:
            est = "-"
        print(f"    {r['config_id'][:58]:58} {est:>14} {ms('wall_exec_ns'):>17} {ms(cpu) if cpu else '-':>16}")
sys.exit(0 if ok else 1)
PY

echo
if [[ -z "$FAILED" ]]; then
  mkdir -p "$CM/.deps" && echo "passed $(date '+%F %T')" >"$CM/.deps/TEST_PASSED"
  say "TEST PASSED: every part works. Next (inside tmux):  ./collect.sh start --until \"YYYY-MM-DD HH:MM\""
  say "full output: $LOG"
else
  say "TEST FAILED in:$(printf "$FAILED")"
  say "full output: $LOG (and the files named above). Do not start the big run before this passes."
  exit 1
fi

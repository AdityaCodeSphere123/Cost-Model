#!/usr/bin/env bash
# The big data collection: every profiler at the same time, each in its own lanes pinned to its own CPUs, for days.
# Offline (run ./prepare.sh once before, with internet), no root. Start it inside tmux, so it survives logging out:
#
#   tmux new -s wayang
#   ./collect.sh start                                 # runs until the data is ready Sunday 2026-11-15 16:00
#   ./collect.sh start --until "2026-11-15 16:00"     # the same, said explicitly (or --hours 60); Ctrl+B, D to detach
#   ./collect.sh status                                # any time, from any terminal (also in collect/STATUS.txt)
#   tail -f ../collect/collect.log                     # everything that happens, with times
#   ./collect.sh merge                                 # dataset snapshot now (also automatic every 3 h and at the end)
#   ./collect.sh stop                                  # stop everything; "start" again continues where it stopped
#
# Lanes (see plan_lanes.py; "./collect.sh plan" shows the plan for this machine without starting):
#   java-N      Wayang's Java platform, 13 operators, one config at a time per lane
#   spark-N     Spark local mode, 13 operators, local[1..lane CPUs]
#   postgres-N  Wayang's PostgreSQL platform (6 operators), each lane with its own PostgreSQL server
#   cluster     Spark standalone clusters of 1, 2, ..., 10 executors (one cluster at a time, shapes take turns,
#               every shape runs the same data configs)
# Every lane works through its own share of a fixed random config list (all operators, input sizes 1k..3M rows,
# keys, selectivity, row width, heap / GC / cores settings), so any amount of finished work is an evenly spread
# sample of the whole space. Each config: 2 warm-up + 5 measured pairs (operator plan vs baseline plan) with
# Wayang's own cost estimate stored next to every measurement.
#
# Robust for days: a lane that crashes is restarted (after 1 min; it gives up after 10 failures in a row), a config
# that hangs is killed after 30 min, a stop or a reboot loses at most the configs that were running, a lane whose
# list is finished ends by itself.
#
# The time limit is when the DATA must be ready (--until, default Sunday 2026-11-15 16:00): new configs stop 50 min
# before it, configs still running 30 min before it are stopped (only those are lost), and the final merge and
# export finish before it. Everything finished up to then is kept: each config is written to disk as soon as it
# ends, and the dataset is exported every 3 h along the way.
#
# Output (default ../collect/):
#   collect.log                   THE log: plan, lane starts / ends / failures, status every 10 min, merges
#   STATUS.txt                    the latest status table (configs per lane and platform, per cluster size, rates)
#   data/<platform>/              merged dataset: raw.csv, pairs.csv, operator_costs.csv, baseline_costs.csv
#   data/validation.txt           data checks of the merged dataset
#   export/wayang-cost-data.tar.gz  data + logs in one file, refreshed with every merge (scp this one)
#   data/calibration.txt / .csv   lanes vs alone: the same configs measured alone before the lanes started and by
#                                 their lane under load (same CPUs), see calibration_report.py
#   calibration/<platform>/       the alone runs
#   lanes/<lane>/                 per lane: run.log (every config, with times), runner.out, raw.csv, lane.list
#   plan.txt, plan.json, lists/   the plan and the config lists (kept, so a restart continues the same run)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CM="$(cd "$HERE/.." && pwd)"

CMD="${1:-help}"; [[ $# -gt 0 ]] && shift
HOURS=""
UNTIL=""
DEFAULT_READY_BY="2026-11-15 16:00"   # the dataset must be complete by then (Sunday 4 pm, server clock)
STOP_NEW_MIN=${STOP_NEW_MIN:-50}                      # no new configs in the last 50 min before it ...
HARD_STOP_MIN=${HARD_STOP_MIN:-30}                     # ... configs still running 30 min before are stopped; then the final merge
CPUS=""
MEM_GB=""
PLAN=""
RUN="$CM/collect"
TEST=0
WARMUP=2
REPS=5
MERGE_EVERY_H=3
while [[ $# -gt 0 ]]; do
  case "$1" in
    --hours) HOURS="$2"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --cpus) CPUS="$2"; shift 2 ;;
    --mem-gb) MEM_GB="$2"; shift 2 ;;
    --plan) PLAN="$2"; shift 2 ;;
    --out) RUN="$(mkdir -p "$2" && cd "$2" && pwd)"; shift 2 ;;
    --test) TEST=1; shift ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    -h|--help) CMD=help; shift ;;
    *) echo "Unknown option: $1 (./collect.sh help)"; exit 2 ;;
  esac
done

usage() {
  sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
  cat <<EOF

Options (start / plan)
  --until "YYYY-MM-DD HH:MM"   the final dataset must be ready by then (server's clock; default $DEFAULT_READY_BY;
                               remembered for restarts), or
  --hours H                    ... H hours from now
  --cpus LIST                  CPUs to use, e.g. "0-47" (default: all this process may use). On a shared server,
                               leave some for others: e.g. --cpus 0-55
  --mem-gb N                   memory to use (default: 85% of the memory available now)
  --plan "cluster=10x3 java=4x2 spark=3x4 postgres=2x6"   lanes by hand (lanes x CPUs; cluster = executors x CPUs)
  --out DIR                    output folder (default $RUN)
EOF
}

mkdir -p "$RUN"
LOG="$RUN/collect.log"
say() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }
now() { date +%s; }
fmt_time() { date -d "@$1" '+%F %T' 2>/dev/null || date -r "$1" '+%F %T'; }
PYTHON="${PYTHON:-python3}"

# ---- the plan: lanes, CPUs, memory ---------------------------------------------------------------------
allowed_cpus() {
  if [[ -n "$CPUS" ]]; then echo "$CPUS"; return; fi
  if command -v taskset >/dev/null; then taskset -pc $$ 2>/dev/null | sed 's/.*: *//' && return; fi
  echo "0-$(( $(nproc 2>/dev/null || sysctl -n hw.ncpu) - 1 ))"
}
mem_budget() {
  if [[ -n "$MEM_GB" ]]; then echo "$MEM_GB"; return; fi
  # --test runs fixed tiny lists (1-2 GB heaps): the plan's sizes do not matter, it must only fit the test lanes.
  if (( TEST )); then echo 40; return; fi
  if [[ -r /proc/meminfo ]]; then awk '/MemAvailable/ {printf "%d", $2 / 1048576 * 0.85}' /proc/meminfo
  else echo $(( $(sysctl -n hw.memsize) / 1073741824 * 6 / 10 )); fi
}
make_plan() {
  local args=(--cpus "$(allowed_cpus)" --mem-gb "$(mem_budget)")
  [[ -n "$PLAN" ]] && args+=(--plan "$PLAN")
  "$PYTHON" "$HERE/plan_lanes.py" "${args[@]}" >"$RUN/plan.json.tmp" || { rm -f "$RUN/plan.json.tmp"; return 1; }
  "$PYTHON" "$HERE/plan_lanes.py" "${args[@]}" --text >"$RUN/plan.txt"
  mv "$RUN/plan.json.tmp" "$RUN/plan.json"
}
lanes_tsv() {  # name platform cpus(comma) mem_gb max_heap port executors executor_cores driver_heap
  "$PYTHON" - "$RUN/plan.json" <<'PY'
import json, sys
for l in json.load(open(sys.argv[1]))["lanes"]:
    # "-" for empty fields: read with IFS=tab would merge two tabs and shift every later field
    print("\t".join(str(x) for x in (l["name"], l["platform"], ",".join(map(str, l["cpus"])), l["mem_gb"],
          l.get("max_heap_gb", "-"), l["port"], l.get("executors", "-"), l.get("executor_cores", "-"), l.get("driver_heap", "-"))))
PY
}

# ---- config lists: one per platform, dealt out to its lanes round-robin ------------------------------------
make_lists() {
  mkdir -p "$RUN/lists"
  local name platform cpus mem heap port n c dh p count seed
  if (( TEST )); then
    printf '%s\n' "1g:G1:1|map|text|20000|0|1000|0.5|8" "1g:G1:2|filter|text|30000|0|1000|0.3|8" >"$RUN/lists/java.list"
    printf '%s\n' "2g:G1:2|reduceby|text|20000|0|100|0.5|8" "2g:G1:1|map|text|20000|0|1000|0.5|8" >"$RUN/lists/spark.list"
    printf '%s\n' "2g:G1:p0|filter|table|20000|0|1000|0.1|8" "2g:G1:p2|join|table|20000|20000|2000|0.5|8" >"$RUN/lists/postgres.list"
    printf '%s\n' "2g:G1:1x1|reduceby|text|20000|0|100|0.5|8" "2g:G1:2x1|reduceby|text|20000|0|100|0.5|8" >"$RUN/lists/cluster.list"
  fi
  for p in java spark postgres; do
    local lanes=() first=""
    while IFS=$'\t' read -r name platform cpus mem heap port n c dh; do
      [[ "$platform" == "$p" ]] || continue
      lanes+=("$name"); [[ -n "$first" ]] || first="$cpus $mem $heap"
    done < <(lanes_tsv)
    (( ${#lanes[@]} )) || continue
    if [[ ! -s "$RUN/lists/$p.list" ]]; then
      read -r cpus mem heap <<<"$first"
      count=100000; [[ $p == postgres ]] && count=40000
      seed=2; [[ $p == spark ]] && seed=3; [[ $p == postgres ]] && seed=1002
      # cores of one lane, memory of one lane: every setting in the list fits every lane of this platform
      "$PYTHON" "$HERE/sweep.py" --platform "$p" --cores "$(awk -F, '{print NF}' <<<"$cpus")" --mem-gb "${mem%.*}" \
        --max-heap-gb "$heap" --count "$count" --seed "$seed" >"$RUN/lists/$p.list" 2>>"$LOG" || return 1
    fi
    local i=0 k=${#lanes[@]}
    for name in "${lanes[@]}"; do
      mkdir -p "$RUN/lanes/$name"
      [[ -s "$RUN/lanes/$name/lane.list" ]] || awk -v k="$k" -v i="$i" 'NR % k == i' "$RUN/lists/$p.list" >"$RUN/lanes/$name/lane.list"
      i=$((i + 1))
    done
  done
}

# ---- one lane: runs its runner until the deadline, restarts it after a crash ----------------------------
lane_done_ids() {  # lane dir -> file with finished config ids
  if [[ -d "$1/spark-cluster" ]]; then echo "$1/spark-cluster/.done_ids"; else echo "$1/.done_ids"; fi
}
count_lines() { [[ -s "$1" ]] && wc -l <"$1" | tr -d ' ' || echo 0; }

run_lane_once() {  # name platform cpus mem heap port n c dh hours
  local name="$1" platform="$2" cpus="$3" mem="$4" heap="$5" port="$6" n="$7" c="$8" dh="$9" hours="${10}"
  local L="$RUN/lanes/$name" pin=()
  command -v taskset >/dev/null && pin=(taskset -c "$cpus")
  local common=(--warmup "$WARMUP" --reps "$REPS" --hours "$hours" --ephemeral-inputs --no-analyze --timeout 1800 --tag "lane-$name")
  case "$platform" in
    java|spark)
      ${pin[@]+"${pin[@]}"} "$CM/$platform-profiler/run-profiling.sh" --config-list "$L/lane.list" --out "$L" \
        --data-dir "$L/data" "${common[@]}" >>"$L/runner.out" 2>&1 </dev/null ;;
    postgres)
      # This lane's own server (its CPUs, its port, its data folder), started if it is not running.
      local url
      url=$(${pin[@]+"${pin[@]}"} "$CM/postgres-profiler/start-local-postgres.sh" url --port "$port" --pgdata "$L/pgdata" 2>>"$L/runner.out") \
        || { echo "PostgreSQL of lane $name did not start" >>"$L/runner.out"; return 1; }
      ${pin[@]+"${pin[@]}"} "$CM/postgres-profiler/run-profiling.sh" --config-list "$L/lane.list" --out "$L" \
        --data-dir "$L/data" --url "$url" --user "$(id -un)" "${common[@]}" >>"$L/runner.out" 2>&1 </dev/null ;;
    spark-cluster)
      local given=(); (( TEST )) && given=(--list "$RUN/lists/cluster.list" --executor-memory-gb 1)
      "$HERE/run-cluster.sh" --shared --cpus "$cpus" --reserve 2 --max-executors "$n" --mem-gb "${mem%.*}" \
        --driver-heap "$dh" --port "$port" --out "$L" --warmup "$WARMUP" --reps "$REPS" --hours "$hours" \
        ${given[@]+"${given[@]}"} >>"$L/runner.out" 2>&1 </dev/null ;;
  esac
}

lane_loop() {  # the lane's fields (tab-separated line of lanes_tsv)
  local name platform cpus mem heap port n c dh
  IFS=$'\t' read -r name platform cpus mem heap port n c dh <<<"$1"
  local L="$RUN/lanes/$name" fails=0 rc before after left hours t0 batch=0 topped=0
  mkdir -p "$L"
  [[ -s "$L/lane.started" ]] || date +%s >"$L/lane.started"   # first start: for the rate in the status
  while :; do
    left=$(( $(cat "$RUN/deadline") - $(now) ))
    (( left > 60 )) || { say "lane $name: deadline reached"; break; }
    hours=$("$PYTHON" -c "print(round($left / 3600, 4))")
    before=$(count_lines "$(lane_done_ids "$L")")
    t0=$(now)
    say "lane $name: start (CPUs $cpus, ${mem} GB)"
    run_lane_once "$name" "$platform" "$cpus" "$mem" "$heap" "$port" "$n" "$c" "$dh" "$hours"
    rc=$?
    after=$(count_lines "$(lane_done_ids "$L")")
    say "lane $name: runner ended (exit $rc) after $(( ($(now) - t0) / 60 )) min: +$((after - before)) configs, $after total"
    if (( rc == 0 && after == before )); then
      # List done before the deadline: a fresh random batch (new seed) so the lane never sits idle. Configs this lane
      # already ran are skipped by their id; if a batch brings nothing new, the space is exhausted for this lane.
      if (( ! TEST && ! topped )) && [[ "$platform" != spark-cluster ]]; then
        batch=$(( $(ls "$L"/batch-*.list 2>/dev/null | wc -l) + 1 ))
        local seed=$(( 100000 + batch * 1000 + $(cksum <<<"$name" | cut -d' ' -f1) % 1000 ))
        if "$PYTHON" "$HERE/sweep.py" --platform "$platform" --cores "$(awk -F, '{print NF}' <<<"$cpus")" \
             --mem-gb "${mem%.*}" $( [[ "$heap" != - ]] && echo "--max-heap-gb $heap") --count 20000 --seed "$seed" \
             >"$L/batch-$batch.list" 2>>"$LOG" && [[ -s "$L/batch-$batch.list" ]]; then
          mv "$L/lane.list" "$L/lane-done-$batch.list"     # finished list kept; the runner reads only the new one
          cp "$L/batch-$batch.list" "$L/lane.list"
          say "lane $name: its list is done before the deadline: added a new random batch of $(count_lines "$L/batch-$batch.list") configs (seed $seed)"
          topped=1
          continue
        fi
      fi
      say "lane $name: nothing left to run in its list: lane finished"
      break
    fi
    topped=0
    if (( rc != 0 )); then
      fails=$((fails + 1))
      { echo "---- lane $name failure $fails, last lines of runner.out:"; tail -15 "$L/runner.out"; } >>"$LOG"
      if (( fails >= 10 )); then say "lane $name: 10 failures in a row: lane GIVES UP (see $L/runner.out)"; break; fi
      sleep 60
    else
      fails=0
    fi
  done
  echo "finished $(date '+%F %T')" >"$L/lane.finished"
}

# ---- calibration: what running the lanes side by side costs ------------------------------------------
# Before the lanes start, the first configs of the first java and spark lane are measured ALONE (nothing else running)
# on that lane's CPUs. The lane then measures the same configs first, on the same CPUs, with all lanes busy. merge_all
# compares the two (calibration_report.py -> data/calibration.txt / .csv): the effect of sharing caches, memory
# bandwidth, disk and power with the other lanes, to report next to results from this data. Once per collection.
CALIB_N=${CALIB_N:-24}   # java configs; spark gets half
calibrate_alone() {
  local C="$RUN/calibration" p line name platform cpus rest k pin
  for p in java spark; do
    line=$(lanes_tsv | awk -F'\t' -v p="$p" '$2 == p' | head -1)
    [[ -n "$line" ]] || continue
    IFS=$'\t' read -r name platform cpus rest <<<"$line"
    mkdir -p "$C/$p"
    if [[ ! -s "$C/$p/lane.list" ]]; then
      k=$CALIB_N; [[ $p == spark ]] && k=$(( (k + 1) / 2 )); (( TEST )) && k=1
      head -n "$k" "$RUN/lanes/$name/lane.list" >"$C/$p/lane.list"
      echo "$name" >"$C/$p/lane.name"
    fi
    (( $(count_lines "$C/$p/.done_ids") >= $(count_lines "$C/$p/lane.list") )) && continue
    [[ -s "$RUN/lanes/$name/.done_ids" ]] && say "calibration ($p): note: lane $name already ran under load before these alone runs"
    pin=(); command -v taskset >/dev/null && pin=(taskset -c "$cpus")
    say "calibration ($p): $(count_lines "$C/$p/lane.list") configs ALONE on the CPUs of lane $name ($cpus), before the lanes start"
    ${pin[@]+"${pin[@]}"} "$CM/$p-profiler/run-profiling.sh" --config-list "$C/$p/lane.list" --out "$C/$p" \
      --data-dir "$C/$p/data" --warmup "$WARMUP" --reps "$REPS" --hours 1 --timeout 1800 --no-analyze --ephemeral-inputs \
      --tag "calibration-alone" >>"$C/$p/runner.out" 2>&1 </dev/null \
      || say "calibration ($p): runner failed (see $C/$p/runner.out); the collection goes on without it"
  done
}

# ---- status ---------------------------------------------------------------------------------------
status_table() {
  "$PYTHON" - "$RUN" <<'PY'
import glob, os, re, sys, time
run = sys.argv[1]
def lines(p):
    try:
        with open(p, errors="replace") as f:
            return f.read().splitlines()
    except OSError:
        return []
now = time.time()
try:
    deadline = int(open(os.path.join(run, "deadline")).read())
    ready = int(open(os.path.join(run, "ready_by")).read())
    fmt = lambda t: time.strftime('%a %Y-%m-%d %H:%M', time.localtime(t))
    print(f"data ready by {fmt(ready)}; new configs until {fmt(deadline)} ({max(0, deadline - now) / 3600:.1f} h of collection left)")
except (OSError, ValueError):
    pass
print(f"{'lane':12} {'state':9} {'done':>6} {'failed':>6} {'list':>7} {'per h':>6}  current config")
totals, shapes = {}, {}
for d in sorted(glob.glob(os.path.join(run, "lanes", "*"))):
    name = os.path.basename(d)
    cl = os.path.isdir(os.path.join(d, "spark-cluster"))
    sub = os.path.join(d, "spark-cluster") if cl else d
    done = lines(os.path.join(sub, ".done_ids"))
    log = lines(os.path.join(sub, "run.log"))
    failed = sum(1 for l in log if l.startswith("FAILED "))
    lst = len(lines(os.path.join(d, "cluster.list" if cl else "lane.list")))
    cur = ""
    for l in reversed(log):
        m = re.search(r"\[(\d+)/(\d+)\] (\S+)", l)
        if m:
            cur = m.group(3)[:70]
            break
    state = "finished" if os.path.exists(os.path.join(d, "lane.finished")) else "running"
    try:
        start = int(open(os.path.join(d, "lane.started")).read())
        rate = len(done) / max(0.05, (now - start) / 3600)
    except (OSError, ValueError):
        rate = 0
    print(f"{name:12} {state:9} {len(done):6} {failed:6} {lst:7} {rate:6.0f}  {cur}")
    plat = "spark-cluster" if cl else name.rsplit("-", 1)[0]
    t = totals.setdefault(plat, [0, 0])
    t[0] += len(done); t[1] += failed
    if cl:
        for i in done:
            m = re.search(r"cpu(\d+)x(\d+)", i)
            if m:
                shapes[int(m.group(1))] = shapes.get(int(m.group(1)), 0) + 1
print("\nper platform (configs done / failed):  " + "   ".join(f"{p} {d}/{f}" for p, (d, f) in sorted(totals.items())))
if shapes:
    print("cluster configs per number of executors: " + "  ".join(f"{k}:{v}" for k, v in sorted(shapes.items())))
PY
  local la mem disk
  la=$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime | sed 's/.*averages*: //')
  mem=$(awk '/MemAvailable/ {printf "%.0f GB", $2/1048576}' /proc/meminfo 2>/dev/null)
  disk=$(df -Ph "$RUN" | awk 'NR==2 {print $4}')
  echo "machine: load $la | memory available ${mem:-?} | disk free $disk"
  local others
  others=$(ps -eo user:24,pcpu,comm --sort=-pcpu 2>/dev/null | awk -v me="$(id -un)" 'NR>1 && $1 != me && $2 > 20 {printf "%s %s%% %s; ", $1, $2, $3}' | head -c 300)
  [[ -n "$others" ]] && echo "other users' busy processes: $others"
  [[ -s "$RUN/data/validation.txt" ]] && echo "last validation: $(grep -E 'DATA OK|ERROR' "$RUN/data/validation.txt" | tail -1)"
  [[ -s "$RUN/export/wayang-cost-data.tar.gz" ]] && echo "dataset: $RUN/export/wayang-cost-data.tar.gz ($(du -h "$RUN/export/wayang-cost-data.tar.gz" | cut -f1), $(date -r "$RUN/export/wayang-cost-data.tar.gz" '+%F %T'))"
  return 0
}

# ---- merge + analyze + validate + export -------------------------------------------------------------
merge_all() {
  say "merge: lanes -> $RUN/data"
  "$PYTHON" "$HERE/merge_lanes.py" "$RUN" 2>&1 | tee -a "$LOG"
  local p a raws=()
  for p in java spark postgres spark-cluster; do
    [[ -s "$RUN/data/$p/raw.csv" ]] || continue
    a="$CM/$p-profiler/analyze.py"; [[ $p == spark-cluster ]] && a="$CM/spark-profiler/analyze.py"
    "$PYTHON" "$a" "$RUN/data/$p/raw.csv" --out "$RUN/data/$p" --quiet >>"$LOG" 2>&1 \
      || say "WARNING: analysis of $p failed (see log)"
    raws+=("$RUN/data/$p/raw.csv")
  done
  (( ${#raws[@]} )) || { say "merge: no data yet"; return 0; }
  "$PYTHON" "$HERE/validate_data.py" "${raws[@]}" >"$RUN/data/validation.txt" 2>&1
  say "validation: $(grep -cE '^ *ERROR +[0-9.]+%' "$RUN/data/validation.txt") error checks hit, $(grep -E 'DATA OK|DATA HAS ERRORS|ERROR' "$RUN/data/validation.txt" | tail -1 | sed 's/^ *//')"
  if [[ -d "$RUN/calibration" ]]; then
    "$PYTHON" "$HERE/calibration_report.py" "$RUN" >"$RUN/data/calibration.txt" 2>&1
    say "calibration: $(head -1 "$RUN/data/calibration.txt" | sed 's/^SUMMARY //')"
  fi
  status_table >"$RUN/STATUS.txt" 2>&1
  local tmp="$RUN/export/tmp"; rm -rf "$tmp"; mkdir -p "$tmp/wayang-cost-data/lane-logs"
  cp -R "$RUN/data/." "$tmp/wayang-cost-data/"
  cp "$LOG" "$RUN/STATUS.txt" "$RUN/plan.txt" "$tmp/wayang-cost-data/" 2>/dev/null
  cp "$RUN/env.txt" "$tmp/wayang-cost-data/" 2>/dev/null
  local d
  for d in "$RUN"/lanes/*; do
    [[ -d "$d" ]] || continue
    cp "$d/run.log" "$tmp/wayang-cost-data/lane-logs/$(basename "$d").run.log" 2>/dev/null
    cp "$d/spark-cluster/run.log" "$tmp/wayang-cost-data/lane-logs/$(basename "$d").run.log" 2>/dev/null
    cp "$d/cluster.log" "$tmp/wayang-cost-data/lane-logs/" 2>/dev/null
  done
  tar -czf "$RUN/export/out.tgz" -C "$tmp" wayang-cost-data && mv "$RUN/export/out.tgz" "$RUN/export/wayang-cost-data.tar.gz"
  rm -rf "$tmp"
  say "export: $RUN/export/wayang-cost-data.tar.gz ($(du -h "$RUN/export/wayang-cost-data.tar.gz" | cut -f1))"
}

# ---- process control ----------------------------------------------------------------------------------
descendants() { local c; for c in $(pgrep -P "$1" 2>/dev/null); do echo "$c"; descendants "$c"; done; }
kill_tree() {  # pid...: TERM to all of them and their descendants, KILL after 15 s
  local all=() p
  for p in "$@"; do all+=("$p" $(descendants "$p")); done
  (( ${#all[@]} )) || return 0
  kill -TERM "${all[@]}" 2>/dev/null
  for _ in $(seq 1 15); do
    local alive=0; for p in "${all[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    (( alive )) || return 0; sleep 1
  done
  kill -KILL "${all[@]}" 2>/dev/null; return 0
}
kill_leftovers() {  # processes of this run left over from a crash (not the PostgreSQL servers: they are reused)
  local p pids=()
  for p in $(pgrep -u "$(id -u)" -f "$RUN/(lanes|calibration)/" 2>/dev/null); do  # calibration: stop before the lanes start
    [[ "$p" == "$$" ]] && continue
    [[ "$(ps -o comm= -p "$p" 2>/dev/null)" == postgres* ]] && continue
    pids+=("$p")
  done
  (( ${#pids[@]} )) || return 0
  say "stopping ${#pids[@]} leftover processes of an earlier run of this collection"
  kill_tree "${pids[@]}"
}
stop_postgres_lanes() {
  local d
  for d in "$RUN"/lanes/postgres-*/pgdata; do
    [[ -d "$d" ]] && "$CM/postgres-profiler/start-local-postgres.sh" stop --pgdata "$d" >/dev/null 2>&1
  done
}

# ---- start -----------------------------------------------------------------------------------------
start_cmd() {
  # shellcheck source=env.sh
  source "$HERE/env.sh"
  env_ok || { env_report; echo "Something is missing: run ./prepare.sh first (with internet)."; exit 1; }
  if [[ -s "$RUN/collect.pid" ]] && kill -0 "$(cat "$RUN/collect.pid")" 2>/dev/null; then
    echo "already running (pid $(cat "$RUN/collect.pid")); ./collect.sh status"; exit 1
  fi
  (( TEST )) || [[ -s "$CM/.deps/TEST_PASSED" ]] || say "WARNING: ./test.sh has not passed on this machine yet"
  trap '' HUP          # survives a closed terminal even outside tmux (the JVMs keep SIGHUP ignored too)
  echo $$ >"$RUN/collect.pid"
  { echo "started $(date '+%F %T') on $(hostname) ($(uname -srm))"; env_report; } >"$RUN/env.txt"
  say "================ collect start (pid $$) ================"
  cat "$RUN/env.txt" | tee -a "$LOG" >/dev/null

  # ready_by: when the final dataset must exist. deadline: when the lanes stop starting new configs.
  local t=""
  if [[ -n "$HOURS" ]]; then
    t=$("$PYTHON" -c "import time; print(int(time.time() + float('$HOURS') * 3600))")
  elif [[ -n "$UNTIL" || ! -s "$RUN/ready_by" ]]; then
    local u="${UNTIL:-$DEFAULT_READY_BY}"
    t=$(date -d "$u" +%s 2>/dev/null || date -j -f '%Y-%m-%d %H:%M' "$u" +%s 2>/dev/null) || { say "cannot read --until '$u' (use \"YYYY-MM-DD HH:MM\")"; exit 2; }
  fi
  [[ -n "$t" ]] && echo "$t" >"$RUN/ready_by"
  READY_BY=$(cat "$RUN/ready_by")
  if (( TEST )); then echo "$READY_BY" >"$RUN/deadline"
  else echo $(( READY_BY - STOP_NEW_MIN * 60 )) >"$RUN/deadline"; fi
  (( $(cat "$RUN/deadline") > $(now) + 600 )) || { say "the data would have to be ready by $(fmt_time "$READY_BY"): too soon (or already past). Use --until with a later time."; exit 2; }
  say "data ready by: $(fmt_time "$READY_BY")  |  new configs until $(fmt_time "$(cat "$RUN/deadline")")  ($(( ($(cat "$RUN/deadline") - $(now)) / 3600 )) h of collection)"
  say "server clock now: $(date '+%F %T %Z')  (check this is the time zone you mean)"

  if [[ ! -s "$RUN/plan.json" ]]; then
    (( TEST )) && [[ -z "$PLAN" ]] && PLAN="cluster=2x1 java=1x2 spark=1x2 postgres=1x2"
    make_plan || { say "could not make a lane plan (see above); use --plan or --cpus"; exit 1; }
  else
    say "plan: reusing $RUN/plan.json (same lanes as before; delete it, or use a new --out, for a new plan)"
  fi
  sed 's/^/  /' "$RUN/plan.txt" | tee -a "$LOG"
  make_lists || { say "could not make the config lists"; exit 1; }
  kill_leftovers
  # A raw.csv line cut by a crash or power loss: drop it (its config runs again).
  local f
  for f in "$RUN"/lanes/*/raw.csv "$RUN"/lanes/*/spark-cluster/raw.csv; do
    [[ -s "$f" ]] || continue
    if [[ -n "$(tail -c1 "$f" | tr -d '\n')" ]]; then
      "$PYTHON" -c "
import sys; p=sys.argv[1]; b=open(p,'rb').read(); c=b.rfind(b'\n')+1; open(p,'r+b').truncate(c)" "$f"
      rm -f "$(dirname "$f")/.done_ids"
      say "repaired a cut last line in $f"
    fi
  done
  rm -f "$RUN"/lanes/*/lane.finished

  # Housekeeping CPUs (plan_lanes.py): this script, and so the merges and the status, run there, never on a lane's
  # CPUs. Lanes leave this mask: run_lane_once pins each runner with taskset, run-cluster.sh pins its own JVMs.
  local hk
  hk=$("$PYTHON" -c "import json, sys; print(','.join(map(str, json.load(open(sys.argv[1])).get('housekeeping_cpus', []))))" "$RUN/plan.json")
  if [[ -n "$hk" ]] && command -v taskset >/dev/null; then
    taskset -cp "$hk" $$ >/dev/null && say "housekeeping (this script, merges, status) pinned to CPUs $hk"
  fi
  # The lanes run at the same time on purpose: the runners skip their one-profiler-at-a-time lock
  # (../common/measure-gate.sh), and every row records the CPU other processes used on its lane's CPUs.
  local n_lanes
  n_lanes=$(lanes_tsv | wc -l | tr -d ' ')
  export WAYANGPROF_LANES="$n_lanes"
  (( TEST )) && export WAYANGPROF_ALLOW_BATTERY=1
  if (( n_lanes > 1 )); then
    # RAPL counts the whole CPU package, i.e. every lane at once: per lane it would be meaningless.
    export JAVA_EXTRA="${JAVA_EXTRA:-} -Dwayangprof.raplPath=/nonexistent/rapl-off-while-lanes-share-the-package"
    say "energy (RAPL) not recorded: $n_lanes lanes share the CPU package"
  fi
  calibrate_alone

  local pids=() line
  while IFS= read -r line; do
    lane_loop "$line" 2>>"$LOG" &     # a lane's own errors go to the log too
    pids+=($!)
    sleep 3       # lanes start a few seconds apart (no stampede of JVM starts)
  done < <(lanes_tsv)
  echo "${pids[*]}" >"$RUN/lane.pids"
  trap 'say "stop requested: stopping all lanes"; kill_tree ${pids[*]}; stop_postgres_lanes; rm -f "$RUN/collect.pid"; say "stopped"; exit 143' TERM INT
  say "${#pids[@]} lanes running. Status every 10 min here and in $RUN/STATUS.txt; dataset every ${MERGE_EVERY_H} h."

  local last_merge; last_merge=$(now)
  local tick=0 alive p
  while :; do
    alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    (( alive )) || break
    if (( ! TEST && $(now) >= READY_BY - HARD_STOP_MIN * 60 )); then
      say "$HARD_STOP_MIN min before the data must be ready: stopping the configs still running (only those are lost)"
      kill_tree "${pids[@]}"
      break
    fi
    sleep 30 & wait $!     # (wait, not sleep: a stop request is handled at once)
    tick=$((tick + 1))
    if (( tick % 20 == 0 )) || (( TEST && tick % 4 == 0 )); then
      status_table >"$RUN/STATUS.txt" 2>&1
      { echo "---- status $(date '+%F %T')"; cat "$RUN/STATUS.txt"; } >>"$LOG"
    fi
    if (( $(now) - last_merge >= MERGE_EVERY_H * 3600 )); then merge_all; last_merge=$(now); fi
  done
  say "all lanes ended"
  merge_all
  stop_postgres_lanes
  status_table >"$RUN/STATUS.txt" 2>&1
  { echo "---- final status $(date '+%F %T')"; cat "$RUN/STATUS.txt"; } >>"$LOG"
  rm -f "$RUN/collect.pid"
  say "================ collect finished. Dataset: $RUN/export/wayang-cost-data.tar.gz ================"
}

stop_cmd() {
  if [[ -s "$RUN/collect.pid" ]] && kill -0 "$(cat "$RUN/collect.pid")" 2>/dev/null; then
    kill -TERM "$(cat "$RUN/collect.pid")"
    echo "stopping (the lanes get 15 s to finish cleanly)..."
    for _ in $(seq 1 40); do [[ -s "$RUN/collect.pid" ]] || break; sleep 1; done
  else
    echo "not running"
  fi
  kill_leftovers
  stop_postgres_lanes
  echo "stopped. Finished configs are kept; ./collect.sh start continues."
}

case "$CMD" in
  start) start_cmd ;;
  status)
    if [[ -s "$RUN/collect.pid" ]] && kill -0 "$(cat "$RUN/collect.pid")" 2>/dev/null; then echo "collect: running (pid $(cat "$RUN/collect.pid"))"
    else echo "collect: not running"; fi
    status_table ;;
  merge) merge_all ;;
  stop) stop_cmd ;;
  plan)
    args=(--cpus "$(allowed_cpus)" --mem-gb "$(mem_budget)"); [[ -n "$PLAN" ]] && args+=(--plan "$PLAN")
    "$PYTHON" "$HERE/plan_lanes.py" "${args[@]}" --text ;;
  help|-h|--help) usage ;;
  *) echo "Unknown command: $CMD"; usage; exit 2 ;;
esac

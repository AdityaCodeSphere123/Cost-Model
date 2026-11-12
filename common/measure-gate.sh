# Sourced by the profilers' run-profiling.sh. Keeps every measured config on a machine that is fit to measure:
#
#   gate_enter   before a config: waits until
#                  - the Mac runs on mains power with Low Power Mode off (on battery or in Low Power Mode macOS
#                    lowers the CPU clock: 31 Oct 2026 configs ran at ~2.6 instead of ~4 GHz, 1.5x the CPU time);
#                  - no other profiler measures on this machine: a machine-wide lock (one config at a time, so two
#                    sweeps started at once take turns instead of disturbing each other), plus a check that no
#                    profiler JVM started some other way (old scripts, job-eval) is running.
#   gate_leave   after the config: releases the lock.
#
# Exceptions:
#   WAYANGPROF_LANES=N            set by server/collect.sh, whose lanes run at the same time ON PURPOSE, each pinned to
#                                 its own CPUs: no lock, no JVM check (each row records the foreign CPU load instead).
#   WAYANGPROF_ALLOW_BATTERY=1    measure on battery / in Low Power Mode anyway (only for tests).
#   WAYANGPROF_ALLOW_CONCURRENT=1 skip the lock and the JVM check (only for tests).
# Every row also records how much CPU other processes used during the run (foreign_cpu_cores in analyze.py), so a
# disturbance the gate cannot see (another program) is still caught afterwards.

GATE_CM="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GATE_LOCK="${WAYANGPROF_LOCK:-$GATE_CM/.measure.lock}"
GATE_HELD=0

gate_say() { echo "[$(date +%H:%M:%S)] $*"; }

# Why this Mac should not measure now (empty = fine). Linux servers: always fine.
gate_power_problem() {
  [[ "$(uname -s)" == Darwin ]] || return 0
  [[ "${WAYANGPROF_ALLOW_BATTERY:-0}" == 1 ]] && return 0
  if pmset -g batt 2>/dev/null | grep -q "Battery Power"; then echo "running on battery"; return 0; fi
  if [[ "$(pmset -g 2>/dev/null | awk '/lowpowermode/ {print $2}')" == 1 ]]; then echo "Low Power Mode is on"; return 0; fi
  return 0
}

# Profiler JVMs running on this machine (pids), other than our own children.
gate_other_jvms() {
  local p out=()
  for p in $(pgrep -f 'wayangprof\.(JavaOperatorProfiler|SparkOperatorProfiler|PostgresOperatorProfiler|JobBench)' 2>/dev/null); do
    [[ "$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' ')" == "$$" ]] && continue
    out+=("$p")
  done
  echo "${out[*]:-}"
}

gate_concurrency_checked() {
  [[ -z "${WAYANGPROF_LANES:-}" && "${WAYANGPROF_ALLOW_CONCURRENT:-0}" != 1 ]]
}

gate_enter() {
  local why said="" owner jvms
  while why=$(gate_power_problem) && [[ -n "$why" ]]; do
    [[ "$said" == "$why" ]] || gate_say "   WAITING: $why (plug in / turn Low Power Mode off; WAYANGPROF_ALLOW_BATTERY=1 to override)"
    said="$why"; sleep 30
  done
  gate_concurrency_checked || return 0
  said=""
  while ! mkdir "$GATE_LOCK" 2>/dev/null; do
    owner=$(cat "$GATE_LOCK/pid" 2>/dev/null)
    [[ "$owner" == "$$" ]] && break  # still ours (a config that ended without gate_leave)
    if [[ -n "$owner" ]] && ! kill -0 "$owner" 2>/dev/null; then rm -rf "$GATE_LOCK"; continue; fi  # stale lock
    [[ -n "$said" ]] || gate_say "   WAITING: another profiler (pid ${owner:-?}) is measuring on this machine"
    said=1; sleep 5
  done
  echo $$ >"$GATE_LOCK/pid"
  GATE_HELD=1
  trap gate_leave EXIT
  said=""
  while jvms=$(gate_other_jvms) && [[ -n "$jvms" ]]; do
    [[ -n "$said" ]] || gate_say "   WAITING: profiler JVM(s) $jvms started outside this lock are still running"
    said=1; sleep 10
  done
  # Our own power check may have waited minutes; check once more right before the run.
  why=$(gate_power_problem)
  [[ -z "$why" ]] || { gate_leave; gate_enter; }
}

gate_leave() {
  if (( GATE_HELD )) && [[ "$(cat "$GATE_LOCK/pid" 2>/dev/null)" == "$$" ]]; then rm -rf "$GATE_LOCK"; fi
  GATE_HELD=0
  return 0  # safe under set -e
}

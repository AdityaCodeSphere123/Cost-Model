"""Was a measured run disturbed? Shared by the profilers' analyze.py and server/validate_data.py.

Per run (one raw.csv row), from the OS counters of the run's own window:

  eff_ghz                 os_cycles / own CPU time = the average clock while the run's process was on a CPU
                          (macOS: cycles and CPU time come from the same proc_pid_rusage reading, so this also
                          holds for rows of the old profiler whose OS window was too wide).
  pcore_ghz               the same on the performance cores only (Apple Silicon: os_cycles_pcore / P-core CPU
                          time), so it does not depend on how many threads spilled onto efficiency cores. On
                          battery or in Low Power Mode it is capped (31 Oct / 2 Nov 2026: ~2.6 GHz instead of ~4),
                          and the same work then takes ~1.5x the CPU time.
  pcore_share             share of the process's CPU time on performance cores (Apple Silicon).
  foreign_cpu_cores       CPU that OTHER processes used during the run, in cores: (machine busy - own CPU) / wall.
  lane_foreign_cpu_cores  the same on the CPUs the run was pinned to (server lanes, Linux): other lanes use their
                          own CPUs, so only an intruder on this lane's CPUs counts here.

Verdicts per run (1 = ok, 0 = disturbed, '' = cannot tell, e.g. no counters or too short to judge):

  clock_ok  pcore_ghz (else eff_ghz) >= MIN_CLOCK_RATIO x the clock this CPU model reaches (90th percentile of the
            judged runs). Judged only on runs with >= MIN_CLOCK_JUDGE_NS of CPU: shorter runs start while the cores are
            still clocking up and read low anyway. A short run gets the majority verdict of the 3 judged runs
            nearest in time, if all are within CLOCK_NEIGHBOUR_S (the power state belongs to the machine at that time).
  quiet_ok  lane_foreign_cpu_cores <= MAX_LANE_FOREIGN_CORES if the run was pinned to a lane, else
            foreign_cpu_cores <= MAX_FOREIGN_CORES. Judged only when the run is long enough for the 10 ms
            tick counters to resolve that much.

A baseline/operator pair is disturbed if either of its runs is (pair_flags).
Only the Python standard library is used.
"""
import bisect
import math
import statistics
from datetime import datetime
from collections import defaultdict

MIN_CLOCK_RATIO = 0.8
MAX_FOREIGN_CORES = 1.0        # whole machine, unpinned run (macOS): background apps normally stay well below
MAX_LANE_FOREIGN_CORES = 0.25  # on the run's own pinned CPUs: nothing else should run there
MIN_CLOCK_JUDGE_NS = 3e8       # CPU time below this: the clock is still ramping up, no verdict of its own
CLOCK_NEIGHBOUR_S = 300        # ... but the verdict of the 3 judged runs nearest in time, if this close
TICK_NS = 1e7                  # busy-time counters count in 10 ms ticks


def num(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def own_cpu_ns(r):
    """CPU time of the processes doing the measured work, in the OS window: the JVM, plus the database server
    (PostgreSQL) or the executor JVMs (Spark cluster) when they are separate processes."""
    u, s = num(r.get("os_user_ns")), num(r.get("os_system_ns"))
    if u is None or s is None:
        return None
    own = u + s
    for extra in ("db_cpu_ns", "executors_process_cpu_ns"):
        v = num(r.get(extra))
        if v is not None and v > 0:
            own += v
    return own


def window_ns(r):
    """Wall time of the OS counters' window: the exec window, or the whole execute() calls."""
    if r.get("os_window") == "exec":
        return num(r.get("wall_exec_ns"))
    if r.get("os_window") == "run":
        return num(r.get("wall_total_ns"))
    return None  # old profiler: the OS window is not known exactly


def _fmt(x, digits=4):
    return "" if x is None else f"{x:.{digits}g}"


def _foreign(busy, own, wall, cpus, limit):
    """(foreign cores, verdict) for one busy-time counter."""
    if busy is None or own is None or not wall or wall <= 0:
        return None, ""
    cores = max(0.0, (busy - own) / wall)
    # tick error of busy (cpus counters) and own (1 counter), each up to 1 tick
    err = ((cpus or 1) + 1) * TICK_NS / wall
    if err > limit / 2:
        return cores, ""
    return cores, "1" if cores <= limit else "0"


def _pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


def _time(r):
    try:
        return datetime.fromisoformat(r.get("timestamp", "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def annotate(rows):
    """Adds eff_ghz, pcore_ghz, pcore_share, foreign_cpu_cores, lane_foreign_cpu_cores, clock_ok, quiet_ok to every row."""
    for r in rows:
        own = own_cpu_ns(r)
        u, s = num(r.get("os_user_ns")), num(r.get("os_system_ns"))
        proc_cpu = u + s if u is not None and s is not None else None
        cyc = num(r.get("os_cycles"))
        r["eff_ghz"] = _fmt(cyc / proc_cpu) if cyc and proc_cpu else ""
        pu, ps = num(r.get("os_user_pcore_ns")), num(r.get("os_system_pcore_ns"))
        pcpu = pu + ps if pu is not None and ps is not None else None
        pcyc = num(r.get("os_cycles_pcore"))
        r["pcore_share"] = _fmt(pcpu / proc_cpu, 3) if pcpu is not None and proc_cpu else ""
        r["pcore_ghz"] = _fmt(pcyc / pcpu) if pcyc and pcpu else ""
        # the clock this run is judged on: P-cores if known, else all cores; only long enough runs
        if pcyc and pcpu and pcpu >= MIN_CLOCK_JUDGE_NS:
            r["_clock"] = pcyc / pcpu
        elif cyc and proc_cpu and proc_cpu >= MIN_CLOCK_JUDGE_NS and not pcpu:
            r["_clock"] = cyc / proc_cpu
        else:
            r["_clock"] = None
        wall = window_ns(r)
        cores, verdict_machine = _foreign(num(r.get("os_sys_busy_ns")), own, wall,
                                          num(r.get("os_sys_cpus_after")), MAX_FOREIGN_CORES)
        lane, verdict_lane = _foreign(num(r.get("os_lane_busy_ns")), own, wall,
                                      num(r.get("os_lane_cpus_after")), MAX_LANE_FOREIGN_CORES)
        r["foreign_cpu_cores"] = _fmt(cores, 3)
        r["lane_foreign_cpu_cores"] = _fmt(lane, 3)
        # Pinned to a lane: other lanes use other CPUs by design, so only the lane's own CPUs decide.
        r["quiet_ok"] = verdict_lane if r.get("os_lane_busy_ns") not in (None, "") else verdict_machine

    # Usual clock per CPU model (host names change between sessions, the chip does not): median of judged runs.
    machine = lambda r: r.get("cpu_model") or r.get("host", "")
    groups = defaultdict(list)
    for r in rows:
        if r["_clock"] is not None:
            groups[machine(r)].append(r["_clock"])
    # The reference is a high percentile, not the median: a capped session makes its runs longer, so it can hold
    # most of the long runs of a file (31 Oct 2026: most Java runs >= 300 ms ran at the 2.6 GHz cap).
    ref = {k: _pct(v, 0.9) for k, v in groups.items()}
    judged = defaultdict(list)  # machine -> [(time, ok)]
    for r in rows:
        base = ref.get(machine(r))
        if r["_clock"] is None or not base:
            r["clock_ok"] = ""
            continue
        r["clock_ok"] = "1" if r["_clock"] >= MIN_CLOCK_RATIO * base else "0"
        t = _time(r)
        if t is not None:
            judged[machine(r)].append((t, r["clock_ok"] == "1"))
    for v in judged.values():
        v.sort()
    for r in rows:
        if r["clock_ok"] or r["_clock"] is not None:
            continue
        t, v = _time(r), judged.get(machine(r), [])
        if t is None or not v:
            continue
        # the 3 judged runs nearest in time (power states change at a moment, e.g. unplugging), if close enough
        i = bisect.bisect_left(v, (t,))
        cand = sorted(v[max(0, i - 3):i + 3], key=lambda x: abs(x[0] - t))[:3]
        if len(cand) == 3 and all(abs(x[0] - t) <= CLOCK_NEIGHBOUR_S for x in cand):
            r["clock_ok"] = "1" if sum(ok for _, ok in cand) >= 2 else "0"
    for r in rows:
        del r["_clock"]
    return ref


PAIR_COLS = ["clock_ok", "quiet_ok", "disturbed", "eff_ghz", "pcore_ghz", "foreign_cpu_cores", "lane_foreign_cpu_cores", "pcore_share"]


def _verdict(a, b):
    if "0" in (a, b):
        return "0"
    if a == "1" or b == "1":
        return "1"
    return ""


def pair_flags(o, b):
    """Flags of one baseline/operator pair: a verdict is 0 if either run is 0; worst values of the two runs."""
    out = {"clock_ok": _verdict(o.get("clock_ok", ""), b.get("clock_ok", "")),
           "quiet_ok": _verdict(o.get("quiet_ok", ""), b.get("quiet_ok", ""))}
    out["disturbed"] = "1" if "0" in (out["clock_ok"], out["quiet_ok"]) else "0"
    for c in ("eff_ghz", "pcore_ghz"):
        lo = [num(x.get(c)) for x in (o, b)]
        lo = [x for x in lo if x is not None]
        out[c] = _fmt(min(lo)) if lo else ""
    for c in ("foreign_cpu_cores", "lane_foreign_cpu_cores"):
        hi = [num(x.get(c)) for x in (o, b)]
        hi = [x for x in hi if x is not None]
        out[c] = _fmt(max(hi), 3) if hi else ""
    ps = [num(x.get("pcore_share")) for x in (o, b)]
    ps = [x for x in ps if x is not None]
    out["pcore_share"] = _fmt(min(ps), 3) if ps else ""
    return out


def summary(rows):
    """One line: how many runs were judged and how many disturbed."""
    n = len(rows)
    c0 = sum(1 for r in rows if r.get("clock_ok") == "0")
    cj = sum(1 for r in rows if r.get("clock_ok") in ("0", "1"))
    q0 = sum(1 for r in rows if r.get("quiet_ok") == "0")
    qj = sum(1 for r in rows if r.get("quiet_ok") in ("0", "1"))
    med = [num(r.get("pcore_ghz")) or num(r.get("eff_ghz")) for r in rows]
    med = [x for x in med if x is not None]
    return (f"runs {n}: low clock {c0} of {cj} judged"
            + (f" (median {statistics.median(med):.2f} GHz)" if med else "")
            + f", other processes busy {q0} of {qj} judged")

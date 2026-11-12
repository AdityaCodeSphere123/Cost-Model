#!/usr/bin/env python3
"""Checks profiler data for impossible or suspicious values. Run on a raw.csv (Java or Spark):

    python3 validate_data.py ../results-server/java/raw.csv ../results-server/spark/raw.csv

Every check prints how many runs fail it. "ERROR" checks must be 0 (a bug or a broken run); "WARN" checks are
allowed to be small (noise, a busy machine). Exit code 1 if any ERROR check fails.
"""
import csv
import math
import sys
from collections import defaultdict

BINARY = {"join", "union", "cartesian"}


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return float("nan")
    return x


def check_file(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print(f"{path}: no rows")
        return True
    spark = "spark_execution_operator" in rows[0]
    name = "spark" if spark else "java"
    results = []  # (level, description, failing, total)

    def check(level, desc, rs, bad):
        n_bad = sum(1 for r in rs if bad(r))
        results.append((level, desc, n_bad, len(rs)))

    f = num
    check("ERROR", "no first-row marker (wall_exec_ns empty)", rows, lambda r: r.get("wall_exec_ns", "") == "")
    check("ERROR", "wall_exec_ns > wall_total_ns", rows, lambda r: f(r["wall_exec_ns"]) > f(r["wall_total_ns"]))
    text_base = [r for r in rows if r["plan"] == "baseline" and r["source"] == "text"]
    expected = lambda r: f(r["rows"]) + (f(r["right_rows"]) if r["operator"] in BINARY else 0)
    check("ERROR", "baseline sink did not see every input row", text_base,
          lambda r: abs(f(r["out_rows"]) - expected(r)) > 0)
    filt = [r for r in rows if r["plan"] == "operator" and r["operator"] == "filter"]
    check("ERROR", "filter output off by > 2% from sel x rows", filt,
          lambda r: abs(f(r["out_rows"]) - f(r["sel"]) * f(r["rows"])) > 0.02 * f(r["rows"]) + 5)
    ops = [r for r in rows if r["plan"] == "operator"]
    check("ERROR", "operator run without Wayang's estimate", ops, lambda r: r.get("wayang_est_time_ms_gmean", "") == "")
    if spark:
        check("ERROR", "Spark task metrics incomplete (spark_listener_drained = 0)", rows,
              lambda r: r.get("spark_listener_drained") == "0")
        # executorRunTime is whole ms per task (rounded down), so allow 1 ms per task
        check("ERROR", "task CPU > task run time (+1 ms per task)", rows,
              lambda r: f(r["task_cpu_ns"]) / 1e6 > f(r["task_run_ms"]) + f(r["spark_tasks"]) + 5)
        check("ERROR", "task run time > task threads x wall time", rows,
              lambda r: f(r["task_run_ms"]) > f(r["task_slots"]) * f(r["wall_total_ns"]) / 1e6 * 1.05 + 50)
        check("ERROR", "shuffle records read != written", rows,
              lambda r: abs(f(r["shuffle_read_records"]) - f(r["shuffle_write_records"])) > 0)
        clus = [r for r in rows if r.get("deploy_mode") == "standalone-cluster"]
        if clus:
            shuf = [r for r in clus if f(r.get("executors")) >= 2
                    and f(r.get("shuffle_read_local_bytes")) + f(r.get("shuffle_read_remote_bytes")) > 1e6]
            check("ERROR", "cluster: >1 MB shuffled between >= 2 executors but network_bytes = 0 (readHostLocalDisk on?)",
                  shuf, lambda r: not f(r.get("network_bytes")) > 0)
            check("WARN", "cluster: not every executor ran a task (few input partitions; real Wayang behaviour)",
                  [r for r in clus if r["plan"] == "operator"],
                  lambda r: f(r.get("executors_used")) < f(r.get("executors")))
    if "jit_compile_exec_ms" in rows[0]:
        jr = [r for r in rows if r.get("jit_compile_exec_ms") not in ("", None) and f(r["wall_exec_ns"]) >= 20e6]
        check("WARN", "JIT still compiling during the measured part (> 10% of wall_exec_ns, runs >= 20 ms)", jr,
              lambda r: f(r["jit_compile_exec_ms"]) * 1e6 > 0.1 * f(r["wall_exec_ns"]))
    else:
        check("ERROR", "operator not on one thread (same_thread = 0)", rows, lambda r: r.get("same_thread") == "0")
        check("ERROR", "thread CPU > wall time (one thread)", rows,
              lambda r: f(r["thread_cpu_exec_ns"]) > f(r["wall_exec_ns"]) * 1.02 + 2e6)
        check("WARN", "held memory not measured (mem_held_bytes empty)", rows, lambda r: r.get("mem_held_bytes", "") == "")
    if "os_sys_steal_ns" in rows[0]:
        share = lambda r: f(r["os_sys_steal_ns"]) / (f(r["wall_total_ns"]) * f(r["os_sys_cpus_after"]) or float("nan"))
        check("WARN", "VM steal time > 2% of the machine's CPU (pair dropped by analyze.py)", rows,
              lambda r: share(r) > 0.02)
    if "os_energy_pkg_nj" in rows[0] or "os_energy_nj" in rows[0]:
        col = "os_energy_pkg_nj" if "os_energy_pkg_nj" in rows[0] else "os_energy_nj"
        check("WARN", f"energy not measured ({col} empty)", rows, lambda r: r.get(col, "") == "")
    else:
        results.append(("WARN", "no energy counter on this machine (RAPL not readable)", len(rows), len(rows)))

    # Noise: spread of the main CPU metric between the pairs of one config (operator - baseline).
    cpu_col = "task_cpu_ns" if spark else "thread_cpu_exec_ns"
    by = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        by[r["config_id"]][(r["run_id"], r["pair"])][r["plan"]] = f(r[cpu_col])
    noisy = total = 0
    for cid, pairs in by.items():
        d = sorted(p["operator"] - p["baseline"] for p in pairs.values() if "operator" in p and "baseline" in p)
        if len(d) < 3 or abs(d[len(d) // 2]) < 20e6:  # skip tiny operators (< 20 ms CPU): mostly noise anyway
            continue
        total += 1
        iqr = d[(3 * len(d)) // 4] - d[len(d) // 4]
        noisy += iqr > 0.25 * abs(d[len(d) // 2])
    results.append(("WARN", f"configs (>= 20 ms CPU) whose pairs spread > 25% of the median ({cpu_col})", noisy, total))

    ok = True
    machine = rows[0].get("cpu_model") or rows[0].get("host", "")
    print(f"== {name}: {len(rows)} runs, {len({r['config_id'] for r in rows})} configs, {machine}, {rows[0].get('os_probe', '')}")
    for level, desc, bad, n in results:
        share = bad / n if n else 0
        flag = "ok" if bad == 0 else ("FAIL" if level == "ERROR" else f"{share:.1%}")
        print(f"  {level:5} {flag:>6}  {bad:6} of {n:<6} {desc}")
        ok &= not (level == "ERROR" and bad > 0)
    return ok


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    ok = all([check_file(p) for p in sys.argv[1:]])
    print("DATA OK" if ok else "DATA HAS ERRORS")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

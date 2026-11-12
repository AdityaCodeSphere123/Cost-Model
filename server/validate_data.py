#!/usr/bin/env python3
"""Checks profiler data for impossible or suspicious values. Run on a raw.csv (Java or Spark):

    python3 validate_data.py ../results-server/java/raw.csv ../results-server/spark/raw.csv

Every check prints how many runs fail it. "ERROR" checks must be 0 (a bug or a broken run); "WARN" checks are
allowed to be small (noise, a busy machine). Exit code 1 if any ERROR check fails.
"""
import csv
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import run_checks  # noqa: E402

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
    postgres = "postgres_execution_operator" in rows[0]
    name = "spark" if spark else "postgres" if postgres else "java"
    results = []  # (level, description, failing, total)

    def check(level, desc, rs, bad):
        n_bad = sum(1 for r in rs if bad(r))
        results.append((level, desc, n_bad, len(rs)))

    f = num
    check("ERROR", "no first-row marker (wall_exec_ns empty)", rows, lambda r: r.get("wall_exec_ns", "") == "")
    check("ERROR", "wall_exec_ns > wall_total_ns", rows, lambda r: f(r["wall_exec_ns"]) > f(r["wall_total_ns"]))
    # Baselines that read every input row into the sink (Postgres: SELECT * FROM t; not the sqltostream baseline,
    # which sends one aggregate row).
    text_base = [r for r in rows if r["plan"] == "baseline" and r["source"] in ("text", "table")
                 and r["operator"] != "sqltostream"]
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
    elif postgres:
        check("ERROR", "operator not on one thread (same_thread = 0)", rows, lambda r: r.get("same_thread") == "0")
        check("ERROR", "no traced database connection in the run (db_connections = 0)", rows,
              lambda r: not f(r.get("db_connections")) > 0)
        to_java = [r for r in rows if f(r["out_rows"]) > 0 and not (r["operator"] == "tablesink" and r["plan"] == "operator")]
        check("ERROR", "rows reached Java but no bytes were counted on the socket (network_bytes_in = 0)", to_java,
              lambda r: not f(r.get("network_bytes_in")) > 0)
        check("ERROR", "statement time > exec time", rows,
              lambda r: f(r["wall_query_ns"]) + f(r["wall_connect_ns"]) > f(r["wall_exec_ns"]) * 1.01 + 1e6)
        local = [r for r in rows if r.get("db_local") == "1"]
        check("ERROR", "database on this machine but its CPU was not measured (db_cpu_ns empty)", local,
              lambda r: r.get("db_cpu_ns", "") == "")
        if not local:
            results.append(("WARN", "database CPU not measured (database not on this machine, or not Linux)", len(rows), len(rows)))
        check("WARN", "EXPLAIN of the plan failed (db_explain_exec_ms empty)", rows, lambda r: r.get("db_explain_exec_ms", "") == "")
        check("WARN", "operator plan whose Wayang estimate is Wayang's generic fallback (join, globalreduce, tablesink)",
              [r for r in rows if r["plan"] == "operator"], lambda r: r.get("wayang_est_fallback", "") != "")
    else:
        check("ERROR", "operator not on one thread (same_thread = 0)", rows, lambda r: r.get("same_thread") == "0")
        check("ERROR", "thread CPU > wall time (one thread)", rows,
              lambda r: f(r["thread_cpu_exec_ns"]) > f(r["wall_exec_ns"]) * 1.02 + 2e6)
        check("WARN", "held memory not measured (mem_held_bytes empty)", rows, lambda r: r.get("mem_held_bytes", "") == "")
    if "jit_compile_exec_ms" in rows[0]:
        jr = [r for r in rows if r.get("jit_compile_exec_ms") not in ("", None) and f(r["wall_exec_ns"]) >= 20e6]
        check("WARN", "JIT still compiling during the measured part (> 10% of wall_exec_ns, runs >= 20 ms)", jr,
              lambda r: f(r["jit_compile_exec_ms"]) * 1e6 > 0.1 * f(r["wall_exec_ns"]))
    if "os_sys_steal_ns" in rows[0]:
        share = lambda r: f(r["os_sys_steal_ns"]) / (f(r["wall_total_ns"]) * f(r["os_sys_cpus_after"]) or float("nan"))
        check("WARN", "VM steal time > 2% of the machine's CPU (pair dropped by analyze.py)", rows,
              lambda r: share(r) > 0.02)
    if "os_energy_pkg_nj" in rows[0] or "os_energy_nj" in rows[0]:
        col = "os_energy_pkg_nj" if "os_energy_pkg_nj" in rows[0] else "os_energy_nj"
        check("WARN", f"energy not measured ({col} empty)", rows, lambda r: r.get(col, "") == "")
    else:
        results.append(("WARN", "no energy counter on this machine (RAPL not readable)", len(rows), len(rows)))

    # Disturbed runs (../common/run_checks.py): low CPU clock (battery, Low Power Mode, throttling) or other processes
    # busy during the run (on a lane's own CPUs when pinned, else on the whole machine). analyze.py leaves such pairs
    # out of operator_costs.csv.
    run_checks.annotate(rows)
    judged = [r for r in rows if r["clock_ok"] in ("0", "1")]
    if judged:
        check("WARN", "CPU clock < 80% of what this CPU reaches (battery, Low Power Mode, throttling)", judged,
              lambda r: r["clock_ok"] == "0")
    judged = [r for r in rows if r["quiet_ok"] in ("0", "1")]
    if judged:
        lane = any(r.get("os_lane_busy_ns") for r in judged)
        check("WARN", "other processes busy on this run's own CPUs (> 0.25 cores; lanes: not another lane)" if lane
              else "other processes busy on the machine during the run (> 1 core)", judged, lambda r: r["quiet_ok"] == "0")
    elif "os_sys_busy_ns" not in rows[0]:
        results.append(("WARN", "no machine busy-time counter (old profiler): other processes cannot be detected",
                        len(rows), len(rows)))

    # Noise: spread of the main CPU metric between the pairs of one config (operator - baseline).
    cpu_col = "task_cpu_ns" if spark else "wall_exec_ns" if postgres else "thread_cpu_exec_ns"
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

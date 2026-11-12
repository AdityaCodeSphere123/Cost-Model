#!/usr/bin/env python3
"""Turns raw.csv from the PostgreSQL profiler into per-operator resource costs.

Every config has pairs of runs (same JVM, same tables):
    baseline : table source -> (SqlToStream) -> Java sink                SELECT * FROM t
    operator : table source -> OPERATOR -> (SqlToStream) -> Java sink     (join: baseline = two scans; sqltostream:
                                                                           baseline = SELECT COUNT(id))
For each pair: operator cost = operator run - baseline run, for every measured metric.

Outputs (in --out):
    pairs.csv           one row per pair: features + the difference of every metric (most samples, for ML)
    operator_costs.csv  one row per config: features + median difference, and its spread (IQR)
    baseline_costs.csv  one row per config: median of the baseline itself (source + parse + sink cost)

Only the Python standard library is used.
"""
import argparse
import csv
import math
import os
import statistics
import sys
from collections import defaultdict

# Columns that describe the run rather than measure it. They are copied, not subtracted.
ID_COLS = [
    "resource", "config_id", "operator", "wayang_operator", "postgres_execution_operator",
    "measured_execution_operators", "baseline_plan", "source", "rows", "right_rows", "keys", "sel", "words",
    "in_bytes", "right_in_bytes", "tag", "host", "os_name", "os_arch", "cpu_model", "cpus",
    "machine_memory_bytes", "java_version", "jvm_args", "gc_name", "max_heap_bytes", "os_probe",
    # Wayang settings used to turn its load estimates into time and cost
    "wayang_cpu_mhz", "wayang_cores", "wayang_disk_ms_per_mb", "wayang_network_ms_per_mb", "wayang_cost_per_ms",
    "wayang_cost_model", "warmup_runs", "wayang_java_cpu_mhz", "wayang_java_cores",
    # the database
    "pg_version", "pg_parallel_workers", "pg_indexed", "pg_shared_buffers", "pg_work_mem", "pg_effective_cache_size",
    "pg_jit", "pg_autovacuum", "pg_random_page_cost", "pg_options", "db_local", "table_bytes", "right_table_bytes",
]
# Columns that are neither features nor metrics to subtract.
SKIP_COLS = {"timestamp", "run_id", "plan", "pair", "plan_execution_operators",
             "wayang_est_breakdown", "wayang_est_time_prob", "same_thread", "jobs",
             "wayang_est_missing"}
# Taken from the operator run (not subtracted): what the operator produced and the machine state.
# wayang_est_out_rows_*: rows Wayang expects to reach the sink, to compare with out_rows.
# sql / db_plan: what the operator plan ran. wayang_est_complete / _fallback: whether Wayang's estimate for the
# operator plan is a configured one or its generic fallback.
OPERATOR_SIDE = ["out_rows", "out_bytes", "sys_load_avg_1m", "sys_free_memory_bytes",
                 "wayang_est_out_rows_lower", "wayang_est_out_rows_upper", "sql", "db_plan",
                 "wayang_est_complete", "wayang_est_fallback"]
# Main metrics shown in the printed summary: (column, unit divisor, unit label).
SUMMARY = [
    ("wall_exec_ns", 1e6, "wall ms"),
    ("wayang_est_time_ms_gmean", 1, "wayang ms"),
    ("wall_query_ns", 1e6, "query ms"),
    ("thread_cpu_exec_ns", 1e6, "java cpu ms"),
    ("db_cpu_ns", 1e6, "db cpu ms"),
    ("db_explain_exec_ms", 1, "db exec ms"),
    ("network_bytes_in", 1e6, "net MB"),
]


# A run is disturbed if the hypervisor "stole" more than this share of the machine's CPU time (Linux VMs only).
MAX_STEAL = 0.02


def steal_share(r):
    steal, wall, cpus = num(r.get("os_sys_steal_ns")), num(r.get("wall_total_ns")), num(r.get("os_sys_cpus_after"))
    if steal is None or not wall or not cpus:
        return 0.0
    return steal / (wall * cpus)


def num(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def quantile(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    pos = (len(xs) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def fmt(x):
    if x is None:
        return ""
    if float(x).is_integer():
        return str(int(x))
    return f"{x:.6g}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("raw", help="raw.csv written by the profiler")
    ap.add_argument("--out", default=None, help="output folder (default: next to raw.csv)")
    ap.add_argument("--quiet", action="store_true", help="do not print the summary table")
    args = ap.parse_args()
    out_dir = args.out or os.path.dirname(os.path.abspath(args.raw))
    os.makedirs(out_dir, exist_ok=True)

    with open(args.raw, newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = list(reader)
    if not rows:
        sys.exit("raw.csv has no rows")

    id_cols = [c for c in ID_COLS if c in header]
    metric_cols = [
        c for c in header
        if c not in id_cols and c not in SKIP_COLS and c not in OPERATOR_SIDE
        and not c.endswith("_after")  # gauge levels; their change is in the matching *_delta column
    ]

    # Pair baseline and operator runs: same JVM run (run_id) and same pair number.
    pairs = defaultdict(dict)
    for r in rows:
        pairs[(r["run_id"], r["pair"])][r["plan"]] = r

    pair_rows = []
    by_config = defaultdict(list)       # config_id -> list of (operator row, baseline row, delta dict)
    dropped = 0
    stolen = 0
    for (run_id, pair), d in pairs.items():
        if "baseline" not in d or "operator" not in d:
            dropped += 1
            continue
        o, b = d["operator"], d["baseline"]
        if max(steal_share(o), steal_share(b)) > MAX_STEAL:
            stolen += 1  # a VM's hypervisor took CPU time away during the run: timings are disturbed
            continue
        delta = {}
        for c in metric_cols:
            x, y = num(o.get(c)), num(b.get(c))
            delta[c] = None if x is None or y is None else x - y
        rec = {c: o.get(c, "") for c in id_cols}
        rec["run_id"] = run_id
        rec["pair"] = pair
        for c in OPERATOR_SIDE:
            rec[c] = o.get(c, "")
        rec["baseline_out_rows"] = b.get("out_rows", "")
        rec["same_thread"] = min(int(num(o.get("same_thread")) or 0), int(num(b.get("same_thread")) or 0))
        rec.update({c: fmt(v) for c, v in delta.items()})
        pair_rows.append(rec)
        by_config[o["config_id"]].append((o, b, delta))

    # pairs.csv
    pair_header = id_cols + ["run_id", "pair"] + OPERATOR_SIDE + ["baseline_out_rows", "same_thread"] + metric_cols
    write_csv(os.path.join(out_dir, "pairs.csv"), pair_header, pair_rows)

    # operator_costs.csv and baseline_costs.csv
    cost_rows, base_rows = [], []
    for cid, items in by_config.items():
        o0 = items[0][0]
        rec = {c: o0.get(c, "") for c in id_cols}
        rec["n_pairs"] = len(items)
        for c in OPERATOR_SIDE:
            vals = [num(o.get(c)) for o, _, _ in items]
            vals = [v for v in vals if v is not None]
            rec[c] = fmt(statistics.median(vals)) if vals else ""
        brec = {c: o0.get(c, "") for c in id_cols}
        brec["n_runs"] = len(items)
        brec["out_rows"] = items[0][1].get("out_rows", "")
        for c in metric_cols:
            ds = [d[c] for _, _, d in items if d[c] is not None]
            if ds:
                med = statistics.median(ds)
                iqr = quantile(ds, 0.75) - quantile(ds, 0.25)
                rec[c] = fmt(med)
                rec[c + "_iqr"] = fmt(iqr)
            bs = [num(b.get(c)) for _, b, _ in items]
            bs = [v for v in bs if v is not None]
            if bs:
                brec[c] = fmt(statistics.median(bs))
        # Is the operator's effect clearly larger than the noise between pairs? For Postgres the end-to-end time
        # (wall_exec_ns) is used, and its size: filter and projection usually make the plan cheaper (fewer rows
        # or bytes to Java), so their cost is negative. The column keeps the name used by the other profilers.
        cpu = [d.get("wall_exec_ns") for _, _, d in items]
        cpu = [v for v in cpu if v is not None]
        if len(cpu) >= 3:
            med, iqr = statistics.median(cpu), quantile(cpu, 0.75) - quantile(cpu, 0.25)
            rec["cpu_signal_to_noise"] = fmt(abs(med) / iqr) if iqr > 0 else "inf"
            rec["cpu_signal_ok"] = int(med != 0 and (iqr == 0 or abs(med) > iqr))
        else:
            rec["cpu_signal_to_noise"], rec["cpu_signal_ok"] = "", ""
        cost_rows.append(rec)
        base_rows.append(brec)

    sort_key = lambda r: (r.get("resource", ""), r.get("operator", ""), r.get("source", ""),
                          num(r.get("rows")) or 0, num(r.get("right_rows")) or 0, num(r.get("keys")) or 0,
                          num(r.get("sel")) or 0, num(r.get("words")) or 0)
    cost_rows.sort(key=sort_key)
    base_rows.sort(key=sort_key)
    cost_header = id_cols + ["n_pairs"] + OPERATOR_SIDE + ["cpu_signal_ok", "cpu_signal_to_noise"]
    for c in metric_cols:
        cost_header += [c, c + "_iqr"]
    write_csv(os.path.join(out_dir, "operator_costs.csv"), cost_header, cost_rows)
    write_csv(os.path.join(out_dir, "baseline_costs.csv"), id_cols + ["n_runs", "out_rows"] + metric_cols, base_rows)

    print(f"pairs: {len(pair_rows)}  configs: {len(cost_rows)}  unpaired runs dropped: {dropped}  "
          f"pairs disturbed by VM steal time dropped: {stolen}")
    print(f"wrote {out_dir}/pairs.csv, operator_costs.csv, baseline_costs.csv")
    if not args.quiet:
        print_summary(cost_rows)


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def print_summary(cost_rows):
    cols = [c for c in SUMMARY if any(r.get(c[0]) not in (None, "") for r in cost_rows)]
    head = f"{'resource':<20} {'operator':<12} {'src':<5} {'rows':>9} {'param':>10} {'out':>9} " + \
           " ".join(f"{label:>10}" for _, _, label in cols) + "  ok"
    print("\nOperator cost = operator run - baseline run (median over pairs)")
    print(head)
    print("-" * len(head))
    for r in cost_rows:
        op = r.get("operator", "")
        param = {"filter": f"sel={r.get('sel')}", "flatmap": f"w={r.get('words')}",
                 "reduceby": f"k={r.get('keys')}", "groupby": f"k={r.get('keys')}",
                 "distinct": f"k={r.get('keys')}", "join": f"k={r.get('keys')}",
                 "cartesian": f"r={r.get('right_rows')}"}.get(op, "")
        vals = []
        for c, div, _ in cols:
            v = num(r.get(c))
            vals.append(f"{v / div:>10.2f}" if v is not None else f"{'':>10}")
        print(f"{r.get('resource', ''):<20} {op:<12} {r.get('source', ''):<5} {r.get('rows', ''):>9} "
              f"{param:>10} {r.get('out_rows', ''):>9} " + " ".join(vals) +
              f"  {'yes' if str(r.get('cpu_signal_ok')) == '1' else 'no'}")
    print("\nok = the operator's effect on wall_exec_ns (either sign) is larger than the run-to-run spread.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Which resources matter for which operator on which platform: the basis for the cost model's per-resource experts.

    python3 feature_importance.py ../java-profiler/results-2026-11-02 ../spark-profiler/results
    python3 feature_importance.py ~/Downloads/wayang-cost-data            # the server export (all platforms)

Arguments are pairs.csv files or folders holding them (searched recursively). Writes to --out (default
results/feature-importance/): feature_importance.tex (the table), feature_importance.csv (every number behind it).

Method. One pairs.csv row = one operator plan run minus its baseline plan run (same JVM, same input), so every value
is what the operator itself adds. Pairs flagged disturbed=1 (low CPU clock, other processes busy) are left out. Per
config: the median over its pairs and the spread (IQR). For each platform, operator and resource:

  signal     share of configs whose |median| is above the spread between pairs (IQR) and above a floor (1 ms, 64 KiB)
  magnitude  how large the resource is in a typical config, in comparable units:
               time resources   share of the operator's added runtime: CPU = CPU time / (wall time x threads),
                                GC = GC time / (wall time x threads)
               byte resources   bytes per input byte (memory held, disk read+written, data moved)
               fixed overhead   fixed part of runtime = fixed + per_unit x (input + output rows) (two-point fit through
                                the smallest and largest 20% of configs) / the median added runtime
             reported as the median over configs (p90 too: resources that matter only in some configs)
  rating     important  (●): signal >= 50% of configs and median magnitude >= HIGH
             sometimes  (◐): signal >= 20% of configs and (median >= LOW or p90 >= HIGH)
             negligible (○): otherwise;   -- : not applicable / not measured on this platform
  measurable an operator whose added runtime is above noise in < 20% of its configs is marked "cost below noise"
  sign       a resource the operator REDUCES (median < 0, e.g. a filter in PostgreSQL sends fewer rows to Java)
             is rated by its size and marked with a minus
  drivers    for every important resource: the input properties whose Spearman rank correlation with it is >= 0.6
             (input rows, output rows, distinct keys, row width, selectivity, left x right rows)

Only the Python standard library is used.
"""
import argparse
import csv
import math
import os
import statistics
import sys
from collections import defaultdict

LOW = {"time": 0.05, "bytes": 0.05, "fixed": 0.10}
HIGH = {"time": 0.30, "bytes": 0.50, "fixed": 0.30}
FLOOR = {"time": 1e6, "bytes": 65536, "fixed": 1e6}
MIN_PAIRS = 3
DRIVER_RHO = 0.6

RESOURCES = ["cpu", "mem", "gc", "disk", "move", "fixed"]
LABEL = {"cpu": "CPU", "mem": "Memory", "gc": "GC", "disk": "Disk I/O", "move": "Data movement", "fixed": "Fixed overhead"}
KIND = {"cpu": "time", "gc": "time", "mem": "bytes", "disk": "bytes", "move": "bytes", "fixed": "fixed"}
PLATFORMS = ["java", "spark", "spark-cluster", "postgres"]
PLATFORM_LABEL = {"java": "Java", "spark": "Spark (single node)", "spark-cluster": "Spark (cluster)",
                  "postgres": "PostgreSQL"}
OP_LABEL = {"map": "Map", "filter": "Filter", "flatmap": "FlatMap", "reduceby": "ReduceBy", "groupby": "GroupBy",
            "globalreduce": "GlobalReduce", "count": "Count", "distinct": "Distinct", "sort": "Sort",
            "textsink": "TextFileSink", "join": "Join", "union": "UnionAll", "cartesian": "Cartesian",
            "projection": "Projection", "tablesink": "TableSink", "sqltostream": "SqlToStream (PG$\\to$Java)"}
OP_ORDER = ["map", "filter", "flatmap", "projection", "reduceby", "groupby", "globalreduce", "count", "distinct",
            "sort", "join", "union", "cartesian", "textsink", "tablesink", "sqltostream"]


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def first(r, *cols):
    for c in cols:
        v = num(r.get(c))
        if v is not None:
            return v
    return None


def total(r, *cols):
    vals = [num(r.get(c)) for c in cols]
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None


def platform_of(r):
    if r.get("postgres_execution_operator"):
        return "postgres"
    if r.get("spark_execution_operator"):
        return "spark-cluster" if r.get("deploy_mode") == "standalone-cluster" else "spark"
    if r.get("java_execution_operator"):
        return "java"
    return None


# Per platform: the measured column(s) of each resource, as operator - baseline. None = not applicable.
def resource_values(r, p):
    if p == "java":
        return {"cpu": num(r.get("thread_cpu_exec_ns")),
                "mem": num(r.get("mem_held_bytes")),  # only the memory probe: older rows have none = not measured
                "gc": None if first(r, "gc_time_exec_ms", "gc_time_ms") is None else first(r, "gc_time_exec_ms", "gc_time_ms") * 1e6,
                "disk": total(r, "os_disk_read_bytes", "os_disk_write_bytes"),
                "move": None}
    if p in ("spark", "spark-cluster"):
        slots = num(r.get("task_slots")) or 1
        peak = num(r.get("task_peak_execution_memory_bytes_max"))
        return {"cpu": num(r.get("task_cpu_ns")),
                "mem": None if peak is None else peak * slots,
                "gc": None if num(r.get("task_gc_ms")) is None else num(r.get("task_gc_ms")) * 1e6,
                "disk": total(r, "spill_disk_bytes", "output_bytes", "os_disk_read_bytes"),
                # single node: shuffle bytes (serialized, written and read back); cluster: bytes between executors
                "move": num(r.get("network_bytes")) if p == "spark-cluster" else num(r.get("shuffle_write_bytes"))}
    if p == "postgres":
        temp = num(r.get("db_temp_written_blocks"))
        return {"cpu": num(r.get("db_cpu_ns")),
                "mem": num(r.get("mem_held_bytes")),  # Java side: rows received (only the memory probe)
                "gc": None if first(r, "gc_time_exec_ms", "gc_time_ms") is None else first(r, "gc_time_exec_ms", "gc_time_ms") * 1e6,
                "disk": total(r, "db_disk_read_bytes", "db_disk_write_bytes") if temp is None
                else (total(r, "db_disk_read_bytes", "db_disk_write_bytes") or 0) + temp * 8192,
                "move": num(r.get("network_bytes_in"))}  # result rows sent from PostgreSQL to Java
    return {}


def threads(r, p):
    if p in ("spark", "spark-cluster"):
        return num(r.get("task_slots")) or 1
    return 1.0


def features(r):
    rows, right = num(r.get("rows")) or 0, num(r.get("right_rows")) or 0
    if r.get("operator") not in ("join", "union", "cartesian"):
        right = 0
    in_bytes = (num(r.get("in_bytes")) or 0) + (num(r.get("right_in_bytes")) or 0)
    f = {"input rows": rows + right, "output rows": num(r.get("out_rows")), "distinct keys": num(r.get("keys")),
         "row width": in_bytes / (rows + right) if rows + right else None}
    if r.get("operator") == "filter":
        f["selectivity"] = num(r.get("sel"))
    if r.get("operator") in ("join", "cartesian"):
        f["left x right rows"] = rows * right
    return f


def ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    rk = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            rk[order[k]] = (i + j) / 2
        i = j + 1
    return rk


def spearman(a, b):
    pts = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    if len(pts) < 8:
        return None
    ra, rb = ranks([p[0] for p in pts]), ranks([p[1] for p in pts])
    ma, mb = statistics.mean(ra), statistics.mean(rb)
    sa = math.sqrt(sum((x - ma) ** 2 for x in ra))
    sb = math.sqrt(sum((y - mb) ** 2 for y in rb))
    if sa == 0 or sb == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(ra, rb)) / (sa * sb)


def quantile(xs, q):
    xs = sorted(xs)
    pos = (len(xs) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def find_pairs(paths):
    out = []
    for p in paths:
        if os.path.isfile(p):
            out.append(p)
        else:
            for d, _, files in os.walk(p):
                out += [os.path.join(d, f) for f in files if f == "pairs.csv"]
    return sorted(set(out))


def load(paths):
    """platform -> operator -> config_id -> list of pair rows (clean pairs only)."""
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    dropped = defaultdict(int)
    for path in find_pairs(paths):
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                p = platform_of(r)
                if p is None or r.get("operator") in ("burn", "sleep"):
                    continue
                if r.get("disturbed") == "1":
                    dropped[p] += 1
                    continue
                data[p][r["operator"]][r["config_id"]].append(r)
    return data, dropped


def config_stats(prs, p):
    """One config: median and IQR of every resource, the wall cost, and the input features."""
    if len(prs) < MIN_PAIRS:
        return None
    vals = defaultdict(list)
    walls = []
    for r in prs:
        for k, v in resource_values(r, p).items():
            if v is not None:
                vals[k].append(v)
        w = num(r.get("wall_exec_ns"))
        if w is not None:
            walls.append(w)
    if len(walls) < MIN_PAIRS:
        return None
    out = {"wall": statistics.median(walls), "wall_iqr": quantile(walls, .75) - quantile(walls, .25),
           "threads": threads(prs[0], p), "features": features(prs[0]), "in_bytes": None, "res": {}}
    out["features"]["output rows"] = statistics.median([num(r.get("out_rows")) or 0 for r in prs])
    ib = (num(prs[0].get("in_bytes")) or 0) + (num(prs[0].get("right_in_bytes")) or 0)
    if prs[0].get("operator") not in ("join", "union", "cartesian"):
        ib = num(prs[0].get("in_bytes")) or 0
    out["in_bytes"] = ib or None
    out["rows_in"] = out["features"]["input rows"]
    out["work"] = out["features"]["input rows"] + (out["features"]["output rows"] or 0)
    for k, v in vals.items():
        if len(v) >= MIN_PAIRS:
            out["res"][k] = (statistics.median(v), quantile(v, .75) - quantile(v, .25))
    return out


def rate(signal, med, p90, kind):
    if signal is None or med is None:
        return "na"
    if signal >= 0.5 and med >= HIGH[kind]:
        return "high"
    if signal >= 0.2 and (med >= LOW[kind] or (p90 is not None and p90 >= HIGH[kind])):
        return "some"
    return "low"


def analyse(data):
    rows = []
    for p in PLATFORMS:
        if p not in data:
            continue
        for op in sorted(data[p], key=lambda o: OP_ORDER.index(o) if o in OP_ORDER else 99):
            cfgs = [s for s in (config_stats(v, p) for v in data[p][op].values()) if s]
            if len(cfgs) < 5:
                continue
            wall_ok = [c for c in cfgs if c["wall"] > max(FLOOR["time"], c["wall_iqr"])]
            rec = {"platform": p, "operator": op, "configs": len(cfgs), "configs_wall_signal": len(wall_ok),
                   "measurable": len(wall_ok) / len(cfgs)}
            for k in RESOURCES:
                kind = KIND[k]
                if k == "fixed":
                    # runtime = fixed + per_unit x work, work = input rows + output rows; two-point fit through the
                    # medians of the smallest and the largest 20% of configs; share = fixed / median added runtime
                    ok = sorted(cfgs, key=lambda c: c["work"])
                    n = max(3, len(ok) // 5)
                    lo, hi = ok[:n], ok[-n:]
                    ws, wl = statistics.median([c["wall"] for c in lo]), statistics.median([c["wall"] for c in hi])
                    ns, nl = statistics.median([c["work"] for c in lo]), statistics.median([c["work"] for c in hi])
                    typical = statistics.median([c["wall"] for c in cfgs])
                    if typical <= FLOOR["time"] or nl <= ns:
                        rec[k] = dict(rating="low", median=None, p90=None, signal=None, sign=1, drivers="")
                        continue
                    fixed = ws - (wl - ws) / (nl - ns) * ns
                    share = max(0.0, fixed) / typical
                    sig = sum(1 for c in lo if c["wall"] > max(FLOOR["time"], c["wall_iqr"])) / n
                    rec[k] = dict(rating=rate(sig, min(share, 1.0), None, kind), median=share, p90=None, signal=sig,
                                  sign=1, drivers="")
                    continue
                have = [c for c in cfgs if k in c["res"]]
                if not have:
                    rec[k] = dict(rating="na", median=None, p90=None, signal=None, sign=1, drivers="")
                    continue
                sig = sum(1 for c in have if abs(c["res"][k][0]) > max(FLOOR[kind], c["res"][k][1])) / len(have)
                if kind == "time":
                    mags = [abs(c["res"][k][0]) / (c["wall"] * c["threads"]) for c in wall_ok if k in c["res"]]
                    mags = [min(m, 1.0) for m in mags]
                else:
                    mags = [abs(c["res"][k][0]) / c["in_bytes"] for c in have if c["in_bytes"]]
                med = statistics.median(mags) if mags else None
                p90 = quantile(mags, .9) if mags else None
                sign = -1 if statistics.median([c["res"][k][0] for c in have]) < 0 else 1
                rating = rate(sig, med, p90, kind)
                drivers = ""
                if rating == "high":
                    vals = [c["res"][k][0] for c in have]
                    cand = []
                    for fname in have[0]["features"]:
                        rho = spearman([c["features"].get(fname) for c in have], vals)
                        if rho is not None and abs(rho) >= DRIVER_RHO:
                            cand.append((abs(rho), fname))
                    drivers = ", ".join(f for _, f in sorted(cand, reverse=True)[:2])
                rec[k] = dict(rating=rating, median=med, p90=p90, signal=sig, sign=sign, drivers=drivers)
            # Runtime drivers
            cand = []
            rec["rho"] = {}
            for fname in cfgs[0]["features"]:
                rho = spearman([c["features"].get(fname) for c in cfgs], [c["wall"] for c in cfgs])
                rec["rho"][fname] = rho
                if rho is not None and abs(rho) >= DRIVER_RHO:
                    cand.append((abs(rho), fname))
            rec["runtime_drivers"] = ", ".join(f for _, f in sorted(cand, reverse=True)[:2])
            rows.append(rec)
    return rows


SYMBOL = {"high": "\\high", "some": "\\some", "low": "\\low", "na": "--"}
MIN_MEASURABLE = 0.2  # operators whose added runtime is above noise in fewer configs: "cost below noise"


def cheap(rec):
    return rec["measurable"] < MIN_MEASURABLE
TEXT = {"high": "important", "some": "sometimes", "low": "negligible", "na": "n/a"}
APPLICABLE = {"java": {"move": False}, "spark": {}, "spark-cluster": {}, "postgres": {}}


def cell(rec, k):
    r = rec[k]
    if APPLICABLE.get(rec["platform"], {}).get(k) is False:
        return "--"
    if cheap(rec):
        return "$\\cdot$"
    if r["rating"] == "na":
        return "--"
    s = SYMBOL[r["rating"]]
    if r["sign"] < 0 and r["rating"] != "low":
        s += "$^{-}$"
    return s


def dominant(rec):
    order = {"high": 2, "some": 1}
    best = [k for k in RESOURCES if k != "fixed" and rec[k]["rating"] == "high"]
    return best


def latex(rows, sources, dropped):
    out = []
    out.append("% Generated by Cost-Model/model/feature_importance.py from: " + "; ".join(sources))
    out.append("% Needs \\usepackage{booktabs,wasysym}. Symbols:")
    out.append("\\providecommand{\\high}{\\CIRCLE}\\providecommand{\\some}{\\LEFTcircle}\\providecommand{\\low}{\\Circle}")
    out.append("\\begin{table*}[t]")
    out.append("\\centering\\small")
    out.append("\\caption{Which resources each operator consumes, per platform, measured as the operator plan minus its "
               "baseline plan (source$\\to$sink). \\high\\ important: above run-to-run noise in $\\geq$50\\% of the "
               "configurations and, in the median configuration, $\\geq$30\\% of the operator's added runtime (CPU, GC) or "
               "$\\geq$0.5 byte per input byte (memory, disk, data movement), or $\\geq$30\\% of the runtime already at the "
               "fixed part of a linear fit of runtime to input+output rows (fixed overhead); \\some\\ matters in some configurations; \\low\\ negligible; "
               "-- not applicable; $\\cdot$ the operator's added runtime is below run-to-run noise in $>$80\\% of the configurations. $^{-}$: the operator reduces this resource relative to its baseline. "
               "Data movement: Spark shuffle bytes (single node), bytes between executors (cluster), "
               "result bytes PostgreSQL$\\to$Java. Drivers: input properties with Spearman $|\\rho|\\geq 0.6$ "
               "to the operator's added runtime.}")
    out.append("\\label{tab:resource-importance}")
    out.append("\\begin{tabular}{l" + "c" * len(RESOURCES) + "l}")
    out.append("\\toprule")
    out.append("Operator & " + " & ".join(LABEL[k] for k in RESOURCES) + " & Runtime driven by \\\\")
    out.append("\\midrule")
    first_block = True
    for p in PLATFORMS:
        prs = [r for r in rows if r["platform"] == p]
        if not prs:
            continue
        if not first_block:
            out.append("\\midrule")
        first_block = False
        n_cfg = sum(r["configs"] for r in prs)
        out.append(f"\\multicolumn{{{len(RESOURCES) + 2}}}{{l}}{{\\textbf{{{PLATFORM_LABEL[p]}}} "
                   f"\\footnotesize({n_cfg:,} configurations)}} \\\\".replace(",", "{,}"))
        for r in prs:
            drv = "\\emph{cost below noise}" if cheap(r) else (r["runtime_drivers"].replace(" x ", " $\\times$ ") or "--")
            out.append(f"{OP_LABEL.get(r['operator'], r['operator'])} & "
                       + " & ".join(cell(r, k) for k in RESOURCES) + f" & {drv} \\\\")
    out.append("\\bottomrule")
    out.append("\\end{tabular}")
    out.append("\\end{table*}")
    return "\n".join(out) + "\n"


def write_csv(rows, path):
    cols = ["platform", "operator", "configs", "configs_wall_signal"]
    for k in RESOURCES:
        cols += [f"{k}_rating", f"{k}_median", f"{k}_p90", f"{k}_signal", f"{k}_sign", f"{k}_drivers"]
    cols += ["runtime_drivers", "measurable"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            d = {c: r.get(c, "") for c in ("platform", "operator", "configs", "configs_wall_signal", "runtime_drivers")}
            d["measurable"] = f"{r['measurable']:.2f}"
            for k in RESOURCES:
                x = r[k]
                d.update({f"{k}_rating": TEXT[x["rating"]] if APPLICABLE.get(r["platform"], {}).get(k) is not False else "n/a",
                          f"{k}_median": "" if x["median"] is None else f"{x['median']:.4g}",
                          f"{k}_p90": "" if x["p90"] is None else f"{x['p90']:.4g}",
                          f"{k}_signal": "" if x["signal"] is None else f"{x['signal']:.2f}",
                          f"{k}_sign": x["sign"], f"{k}_drivers": x["drivers"]})
            w.writerow(d)


# ------------------------------------------------------------------------------------------
# The report: a stand-alone LaTeX document (python3 feature_importance.py DATA... --report)
# ------------------------------------------------------------------------------------------
FEATURES = ["input rows", "output rows", "distinct keys", "row width", "selectivity", "left x right rows"]
FEATURE_TEX = {"input rows": "input rows", "output rows": "output rows", "distinct keys": "distinct keys",
               "row width": "row width", "selectivity": "selectivity", "left x right rows": "left$\\times$right rows"}
RES_TEX = {"cpu": "CPU", "mem": "memory", "gc": "garbage collection", "disk": "disk I/O",
           "move": "data movement", "fixed": "fixed per-job overhead"}
SOURCES_TABLE = {
    "java": {"cpu": "thread\\_cpu\\_exec\\_ns", "mem": "mem\\_held\\_bytes", "gc": "gc\\_time\\_exec\\_ms",
             "disk": "os\\_disk\\_read/write\\_bytes", "move": "--"},
    "spark": {"cpu": "task\\_cpu\\_ns", "mem": "task\\_peak\\_execution\\_memory\\_bytes\\_max $\\times$ task\\_slots",
              "gc": "task\\_gc\\_ms", "disk": "spill\\_disk\\_bytes + output\\_bytes + os\\_disk\\_read\\_bytes",
              "move": "shuffle\\_write\\_bytes"},
    "spark-cluster": {"cpu": "task\\_cpu\\_ns", "mem": "task\\_peak\\_execution\\_memory\\_bytes\\_max $\\times$ task\\_slots",
                      "gc": "task\\_gc\\_ms", "disk": "spill\\_disk\\_bytes + output\\_bytes + os\\_disk\\_read\\_bytes",
                      "move": "network\\_bytes (shuffle read from another executor)"},
    "postgres": {"cpu": "db\\_cpu\\_ns (database server processes)", "mem": "mem\\_held\\_bytes (Java side)",
                 "gc": "gc\\_time\\_exec\\_ms (Java side)",
                 "disk": "db\\_disk\\_read/write\\_bytes + 8\\,KiB $\\times$ db\\_temp\\_written\\_blocks",
                 "move": "network\\_bytes\\_in (result rows PostgreSQL$\\to$Java)"},
}


def tex_escape(t):
    return t.replace("_", "\\_").replace("%", "\\%").replace("&", "\\&")


def ops_list(rows, pred):
    return ", ".join(OP_LABEL.get(r["operator"], r["operator"]) for r in rows if pred(r))


def platform_summary(prs, p, stat):
    """Plain-language verdict for one platform, computed from the ratings: (needed, some, dropped, unmeasured, cheap)."""
    live = [r for r in prs if not cheap(r)]
    needed, some, dropped, unmeasured = [], [], [], []
    for k in RESOURCES:
        if APPLICABLE.get(p, {}).get(k) is False:
            continue
        rs = [r for r in live if r[k]["rating"] != "na"]
        if not rs:
            unmeasured.append(k)
            continue
        high = [r for r in rs if r[k]["rating"] == "high"]
        mid = [r for r in rs if r[k]["rating"] == "some"]
        if high:
            needed.append((k, high))
        elif mid:
            some.append((k, mid))
        else:
            dropped.append(k)
    return needed, some, dropped, unmeasured, [r for r in prs if cheap(r)]


def report_tex(rows, data, dropped_pairs, sources):
    L = []

    def w(t):  # prose and bold run-in headings start a new paragraph; LaTeX commands go on as they are
        L.append(("\n" + t) if (t[:1].isalpha() or t.startswith("\\textbf{") or t.startswith("\\hl{")) else t)

    plats = [p for p in PLATFORMS if p in data]
    missing = [PLATFORM_LABEL[p] for p in PLATFORMS if p not in data]
    stats = {}
    for p in plats:
        cfg = sum(len(v) for v in data[p].values())
        prs = sum(len(c) for v in data[p].values() for c in v.values())
        stats[p] = dict(configs=cfg, pairs=prs, disturbed=dropped_pairs.get(p, 0))
    w(r"\documentclass[11pt,a4paper]{article}")
    w(r"\usepackage[margin=2.2cm]{geometry}")
    w(r"\usepackage{amsmath,booktabs,wasysym,array,xcolor,url,hyperref}")
    w(r"\usepackage[T1]{fontenc}\usepackage{lmodern}")
    w(r"\providecommand{\high}{\CIRCLE}\providecommand{\some}{\LEFTcircle}\providecommand{\low}{\Circle}")
    w(r"\newcommand{\hl}[1]{\textbf{#1}}")
    w(r"\setlength{\parindent}{0pt}\setlength{\parskip}{0.55em}")
    w(r"\title{Which resources and inputs does a cost model for Apache Wayang operators need?\\[2pt]"
      r"\large A measurement-based analysis for the Java, Spark and PostgreSQL platforms}")
    w(r"\author{}\date{}")
    w(r"\begin{document}\maketitle")
    # ---- summary
    w(r"\section{Summary}")
    w(r"A cost model for a Wayang operator predicts how much of each resource the operator uses: CPU time, memory, "
      r"garbage collection (GC), disk I/O, data moved between processes or machines, and a fixed overhead per job. "
      r"Every extra resource is another prediction target and another source of error, so we measured which ones "
      r"actually matter. For each platform and operator we ran the operator inside a plan and subtracted the same "
      r"plan without it, so every number below is what the operator itself adds. A resource is \hl{needed} when "
      r"it is clearly above measurement noise and large relative to the operator's total cost; it is \hl{not needed} "
      r"when it is negligible for every operator of the platform.")
    for p in plats:
        prs = [r for r in rows if r["platform"] == p]
        needed, some, dropped, unmeasured, cheap_ops = platform_summary(prs, p, stats[p])
        w(r"\textbf{" + PLATFORM_LABEL[p] + r"}\vspace{-0.3em}\begin{itemize}\itemsep0pt")
        if needed:
            w(r"\item \hl{Needed:} " + "; ".join(f"{RES_TEX[k]} ({', '.join(OP_LABEL.get(r['operator'], r['operator']) for r in rs)})"
                                              for k, rs in needed) + ".")
        if some:
            w(r"\item Only in some configurations: " + "; ".join(
                f"{RES_TEX[k]} ({', '.join(OP_LABEL.get(r['operator'], r['operator']) for r in rs)})" for k, rs in some) + ".")
        if dropped:
            w(r"\item \hl{Not needed} (negligible for every operator): " + ", ".join(RES_TEX[k] for k in dropped) + ".")
        if unmeasured:
            w(r"\item \emph{Not assessed in this data} (not measured): " + ", ".join(RES_TEX[k] for k in unmeasured) + ".")
        if cheap_ops:
            w(r"\item Operators whose own cost is below measurement noise: "
              + ", ".join(OP_LABEL.get(r["operator"], r["operator"]) for r in cheap_ops)
              + r" (a constant or a per-row cost from the source/sink is enough).")
        w(r"\end{itemize}")
    if missing:
        w(r"\textbf{Pending:} " + ", ".join(missing) + r" are not in the data used for this version of the report.")
    # ---- method
    w(r"\section{Method}")
    w(r"\textbf{Measurement.} Each configuration (operator, input size, number of distinct keys, row width, "
      r"selectivity, JVM and platform settings) runs in a fresh JVM. It executes two plans alternately after warm-up "
      r"runs: a \emph{baseline} (source $\to$ parse $\to$ sink) and the \emph{operator plan} (the same with the "
      r"operator inserted). The operator's cost is the difference of the two runs of a pair, in every metric; a "
      r"configuration has 3--7 pairs. Metrics are read from the JVM and the operating system over the window from "
      r"the first record to the end of the job, so Wayang's own optimisation is excluded. Pairs with a run "
      r"measured at a reduced CPU clock or next to other busy processes are flagged by the profiler and left out here.")
    w(r"\textbf{Resources and the columns they come from.}")
    w(r"\begin{center}\small\begin{tabular}{@{}llp{9.2cm}@{}}\toprule")
    w(r"Platform & Resource & Measured column\\\midrule")
    for p in plats:
        first = True
        for k in ["cpu", "mem", "gc", "disk", "move"]:
            src = SOURCES_TABLE[p][k]
            if src == "--":
                continue
            w((PLATFORM_LABEL[p] if first else "") + f" & {LABEL[k]} & {src}\\\\")
            first = False
        w(r"\midrule")
    w(r"\bottomrule\end{tabular}\end{center}")
    w(r"The \emph{fixed overhead} is not a column: runtime is fitted as $\text{fixed} + \text{per-unit}\times"
      r"(\text{input rows}+\text{output rows})$ through the medians of the smallest and the largest 20\% of the "
      r"configurations of the operator, and the fixed part is compared with the median added runtime.")
    w(r"\textbf{Rating.} For every resource and operator we compute (i) the \emph{signal}: the share of "
      r"configurations whose median added amount is above the spread between pairs (IQR) and above a floor "
      r"(1\,ms, 64\,KiB), and (ii) the \emph{magnitude} in the median configuration: for CPU and GC the share of the "
      r"operator's added runtime (CPU time $/$ (wall time $\times$ threads)); for memory, disk and data movement "
      r"bytes per input byte; for the fixed overhead its share of the median added runtime. "
      r"\high~\emph{important}: signal $\geq$ 50\% and magnitude $\geq$ 0.30 (time) or 0.50 (bytes); "
      r"\some~\emph{sometimes}: signal $\geq$ 20\% and magnitude $\geq$ 0.05, or the 90th percentile $\geq$ the "
      r"important threshold; \low~\emph{negligible}: otherwise. A superscript $-$ means the operator \emph{reduces} the "
      r"resource compared to its baseline (e.g.\ a filter sends fewer rows onward). A dot means the operator's added "
      r"runtime is below noise in more than 80\% of its configurations, so no rating is meaningful.")
    w(r"\textbf{Input properties.} For every operator we compute the Spearman rank correlation $\rho$ between its "
      r"added runtime and each property of the configuration (input rows, output rows, distinct keys, row width, "
      r"selectivity, left$\times$right rows for binary operators). $|\rho|\geq 0.6$ counts as \emph{needed}, "
      r"$0.3\leq|\rho|<0.6$ as \emph{weak}, below as \emph{not needed}.")
    # ---- data used
    w(r"\subsection*{Data used}")
    w(r"\begin{center}\small\begin{tabular}{lrrr}\toprule Platform & Configurations & Clean pairs & Disturbed pairs left out\\\midrule")
    for p in plats:
        w(f"{PLATFORM_LABEL[p]} & {stats[p]['configs']:,} & {stats[p]['pairs']:,} & {stats[p]['disturbed']:,}\\\\".replace(",", "\\,"))
    w(r"\bottomrule\end{tabular}\end{center}")
    w(r"Source files: " + ", ".join(r"\path{" + x + "}" for x in sources) + ".")
    # ---- results per platform
    w(r"\section{Results by platform}")
    for p in plats:
        prs = [r for r in rows if r["platform"] == p]
        w(r"\subsection{" + PLATFORM_LABEL[p] + "}")
        w(r"\begin{center}\small\begin{tabular}{l" + "c" * len(RESOURCES) + r"}\toprule")
        w("Operator & " + " & ".join(LABEL[k] for k in RESOURCES) + r"\\\midrule")
        for r in prs:
            w(OP_LABEL.get(r["operator"], r["operator"]) + " & " + " & ".join(cell(r, k) for k in RESOURCES) + r"\\")
        w(r"\bottomrule\end{tabular}\end{center}")
        w(r"\textbf{Magnitudes} (median configuration; CPU, GC and fixed overhead: share of added runtime, "
          r"memory, disk, data movement: bytes per input byte).")
        w(r"\begin{center}\footnotesize\begin{tabular}{lr" + "r" * len(RESOURCES) + r"}\toprule")
        w("Operator & Configs & " + " & ".join(LABEL[k] for k in RESOURCES) + r"\\\midrule")
        for r in prs:
            if cheap(r):
                w(OP_LABEL.get(r["operator"], r["operator"]) + f" & {r['configs']} & \\multicolumn{{{len(RESOURCES)}}}{{c}}{{\\emph{{cost below noise}}}}\\\\")
                continue
            cells = []
            for k in RESOURCES:
                x = r[k]
                cells.append("--" if x["rating"] == "na" or APPLICABLE.get(p, {}).get(k) is False
                             else ("--" if x["median"] is None else f"{x['median']:.2f}"))
            w(OP_LABEL.get(r["operator"], r["operator"]) + f" & {r['configs']} & " + " & ".join(cells) + r"\\")
        w(r"\bottomrule\end{tabular}\end{center}")
        w(r"A dash means not applicable or not measured. ``Configs'' is the number of configurations (each with 3--7 pairs) "
          r"behind the operator's row: ratings from a handful of configurations are weak evidence.")
        # input properties
        feats = [x for x in FEATURES if any(r.get("rho", {}).get(x) is not None for r in prs)]
        w(r"\textbf{Input properties} (Spearman $\rho$ with the operator's added runtime; bold: needed; a dash: not applicable, constant in the data, or fewer than 8 configurations).")
        w(r"\begin{center}\footnotesize\begin{tabular}{l" + "r" * len(feats) + r"}\toprule")
        w("Operator & " + " & ".join(FEATURE_TEX[x] for x in feats) + r"\\\midrule")
        for r in prs:
            if cheap(r):
                continue
            cells = []
            for x in feats:
                v = r["rho"].get(x)
                if v is None:
                    cells.append("--")
                elif abs(v) >= DRIVER_RHO:
                    cells.append(f"\\hl{{{v:+.2f}}}")
                else:
                    cells.append(f"{v:+.2f}")
            w(OP_LABEL.get(r["operator"], r["operator"]) + " & " + " & ".join(cells) + r"\\")
        w(r"\bottomrule\end{tabular}\end{center}")
    # ---- implications
    w(r"\section{What this means for the cost model}")
    w(r"\begin{itemize}\itemsep2pt")
    for p in plats:
        prs = [r for r in rows if r["platform"] == p]
        needed, some, dropped, unmeasured, cheap_ops = platform_summary(prs, p, stats[p])
        keep = [RES_TEX[k] for k, _ in needed]
        fixed_ops = [r for r in prs if not cheap(r) and r["fixed"]["rating"] in ("high", "some")]
        t = r"\item \textbf{" + PLATFORM_LABEL[p] + r".} Predict " + (", ".join(keep) if keep else "no resource separately")
        t += "."
        if dropped:
            t += " " + ", ".join(RES_TEX[k] for k in dropped).capitalize() + " can be left out or kept as a constant."
        if fixed_ops:
            t += (" A per-job fixed cost matters for " + ", ".join(OP_LABEL.get(r["operator"], r["operator"]) for r in fixed_ops) + ".")
        w(t)
    w(r"\item \textbf{Inputs.} Use the properties marked needed in the matrices above per operator; properties with "
      r"$|\rho|<0.3$ for an operator carry no usable signal for it. Output rows is often the strongest driver of "
      r"filtering and grouping operators, but is unknown before execution: the model should receive Wayang's cardinality "
      r"estimate for it.")
    w(r"\end{itemize}")
    # ---- limits
    w(r"\section{Limitations}")
    w(r"\begin{itemize}\itemsep2pt")
    w(r"\item On the Java platform an operator runs on one thread, so its CPU time equals its runtime: CPU is then "
      r"the resource by construction, and the other resources decide only what \emph{else} must be predicted.")
    w(r"\item Ratings describe the configurations that were measured (inputs of $10^3$--$3\cdot10^6$ rows, synthetic "
      r"data with one row layout); a resource rated negligible here can matter for much larger inputs.")
    w(r"\item An operator whose cost is below noise in most configurations is not ``unimportant'': it is cheap next to "
      r"reading and parsing its input. Its cost can still add up in long pipelines.")
    w(r"\item Data movement for Spark on a single node is the shuffle volume written; only a cluster measures real network "
      r"traffic between executors.")
    w(r"\item Energy and hardware counters (cycles, instructions) exist on macOS only and are not part of this analysis.")
    w(r"\end{itemize}")
    w(r"\end{document}")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data", nargs="+", help="pairs.csv files or folders containing them")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "feature-importance"))
    ap.add_argument("--report", action="store_true", help="also write report.tex, a stand-alone LaTeX report")
    a = ap.parse_args()
    data, dropped = load(a.data)
    if not data:
        sys.exit("no pairs.csv rows found")
    rows = analyse(data)
    os.makedirs(a.out, exist_ok=True)
    sources = find_pairs(a.data)
    with open(os.path.join(a.out, "feature_importance.tex"), "w") as f:
        f.write(latex(rows, [os.path.relpath(s) for s in sources], dropped))
    write_csv(rows, os.path.join(a.out, "feature_importance.csv"))
    if a.report:
        with open(os.path.join(a.out, "report.tex"), "w") as f:
            f.write(report_tex(rows, data, dropped, [os.path.relpath(x) for x in sources]))
    for p in PLATFORMS:
        if p in data:
            print(f"{PLATFORM_LABEL[p]}: {sum(len(v) for v in data[p].values())} configs, "
                  f"{dropped.get(p, 0)} disturbed pairs left out")
    sym = {"high": "●", "some": "◐", "low": "○", "na": "-"}
    print(f"\n{'platform':14} {'operator':13} " + " ".join(f"{LABEL[k][:8]:>8}" for k in RESOURCES) + "  runtime driven by")
    for r in rows:
        cells = []
        for k in RESOURCES:
            if APPLICABLE.get(r["platform"], {}).get(k) is False:
                cells.append(f"{'-':>8}")
                continue
            x = r[k]
            m = "" if x["median"] is None else f"{x['median']:.2f}"
            if cheap(r):
                cells.append(f"{'·':>8}")
                continue
            cells.append(f"{sym[x['rating']] + ('-' if x['sign'] < 0 and x['rating'] != 'low' else ' ') + m:>8}")
        print(f"{r['platform']:14} {r['operator']:13} " + " ".join(cells) + "  "
              + ("cost below noise" if cheap(r) else r["runtime_drivers"]) + f"   [{r['measurable']:.0%} measurable]")
    print(f"\nwrote {a.out}/feature_importance.tex and .csv" + (" and report.tex" if a.report else ""))


if __name__ == "__main__":
    main()

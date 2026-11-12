#!/usr/bin/env python3
"""What running lanes side by side does to the measurements.

collect.sh measures the first configs of the first java and spark lane ALONE before any lane starts
(RUN/calibration/<platform>/raw.csv), on that lane's CPUs. The lane then measures the same configs first, on the same
CPUs, while all lanes run (RUN/lanes/<lane>/raw.csv). This script compares the two:

    python3 calibration_report.py ../collect          text report on stdout, per-config ratios in RUN/data/calibration.csv

For each config: median over runs of the operator plan, of the baseline plan, and of the operator cost (operator -
baseline per pair), then the ratio under load / alone. 1.00 = the lanes change nothing, 1.10 = 10 % more under load.
Reported per metric as median and 10th..90th percentile over configs. The first line starts with SUMMARY (collect.log).
Only the Python standard library is used.
"""
import csv
import os
import statistics
import sys
from collections import defaultdict

METRICS = {
    "java": [("wall_exec_ns", "wall"), ("thread_cpu_exec_ns", "operator-thread CPU"), ("process_cpu_exec_ns", "process CPU")],
    "spark": [("wall_exec_ns", "wall"), ("task_cpu_ns", "task CPU"), ("process_cpu_exec_ns", "process CPU")],
}
MIN_NS = 5e6  # values below 5 ms are mostly noise: no ratio


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def read(path):
    try:
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    except OSError:
        return []


def medians(rows, col):
    """config_id -> {"operator": median, "baseline": median, "cost": median of per-pair differences}."""
    runs = defaultdict(lambda: defaultdict(list))
    pairs = defaultdict(dict)
    for r in rows:
        v = num(r.get(col))
        if v is None:
            continue
        runs[r["config_id"]][r["plan"]].append(v)
        pairs[(r["config_id"], r["run_id"], r["pair"])][r["plan"]] = v
    cost = defaultdict(list)
    for (cid, _, _), p in pairs.items():
        if "operator" in p and "baseline" in p:
            cost[cid].append(p["operator"] - p["baseline"])
    out = {}
    for cid, plans in runs.items():
        out[cid] = {k: statistics.median(v) for k, v in plans.items()}
        if cost[cid]:
            out[cid]["cost"] = statistics.median(cost[cid])
    return out


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run = sys.argv[1]
    lines, summary, out_rows = [], [], []
    for platform, metrics in METRICS.items():
        c = os.path.join(run, "calibration", platform)
        alone = read(os.path.join(c, "raw.csv"))
        try:
            lane = open(os.path.join(c, "lane.name")).read().strip()
        except OSError:
            continue
        ids = {r["config_id"] for r in alone}
        loaded = [r for r in read(os.path.join(run, "lanes", lane, "raw.csv"))
                  if r["config_id"] in ids and r.get("tag", "").startswith("lane-")]
        both = ids & {r["config_id"] for r in loaded}
        lines.append(f"== {platform}: {len(ids)} configs measured alone, {len(both)} of them also measured by lane {lane} "
                     f"under load")
        if not both:
            summary.append(f"{platform}: no config measured both ways yet")
            continue
        parts = []
        for col, label in metrics:
            a, l = medians(alone, col), medians(loaded, col)
            for kind in ("operator", "baseline", "cost"):
                ratios = []
                for cid in sorted(both):
                    x, y = a.get(cid, {}).get(kind), l.get(cid, {}).get(kind)
                    if x is None or y is None or x < MIN_NS or y <= 0:
                        continue
                    ratios.append(y / x)
                    out_rows.append({"platform": platform, "lane": lane, "config_id": cid, "metric": col, "kind": kind,
                                     "alone": f"{x:.0f}", "loaded": f"{y:.0f}", "ratio": f"{y / x:.4f}"})
                if not ratios:
                    continue
                med = statistics.median(ratios)
                lines.append(f"   {label:20} {kind:9} x{med:.3f}  (p10..p90 {pct(ratios, .1):.3f}..{pct(ratios, .9):.3f}, "
                             f"{len(ratios)} configs)")
                if kind == "operator":
                    parts.append(f"{label} x{med:.2f}")
        summary.append(f"{platform}: " + (", ".join(parts) or f"runs under {MIN_NS / 1e6:.0f} ms, too short to compare"))
    print("SUMMARY lanes vs alone (operator runs, median over configs): " + "; ".join(summary or ["no calibration data"]))
    print("\n".join(lines))
    print("\nRatio = under load / alone, same configs, same CPUs. Report it next to results from lane data.")
    if out_rows:
        os.makedirs(os.path.join(run, "data"), exist_ok=True)
        with open(os.path.join(run, "data", "calibration.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
            w.writeheader()
            w.writerows(out_rows)


if __name__ == "__main__":
    main()

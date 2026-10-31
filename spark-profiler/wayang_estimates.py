#!/usr/bin/env python3
"""Adds Wayang's own cost estimates to rows that are already in raw.csv.

New runs of the profiler write the wayang_* columns themselves. This script is for rows measured before
that: Wayang's estimate of a plan depends only on the config (operator, input, settings), not on the run,
so it can be computed afterwards without measuring again. `run-profiling.sh --add-estimates` uses it:

    wayang_estimates.py configs raw.csv
        prints one line per config in raw.csv:  op|source|rows|right_rows|keys|sel|words|resource|cores
        (cores = Spark task threads, from shuffle_partitions / 2; empty for the Java platform)

    wayang_estimates.py merge raw.csv wayang_estimates.csv
        copies the wayang_* columns of each (config_id, plan) from the profiler's --estimate-only output
        into every matching raw.csv row. Existing wayang_* values are replaced, so it can be run again.

Only the Python standard library is used.
"""
import csv
import os
import sys

BINARY = {"join", "union", "cartesian"}


def read(path):
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        return list(r.fieldnames or []), list(r)


def configs(raw_path):
    _, rows = read(raw_path)
    seen = set()
    for r in rows:
        cid = r["config_id"]
        if cid in seen:
            continue
        seen.add(cid)
        right = r.get("right_rows") or "0"
        if r["operator"] not in BINARY:
            right = "0"
        sel = r.get("sel") or "0.5"
        cores = ""
        sp = r.get("shuffle_partitions")
        if sp:  # Spark: Wayang uses 2 x cores shuffle partitions
            cores = str(int(float(sp)) // 2)
        print("|".join([r["operator"], r["source"], r["rows"], right, r["keys"], sel, r["words"],
                        r["resource"], cores]))


def merge(raw_path, est_path):
    header, rows = read(raw_path)
    est_header, est_rows = read(est_path)
    est = {(e["config_id"], e["plan"]): e for e in est_rows}
    cols = [c for c in est_header if c.startswith("wayang_") and c != "wayang_operator"]

    missing, plan_mismatch, filled = set(), set(), 0
    for r in rows:
        key = (r["config_id"], r["plan"])
        e = est.get(key)
        if e is None:
            missing.add(key)
            continue
        # Correctness check: the estimate must be for the same execution plan that was measured.
        if r.get("plan_execution_operators") and e.get("plan_execution_operators") != r["plan_execution_operators"]:
            plan_mismatch.add((key, r["plan_execution_operators"], e.get("plan_execution_operators")))
            continue
        for c in cols:
            r[c] = e.get(c, "")
        filled += 1

    if plan_mismatch:
        for (cid, plan), measured, estimated in sorted(plan_mismatch):
            print(f"ERROR {cid} {plan}: measured plan {measured} but Wayang now picks {estimated}", file=sys.stderr)
        sys.exit("raw.csv not changed: the Wayang version or settings differ from the measured runs")

    new_header = header + [c for c in cols if c not in header]
    tmp = raw_path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=new_header, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, raw_path)
    print(f"added Wayang estimates to {filled} of {len(rows)} rows in {raw_path}")
    if missing:
        for cid, plan in sorted(missing):
            print(f"WARNING no estimate for {cid} {plan}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "configs":
        configs(sys.argv[2])
    elif len(sys.argv) == 4 and sys.argv[1] == "merge":
        merge(sys.argv[2], sys.argv[3])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()

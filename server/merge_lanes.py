#!/usr/bin/env python3
"""Merges the raw.csv files of all lanes into one raw.csv per platform (safe while the lanes are still writing).

    python3 merge_lanes.py RUN_DIR      ->  RUN_DIR/data/{java,spark,postgres,spark-cluster}/raw.csv

A lane appends a whole config (all its runs) at once; a line still being written at the moment of reading is left
out (only complete lines up to the last newline are taken), so the merged file never holds a cut row. Columns are
the union of all lanes' columns (the same profiler code writes the same columns; the union only matters if a lane
ran an older build). Prints one line per platform: files, rows, configs.
"""
import csv
import glob
import io
import os
import sys

PLATFORMS = {
    "java": "lanes/java-*/raw.csv",
    "spark": "lanes/spark-*/raw.csv",
    "postgres": "lanes/postgres-*/raw.csv",
    "spark-cluster": "lanes/cluster/spark-cluster/raw.csv",
}


def complete_text(path):
    with open(path, "rb") as f:
        b = f.read()
    cut = b.rfind(b"\n") + 1
    return b[:cut].decode("utf-8", errors="replace")


def main():
    run = sys.argv[1]
    csv.field_size_limit(1 << 30)
    for platform, pattern in PLATFORMS.items():
        files = sorted(glob.glob(os.path.join(run, pattern)))
        files = [f for f in files if os.path.getsize(f) > 0]
        if not files:
            continue
        header, rows = [], []
        for f in files:
            reader = csv.DictReader(io.StringIO(complete_text(f)))
            for col in reader.fieldnames or []:
                if col not in header:
                    header.append(col)
            rows += list(reader)
        out_dir = os.path.join(run, "data", platform)
        os.makedirs(out_dir, exist_ok=True)
        tmp = os.path.join(out_dir, "raw.csv.tmp")
        with open(tmp, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, os.path.join(out_dir, "raw.csv"))
        configs = len({r.get("config_id") for r in rows})
        print(f"  {platform:14} {len(files):3} lane files  {len(rows):8} runs  {configs:7} configs")


if __name__ == "__main__":
    main()

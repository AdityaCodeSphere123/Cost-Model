#!/usr/bin/env python3
"""Random configs for a long profiling run, in the format run-profiling.sh --config-list reads.

    python3 sweep.py --platform java  --cores 32 --mem-gb 128 --count 60000 --seed 1 > java.list
    python3 sweep.py --platform spark --cores 32 --mem-gb 128 --count 60000 --seed 2 > spark.list
    python3 sweep.py --platform spark-cluster --shapes "1x6 2x6 4x6" --executor-memory-gb 6 --count 5000 > cluster.list

Each line: HEAP:GC:CPUS|op|source|rows|right_rows|keys|sel|words. The list is in random order, so a run that
stops early (time limit, crash) still covers the whole space evenly. Values are taken on log scales because
cost changes noticeably only when a value changes by ~25% (see ../plan.md):

    rows        1,000 .. 3,000,000 in x1.25 steps
    keys        1, 2, 4, ... up to rows          (reduceby, groupby, distinct; join via fan-out)
    sel         0.05 .. 1.0 in steps of 0.05     (filter)
    words       1 .. 64 (flatmap), else 2 / 8 / 32
    join        fan-out 1, 2, 10, 100 (keys = rows / fan-out); output <= 20M rows
    cartesian   1k .. 100k  x  10 .. 1,000; output <= 20M rows
    settings    the main setting (8 GB heap, G1, all cores) preferred (~40% of configs), the rest random heap / GC / cores

spark-cluster: every data config is written once per cluster shape (NxC = N executors with C cores), next to each
other in random shape order, so all shapes are measured on the same configs and any prefix of the list is
balanced across shapes. --count is the number of data configs (lines = count x shapes). Lines look like
DRIVERHEAP:G1:NxC|op|...; heap checks use the executor memory, since the executors hold the data.
"""
import argparse
import random
import sys

OPS = ["map", "filter", "flatmap", "reduceby", "groupby", "globalreduce", "count", "distinct", "sort",
       "textsink", "join", "union", "cartesian"]
BINARY = {"join", "union", "cartesian"}
ROW_LEVELS = sorted({int(round(1000 * 1.25 ** i)) for i in range(40) if 1000 * 1.25 ** i <= 3_000_000})
MAX_OUT = 20_000_000


def heap_bytes(h):
    return int(h[:-1]) * (1 << 30 if h[-1] == "g" else 1 << 20)


def fits_heap(op, rows, right, words, heap):
    """Same rule as run-profiling.sh: operators that hold their whole input must fit in 60% of the heap."""
    per = 120 + words * 14
    need = {"sort": rows, "groupby": rows, "globalreduce": rows, "join": right, "cartesian": right}.get(op, 0) * per * 3
    return need < heap_bytes(heap) * 0.6


def settings(platform, cores, mem_gb):
    # Each heap must leave room for the OS, Spark's off-heap memory and the page cache: at most a third of RAM.
    heaps = [h for h in ("2g", "4g", "8g", "16g") if int(h[:-1]) <= mem_gb / 3] or ["1g"]
    main_heap = "8g" if "8g" in heaps else heaps[-1]
    core_opts = sorted({c for c in (1, 2, 4, 8, 16, 32, 64) if c <= cores} | {cores})
    if platform == "java":  # one operator thread; cores only change GC/JIT threads, so fewer options
        core_opts = sorted({c for c in (1, 2, 4) if c <= cores} | {cores})
    gcs = ["G1", "Parallel", "Serial"]
    main = f"{main_heap}:G1:{cores}"
    others = [f"{h}:{g}:{c}" for h in heaps for g in gcs for c in core_opts if f"{h}:{g}:{c}" != main]
    return main, others


def sample(rng, op, main, others, p_main=0.6):
    res = main if rng.random() < p_main else rng.choice(others)
    data = sample_data(rng, op, res.split(":")[0])
    return f"{res}|{data}" if data else None


def sample_data(rng, op, heap):
    """A data config without the setting: op|source|rows|right_rows|keys|sel|words, or None if it does not fit."""
    source = "collection" if rng.random() < 0.15 else "text"
    levels = ROW_LEVELS if source == "text" else [r for r in ROW_LEVELS if r <= 1_000_000]
    rows = rng.choice(levels)
    right, keys, sel = 0, 1000, 0.5
    words = rng.choice([1, 2, 4, 8, 16, 32, 64]) if op == "flatmap" else rng.choice([2, 8, 8, 32])
    if op == "filter":
        sel = round(rng.randint(1, 20) * 0.05, 2)
    elif op in ("reduceby", "groupby", "distinct"):
        keys = min(rows, 2 ** rng.randint(0, max(0, rows.bit_length() - 1)))
    elif op == "join":
        right = rng.choice([r for r in levels if rows / 10 <= r <= rows * 10])
        keys = max(1, rows // rng.choice([1, 2, 10, 100]))
        if rows * right / keys > MAX_OUT:
            return None
    elif op == "union":
        right = rng.choice(levels)
    elif op == "cartesian":
        rows = rng.choice([r for r in levels if r <= 100_000])
        right = rng.choice([10, 20, 50, 100, 200, 500, 1000])
        if rows * right > MAX_OUT:
            return None
    if not fits_heap(op, rows, right, words, heap):
        return None
    return f"{op}|{source}|{rows}|{right}|{keys}|{sel}|{words}"


def cluster_list(rng, shapes, driver_heap, exec_heap, count):
    """count data configs, each once per shape (shape order shuffled per config), operators round-robin."""
    seen, out = set(), []
    while len(seen) < count:
        before = len(seen)
        for op in rng.sample(OPS, len(OPS)):
            for _ in range(300):
                d = sample_data(rng, op, exec_heap)
                if d and d not in seen:
                    seen.add(d)
                    out += [f"{driver_heap}:G1:{s}|{d}" for s in rng.sample(shapes, len(shapes))]
                    break
            if len(seen) >= count:
                break
        if len(seen) == before:
            break
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--platform", choices=["java", "spark", "spark-cluster"], required=True)
    ap.add_argument("--cores", type=int, help="cores of the server (java, spark)")
    ap.add_argument("--mem-gb", type=int, help="RAM of the server in GB (java, spark)")
    ap.add_argument("--count", type=int, default=60000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--shapes", help='spark-cluster: cluster shapes "NxC ...", e.g. "1x6 2x6 3x6"')
    ap.add_argument("--driver-heap", default="8g", help="spark-cluster: heap of the driver (default 8g)")
    ap.add_argument("--executor-memory-gb", type=int, default=4, help="spark-cluster: heap of each executor")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    if args.platform == "spark-cluster":
        shapes = (args.shapes or "").split()
        if not shapes or not all(s.count("x") == 1 and all(p.isdigit() and int(p) > 0 for p in s.split("x")) for s in shapes):
            ap.error('--shapes "NxC ..." is needed for spark-cluster')
        out = cluster_list(rng, shapes, args.driver_heap, f"{args.executor_memory_gb}g", args.count)
        print(f"# spark-cluster: {len(out) // len(shapes)} data configs x {len(shapes)} shapes, seed {args.seed}", file=sys.stderr)
        print("\n".join(out))
        return
    if args.cores is None or args.mem_gb is None:
        ap.error("--cores and --mem-gb are needed")
    main_res, others = settings(args.platform, args.cores, args.mem_gb)
    seen, out = set(), []
    # Operators round-robin (reshuffled each round), so every prefix of the list is balanced across
    # operators: a run that only gets through the first 15k configs is still an even sample.
    while len(out) < args.count:
        before = len(out)
        for op in rng.sample(OPS, len(OPS)):
            for attempt in range(300):
                # Prefer the main setting; once its unique configs for this operator run out, use others.
                c = sample(rng, op, main_res, others, p_main=0.6 if attempt < 100 else 0.0)
                if c and c not in seen:
                    seen.add(c)
                    out.append(c)
                    break
            if len(out) >= args.count:
                break
        if len(out) == before:
            break  # the space is exhausted
    print(f"# {args.platform}: {len(out)} configs, main setting {main_res}, seed {args.seed}", file=sys.stderr)
    print("\n".join(out))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Random configs for a long profiling run, in the format run-profiling.sh --config-list reads.

    python3 sweep.py --platform java  --cores 32 --mem-gb 128 --count 60000 --seed 1 > java.list
    python3 sweep.py --platform spark --cores 32 --mem-gb 128 --count 60000 --seed 2 > spark.list

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
    heap = res.split(":")[0]
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
    return f"{res}|{op}|{source}|{rows}|{right}|{keys}|{sel}|{words}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--platform", choices=["java", "spark"], required=True)
    ap.add_argument("--cores", type=int, required=True, help="cores of the server")
    ap.add_argument("--mem-gb", type=int, required=True, help="RAM of the server in GB")
    ap.add_argument("--count", type=int, default=60000)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    rng = random.Random(args.seed)
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

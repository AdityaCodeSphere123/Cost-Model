#!/usr/bin/env python3
"""Splits the machine into profiling lanes that run at the same time without sharing CPUs.

    python3 plan_lanes.py --cpus 0-63 --mem-gb 200                 # automatic plan, as JSON
    python3 plan_lanes.py --cpus 0-63 --mem-gb 200 --plan "cluster=10x3 java=4x2 spark=3x4 postgres=2x6"
    python3 plan_lanes.py ... --text                                # human-readable

A lane is one profiler loop pinned (taskset) to its own CPUs:
  java      one JVM at a time (Wayang's Java platform runs the operator on one thread; the 2nd CPU takes GC/JIT)
  spark     Spark local mode, local[1..lane CPUs]
  postgres  its own PostgreSQL server (own port, own data folder) + the Java side, both on the lane's CPUs, so the
            database CPU the profiler reads belongs to this lane only
  cluster   one Spark standalone cluster at a time: 2 CPUs for driver + master, then 1..10 executors of C CPUs

Housekeeping: the last CPUs (one physical core by default, --housekeeping) belong to no lane. collect.sh runs
itself, the merges and the status there, so that work never lands on a lane's CPUs during a measurement.

CPUs are handed out by physical core (both hyperthreads of a core go to the same lane) and NUMA node in order,
so lanes do not share a core's execution units. Memory is split: the cluster lane gets 40%, the other lanes share
the rest in proportion to their CPUs; each lane's largest JVM heap is ~60% of its share.
"""
import argparse
import json
import subprocess
import sys


def expand(spec):
    out = []
    for part in spec.split(","):
        a, _, b = part.strip().partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


def topology(cpus):
    """Logical CPUs ordered so that hyperthread siblings and NUMA nodes are adjacent."""
    key = {c: (0, 0, c) for c in cpus}
    try:
        out = subprocess.run(["lscpu", "-p=CPU,CORE,NODE"], capture_output=True, text=True, timeout=10).stdout
        for line in out.splitlines():
            if line.startswith("#"):
                continue
            f = line.split(",")
            c, core = int(f[0]), int(f[1] or 0)
            node = int(f[2]) if len(f) > 2 and f[2] else 0
            if c in key:
                key[c] = (node, core, c)
    except (OSError, subprocess.SubprocessError, ValueError):
        pass
    return sorted(cpus, key=lambda c: key[c])


def heap_for(mem_gb):
    """Largest heap level (GB) that fits ~60% of a lane's memory."""
    best = 1
    for h in (2, 4, 8, 16, 32):
        if h <= 0.6 * mem_gb:
            best = h
    return best


def auto_counts(k):
    """Lane counts for k CPUs: (cluster executors, cores per executor, [(platform, lanes, cpus per lane)])."""
    c = max(1, min(6, int((0.5 * k - 2) // 10)))
    n_exec = 10
    if 2 + 10 * c > k - 6:                       # small machine: fewer executors so the other lanes get CPUs
        n_exec = max(1, (k - 8) // c // 1)
        n_exec = min(10, n_exec)
    rest = k - (2 + n_exec * c)
    pg_c = 6 if rest >= 24 else (4 if rest >= 10 else 2)
    n_java = max(1, round(rest * 0.25 / 2))
    n_pg = max(1, round(rest * 0.375 / pg_c))
    left = rest - 2 * n_java - pg_c * n_pg
    while left < 2 and n_java > 1:              # keep at least 2 CPUs for a Spark lane
        n_java -= 1
        left += 2
    spark_c = 4 if left >= 4 else max(2, left)
    n_spark = max(1, left // spark_c)
    return n_exec, c, [("java", n_java, 2), ("postgres", n_pg, pg_c), ("spark", n_spark, spark_c)]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cpus", required=True, help='logical CPUs to use, e.g. "0-63"')
    ap.add_argument("--mem-gb", type=float, required=True, help="memory to use in GB")
    ap.add_argument("--plan", default="", help='e.g. "cluster=10x3 java=4x2 spark=3x4 postgres=2x6" (lanes x CPUs; '
                                               'cluster=NxC: up to N executors of C CPUs; "cluster=0" = none)')
    ap.add_argument("--base-port", type=int, default=7300, help="first port (Spark masters, PostgreSQL servers)")
    ap.add_argument("--housekeeping", type=int, default=-1,
                    help="CPUs kept free of lanes for collect.sh, merges and the OS (default: 2 on 24+ CPUs, 1 on 8+, else 0)")
    ap.add_argument("--text", action="store_true")
    a = ap.parse_args()

    cpus = topology(expand(a.cpus))
    hk_n = a.housekeeping if a.housekeeping >= 0 else (2 if len(cpus) >= 24 else 1 if len(cpus) >= 8 else 0)
    housekeeping = cpus[len(cpus) - hk_n:] if hk_n else []  # last in topology order: siblings of one core
    cpus = cpus[:len(cpus) - hk_n]
    k = len(cpus)
    if a.plan:
        n_exec, c, lanes = 0, 0, []
        for item in a.plan.split():
            name, _, val = item.partition("=")
            if name == "cluster":
                if val not in ("0", ""):
                    n_exec, c = (int(x) for x in val.split("x"))
            elif name in ("java", "spark", "postgres"):
                n, per = (int(x) for x in val.split("x"))
                lanes.append((name, n, per))
            else:
                sys.exit(f"unknown lane type '{name}' in --plan")
    else:
        n_exec, c, lanes = auto_counts(k)

    need = (2 + n_exec * c if n_exec else 0) + sum(n * per for _, n, per in lanes)
    if need > k:
        sys.exit(f"the plan needs {need} CPUs but only {k} are available ({len(housekeeping)} more kept for "
                 f"housekeeping): use --plan with fewer lanes")

    mem = a.mem_gb
    plan = {"cpus_total": k + len(housekeeping), "mem_gb": mem, "housekeeping_cpus": housekeeping, "lanes": []}
    pos = 0
    port = a.base_port
    # Memory weight: Java lanes count double (one CPU, but the big sort / group / join inputs need a 4g+ heap).
    weight = {"java": 2.0, "spark": 1.0, "postgres": 1.0}
    rest_w = sum(n * per * weight[p] for p, n, per in lanes)
    cluster_mem = 0.4 * mem if n_exec else 0
    if n_exec:
        # The executors need >= 1 GB heap each (same formula as run-cluster.sh): take more memory for the cluster
        # lane if 40% is not enough, as long as the other lanes keep at least half; otherwise stop with a clear message.
        driver = 8 if cluster_mem >= 64 else 4
        need = (driver + 0.5 * (n_exec + 1) + 2 + n_exec * 1.2 * 1) / 0.75
        if cluster_mem < need:
            if need <= 0.5 * mem:
                cluster_mem = need + 1
            else:
                fit = int((0.5 * mem * 0.75 - driver - 2.5) // 1.7)
                hint = f"cluster={fit}x{c}" if fit >= 1 else "cluster=0"
                sys.exit(f"{mem:.0f} GB memory is not enough for a cluster of {n_exec} executors ({need:.0f} GB) next to the "
                         f"other lanes: use --plan with fewer executors (e.g. {hint}) or more --mem-gb")
        take = cpus[pos:pos + 2 + n_exec * c]
        pos += len(take)
        plan["lanes"].append({"name": "cluster", "platform": "spark-cluster", "cpus": take, "reserve": 2,
                              "executors": n_exec, "executor_cores": c, "mem_gb": round(cluster_mem, 1),
                              "driver_heap": f"{driver}g", "port": port,
                              "executor_heap_gb": max(1, min(8, int((cluster_mem * 0.75 - driver - 0.5 * (n_exec + 1) - 2)
                                                                    / (n_exec * 1.2))))})
        port += 10
    other_mem = mem - cluster_mem
    for platform, n, per in lanes:
        for i in range(1, n + 1):
            take = cpus[pos:pos + per]
            pos += per
            lane_mem = other_mem * per * weight[platform] / max(1, rest_w)
            plan["lanes"].append({"name": f"{platform}-{i}", "platform": platform, "cpus": take,
                                  "mem_gb": round(lane_mem, 1), "max_heap_gb": heap_for(lane_mem), "port": port})
            port += 1
    plan["cpus_used"] = pos
    if a.text:
        print(f"{k + len(housekeeping)} CPUs, {mem:.0f} GB memory -> {len(plan['lanes'])} lanes, {pos} CPUs used, "
              f"housekeeping on CPUs {','.join(map(str, housekeeping)) or 'none'}")
        for l in plan["lanes"]:
            extra = (f"executors 1..{l['executors']} x {l['executor_cores']} CPUs, {l['executor_heap_gb']}g each, driver {l['driver_heap']}"
                     if l["platform"] == "spark-cluster" else f"heaps up to {l['max_heap_gb']}g")
            print(f"  {l['name']:11} CPUs {len(l['cpus']):3}  memory {l['mem_gb']:6.1f} GB  port {l['port']}  {extra}")
    else:
        print(json.dumps(plan, indent=1))


if __name__ == "__main__":
    main()

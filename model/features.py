"""Turns the profilers' pairs.csv rows into model inputs and training targets.

One row of pairs.csv = one measured operator (operator run - baseline run). For each row we build:

    network inputs   operator, implementation, platform, GC, source  (categories -> embeddings)
                     platform config: cores, heap, partitions        (numbers -> per-platform adapter)
                     context: words, bytes per row, sel, keys/rows   (numbers)
    basis            row counts n_left, n_right, n_out and distinct keys (never shown to the network, not even
                     as ratios, so a prediction can only grow when any of them grows)
                     Wayang's estimate per resource (CPU core-s, I/O GB, network GB, memory GB)
    aux              raw values the formulas need: task threads, heap GB, price per ms
    targets          measured CPU core-s, I/O GB, network GB, memory GB, runtime s, energy J

All definitions of what a target means are in TARGETS below, in one place.
"""
import csv
import math
import os
from dataclasses import dataclass, field

import numpy as np

RESOURCES = ["cpu", "io", "net", "mem"]          # one expert each, in this order
OUTPUTS = RESOURCES + ["runtime", "energy"]       # everything the model predicts and is trained on

CATEGORIES = ["operator", "impl", "platform", "gc", "source"]
CFG_COLS = ["log_cores", "log_heap_gb", "log_shuffle_partitions", "log_in_partitions"]
# No feature here may be computed from a row count (e.g. keys / rows): that would let the network see the
# row counts and break the guarantee that more rows never give a lower prediction.
CTX_COLS = ["log_words", "log_bytes_per_row", "sel", "binary"]
N_BASIS = 6                                       # 1, n_left, n_right, n_out, n_in*log(n_in), n_keys  (see basis())


def num(v, default=float("nan")):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def pos(x):
    """Operator - baseline differences can be slightly negative (noise); costs are >= 0."""
    return max(0.0, x) if math.isfinite(x) else x


def platform_of(r):
    """A 'platform' column if present, else the prefix of the filled <platform>_execution_operator column."""
    if (r.get("platform") or "").strip():
        return r["platform"].strip().lower()
    for k, v in r.items():
        if k.endswith("_execution_operator") and (v or "").strip():
            return k[: -len("_execution_operator")].lower()
    return "unknown"


def impl_of(r):
    return (r.get("execution_operator") or r.get(r["_platform"] + "_execution_operator") or "").strip()


def gc_kind(name):
    n = (name or "").lower()
    if "g1" in n:
        return "G1"
    if "ps " in n or "parallel" in n:
        return "Parallel"
    if "copy" in n or "marksweepcompact" in n:
        return "Serial"
    if "zgc" in n:
        return "Z"
    return "other"


def wayang_mid(r, col):
    """Middle of Wayang's lower..upper interval (negative differences clipped to 0)."""
    lo, hi = pos(num(r.get(col + "_lower"), 0.0)), pos(num(r.get(col + "_upper"), 0.0))
    return (lo + hi) / 2


# ------------------------------------------------------------------------------------------
# Targets: what the model learns. Columns are in ns, bytes or nJ; targets are seconds, GB, joules (/ 1e9).
# A new platform either writes the generic "default" columns or gets its own entry here.
# ------------------------------------------------------------------------------------------
TARGET_COLUMNS = {
    "java": {
        "cpu": ["thread_cpu_exec_ns"],                     # the one thread that runs the operator
        "io": ["os_disk_read_bytes", "os_disk_write_bytes"],
        "net": ["network_bytes"],                          # always 0: one JVM
        # Held memory from the profiler's memory run; older rows only have the live heap after a GC.
        "mem": ["mem_held_bytes|live_heap_after_gc_max_bytes"],
        "runtime": ["wall_exec_ns"],
        "energy": ["os_energy_nj"],
    },
    "spark": {
        "cpu": ["task_cpu_ns"],                            # all Spark tasks
        "io": ["spill_disk_bytes", "os_disk_read_bytes"],  # disk only; shuffle is counted as network demand
        # Network demand: shuffle bytes (on a cluster ~(1 - 1/executors) of them cross the network) plus the
        # results sent to the driver. On one machine nothing crosses a network, but the demand is the same.
        "net": ["shuffle_write_bytes", "task_result_bytes"],
        # Memory used at the same time: largest per-task peak x task threads (the sum over all tasks
        # counts tasks that ran one after the other).
        "mem": ["task_peak_execution_memory_bytes_max*task_slots"],
        "runtime": ["wall_exec_ns"],
        "energy": ["os_energy_nj"],
    },
    "default": {
        "cpu": ["cpu_ns"],
        "io": ["io_bytes"],
        "net": ["network_bytes"],
        "mem": ["mem_bytes"],
        "runtime": ["wall_exec_ns"],
        "energy": ["os_energy_nj"],
    },
}


def _column(r, spec):
    """One column, 'a|b' = a if present else b, or 'a*b' = a times b."""
    if "|" in spec:
        for c in spec.split("|"):
            v = num(r.get(c))
            if math.isfinite(v):
                return v
        return float("nan")
    if "*" in spec:
        a, b = spec.split("*")
        return num(r.get(a)) * num(r.get(b))
    return num(r.get(spec))


def target(r, platform, k):
    """Measured value of output k (sum of its columns), or NaN if none of them is in the row."""
    cols = TARGET_COLUMNS.get(platform, TARGET_COLUMNS["default"])[k]
    vals = [_column(r, c) for c in cols]
    vals = [v for v in vals if math.isfinite(v)]
    return sum(vals) / 1e9 if vals else float("nan")

# Below these sizes a difference is noise; the loss and q-error treat values in units of this size.
SCALES = {"cpu": 1e-3, "io": 1e-4, "net": 1e-4, "mem": 1e-3, "runtime": 1e-3, "energy": 1e-2}


# ------------------------------------------------------------------------------------------
# Rows -> arrays
# ------------------------------------------------------------------------------------------
def load_rows(paths):
    """Rows of pairs.csv files; baseline_costs.csv files become 'scan' rows (source -> parse -> sink)."""
    rows = []
    for path in paths:
        if "baseline_costs" in os.path.basename(path):
            rows.extend(scan_rows(path))
            continue
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                r["_platform"] = platform_of(r)
                r["_group"] = r["_platform"] + "|" + r["config_id"]  # all pairs of one config stay together
                rows.append(r)
    return rows


def scan_rows(path):
    """The profilers' baseline (source -> parse -> sink, one input) as an operator called 'scan'.

    Many operator configs share the same baseline, so each (platform, setting, input) is kept once. With it the
    model can predict a whole job: its scans plus its operators.
    """
    seen, out = set(), []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("baseline_plan") != "source>parse>sink":
                continue  # two-input baselines (join, union, cartesian) are not a single scan
            p = platform_of(r)
            key = (p, r.get("resource"), r.get("source"), r.get("rows"), r.get("keys"), r.get("words"))
            if key in seen:
                continue
            seen.add(key)
            r = dict(r)
            r[p + "_execution_operator"] = ("Spark" if p == "spark" else p.capitalize()) + "Scan"
            r.update({"operator": "scan", "sel": "", "right_rows": "0", "right_in_bytes": "0", "pair": "0",
                      "out_rows": r.get("out_rows") or r.get("rows")})
            r["config_id"] = "|".join(str(x) for x in key[1:]) + "|scan"
            r["_platform"] = p
            r["_group"] = p + "|" + r["config_id"]
            out.append(r)
    return out


def is_binary(r):
    return r.get("operator") in ("join", "union", "cartesian") or num(r.get("right_rows"), 0.0) > 0


def cardinalities(r, mode="true"):
    """n_left, n_right, n_out, n_keys. mode='true': measured output rows; 'wayang': Wayang's estimate of them."""
    n_left = max(0.0, num(r.get("rows"), 0.0))
    n_right = max(0.0, num(r.get("right_rows"), 0.0)) if is_binary(r) else 0.0
    if mode == "wayang":
        if not r.get("wayang_est_out_rows_lower"):
            raise ValueError("--cardinalities wayang needs the wayang_est_out_rows_* columns")
        n_out = (num(r.get("wayang_est_out_rows_lower"), 0.0) + num(r.get("wayang_est_out_rows_upper"), 0.0)) / 2
    else:
        n_out = num(r.get("out_rows"), 0.0)
    n_keys = min(max(1.0, num(r.get("keys"), 1.0)), max(1.0, n_left + n_right))  # can't exceed the rows
    return n_left, n_right, max(0.0, n_out), n_keys


def basis(n_left, n_right, n_out, n_keys):
    """Basis functions of the counts (in millions). Each is >= 0 and never decreases when a count grows."""
    n_in = n_left + n_right
    u = 1e-6
    return [1.0, n_left * u, n_right * u, n_out * u, n_in * u * math.log2(1.0 + n_in) / 20.0, n_keys * u]


@dataclass
class Vocab:
    """Category -> index. Index 0 is 'unknown'."""
    maps: dict = field(default_factory=lambda: {c: {"<unk>": 0} for c in CATEGORIES})

    def fit(self, rows):
        for r in rows:
            for c, v in category_values(r).items():
                self.maps[c].setdefault(v, len(self.maps[c]))
        return self

    def index(self, c, v):
        return self.maps[c].get(v, 0)

    def size(self, c):
        return len(self.maps[c])


def category_values(r):
    return {
        "operator": r.get("operator", ""),
        "impl": impl_of(r),
        "platform": r["_platform"],
        "gc": gc_kind(r.get("gc_name")),
        "source": r.get("source", ""),
    }


def threads(r):
    """Threads that run the operator: task_slots (Spark) or a 'threads' column; else 1 (Java runs one thread)."""
    for c in ("task_slots", "threads"):
        v = num(r.get(c))
        if math.isfinite(v):
            return max(1.0, v)
    return 1.0


def numeric_features(r):
    rows = max(1.0, num(r.get("rows"), 1.0))
    binary = 1.0 if is_binary(r) else 0.0
    cores = num(r.get("task_slots"))
    if not math.isfinite(cores):
        cores = num(r.get("cpus"), 1.0)
    in_parts = num(r.get("in_partitions"), 1.0) + num(r.get("right_in_partitions"), 0.0)
    cfg = [
        math.log2(max(1.0, cores if math.isfinite(cores) else 1.0)),
        math.log2(max(1e-3, num(r.get("max_heap_bytes"), 1e9) / 1e9)),
        math.log2(1.0 + num(r.get("shuffle_partitions"), 0.0)),
        math.log2(1.0 + in_parts),
    ]
    sel = num(r.get("sel"))
    ctx = [
        math.log2(max(1.0, num(r.get("words"), 1.0))),
        math.log2(max(1.0, num(r.get("in_bytes"), rows) / rows)),
        sel if r.get("operator") == "filter" and math.isfinite(sel) else 1.0,
        binary,
    ]
    return cfg, ctx


def wayang_basis(r):
    """Wayang's estimate per resource, in the targets' units, plus its runtime estimate (seconds)."""
    mhz = num(r.get("wayang_cpu_mhz"), 2700.0)
    return {
        "cpu": wayang_mid(r, "wayang_est_cpu_cycles") / (mhz * 1e6),  # cycles at Wayang's clock -> core-s
        "io": wayang_mid(r, "wayang_est_disk_bytes") / 1e9,
        "net": wayang_mid(r, "wayang_est_network_bytes") / 1e9,
        "mem": wayang_mid(r, "wayang_est_ram_bytes") / 1e9,
        "runtime": pos(num(r.get("wayang_est_time_ms_gmean"), 0.0)) / 1e3,
    }


def build_arrays(rows, vocab, cards="true"):
    """All model inputs and targets as numpy arrays (targets are NaN where not measured)."""
    n = len(rows)
    cat = np.zeros((n, len(CATEGORIES)), dtype=np.int64)
    cfg = np.zeros((n, len(CFG_COLS)), dtype=np.float32)
    ctx = np.zeros((n, len(CTX_COLS)), dtype=np.float32)
    bas = np.zeros((n, N_BASIS), dtype=np.float32)
    way = np.zeros((n, len(RESOURCES)), dtype=np.float32)
    way_time = np.zeros(n, dtype=np.float32)
    aux = np.zeros((n, 3), dtype=np.float32)       # threads, heap GB, price per ms
    tgt = np.full((n, len(OUTPUTS)), np.nan, dtype=np.float32)
    for i, r in enumerate(rows):
        cv = category_values(r)
        cat[i] = [vocab.index(c, cv[c]) for c in CATEGORIES]
        cfg[i], ctx[i] = numeric_features(r)
        bas[i] = basis(*cardinalities(r, cards))
        wb = wayang_basis(r)
        way[i] = [wb[k] for k in RESOURCES]
        way_time[i] = wb["runtime"]
        aux[i] = [threads(r), num(r.get("max_heap_bytes"), 4e9) / 1e9, num(r.get("wayang_cost_per_ms"), 1.0)]
        tgt[i] = [pos(target(r, r["_platform"], k)) for k in OUTPUTS]
    return {"cat": cat, "cfg": cfg, "ctx": ctx, "basis": bas, "wayang": way, "wayang_time": way_time,
            "aux": aux, "target": tgt}


@dataclass
class Normalizer:
    """Standardizes cfg and ctx with the training set's mean and spread."""
    mean: dict = field(default_factory=dict)
    std: dict = field(default_factory=dict)

    def fit(self, arrays):
        for k in ("cfg", "ctx"):
            self.mean[k] = arrays[k].mean(0).tolist()
            s = arrays[k].std(0)
            self.std[k] = np.where(s > 1e-6, s, 1.0).tolist()
        return self

    def apply(self, arrays):
        out = dict(arrays)
        for k in ("cfg", "ctx"):
            out[k] = ((arrays[k] - np.array(self.mean[k], dtype=np.float32))
                      / np.array(self.std[k], dtype=np.float32)).astype(np.float32)
        return out

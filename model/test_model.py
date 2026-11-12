#!/usr/bin/env python3
"""Checks that the cost model does what it is designed to do. Run: python test_model.py

1. monotone (model):    more rows / keys never give a lower prediction, for random inputs and weights
2. monotone (pipeline): the same through the real CSV -> features path, for each count on its own
3. learns:              on synthetic data with a known answer, recovers Wayang's correction and the row part
4. new platform:        a platform the code has never heard of ("flink", generic columns) is detected and
                        trained, from scratch and few-shot; few-shot leaves old platforms exactly unchanged,
                        improves the new one, and works for models of any size; save/load and predict.py work
"""
import csv
import json
import math
import os
import subprocess
import sys
import tempfile

import numpy as np
import torch

from features import (CATEGORIES, CFG_COLS, CTX_COLS, OUTPUTS, RESOURCES, SCALES, Normalizer, Vocab, basis,
                      build_arrays, load_rows, target)
from model import CostModel
from train import load_checkpoint, loss_fn, predict_np, qerror, to_tensors

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = [os.path.join(HERE, "..", p, "results", "pairs.csv") for p in ("java-profiler", "spark-profiler")]
SIZES = {"operator": 4, "impl": 4, "platform": 3, "gc": 2, "source": 2}
CHECKED = RESOURCES + ["runtime", "energy", "money"]


def random_model(vocab_sizes, seed):
    torch.manual_seed(seed)
    model = CostModel(vocab_sizes)
    with torch.no_grad():  # random weights, so the check covers far more than the starting point
        for p in model.parameters():
            p.add_(torch.randn_like(p) * 0.5)
    return model.eval()


def random_batch(n, gen):
    cat = torch.stack([torch.randint(1, SIZES[c], (n,), generator=gen) for c in CATEGORIES], 1)
    cards = torch.exp(torch.rand(n, 4, generator=gen) * math.log(3e6))   # 1 .. 3M rows / keys
    bas = torch.tensor([basis(*c) for c in cards.tolist()], dtype=torch.float32)
    return {
        "cat": cat,
        "cfg": torch.randn(n, len(CFG_COLS), generator=gen),
        "ctx": torch.randn(n, len(CTX_COLS), generator=gen),
        "basis": bas,
        "wayang": torch.rand(n, len(RESOURCES), generator=gen) * (bas[:, 1:2] + bas[:, 2:3]),
        "aux": torch.stack([torch.randint(1, 16, (n,), generator=gen).float(),
                            torch.full((n,), 4.0), torch.ones(n)], 1),
    }, cards


def test_monotone_model():
    gen = torch.Generator().manual_seed(1)
    b, cards = random_batch(2000, gen)
    for seed in range(5):
        model = random_model(SIZES, seed)
        with torch.no_grad():
            base = model(b)
            for col in range(4):
                for factor in (1.5, 10.0):
                    c2 = cards.clone()
                    c2[:, col] *= factor
                    b2 = dict(b)
                    b2["basis"] = torch.tensor([basis(*c) for c in c2.tolist()], dtype=torch.float32)
                    b2["wayang"] = b["wayang"] * factor   # Wayang's estimate also grows
                    more = model(b2)
                    for k in CHECKED:
                        worse = int((more[k] < base[k] * (1 - 1e-5)).sum())
                        assert worse == 0, f"{k}: {worse} predictions got smaller when count {col} grew {factor}x"
    print("ok  monotone (model): 5 random models x 2000 inputs; growing any count never lowered a prediction")


def scaled(rows, cols, factor):
    out = []
    for r in rows:
        r2 = dict(r)
        for c in cols:
            if r2.get(c) not in (None, ""):
                r2[c] = repr(float(r2[c]) * factor)
        out.append(r2)
    return out


def test_monotone_pipeline():
    rows = load_rows(DATA)
    vocab = Vocab().fit(rows)
    norm = Normalizer().fit(build_arrays(rows, vocab))
    # Each count on its own; the row width (bytes per row) stays the same, so bytes grow with the rows.
    changes = {"left rows": ["rows", "in_bytes"], "right rows": ["right_rows", "right_in_bytes"],
               "output rows": ["out_rows"], "keys": ["keys"],
               "everything": ["rows", "in_bytes", "right_rows", "right_in_bytes", "out_rows", "keys"]}
    total = 0
    for seed in range(20):
        model = random_model({c: vocab.size(c) for c in CATEGORIES}, seed)
        p0, _ = predict_np(model, to_tensors(norm.apply(build_arrays(rows, vocab))))
        for name, cols in changes.items():
            for factor in (2.0, 10.0):
                p1, _ = predict_np(model, to_tensors(norm.apply(build_arrays(scaled(rows, cols, factor), vocab))))
                for k in CHECKED:
                    worse = int((p1[k] < p0[k] * (1 - 1e-5)).sum())
                    total += worse
                    assert worse == 0, f"seed {seed}: {k} got smaller for {worse} rows with {factor}x {name}"
    print(f"ok  monotone (pipeline): 20 random models x {len(rows)} real rows x 5 kinds of growth; none got smaller")


def synthetic_truth(b):
    """Known answer: CPU = Wayang x factor per operator + 0.3 core-s per million input rows; runtime from CPU."""
    op = b["cat"][:, 0]
    factor = torch.tensor([1.0, 0.25, 1.0, 4.0])[op]           # operators 1, 2, 3: 0.25x, 1x, 4x Wayang
    cpu = factor * b["wayang"][:, 0] + 0.3 * (b["basis"][:, 1] + b["basis"][:, 2])
    io = 0.05 * b["basis"][:, 3]                              # 50 bytes per output row
    mem = 0.5 * b["wayang"][:, 3] + 0.02 * b["basis"][:, 5]   # plus 20 bytes per distinct key
    runtime = 0.02 + cpu / b["aux"][:, 0] ** 0.8 + io / 0.5
    energy = 2.0 * runtime + 6.0 * cpu
    return torch.stack([cpu, io, torch.zeros_like(cpu), mem, runtime, energy], 1)


def test_learns():
    gen = torch.Generator().manual_seed(2)
    torch.manual_seed(2)
    model = CostModel(SIZES, wayang_only=["net"])
    train, _ = random_batch(4000, gen)
    test, _ = random_batch(1000, gen)
    train["target"], test["target"] = synthetic_truth(train), synthetic_truth(test)
    opt = torch.optim.AdamW(model.parameters(), lr=3e-3, weight_decay=1e-5)
    weights = {"cpu": 1, "io": 1, "mem": 1, "runtime": 2, "energy": 0.5}
    for _ in range(3000):
        model.train()
        idx = torch.randint(0, 4000, (512,))
        batch = {k: v[idx] for k, v in train.items()}
        loss = loss_fn(model(batch), batch["target"], weights)
        opt.zero_grad()
        loss.backward()
        opt.step()
    model.eval()
    with torch.no_grad():
        pred = model(test)
    for j, k in enumerate(OUTPUTS):
        if k == "net":
            continue
        q = qerror(pred[k].numpy(), test["target"][:, j].numpy(), SCALES[k])
        print(f"    {k:8} median q-error {np.median(q):.3f}  p90 {np.quantile(q, 0.9):.3f}")
        assert np.median(q) < 1.15, f"{k} not learned (median q-error {np.median(q):.2f})"
    a = pred["wayang_factor"]["cpu"].numpy()
    for op, want in ((1, 0.25), (3, 4.0)):
        got = float(np.median(a[test["cat"][:, 0].numpy() == op]))
        print(f"    operator {op}: learned CPU factor on Wayang {got:.2f} (true {want})")
        assert abs(math.log(got / want)) < math.log(1.5)
    print("ok  learns: recovers the known costs and Wayang's per-operator correction on unseen inputs")


def make_flink(path):
    """A made-up new platform: Spark's rows renamed to Flink, with the generic target columns and other costs."""
    with open(DATA[1], newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        g = {k: v for k, v in r.items() if k not in ("spark_execution_operator", "task_cpu_ns",
                                                  "task_peak_execution_memory_bytes", "shuffle_write_bytes")}
        g["flink_execution_operator"] = r["spark_execution_operator"].replace("Spark", "Flink")
        g["cpu_ns"] = repr(float(r["task_cpu_ns"]) * 0.6)
        g["io_bytes"] = r["shuffle_write_bytes"]
        g["mem_bytes"] = r["task_peak_execution_memory_bytes"]
        g["wall_exec_ns"] = repr(float(r["wall_exec_ns"]) * 0.7)
        out.append(g)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)


def run(*args):
    return subprocess.run([sys.executable, "train.py", *args], cwd=HERE, check=True, capture_output=True,
                          text=True).stdout


def test_new_platform():
    if not all(os.path.exists(p) for p in DATA):
        print("skip new platform: no pairs.csv found")
        return
    with tempfile.TemporaryDirectory() as tmp:
        flink = os.path.join(tmp, "flink_pairs.csv")
        make_flink(flink)
        frows = load_rows([flink])
        assert {r["_platform"] for r in frows} == {"flink"}
        assert all(not math.isnan(target(r, "flink", k)) for r in frows for k in ("cpu", "io", "mem", "runtime"))
        old_rows = load_rows(DATA)

        # From scratch with all three platforms.
        out = run("--data", *DATA, flink, "--epochs", "100", "--out", os.path.join(tmp, "all"))
        assert f"platform flink: {len(frows)} rows" in out, out

        # Few-shot, with a non-default model size: Java + Spark model, then add Flink.
        run("--data", *DATA, "--epochs", "200", "--hidden", "32", "--out", os.path.join(tmp, "js"))
        run("--data", flink, "--epochs", "0", "--init-from", os.path.join(tmp, "js", "model.pt"), "--few-shot",
            "--test-frac", "0", "--out", os.path.join(tmp, "fs0"))
        out = run("--data", flink, "--epochs", "300", "--init-from", os.path.join(tmp, "js", "model.pt"),
                  "--few-shot", "--test-frac", "0", "--out", os.path.join(tmp, "fs"))
        assert "few-shot: vocabulary grew" in out

        def predictions(run_dir, rows):
            m, v, n, _ = load_checkpoint(os.path.join(tmp, run_dir, "model.pt"))
            return predict_np(m, to_tensors(n.apply(build_arrays(rows, v))))[0]

        before, after = predictions("js", old_rows), predictions("fs", old_rows)
        for k in CHECKED:
            assert np.array_equal(before[k], after[k]), f"few-shot changed old-platform predictions of {k}"
        q0 = json.load(open(os.path.join(tmp, "fs0", "metrics.json")))["train"]["runtime"]["model_median"]
        q1 = json.load(open(os.path.join(tmp, "fs", "metrics.json")))["train"]["runtime"]["model_median"]
        print(f"    flink runtime median q-error: {q0:.2f} before few-shot -> {q1:.2f} after")
        assert q1 < q0

        p_csv = os.path.join(tmp, "p.csv")
        subprocess.run([sys.executable, "predict.py", os.path.join(tmp, "fs", "model.pt"), *DATA, flink,
                        "--cardinalities", "wayang", "--out", p_csv], cwd=HERE, check=True, capture_output=True)
        assert sum(1 for _ in open(p_csv)) == len(old_rows) + len(frows) + 1
    print("ok  new platform: 'flink' detected with generic columns; trains from scratch and few-shot; "
          "few-shot keeps Java/Spark predictions bit-identical and improves Flink; hidden=32 loads; predict.py runs")


if __name__ == "__main__":
    test_monotone_model()
    test_monotone_pipeline()
    test_learns()
    test_new_platform()
    print("all checks passed")

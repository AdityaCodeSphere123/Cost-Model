#!/usr/bin/env python3
"""Compares, for every multi-operator job: measured runtime vs Wayang's estimate vs the cost model.

    ../model/.venv/bin/python evaluate_jobs.py                     # newest model in ../model/runs
    ../model/.venv/bin/python evaluate_jobs.py --model ../model/runs/X/model.pt

Model prediction of a job = sum over its scans (source -> parse -> sink) and operators of the predicted
runtime, plus Wayang's estimate for channel conversions (e.g. Spark -> Java), which the model has not been
trained on. Two versions:
    model (true rows)    the operators' real input/output row counts
    model (Wayang rows)  Wayang's own row-count estimates, i.e. what the model gets inside Wayang's optimizer
Writes results/job_comparison.csv and results/job_comparison.md.
"""
import argparse
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "model"))

import numpy as np  # noqa: E402

from features import build_arrays, load_rows  # noqa: E402
from train import load_checkpoint, predict_np, to_tensors  # noqa: E402


def read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def with_wayang_rows(rows):
    """The same operator rows, but with Wayang's estimated input rows (bytes scaled to keep the row width)."""
    out = []
    for r in rows:
        r = dict(r)
        for col, bcol, est in (("rows", "in_bytes", "wayang_est_rows"), ("right_rows", "right_in_bytes", "wayang_est_right_rows")):
            true, guess = float(r.get(col) or 0), float(r.get(est) or 0)
            if true > 0 and r.get(bcol):
                r[bcol] = repr(float(r[bcol]) * guess / true)
            r[col] = repr(guess)
        out.append(r)
    return out


def q(a, b):
    a, b = max(a, 1e-4), max(b, 1e-4)
    return max(a / b, b / a)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", default=None)
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    args = ap.parse_args()
    model_path = args.model or max(glob.glob(os.path.join(HERE, "..", "model", "runs", "*", "model.pt")),
                                   key=os.path.getmtime)
    model, vocab, norm, _ = load_checkpoint(model_path)

    runs = read(os.path.join(args.results, "job_runs.csv"))
    tasks = read(os.path.join(args.results, "job_tasks.csv"))
    ops = load_rows([os.path.join(args.results, "job_operators.csv")])

    key = lambda r: (r["job"], r["size"], r["placement"])
    measured = defaultdict(list)
    for r in runs:
        if r.get("exec_ns"):
            measured[key(r)].append(float(r["exec_ns"]) / 1e9)
    wayang, conversion = defaultdict(float), defaultdict(float)
    for t in tasks:
        ms = float(t["wayang_time_ms_gmean"]) / 1e3
        wayang[key(t)] += ms
        if t["label"] == "conversion":
            conversion[key(t)] += ms

    preds = {}
    for name, rows, cards in (("true", ops, "true"), ("wayang", with_wayang_rows(ops), "wayang")):
        p, _ = predict_np(model, to_tensors(norm.apply(build_arrays(rows, vocab, cards=cards))))
        preds[name] = p["runtime"]
    model_true, model_way = defaultdict(float), defaultdict(float)
    parts = defaultdict(list)
    for i, r in enumerate(ops):
        model_true[key(r)] += float(preds["true"][i])
        model_way[key(r)] += float(preds["wayang"][i])
        parts[key(r)].append(f'{r["operator"]}={preds["true"][i] * 1e3:.0f}ms')

    table = []
    for k in sorted(measured):
        m = statistics.median(measured[k])
        mt, mw = model_true[k] + conversion[k], model_way[k] + conversion[k]
        table.append({"job": k[0], "size": k[1], "placement": k[2], "measured_ms": m * 1e3, "wayang_ms": wayang[k] * 1e3,
                      "model_true_rows_ms": mt * 1e3, "model_wayang_rows_ms": mw * 1e3,
                      "q_wayang": q(wayang[k], m), "q_model_true": q(mt, m), "q_model_wayang": q(mw, m),
                      "conversion_wayang_ms": conversion[k] * 1e3, "model_parts": " ".join(parts[k])})

    with open(os.path.join(args.results, "job_comparison.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(table[0]))
        w.writeheader()
        w.writerows(table)

    lines = [f"# Jobs: measured vs Wayang vs cost model", "", f"Model: `{os.path.basename(os.path.dirname(model_path))}`", "",
             "Runtime from the first record to the end (median of the measured runs). q-error = max(a/b, b/a), 1 is perfect.", "",
             "| Job | Size | Placement | Measured ms | Wayang ms | Model ms (true rows) | Model ms (Wayang rows) | q Wayang | q model | q model (Wayang rows) |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for t in table:
        lines.append(f'| {t["job"]} | {t["size"]} | {t["placement"]} | {t["measured_ms"]:.0f} | {t["wayang_ms"]:.0f} | '
                     f'{t["model_true_rows_ms"]:.0f} | {t["model_wayang_rows_ms"]:.0f} | {t["q_wayang"]:.2f} | '
                     f'{t["q_model_true"]:.2f} | {t["q_model_wayang"]:.2f} |')
    lines += ["", "## Summary (median q-error over jobs)", "",
              "| Placement | Jobs | Wayang | Model (true rows) | Model (Wayang rows) | Model better than Wayang |",
              "|---|---|---|---|---|---|"]
    for pl in ["java", "spark", "mixed", "all"]:
        sel = [t for t in table if pl == "all" or t["placement"] == pl]
        if not sel:
            continue
        better = sum(t["q_model_true"] < t["q_wayang"] for t in sel)
        lines.append(f'| {pl} | {len(sel)} | {np.median([t["q_wayang"] for t in sel]):.2f} | '
                     f'{np.median([t["q_model_true"] for t in sel]):.2f} | {np.median([t["q_model_wayang"] for t in sel]):.2f} | '
                     f'{better} of {len(sel)} |')
    report = "\n".join(lines) + "\n"
    with open(os.path.join(args.results, "job_comparison.md"), "w") as f:
        f.write(report)
    print(report)


if __name__ == "__main__":
    main()

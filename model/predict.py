#!/usr/bin/env python3
"""Predicts resource demands, runtime, money and energy with a trained cost model.

    python predict.py runs/X/model.pt ../spark-profiler/results/pairs.csv
    python predict.py runs/X/model.pt pairs.csv --cardinalities wayang --out predictions.csv

--cardinalities true   uses the measured output rows (how the model was trained)
--cardinalities wayang uses Wayang's own estimate of the output rows, which is what the model gets when it
                       runs inside Wayang's optimizer; the gap between the two shows the cardinality error.
"""
import argparse
import csv
import sys

import numpy as np

from features import OUTPUTS, RESOURCES, SCALES, build_arrays, category_values, load_rows
from train import load_checkpoint, predict_np, qerror, to_tensors

UNITS = {"cpu": "core_s", "io": "gb", "net": "gb", "mem": "gb", "runtime": "s", "energy": "j"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("model", help="model.pt written by train.py")
    ap.add_argument("data", nargs="+", help="pairs.csv files")
    ap.add_argument("--cardinalities", choices=["true", "wayang"], default="true")
    ap.add_argument("--out", default=None, help="CSV to write (default: print a summary only)")
    args = ap.parse_args()

    model, vocab, norm, _ = load_checkpoint(args.model)
    rows = load_rows(args.data)
    unknown = {(c, v) for r in rows for c, v in category_values(r).items() if vocab.index(c, v) == 0}
    if unknown:
        print(f"WARNING: not seen in training (predictions are guesses): {sorted(unknown)}", file=sys.stderr)
    raw = build_arrays(rows, vocab, cards=args.cardinalities)
    pred, factors = predict_np(model, to_tensors(norm.apply(raw)))

    print(f"{len(rows)} rows, cardinalities = {args.cardinalities}")
    print(f"{'output':10} {'model median q':>15} {'wayang median q':>16}")
    for j, k in enumerate(OUTPUTS):
        y = raw["target"][:, j]
        m = ~np.isnan(y)
        if not m.any():
            continue
        line = f"{k:10} {np.median(qerror(pred[k][m], y[m], SCALES[k])):15.2f}"
        if k in RESOURCES or k == "runtime":
            w = raw["wayang"][:, RESOURCES.index(k)] if k in RESOURCES else raw["wayang_time"]
            line += f" {np.median(qerror(w[m], y[m], SCALES[k])):16.2f}"
        print(line)

    if args.out:
        with open(args.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["platform", "config_id", "pair"] + [f"pred_{k}_{UNITS[k]}" for k in OUTPUTS]
                       + ["pred_money"] + [f"wayang_factor_{r}" for r in RESOURCES])
            for i, r in enumerate(rows):
                w.writerow([r["_platform"], r.get("config_id", ""), r.get("pair", "")]
                           + [f"{pred[k][i]:.6g}" for k in OUTPUTS] + [f'{pred["money"][i]:.6g}']
                           + [f"{factors[res][i]:.4g}" for res in RESOURCES])
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()

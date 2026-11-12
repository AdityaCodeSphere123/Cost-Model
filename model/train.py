#!/usr/bin/env python3
"""Trains the cost model on the profilers' pairs.csv and compares it with Wayang's own estimates.

    python train.py                                   # Java + Spark results, random 20% of configs for testing
    python train.py --holdout-rows 500000             # test on all configs with more rows: extrapolation
    python train.py --data ../spark-profiler/results/pairs.csv
    python train.py --init-from runs/X/model.pt --few-shot --data new_platform_pairs.csv
                                                      # new platform: train only its own parameters

Writes to runs/<time>/: model.pt, report.md, metrics.json, predictions_test.csv.
"""
import argparse
import csv
import json
import os
import random
import sys
import time

import numpy as np
import torch

from features import (CATEGORIES, OUTPUTS, RESOURCES, SCALES, Normalizer, Vocab, build_arrays, load_rows,
                      target)
from model import CostModel

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = [os.path.join(HERE, "..", p, "results", f) for p in ("java-profiler", "spark-profiler")
                for f in ("pairs.csv", "baseline_costs.csv")]  # baseline_costs.csv -> 'scan' rows
# net: Spark's network demand (shuffle + results); Java's is 0.
DEFAULT_WEIGHTS = {"cpu": 1.0, "io": 1.0, "net": 1.0, "mem": 0.5, "runtime": 2.0, "energy": 0.5}


# ------------------------------------------------------------------------------------------
# Helpers shared with predict.py
# ------------------------------------------------------------------------------------------
def to_tensors(arrays):
    return {k: torch.from_numpy(v) for k, v in arrays.items()}


def save_checkpoint(path, model, vocab, norm, args):
    torch.save({"state": model.state_dict(), "vocab": vocab.maps, "vocab_sizes": model.vocab_sizes,
                "norm": {"mean": norm.mean, "std": norm.std}, "hidden": model.hidden,
                "wayang_only": model.wayang_only, "args": vars(args)}, path)


def load_checkpoint(path):
    ck = torch.load(path, map_location="cpu", weights_only=False)
    vocab = Vocab(maps=ck["vocab"])
    norm = Normalizer(mean=ck["norm"]["mean"], std=ck["norm"]["std"])
    model = CostModel(ck["vocab_sizes"], hidden=ck["hidden"], wayang_only=ck.get("wayang_only", ()))
    model.load_state_dict(ck["state"])
    model.eval()
    return model, vocab, norm, ck


def qerror(pred, true, scale):
    """max(pred/true, true/pred), with values below `scale` treated as noise (so 0 vs 0 is 1)."""
    return np.exp(np.abs(np.log((np.asarray(pred) + scale) / (np.asarray(true) + scale))))


# ------------------------------------------------------------------------------------------
# Training
# ------------------------------------------------------------------------------------------
def loss_fn(pred, target, weights):
    total = torch.zeros(())
    for j, k in enumerate(OUTPUTS):
        w = weights.get(k, 0.0)
        if w == 0:
            continue
        y = target[:, j]
        m = ~torch.isnan(y)
        if not m.any():
            continue
        s = SCALES[k]
        d = torch.log1p(pred[k][m] / s) - torch.log1p(y[m] / s)  # error in log space, like a q-error
        total = total + w * (d ** 2).mean()
    return total


def split(rows, args):
    groups = sorted({r["_group"] for r in rows})
    if args.holdout_rows is not None:
        test = {r["_group"] for r in rows if float(r.get("rows") or 0) > args.holdout_rows}
    else:
        rng = random.Random(args.seed)
        rng.shuffle(groups)
        n_test = max(1, round(len(groups) * args.test_frac)) if len(groups) > 1 and args.test_frac > 0 else 0
        test = set(groups[:n_test])
    tr = [i for i, r in enumerate(rows) if r["_group"] not in test]
    te = [i for i, r in enumerate(rows) if r["_group"] in test]
    return tr, te


def subset(arrays, idx):
    return {k: v[idx] for k, v in arrays.items()}


def predict_np(model, t):
    model.eval()
    with torch.no_grad():
        out = model(t)
    return {k: v.numpy() for k, v in out.items() if k != "wayang_factor"}, \
           {k: v.numpy() for k, v in out["wayang_factor"].items()}


def evaluate(model, arrays_n, arrays_raw):
    """q-error of the model and of Wayang, per output."""
    pred, _ = predict_np(model, to_tensors(arrays_n))
    res = {}
    for j, k in enumerate(OUTPUTS):
        y = arrays_raw["target"][:, j]
        m = ~np.isnan(y)
        if not m.any():
            continue
        q = qerror(pred[k][m], y[m], SCALES[k])
        row = {"n": int(m.sum()), "model_median": float(np.median(q)), "model_p90": float(np.quantile(q, 0.9))}
        if k in RESOURCES or k == "runtime":
            w = arrays_raw["wayang"][:, RESOURCES.index(k)] if k in RESOURCES else arrays_raw["wayang_time"]
            qw = qerror(w[m], y[m], SCALES[k])
            row.update({"wayang_median": float(np.median(qw)), "wayang_p90": float(np.quantile(qw, 0.9))})
        res[k] = row
    return res


def extend_from(ck_path, rows):
    """Few-shot start: the old model, with new platforms/implementations/operators added to the vocabulary."""
    old, vocab, norm, ck = load_checkpoint(ck_path)
    old_sizes = dict(ck["vocab_sizes"])
    vocab.fit(rows)
    sizes = {c: vocab.size(c) for c in CATEGORIES}
    model = CostModel(sizes, hidden=ck["hidden"], wayang_only=old.wayang_only)
    new_state = model.state_dict()
    for k, v in old.state_dict().items():
        if new_state[k].shape == v.shape:
            new_state[k] = v
        else:  # grown along dim 0 (embeddings and per-platform tensors): copy the old rows
            n = v.shape[0]
            new_state[k][:n] = v
            if k.startswith("emb.platform") or not k.startswith("emb."):
                new_state[k][n:] = v[1:n].mean(0) if n > 1 else v[0]  # new platform starts as the average
    model.load_state_dict(new_state)
    print(f"few-shot: vocabulary grew from {old_sizes} to {sizes}")
    return model, vocab, norm, old_sizes


def check_data(rows):
    """Warn about rows the model cannot learn from: unknown platform, or no measured value for a target."""
    by_platform = {}
    for r in rows:
        by_platform.setdefault(r["_platform"], []).append(r)
    for p, rs in sorted(by_platform.items()):
        missing = [k for k in OUTPUTS if all(np.isnan(target(r, p, k)) for r in rs)]
        note = f"  (no measured {', '.join(missing)}: not trained on these)" if missing else ""
        print(f"platform {p}: {len(rs)} rows{note}")
        if p == "unknown":
            print("WARNING: rows without a 'platform' or '<platform>_execution_operator' column", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", nargs="+", default=DEFAULT_DATA, help="pairs.csv files (default: Java and Spark)")
    ap.add_argument("--out", default=None, help="output folder (default: runs/<time>)")
    ap.add_argument("--epochs", type=int, default=800)
    ap.add_argument("--batch", type=int, default=2048)
    ap.add_argument("--lr", type=float, default=3e-3)
    ap.add_argument("--weight-decay", type=float, default=1e-4)
    ap.add_argument("--hidden", type=int, default=64)
    ap.add_argument("--test-frac", type=float, default=0.2, help="share of configs kept for testing")
    ap.add_argument("--holdout-rows", type=float, default=None,
                    help="instead of a random split: test on every config with more input rows than this")
    ap.add_argument("--weights", default="", help='loss weight per output, e.g. "runtime=2,net=0"')
    ap.add_argument("--init-from", default=None, help="start from a trained model.pt")
    ap.add_argument("--few-shot", action="store_true", help="with --init-from: train only platform parameters")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    torch.manual_seed(args.seed)
    np.random.seed(args.seed)
    weights = dict(DEFAULT_WEIGHTS)
    for kv in filter(None, args.weights.split(",")):
        k, v = kv.split("=")
        weights[k.strip()] = float(v)

    if args.few_shot and not args.init_from:
        raise SystemExit("--few-shot needs --init-from <model.pt>")
    for p in args.data:
        if not os.path.exists(p):
            print(f"WARNING: {p} not found, skipped", file=sys.stderr)
    rows = load_rows([p for p in args.data if os.path.exists(p)])
    if not rows:
        raise SystemExit(f"no rows found in {args.data}")
    check_data(rows)
    tr, te = split(rows, args)
    if not tr:
        raise SystemExit("no training rows left after the split (check --holdout-rows / --test-frac)")
    train_rows = [rows[i] for i in tr]

    old_sizes = None
    if args.init_from:
        model, vocab, norm, old_sizes = extend_from(args.init_from, train_rows)
    else:
        vocab = Vocab().fit(train_rows)
        no_data = [r for r in RESOURCES if weights.get(r, 0.0) == 0]  # e.g. network: Wayang's estimate only
        model = CostModel({c: vocab.size(c) for c in CATEGORIES}, hidden=args.hidden, wayang_only=no_data)
        norm = None
    raw = build_arrays(rows, vocab)
    if norm is None:
        norm = Normalizer().fit(subset(raw, tr))
    data_n = norm.apply(raw)
    t_train, t_test = to_tensors(subset(data_n, tr)), to_tensors(subset(data_n, te))

    keep = {}  # few-shot: parameter -> its old rows, put back after every step so old values never change
    if args.few_shot:
        for p in model.parameters():
            p.requires_grad_(False)
        params = []
        for p, cat in model.row_parameters().items():
            n_old = old_sizes[cat]
            if p.shape[0] > n_old:  # only categories that got new values are trained
                p.requires_grad_(True)
                params.append(p)
                keep[p] = p.detach()[:n_old].clone()
        if not params:
            raise SystemExit("--few-shot: the data has no new platform, implementation or operator to learn")
        print(f"few-shot: training {sum(int((p.shape[0] - keep[p].shape[0]) * p[0].numel()) for p in params)} "
              f"new parameters; everything else stays fixed")
    else:
        params = list(model.parameters())
    opt = torch.optim.AdamW(params, lr=args.lr, weight_decay=0.0 if args.few_shot else args.weight_decay)
    n = len(tr)
    steps = args.epochs * max(1, (n + args.batch - 1) // args.batch)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=max(1, steps))

    n_groups = len({rows[i]["_group"] for i in tr}), len({rows[i]["_group"] for i in te})
    print(f"rows: {len(rows)}  train: {len(tr)} rows / {n_groups[0]} configs  test: {len(te)} rows / {n_groups[1]} configs")
    t0 = time.time()
    for epoch in range(1, args.epochs + 1):
        model.train()
        perm = torch.randperm(n)
        total = 0.0
        for s in range(0, n, args.batch):
            idx = perm[s:s + args.batch]
            batch = {k: v[idx] for k, v in t_train.items()}
            loss = loss_fn(model(batch), batch["target"], weights)
            opt.zero_grad()
            loss.backward()
            opt.step()
            sched.step()
            with torch.no_grad():
                for p, old in keep.items():
                    p[: old.shape[0]] = old
            total += loss.item() * len(idx)
        if epoch == 1 or epoch % max(1, args.epochs // 10) == 0 or epoch == args.epochs:
            msg = f"epoch {epoch:5d}  train loss {total / n:.4f}"
            if te:
                model.eval()
                with torch.no_grad():
                    msg += f"  test loss {float(loss_fn(model(t_test), t_test['target'], weights)):.4f}"
            print(msg, flush=True)
    print(f"trained in {time.time() - t0:.1f}s")

    # ---- evaluation and outputs
    out = args.out or os.path.join(HERE, "runs", time.strftime("%Y%m%d-%H%M%S"))
    os.makedirs(out, exist_ok=True)
    metrics = {"train": evaluate(model, subset(data_n, tr), subset(raw, tr))}
    if te:
        metrics["test"] = evaluate(model, subset(data_n, te), subset(raw, te))
    metrics["platform_constants"] = model.platform_constants(vocab.maps["platform"])
    pred_all, factors = predict_np(model, to_tensors(data_n))
    metrics["wayang_factor"] = wayang_factors(rows, factors)
    save_checkpoint(os.path.join(out, "model.pt"), model, vocab, norm, args)
    with open(os.path.join(out, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    write_predictions(os.path.join(out, "predictions_test.csv"), rows, te, pred_all, raw)
    report = make_report(metrics, rows, tr, te, args)
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write(report)
    print(report)
    print(f"saved to {out}")


def wayang_factors(rows, factors):
    """Average a_r per (platform, operator): how much the model scales Wayang's estimate of each resource."""
    acc = {}
    for i, r in enumerate(rows):
        key = f'{r["_platform"]} {r["operator"]}'
        acc.setdefault(key, {res: [] for res in RESOURCES})
        for res in RESOURCES:
            acc[key][res].append(float(factors[res][i]))
    return {k: {res: float(np.mean(v)) for res, v in d.items()} for k, d in sorted(acc.items())}


def write_predictions(path, rows, idx, pred, raw):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        head = ["platform", "config_id", "pair"]
        for k in OUTPUTS:
            head += [f"{k}_measured", f"{k}_model"] + ([f"{k}_wayang"] if k in RESOURCES or k == "runtime" else [])
        w.writerow(head + ["money_model"])
        for i in idx:
            r = rows[i]
            line = [r["_platform"], r["config_id"], r.get("pair", "")]
            for j, k in enumerate(OUTPUTS):
                y = raw["target"][i, j]
                line += ["" if np.isnan(y) else f"{y:.6g}", f"{pred[k][i]:.6g}"]
                if k in RESOURCES:
                    line.append(f'{raw["wayang"][i, RESOURCES.index(k)]:.6g}')
                elif k == "runtime":
                    line.append(f'{raw["wayang_time"][i]:.6g}')
            w.writerow(line + [f'{pred["money"][i]:.6g}'])


def make_report(metrics, rows, tr, te, args):
    units = {"cpu": "core-s", "io": "GB", "net": "GB", "mem": "GB", "runtime": "s", "energy": "J"}
    lines = ["# Cost model run", "",
             f"Data: {len(rows)} rows ({len(tr)} train, {len(te)} test), configs split so all pairs of one "
             f"config are on one side" + (f"; test = configs with more than {args.holdout_rows:g} rows"
                                         if args.holdout_rows is not None else f"; seed {args.seed}") + ".",
             "", "q-error = max(predicted / measured, measured / predicted); 1 is perfect. "
             "Values below the noise size count as equal.", ""]
    for part in ("test", "train"):
        if part not in metrics:
            continue
        lines += [f"## {part.capitalize()} set", "",
                  "| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |",
                  "|---|---|---|---|---|---|"]
        for k, m in metrics[part].items():
            wm = f'{m["wayang_median"]:.2f}' if "wayang_median" in m else "-"
            wp = f'{m["wayang_p90"]:.2f}' if "wayang_p90" in m else "-"
            lines.append(f'| {k} ({units[k]}) | {m["n"]} | {m["model_median"]:.2f} | {m["model_p90"]:.2f} | {wm} | {wp} |')
        lines.append("")
    lines += ["## Learned platform constants", "", "| Platform | " + " | ".join(
        next(iter(metrics["platform_constants"].values())).keys()) + " |",
              "|---" * (1 + len(next(iter(metrics["platform_constants"].values())))) + "|"]
    for p, c in metrics["platform_constants"].items():
        lines.append(f"| {p} | " + " | ".join(f"{v:.3g}" for v in c.values()) + " |")
    lines += ["", "## How much the model scales Wayang's estimate (a_r, 1 = unchanged)", "",
              "| Platform, operator | CPU | I/O | network | memory |", "|---|---|---|---|---|"]
    for k, d in metrics["wayang_factor"].items():
        lines.append(f"| {k} | " + " | ".join(f"{d[r]:.2f}" for r in RESOURCES) + " |")
    if len(tr) < 1000:
        lines += ["", f"Note: only {len(tr)} training rows. These numbers only show that the pipeline works; "
                      "they are not a measure of the model's accuracy."]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()

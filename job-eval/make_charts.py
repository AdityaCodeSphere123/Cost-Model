#!/usr/bin/env python3
"""Charts for the presentation, from results/job_comparison.csv (and a model report, if given).

    ../model/.venv/bin/python make_charts.py [--report ../model/runs/X/metrics.json]

Writes results/charts/jobs_runtime.png, jobs_qerror.png and (with --report) operators_qerror.png.
"""
import argparse
import csv
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
# Validated categorical slots 1-3 (blue, orange, aqua); text stays in ink colors, never the series color.
C = {"measured": "#2a78d6", "wayang": "#eb6834", "model": "#1baf7a"}
SURFACE, INK, INK2, GRID = "#fcfcfb", "#1f1f1e", "#5c5c58", "#e6e6e3"
plt.rcParams.update({"font.family": "sans-serif", "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                     "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "figure.facecolor": SURFACE,
                     "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE})


def style(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)


def legend(fig, labels):
    handles = [plt.Rectangle((0, 0), 1, 1, color=C[k]) for k in labels]
    fig.legend(handles, labels.values(), loc="upper center", ncol=len(labels), frameon=False,
               bbox_to_anchor=(0.5, 0.94))


def jobs_runtime(rows, out):
    """Dot plot on one shared log axis: one row per job, a dot each for measured, Wayang and the model."""
    from matplotlib.ticker import FixedLocator, NullFormatter, NullLocator
    placements = [p for p in ("java", "spark", "mixed") if any(r["placement"] == p for r in rows)]
    jobs = sorted({(r["job"], r["size"]) for r in rows}, key=lambda j: (j[0], j[1]))
    fig, axes = plt.subplots(1, len(placements), figsize=(4.6 * len(placements), 0.48 * len(jobs) + 2.0),
                             sharey=True, sharex=True)
    axes = np.atleast_1d(axes)
    series = [("measured", "measured_ms", "o"), ("wayang", "wayang_ms", "s"), ("model", "model_true_rows_ms", "D")]
    allv = [float(r[c]) for r in rows for _, c, _ in series if float(r[c]) > 0]
    lo, hi = min(allv) / 1.6, max(allv) * 1.6
    ticks = [t for t in (1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000) if lo <= t <= hi]
    for ax, pl in zip(axes, placements):
        by = {(r["job"], r["size"]): r for r in rows if r["placement"] == pl}
        for y, j in enumerate(jobs):
            if j not in by:
                continue
            vals = [float(by[j][c]) for _, c, _ in series]
            ax.plot([min(vals), max(vals)], [y, y], color=GRID, linewidth=2, zorder=1)  # spread between the three
            for (name, _, marker), v in zip(series, vals):
                ax.scatter(v, y, s=58, marker=marker, color=C[name], edgecolors=SURFACE, linewidths=1.5, zorder=3)
        ax.set_xscale("log")
        ax.set_xlim(lo, hi)
        ax.xaxis.set_major_locator(FixedLocator(ticks))
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_title({"java": "Java only", "spark": "Spark only", "mixed": "Mixed: Spark scan, Java rest"}[pl],
                     loc="left", fontsize=11, color=INK)
        ax.set_xlabel("runtime, ms (log scale)")
        ax.set_yticks(range(len(jobs)))
        ax.set_yticklabels([f"{j} · {s}" for j, s in jobs])
        ax.set_ylim(len(jobs) - 0.5, -0.5)
        style(ax)
    fig.suptitle("Job runtime: measured vs Wayang's estimate vs cost model", x=0.01, ha="left", fontsize=13,
                 fontweight="bold", y=0.995)
    handles = [plt.Line2D([], [], linestyle="", marker=m, markersize=8, color=C[n]) for n, _, m in series]
    fig.legend(handles, ["Measured", "Wayang estimate", "Cost model"], loc="upper center", ncol=3, frameon=False,
               bbox_to_anchor=(0.5, 0.95))
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def jobs_qerror(rows, out):
    groups = [p for p in ("java", "spark", "mixed") if any(r["placement"] == p for r in rows)] + ["all"]
    wq = [np.median([float(r["q_wayang"]) for r in rows if g == "all" or r["placement"] == g]) for g in groups]
    mq = [np.median([float(r["q_model_true"]) for r in rows if g == "all" or r["placement"] == g]) for g in groups]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    x = np.arange(len(groups))
    w = 0.36
    b1 = ax.bar(x - w / 2, wq, w - 0.04, color=C["wayang"], linewidth=0)
    b2 = ax.bar(x + w / 2, mq, w - 0.04, color=C["model"], linewidth=0)
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.03, f"{b.get_height():.2f}", ha="center",
                    va="bottom", fontsize=9, color=INK2)
    ax.axhline(1, color=INK2, linewidth=1, linestyle=(0, (3, 3)))
    ax.text(-0.45, 1.02, "perfect = 1", ha="left", va="bottom", fontsize=8, color=INK2)
    ax.set_xticks(x)
    ax.set_xticklabels([{"java": "Java", "spark": "Spark", "mixed": "Mixed", "all": "All jobs"}[g] for g in groups])
    ax.set_ylabel("median q-error (lower is better)")
    ax.set_ylim(0, max(wq + mq) * 1.18)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    fig.suptitle("Job runtime error: Wayang vs cost model", x=0.01, ha="left", fontsize=13, fontweight="bold")
    legend(fig, {"wayang": "Wayang estimate", "model": "Cost model"})
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def operators_qerror(metrics, out):
    part = metrics.get("test") or metrics["train"]
    # I/O is left out: most single operators do ~no disk I/O, so its median compares zeros with zeros.
    names = {"cpu": "CPU", "mem": "Memory", "runtime": "Runtime"}
    keys = [k for k in names if k in part and "wayang_median" in part[k]]
    wq = [part[k]["wayang_median"] for k in keys]
    mq = [part[k]["model_median"] for k in keys]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    x = np.arange(len(keys))
    w = 0.36
    b1 = ax.bar(x - w / 2, wq, w - 0.04, color=C["wayang"], linewidth=0)
    b2 = ax.bar(x + w / 2, mq, w - 0.04, color=C["model"], linewidth=0)
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.08, f"{b.get_height():.2f}", ha="center",
                    va="bottom", fontsize=9, color=INK2)
    ax.set_ylim(0, max(wq + mq) * 1.18)
    ax.axhline(1, color=INK2, linewidth=1, linestyle=(0, (3, 3)))
    ax.text(-0.45, 1.05, "perfect = 1", ha="left", va="bottom", fontsize=8, color=INK2)
    ax.set_xticks(x)
    ax.set_xticklabels([names[k] for k in keys])
    ax.set_ylabel("median q-error (lower is better)")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    n = part[keys[0]]["n"]
    fig.suptitle(f"Single operators on held-out configs ({n} rows): Wayang vs cost model", x=0.01, ha="left",
                 fontsize=12, fontweight="bold")
    legend(fig, {"wayang": "Wayang estimate", "model": "Cost model"})
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    ap.add_argument("--report", default=None, help="metrics.json of the model run (operator-level chart)")
    args = ap.parse_args()
    charts = os.path.join(args.results, "charts")
    os.makedirs(charts, exist_ok=True)
    with open(os.path.join(args.results, "job_comparison.csv"), newline="") as f:
        rows = list(csv.DictReader(f))
    jobs_runtime(rows, os.path.join(charts, "jobs_runtime.png"))
    jobs_qerror(rows, os.path.join(charts, "jobs_qerror.png"))
    if args.report:
        with open(args.report) as f:
            operators_qerror(json.load(f), os.path.join(charts, "operators_qerror.png"))
    print("wrote", charts)


if __name__ == "__main__":
    main()

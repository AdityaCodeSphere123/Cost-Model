# Cost model

A neural cost model for Wayang operators, trained on the measurements in `../java-profiler/results` and
`../spark-profiler/results`. It predicts how much of each resource an operator uses, then turns that into
runtime, money and energy with formulas.

```
operator, implementation, platform,  ──► encoder + per-platform adapter ──► one expert per resource
config, context (no row counts)                                                  │
row counts, keys  ───────────────────────────────────────────────────────────────┤ basis, not in the network
Wayang's estimate per resource  ──────────────────────────────────────────────────┘
                                                                                 ▼
        CPU core-s, I/O GB, network GB, memory GB        demand_r = a_r · Wayang_r + Σ_k b_rk · basis_k(counts)
                                                                                 ▼
        runtime = overhead + (t_cpu^p + t_io^p + t_net^p)^(1/p) · (1 + memory pressure)
        energy  = idle power · runtime + joules per core-s · CPU          money = runtime · price per ms
```

- **Monotone by design**: the counts (left rows, right rows, output rows, distinct keys) only enter through the
  basis (1, n_left, n_right, n_out, n_in·log n_in, n_keys) with coefficients ≥ 0, and every later step only
  grows with the demands. No network input is computed from a count (not even a ratio like keys / rows), so
  growing any count, with the same operator, configuration and row width, never lowers a prediction. This
  helps when predicting beyond the training sizes. `test_model.py` checks it through the whole CSV pipeline.
- **Wayang as the starting point**: with a = 1 and b = 0 a resource is exactly Wayang's estimate. The report
  shows the learned a per operator, e.g. "Java sort CPU = 0.13 × Wayang".
- **Physical outputs**: each expert predicts a real quantity, and the runtime formula has readable constants per
  platform: I/O bandwidth, p (1 = CPU and I/O add up, as on Java; large = they overlap), idle power, J per core-s.
- **Memory** is a limit, not a time: it slows runtime only when it fills the heap.
- **Network** has no measurements on one machine, so it uses Wayang's estimate unchanged until there is
  cluster data (it is trained as soon as its loss weight is > 0).

## Files

| File | What it does |
|---|---|
| `features.py` | reads `pairs.csv`, builds inputs, basis, Wayang estimates and targets (target definitions are in `TARGETS`) |
| `model.py` | the model |
| `train.py` | training, comparison with Wayang, report |
| `predict.py` | predictions for new rows, with measured or Wayang-estimated row counts |
| `test_model.py` | checks: monotone (model and full pipeline), learns a known answer, new platform (from scratch and few-shot), save/load |

## Setup and use

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python test_model.py                       # ~20 s, all checks should pass

.venv/bin/python train.py                            # Java + Spark, 20% of configs held out for testing
.venv/bin/python train.py --holdout-rows 500000      # test only on bigger inputs than seen in training
.venv/bin/python predict.py runs/<time>/model.pt ../spark-profiler/results/pairs.csv --cardinalities wayang

# new platform from a few hundred rows: train only its embedding, adapter and constants
.venv/bin/python train.py --init-from runs/<time>/model.pt --few-shot --data <new platform>/pairs.csv
```

## Adding a new platform

A new platform's `pairs.csv` needs the same feature columns as the Java/Spark ones (`operator`, `source`,
`rows`, `right_rows`, `keys`, `sel`, `words`, `in_bytes`, `max_heap_bytes`, `cpus`, `out_rows`, `config_id`,
`pair`, the `wayang_est_*` columns) plus:

- the platform: a `<platform>_execution_operator` column (e.g. `flink_execution_operator = FlinkJoinOperator`),
  or `platform` + `execution_operator` columns;
- task threads in `task_slots` or `threads` if it runs in parallel (otherwise 1 is assumed);
- the measured values in the generic columns `cpu_ns`, `io_bytes`, `mem_bytes`, `network_bytes`,
  `wall_exec_ns`, `os_energy_nj`, or its own entry in `TARGET_COLUMNS` in `features.py`. Missing ones are
  left out of training (`train.py` lists them per platform).

Then either retrain on all platforms, or use `--few-shot`. Few-shot trains only the new values' rows (the new
platform's embedding, adapter and constants, and new implementations/operators). The shared encoder and
experts stay fixed and old rows are put back after every step, so predictions for the existing platforms stay
exactly the same (`test_model.py` checks this bit for bit).

Each run writes `runs/<time>/`: `model.pt`, `report.md` (q-error of the model and of Wayang, learned constants,
Wayang correction factors), `metrics.json`, `predictions_test.csv`.

## Targets

| Expert | Java | Spark |
|---|---|---|
| CPU core-s | `thread_cpu_exec_ns` | `task_cpu_ns` |
| I/O GB | `os_disk_read_bytes` + `os_disk_write_bytes` | `shuffle_write_bytes` + `spill_disk_bytes` + `os_disk_read_bytes` |
| network GB | `network_bytes` | `network_bytes` |
| memory GB | `live_heap_after_gc_max_bytes` | `task_peak_execution_memory_bytes` |
| runtime s | `wall_exec_ns` | `wall_exec_ns` |
| energy J | `os_energy_nj` | `os_energy_nj` |

A new platform uses `cpu_ns`, `io_bytes`, `network_bytes`, `mem_bytes`, `wall_exec_ns`, `os_energy_nj` (see above).

All are operator run − baseline run, clipped at 0 (small negative values are noise). The loss is the squared
error in log space (like a q-error); values below a noise size (1 ms, 100 KB, 1 MB, 10 mJ) count as equal.

## Training with measured, predicting with estimated row counts

The model is trained with the **measured** row counts, so it learns "cost given the rows". Inside Wayang it
gets Wayang's **estimated** row counts (`--cardinalities wayang`). This keeps row-count errors separate from
cost errors: compare the two settings to see how much of the remaining error comes from cardinalities.

## Results (2 Nov 2026)

Trained on 1,653 rows: 1,560 operator pairs (Java 765, Spark 795, 13 operators) plus 93 scan rows, 1,500
epochs. Reports, metrics and per-row test predictions are in [`results/`](results/) (copies of `runs/`,
which git ignores).

| Run | Test set | Runtime: Wayang → model | CPU: Wayang → model | Memory: Wayang → model |
|---|---|---|---|---|
| [A-random-split](results/A-random-split/report.md) | 20% of configs (118 configs, 320 rows) | 5.70 → **1.31** | 5.48 → **1.41** | 7.84 → **1.01** |
| [B-extrapolate-1M](results/B-extrapolate-1M/report.md) | all configs with > 300k rows (113 configs, 323 rows) | 4.27 → **1.44** | 6.25 → **1.63** | 31.85 → **1.05** |
| [C-all-data](results/C-all-data/report.md) | none: trained on everything | | | |
| [D-no-job-sizes](results/D-no-job-sizes/report.md) | none: trained without any 100k / 1M-row configs; tested on the multi-operator jobs in `../job-eval` | | | |

Median q-error (1 = perfect). Things the model learned (see the reports):

- CPU: the model scales Wayang's CPU estimate by 0.0–0.4 for almost every operator. Wayang assumes a
  2.7 GHz machine with fixed per-row costs and overestimates CPU on this machine many times over.
- p ≈ 1.6–1.8 on both platforms: CPU and I/O time mostly add up rather than overlap.
- Energy is the weakest output (median 1.74, p90 14.9): process energy on macOS is noisy for short runs.

Commands to reproduce: `train.py --epochs 1500 --out runs/A-random-split`, `--holdout-rows 300000` for B,
`--test-frac 0` for C, and for D the same on profiler CSVs with the 100k and 1M-row rows removed. Next steps: more data (HPC), profile cross-platform conversions, export to ONNX and
load it in Wayang through the ML plugin (`wayang-plugins/wayang-ml`, `OrtMLModel`).

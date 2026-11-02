# Cost model run

Data: 1653 rows (1653 train, 0 test), configs split so all pairs of one config are on one side; seed 0.

q-error = max(predicted / measured, measured / predicted); 1 is perfect. Values below the noise size count as equal.

## Train set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 1653 | 1.35 | 2.48 | 5.40 | 53.18 |
| io (GB) | 1653 | 1.00 | 1.08 | 1.00 | 10.43 |
| net (GB) | 1653 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 1653 | 1.01 | 1.54 | 4.88 | 337.81 |
| runtime (s) | 1653 | 1.24 | 2.11 | 5.42 | 35.24 |
| energy (J) | 1653 | 1.83 | 10.38 | - | - |

## Learned platform constants

| Platform | io_bandwidth_gb_s | network_bandwidth_gb_s | p (1=add, large=max) | memory_slowdown | idle_power_w | joules_per_core_s |
|---|---|---|---|---|---|---|
| java | 1.34 | 0.1 | 1.77 | 0.121 | 1.02 | 5.03 |
| spark | 0.848 | 0.1 | 1.61 | 0.127 | 1.42 | 4.97 |

## How much the model scales Wayang's estimate (a_r, 1 = unchanged)

| Platform, operator | CPU | I/O | network | memory |
|---|---|---|---|---|
| java cartesian | 0.06 | 3.18 | 1.00 | 0.00 |
| java count | 0.00 | 7.76 | 1.00 | 0.00 |
| java distinct | 0.02 | 3.88 | 1.00 | 0.00 |
| java filter | 0.00 | 6.58 | 1.00 | 0.00 |
| java flatmap | 0.02 | 4.42 | 1.00 | 0.00 |
| java globalreduce | 0.08 | 3.17 | 1.00 | 0.00 |
| java groupby | 0.05 | 5.21 | 1.00 | 0.00 |
| java join | 0.04 | 5.23 | 1.00 | 0.00 |
| java map | 0.01 | 6.13 | 1.00 | 0.00 |
| java reduceby | 0.01 | 3.48 | 1.00 | 0.00 |
| java scan | 0.26 | 2.23 | 1.00 | 0.00 |
| java sort | 0.24 | 3.26 | 1.00 | 0.00 |
| java textsink | 0.40 | 0.82 | 1.00 | 0.00 |
| java union | 0.00 | 6.39 | 1.00 | 0.00 |
| spark cartesian | 0.05 | 2.78 | 1.00 | 0.00 |
| spark count | 0.04 | 2.52 | 1.00 | 0.00 |
| spark distinct | 0.14 | 0.06 | 1.00 | 0.00 |
| spark filter | 0.00 | 6.76 | 1.00 | 0.00 |
| spark flatmap | 0.06 | 2.16 | 1.00 | 0.00 |
| spark globalreduce | 0.07 | 2.79 | 1.00 | 0.00 |
| spark groupby | 0.22 | 5.60 | 1.00 | 1.41 |
| spark join | 0.22 | 4.18 | 1.00 | 0.92 |
| spark map | 0.02 | 3.15 | 1.00 | 0.00 |
| spark reduceby | 0.24 | 0.03 | 1.00 | 0.01 |
| spark scan | 0.08 | 4.23 | 1.00 | 0.00 |
| spark sort | 0.77 | 0.89 | 1.00 | 2.79 |
| spark textsink | 0.06 | 0.43 | 1.00 | 0.00 |
| spark union | 0.00 | 3.68 | 1.00 | 0.00 |

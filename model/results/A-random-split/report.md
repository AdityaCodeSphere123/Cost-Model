# Cost model run

Data: 1653 rows (1333 train, 320 test), configs split so all pairs of one config are on one side; seed 0.

q-error = max(predicted / measured, measured / predicted); 1 is perfect. Values below the noise size count as equal.

## Test set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 320 | 1.41 | 3.07 | 5.48 | 62.15 |
| io (GB) | 320 | 1.00 | 1.10 | 1.00 | 6.45 |
| net (GB) | 320 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 320 | 1.01 | 1.60 | 7.84 | 337.90 |
| runtime (s) | 320 | 1.31 | 2.63 | 5.70 | 36.38 |
| energy (J) | 320 | 1.74 | 14.92 | - | - |

## Train set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 1333 | 1.34 | 2.42 | 5.33 | 51.45 |
| io (GB) | 1333 | 1.00 | 1.09 | 1.00 | 14.43 |
| net (GB) | 1333 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 1333 | 1.01 | 1.38 | 4.11 | 337.81 |
| runtime (s) | 1333 | 1.22 | 1.98 | 5.31 | 35.00 |
| energy (J) | 1333 | 1.87 | 9.70 | - | - |

## Learned platform constants

| Platform | io_bandwidth_gb_s | network_bandwidth_gb_s | p (1=add, large=max) | memory_slowdown | idle_power_w | joules_per_core_s |
|---|---|---|---|---|---|---|
| java | 1.43 | 0.1 | 1.78 | 0.12 | 0.906 | 4.84 |
| spark | 0.859 | 0.1 | 1.61 | 0.127 | 1.37 | 4.9 |

## How much the model scales Wayang's estimate (a_r, 1 = unchanged)

| Platform, operator | CPU | I/O | network | memory |
|---|---|---|---|---|
| java cartesian | 0.07 | 3.44 | 1.00 | 0.00 |
| java count | 0.00 | 9.10 | 1.00 | 0.00 |
| java distinct | 0.02 | 4.20 | 1.00 | 0.00 |
| java filter | 0.00 | 7.57 | 1.00 | 0.00 |
| java flatmap | 0.02 | 6.02 | 1.00 | 0.00 |
| java globalreduce | 0.04 | 4.20 | 1.00 | 0.00 |
| java groupby | 0.04 | 6.26 | 1.00 | 0.00 |
| java join | 0.03 | 6.23 | 1.00 | 0.00 |
| java map | 0.00 | 6.68 | 1.00 | 0.00 |
| java reduceby | 0.01 | 4.02 | 1.00 | 0.00 |
| java scan | 0.25 | 2.47 | 1.00 | 0.00 |
| java sort | 0.24 | 3.83 | 1.00 | 0.00 |
| java textsink | 0.15 | 1.00 | 1.00 | 0.00 |
| java union | 0.00 | 6.64 | 1.00 | 0.00 |
| spark cartesian | 0.05 | 3.93 | 1.00 | 0.00 |
| spark count | 0.04 | 2.96 | 1.00 | 0.00 |
| spark distinct | 0.22 | 0.04 | 1.00 | 0.00 |
| spark filter | 0.01 | 7.57 | 1.00 | 0.00 |
| spark flatmap | 0.07 | 2.45 | 1.00 | 0.00 |
| spark globalreduce | 0.09 | 4.01 | 1.00 | 0.00 |
| spark groupby | 0.23 | 4.66 | 1.00 | 1.38 |
| spark join | 0.30 | 3.03 | 1.00 | 1.39 |
| spark map | 0.02 | 3.43 | 1.00 | 0.00 |
| spark reduceby | 0.34 | 0.02 | 1.00 | 0.01 |
| spark scan | 0.10 | 1.67 | 1.00 | 0.00 |
| spark sort | 0.89 | 1.02 | 1.00 | 2.57 |
| spark textsink | 0.12 | 0.28 | 1.00 | 0.00 |
| spark union | 0.00 | 3.96 | 1.00 | 0.00 |

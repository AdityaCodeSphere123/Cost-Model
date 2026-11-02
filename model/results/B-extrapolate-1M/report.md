# Cost model run

Data: 1653 rows (1330 train, 323 test), configs split so all pairs of one config are on one side; test = configs with more than 300000 rows.

q-error = max(predicted / measured, measured / predicted); 1 is perfect. Values below the noise size count as equal.

## Test set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 323 | 1.63 | 5.14 | 6.25 | 297.67 |
| io (GB) | 323 | 1.00 | 1.20 | 1.00 | 57.45 |
| net (GB) | 323 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 323 | 1.05 | 18.31 | 31.85 | 1127.24 |
| runtime (s) | 323 | 1.44 | 4.80 | 4.27 | 86.13 |
| energy (J) | 323 | 2.02 | 9.61 | - | - |

## Train set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 1330 | 1.34 | 2.10 | 5.26 | 39.08 |
| io (GB) | 1330 | 1.00 | 1.08 | 1.00 | 6.95 |
| net (GB) | 1330 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 1330 | 1.00 | 1.17 | 2.34 | 112.20 |
| runtime (s) | 1330 | 1.24 | 1.92 | 5.72 | 32.01 |
| energy (J) | 1330 | 1.81 | 10.61 | - | - |

## Learned platform constants

| Platform | io_bandwidth_gb_s | network_bandwidth_gb_s | p (1=add, large=max) | memory_slowdown | idle_power_w | joules_per_core_s |
|---|---|---|---|---|---|---|
| java | 1.18 | 0.1 | 1.75 | 0.127 | 1.15 | 5.23 |
| spark | 0.88 | 0.1 | 1.63 | 0.127 | 1.45 | 5.07 |

## How much the model scales Wayang's estimate (a_r, 1 = unchanged)

| Platform, operator | CPU | I/O | network | memory |
|---|---|---|---|---|
| java cartesian | 0.04 | 5.37 | 1.00 | 0.00 |
| java count | 0.00 | 10.18 | 1.00 | 0.00 |
| java distinct | 0.03 | 5.92 | 1.00 | 0.00 |
| java filter | 0.00 | 8.47 | 1.00 | 0.00 |
| java flatmap | 0.02 | 6.42 | 1.00 | 0.00 |
| java globalreduce | 0.06 | 4.79 | 1.00 | 0.00 |
| java groupby | 0.04 | 6.20 | 1.00 | 0.00 |
| java join | 0.03 | 6.93 | 1.00 | 0.00 |
| java map | 0.01 | 7.36 | 1.00 | 0.00 |
| java reduceby | 0.02 | 5.16 | 1.00 | 0.00 |
| java scan | 0.25 | 2.74 | 1.00 | 0.00 |
| java sort | 0.19 | 3.75 | 1.00 | 0.00 |
| java textsink | 0.42 | 1.53 | 1.00 | 0.00 |
| java union | 0.00 | 7.84 | 1.00 | 0.00 |
| spark cartesian | 0.05 | 3.88 | 1.00 | 0.00 |
| spark count | 0.03 | 3.77 | 1.00 | 0.00 |
| spark distinct | 0.16 | 0.13 | 1.00 | 0.00 |
| spark filter | 0.01 | 6.84 | 1.00 | 0.00 |
| spark flatmap | 0.08 | 3.83 | 1.00 | 0.00 |
| spark globalreduce | 0.07 | 4.91 | 1.00 | 0.00 |
| spark groupby | 0.20 | 4.19 | 1.00 | 1.48 |
| spark join | 0.21 | 2.81 | 1.00 | 1.71 |
| spark map | 0.02 | 3.74 | 1.00 | 0.00 |
| spark reduceby | 0.22 | 0.07 | 1.00 | 0.00 |
| spark scan | 0.10 | 3.84 | 1.00 | 0.00 |
| spark sort | 0.64 | 1.25 | 1.00 | 2.38 |
| spark textsink | 0.10 | 1.73 | 1.00 | 0.00 |
| spark union | 0.03 | 1.86 | 1.00 | 0.00 |

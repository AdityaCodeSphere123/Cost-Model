# Cost model run

Data: 952 rows (952 train, 0 test), configs split so all pairs of one config are on one side; seed 0.

q-error = max(predicted / measured, measured / predicted); 1 is perfect. Values below the noise size count as equal.

## Train set

| Output | Rows | Model median | Model p90 | Wayang median | Wayang p90 |
|---|---|---|---|---|---|
| cpu (core-s) | 952 | 1.33 | 1.99 | 5.26 | 37.95 |
| io (GB) | 952 | 1.00 | 1.07 | 1.00 | 6.39 |
| net (GB) | 952 | 1.00 | 1.00 | 1.00 | 1.00 |
| mem (GB) | 952 | 1.00 | 1.08 | 1.68 | 175.92 |
| runtime (s) | 952 | 1.21 | 1.80 | 5.94 | 34.84 |
| energy (J) | 952 | 1.70 | 11.62 | - | - |

## Learned platform constants

| Platform | io_bandwidth_gb_s | network_bandwidth_gb_s | p (1=add, large=max) | memory_slowdown | idle_power_w | joules_per_core_s |
|---|---|---|---|---|---|---|
| java | 1.16 | 0.1 | 1.75 | 0.127 | 1.35 | 5.5 |
| spark | 0.886 | 0.1 | 1.63 | 0.127 | 1.87 | 5.48 |

## How much the model scales Wayang's estimate (a_r, 1 = unchanged)

| Platform, operator | CPU | I/O | network | memory |
|---|---|---|---|---|
| java cartesian | 0.08 | 3.04 | 1.00 | 0.00 |
| java count | 0.01 | 5.04 | 1.00 | 0.00 |
| java distinct | 0.03 | 3.56 | 1.00 | 0.00 |
| java filter | 0.00 | 7.34 | 1.00 | 0.00 |
| java flatmap | 0.01 | 6.01 | 1.00 | 0.00 |
| java globalreduce | 0.00 | 6.89 | 1.00 | 0.00 |
| java groupby | 0.03 | 5.34 | 1.00 | 0.00 |
| java join | 0.03 | 6.57 | 1.00 | 0.00 |
| java map | 0.00 | 6.45 | 1.00 | 0.00 |
| java reduceby | 0.01 | 3.95 | 1.00 | 0.00 |
| java scan | 0.20 | 3.62 | 1.00 | 0.00 |
| java sort | 0.18 | 2.53 | 1.00 | 0.00 |
| java textsink | 0.06 | 8.54 | 1.00 | 0.00 |
| java union | 0.00 | 4.96 | 1.00 | 0.00 |
| spark cartesian | 0.05 | 0.69 | 1.00 | 0.00 |
| spark count | 0.02 | 2.65 | 1.00 | 0.00 |
| spark distinct | 0.14 | 0.23 | 1.00 | 0.00 |
| spark filter | 0.01 | 5.47 | 1.00 | 0.00 |
| spark flatmap | 0.06 | 3.93 | 1.00 | 0.00 |
| spark globalreduce | 0.03 | 3.20 | 1.00 | 0.00 |
| spark groupby | 0.22 | 4.18 | 1.00 | 0.17 |
| spark join | 0.40 | 3.83 | 1.00 | 0.39 |
| spark map | 0.01 | 4.05 | 1.00 | 0.00 |
| spark reduceby | 0.22 | 0.02 | 1.00 | 0.01 |
| spark scan | 0.05 | 4.71 | 1.00 | 0.00 |
| spark sort | 0.74 | 2.19 | 1.00 | 0.84 |
| spark textsink | 0.06 | 5.72 | 1.00 | 0.00 |
| spark union | 0.00 | 2.04 | 1.00 | 0.00 |

Note: only 952 training rows. These numbers only show that the pipeline works; they are not a measure of the model's accuracy.

# Jobs: measured vs Wayang vs cost model

Model: `D-no-job-sizes`

Runtime from the first record to the end (median of the measured runs). q-error = max(a/b, b/a), 1 is perfect.

| Job | Size | Placement | Measured ms | Wayang ms | Model ms (true rows) | Model ms (Wayang rows) | q Wayang | q model | q model (Wayang rows) |
|---|---|---|---|---|---|---|---|---|---|
| filter-distinct | 100k | java | 13 | 77 | 15 | 16 | 5.78 | 1.15 | 1.17 |
| filter-distinct | 100k | mixed | 23 | 88 | 22 | 23 | 3.85 | 1.02 | 1.01 |
| filter-distinct | 100k | spark | 23 | 115 | 34 | 34 | 4.94 | 1.47 | 1.48 |
| filter-distinct | 1M | java | 133 | 641 | 124 | 127 | 4.81 | 1.07 | 1.05 |
| filter-distinct | 1M | mixed | 157 | 194 | 56 | 58 | 1.23 | 2.80 | 2.73 |
| filter-distinct | 1M | spark | 78 | 219 | 89 | 92 | 2.79 | 1.13 | 1.17 |
| filter-reduceby | 100k | java | 16 | 82 | 15 | 15 | 5.02 | 1.08 | 1.08 |
| filter-reduceby | 100k | mixed | 30 | 93 | 22 | 22 | 3.09 | 1.36 | 1.36 |
| filter-reduceby | 100k | spark | 49 | 125 | 31 | 31 | 2.55 | 1.60 | 1.60 |
| filter-reduceby | 1M | java | 134 | 656 | 121 | 122 | 4.89 | 1.11 | 1.10 |
| filter-reduceby | 1M | mixed | 231 | 209 | 52 | 53 | 1.11 | 4.42 | 4.35 |
| filter-reduceby | 1M | spark | 92 | 190 | 87 | 88 | 2.06 | 1.06 | 1.05 |
| flatmap-count | 100k | java | 33 | 138 | 29 | 18 | 4.20 | 1.15 | 1.81 |
| flatmap-count | 100k | mixed | 104 | 81 | 29 | 26 | 1.28 | 3.57 | 4.02 |
| flatmap-count | 100k | spark | 20 | 101 | 15 | 12 | 5.17 | 1.29 | 1.64 |
| flatmap-count | 1M | java | 238 | 897 | 247 | 142 | 3.77 | 1.04 | 1.67 |
| flatmap-count | 1M | mixed | 1209 | 121 | 105 | 77 | 9.99 | 11.53 | 15.72 |
| flatmap-count | 1M | spark | 132 | 152 | 93 | 65 | 1.15 | 1.42 | 2.03 |
| join-map-reduceby | 100k-k100k | java | 59 | 384 | 69 | 76 | 6.47 | 1.16 | 1.28 |
| join-map-reduceby | 100k-k100k | mixed | 151 | 290 | 199 | 200 | 1.93 | 1.32 | 1.33 |
| join-map-reduceby | 100k-k100k | spark | 209 | 273 | 274 | 277 | 1.30 | 1.31 | 1.32 |
| join-map-reduceby | 100k-k10k | java | 142 | 384 | 75 | 50 | 2.71 | 1.90 | 2.85 |
| join-map-reduceby | 100k-k10k | mixed | 226 | 290 | 172 | 169 | 1.28 | 1.31 | 1.34 |
| join-map-reduceby | 100k-k10k | spark | 181 | 273 | 219 | 208 | 1.51 | 1.21 | 1.15 |
| map-sort | 100k | java | 43 | 124 | 33 | 33 | 2.90 | 1.29 | 1.28 |
| map-sort | 100k | mixed | 64 | 146 | 38 | 38 | 2.29 | 1.69 | 1.69 |
| map-sort | 100k | spark | 136 | 152 | 151 | 151 | 1.12 | 1.11 | 1.11 |
| map-sort | 1M | java | 426 | 1141 | 299 | 301 | 2.68 | 1.42 | 1.42 |
| map-sort | 1M | mixed | 701 | 854 | 229 | 230 | 1.22 | 3.06 | 3.05 |
| map-sort | 1M | spark | 597 | 285 | 849 | 863 | 2.09 | 1.42 | 1.45 |

## Summary (median q-error over jobs)

| Placement | Jobs | Wayang | Model (true rows) | Model (Wayang rows) | Model better than Wayang |
|---|---|---|---|---|---|
| java | 10 | 4.51 | 1.15 | 1.28 | 10 of 10 |
| spark | 10 | 2.08 | 1.30 | 1.39 | 8 of 10 |
| mixed | 10 | 1.61 | 2.24 | 2.21 | 4 of 10 |
| all | 30 | 2.69 | 1.31 | 1.39 | 22 of 30 |

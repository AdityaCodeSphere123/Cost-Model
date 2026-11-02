# Jobs: measured vs Wayang vs cost model

Model: `C-all-data`

Runtime from the first record to the end (median of the measured runs). q-error = max(a/b, b/a), 1 is perfect.

| Job | Size | Placement | Measured ms | Wayang ms | Model ms (true rows) | Model ms (Wayang rows) | q Wayang | q model | q model (Wayang rows) |
|---|---|---|---|---|---|---|---|---|---|
| filter-distinct | 100k | java | 13 | 77 | 15 | 16 | 5.78 | 1.15 | 1.22 |
| filter-distinct | 100k | mixed | 23 | 88 | 21 | 22 | 3.85 | 1.07 | 1.02 |
| filter-distinct | 100k | spark | 23 | 115 | 34 | 38 | 4.94 | 1.45 | 1.64 |
| filter-distinct | 1M | java | 133 | 641 | 122 | 132 | 4.81 | 1.09 | 1.01 |
| filter-distinct | 1M | mixed | 157 | 194 | 43 | 53 | 1.23 | 3.63 | 2.97 |
| filter-distinct | 1M | spark | 78 | 219 | 93 | 185 | 2.79 | 1.19 | 2.35 |
| filter-reduceby | 100k | java | 16 | 82 | 15 | 15 | 5.02 | 1.08 | 1.08 |
| filter-reduceby | 100k | mixed | 30 | 93 | 21 | 21 | 3.09 | 1.42 | 1.42 |
| filter-reduceby | 100k | spark | 49 | 125 | 31 | 31 | 2.55 | 1.60 | 1.58 |
| filter-reduceby | 1M | java | 134 | 656 | 119 | 120 | 4.89 | 1.13 | 1.12 |
| filter-reduceby | 1M | mixed | 231 | 209 | 42 | 42 | 1.11 | 5.54 | 5.47 |
| filter-reduceby | 1M | spark | 92 | 190 | 83 | 94 | 2.06 | 1.11 | 1.02 |
| flatmap-count | 100k | java | 33 | 138 | 30 | 19 | 4.20 | 1.08 | 1.70 |
| flatmap-count | 100k | mixed | 104 | 81 | 31 | 26 | 1.28 | 3.38 | 4.01 |
| flatmap-count | 100k | spark | 20 | 101 | 16 | 11 | 5.17 | 1.24 | 1.79 |
| flatmap-count | 1M | java | 238 | 897 | 251 | 140 | 3.77 | 1.06 | 1.70 |
| flatmap-count | 1M | mixed | 1209 | 121 | 104 | 60 | 9.99 | 11.57 | 20.07 |
| flatmap-count | 1M | spark | 132 | 152 | 92 | 47 | 1.15 | 1.44 | 2.79 |
| join-map-reduceby | 100k-k100k | java | 59 | 384 | 79 | 91 | 6.47 | 1.33 | 1.53 |
| join-map-reduceby | 100k-k100k | mixed | 151 | 290 | 227 | 232 | 1.93 | 1.51 | 1.54 |
| join-map-reduceby | 100k-k100k | spark | 209 | 273 | 281 | 287 | 1.30 | 1.34 | 1.37 |
| join-map-reduceby | 100k-k10k | java | 142 | 384 | 99 | 61 | 2.71 | 1.43 | 2.34 |
| join-map-reduceby | 100k-k10k | mixed | 226 | 290 | 221 | 207 | 1.28 | 1.02 | 1.09 |
| join-map-reduceby | 100k-k10k | spark | 181 | 273 | 261 | 243 | 1.51 | 1.44 | 1.34 |
| map-sort | 100k | java | 43 | 124 | 38 | 38 | 2.90 | 1.13 | 1.12 |
| map-sort | 100k | mixed | 64 | 146 | 42 | 42 | 2.29 | 1.53 | 1.53 |
| map-sort | 100k | spark | 136 | 152 | 160 | 161 | 1.12 | 1.18 | 1.18 |
| map-sort | 1M | java | 426 | 1141 | 354 | 355 | 2.68 | 1.20 | 1.20 |
| map-sort | 1M | mixed | 701 | 854 | 275 | 276 | 1.22 | 2.55 | 2.54 |
| map-sort | 1M | spark | 597 | 285 | 714 | 724 | 2.09 | 1.20 | 1.21 |

## Summary (median q-error over jobs)

| Placement | Jobs | Wayang | Model (true rows) | Model (Wayang rows) | Model better than Wayang |
|---|---|---|---|---|---|
| java | 10 | 4.51 | 1.13 | 1.21 | 10 of 10 |
| spark | 10 | 2.08 | 1.29 | 1.47 | 7 of 10 |
| mixed | 10 | 1.61 | 2.04 | 2.04 | 5 of 10 |
| all | 30 | 2.69 | 1.29 | 1.53 | 22 of 30 |

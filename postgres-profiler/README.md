# PostgreSQL operator profiler

Measures the **real** resources Wayang's PostgreSQL platform uses, as training data for the ML cost model. It is
the Postgres counterpart of [`../java-profiler`](../java-profiler/README.md) and
[`../spark-profiler`](../spark-profiler/README.md): same method (baseline / operator pairs), same generated input
rows, same CSV layout, and Wayang's own estimate next to every measurement (`wayang_est_*`).

## How Wayang runs Postgres (and why that shapes the method)

Wayang does not run Postgres operators one by one. `JdbcExecutor` turns all Postgres operators of a stage into
**one SQL statement**, and `SqlToStreamOperator` (a Java operator, inserted automatically where the data moves to
Java) opens a **new connection**, runs the statement and streams the rows into Java:

```
PostgresTableSource -> PostgresFilterOperator -> [SqlToStream] -> Java sink
        \_______________ SELECT * FROM t WHERE key < 300 _______/     rows over the socket into Java
```

So a Postgres operator's cost is **what it adds to that statement**: database work, plus the change in rows (and
bytes) sent to Java. A filter or projection usually makes the plan *cheaper* (fewer rows/bytes to Java), so its
operator − baseline cost is negative. That is real and is exactly what Wayang's estimates get wrong (see below).

## Plans

The Postgres operators are execution operators placed directly in the plan, so Wayang cannot move them to Java.

| `--op` | Operator plan (SQL Wayang generates) | Baseline |
|---|---|---|
| `filter` | `SELECT * FROM t WHERE key < sel*keys` (output = sel x rows) | `SELECT * FROM t` |
| `projection` | `SELECT id, value FROM t` (2 of 4 columns) | `SELECT * FROM t` |
| `join` | `SELECT * FROM l JOIN r ON r.key = l.key` | two jobs: `SELECT * FROM l`, `SELECT * FROM r` |
| `globalreduce` | `SELECT SUM(value) FROM t` (one row to Java) | `SELECT * FROM t` |
| `tablesink` | `CREATE TABLE out AS SELECT * FROM t` (nothing to Java) | `SELECT * FROM t` |
| `sqltostream` | `SELECT * FROM t` (all rows to Java) | `SELECT COUNT(id) FROM t` (one row) |

`sqltostream` isolates the **Postgres -> Java hand-off** (connection, transfer, turning rows into Java `Record`s):
the cross-platform cost that mixed jobs need and Wayang estimates poorly. These are all the Postgres operators this
Wayang version has (`wayang-postgres`: table source, filter, projection, join, global reduce, table sink; sort and
group-by exist only in `wayang-jdbc-template`, not for Postgres). `measured_execution_operators` says what each
difference covers.

Tables: `wp_r<rows>_k<keys>_w<words>_s<seed>[_i]`, columns `id bigint, key int, value double precision, text text`,
loaded with `COPY` from the **same generated file** the Java and Spark profilers read (same rows), then
`VACUUM ANALYZE`, autovacuum off for the table, `CHECKPOINT`. They are kept and reused (`--drop-tables` /
`--ephemeral-inputs` removes them after the config).

## What is measured

Every JDBC connection Wayang opens goes through a thin wrapper driver (`jdbc:wayangprof:postgresql://...`, set as
Wayang's `wayang.postgres.jdbc.url`). It records when each connection is opened, every statement with start and end,
and, via a counting socket, the bytes on the wire. Nothing per row is wrapped, so it adds no per-row cost.

| Group | Columns | Notes |
|---|---|---|
| Time | `wall_exec_ns` | from the first connection Wayang opens to **execute** the job (after its optimizer) to the end: connections + query + transfer + Java side |
| | `wall_connect_ns`, `wall_query_ns`, `wall_client_ns` | its parts: opening connections (server process start, login) / statement calls (database work + receiving all rows: the driver reads the whole result) / the rest (Java turns rows into Records, sink, Wayang) |
| | `wall_total_ns` | the whole `execute()`, incl. Wayang's optimizer |
| Network | `network_bytes_in`, `network_bytes_out`, `network_bytes` | bytes Postgres -> Java (result rows) and Java -> Postgres, counted on the socket |
| Database CPU | `db_backend_cpu_ns`, `db_parallel_cpu_ns`, `db_cpu_ns` | **Linux, database on this machine**: the server process of each connection, read from `/proc` just before it closes; the parallel query workers (separate processes) from the postmaster's "CPU of exited children" counter |
| Database disk | `db_disk_read_bytes`, `db_disk_write_bytes` | the server processes' `/proc/PID/io` (needs the profiler to run as the database's OS user) |
| EXPLAIN | `db_explain_exec_ms`, `db_explain_planning_ms`, `db_explain_jit_ms`, `db_shared_hit_blocks`, `db_shared_read_blocks`, `db_temp_read_blocks`, `db_temp_written_blocks` (spill), `db_workers_launched`, `db_explain_rows`, `db_plan` (e.g. `Hash Join(Seq Scan,Hash(Seq Scan))`) | one extra, untimed `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` of each plan's SQL (median of 3), same session settings. Server-side only: no transfer to Java |
| Java side | `thread_cpu_exec_ns`, `process_cpu_exec_ns`, `thread_alloc_exec_bytes`, GC/JIT, heap pools, `os_*` | as in the Java profiler (the Java side runs on one thread) |
| Wayang's optimizer | `optimizer_db_query_ns`, `optimizer_db_cpu_ns` | Wayang runs `SELECT count(*) FROM t` for every table source while optimizing (its cardinality estimate); kept out of the exec window and recorded here |
| What ran | `sql`, `db_connections`, `db_statements`, `out_rows`, `out_bytes` (table sink: size of the new table) | |

Features besides the usual ones (`rows`, `right_rows`, `keys`, `sel`, `words`, `in_bytes`, JVM): `pg_parallel_workers`,
`pg_indexed`, `table_bytes`, `right_table_bytes`, `pg_version`, `pg_shared_buffers`, `pg_work_mem`,
`pg_effective_cache_size`, `pg_jit`, `pg_autovacuum`, `pg_random_page_cost`, `pg_options`, `db_local`.

Resource settings are `HEAP:GC:pN[i]`: JVM heap and GC of the Java side, `max_parallel_workers_per_gather = N` for
every Postgres session, and `i` = a B-tree index on `key` (filter and join can then use index / bitmap scans).

## Wayang's own estimates

As in the other profilers (`wayang_est_*`, operator plan − baseline plan in `pairs.csv`), plus
`wayang_est_time_ms_gmean_postgres` / `_java` (the plan's estimated time split by platform) and:

- `wayang_est_fallback`: Wayang has **no load-estimate setting** for Postgres join, global reduce and table sink
  (`wayang-postgres-defaults.properties` only has table source, filter, projection and SqlToStream). It then uses a
  generic fallback (a fixed range of CPU cycles per row). Such rows have `wayang_est_complete = 0`.
- Wayang's Postgres converter counts only CPU (`wayang.postgres.cores = 2`, `cpu.mhz = 2700`): no disk or network.

First observations (laptop, 100k rows): Wayang predicts that a filter with selectivity 0.1 makes the plan
**16 ms more expensive**; measured it is **28 ms cheaper**, because 90% fewer rows go to Java. It predicts that
sending 100k rows to Java instead of one aggregate saves 7 ms (`sqltostream`); measured it costs **+31 ms**.

## Run

```bash
./start-local-postgres.sh            # once: a server used only for profiling, 127.0.0.1:5433, database wayangprof
./run-profiling.sh --dry-run
./run-profiling.sh --profile quick   # ~1 minute
./run-profiling.sh                   # default grid
./run-profiling.sh --profile full    # adds parallel workers and indexes
./run-profiling.sh --ops "filter join" --rows "100000 1000000" --resources "4g:G1:p0 4g:G1:p2i"
./run-profiling.sh --pg-options "-c work_mem=64MB -c jit=off"    # other session settings (recorded in pg_options)
```

Database: `--url postgresql://HOST:PORT/DB --user U` (or `PGURL`, `PGUSER`, `PGPASSWORD`). The user needs to
create tables in that database; `CHECKPOINT` needs superuser or `pg_checkpoint` (otherwise a warning, and dirty
pages from loading may be flushed during runs). If the database is not reachable the runner stops instead of marking
configs as failed. In the long server run: `../server/run-server.sh --postgres postgresql://127.0.0.1:5433/wayangprof`.

## For clean data

- **Use a dedicated server on the same machine** (`start-local-postgres.sh`): nothing else queries it, autovacuum is
  off, and the database CPU can be measured. With a remote database everything except `db_*_cpu_ns` / `db_disk_*`
  still works (`db_local = 0`).
- **Warm cache.** Warm-up runs bring the tables into shared buffers and the OS cache, so measurements are warm
  (`db_shared_read_blocks` in the EXPLAIN shows when a table did not fit in shared buffers). Cold-cache data would
  need a server restart and dropping the OS cache before every run, which this profiler does not do.
- **Connections.** Wayang's `SqlToStreamOperator` never closes its connection (a leak: a long job would exhaust
  `max_connections`). The profiler closes it after every run, so each run starts with no leftover server processes.
- **Table sink.** Before every run the output table is dropped and a `CHECKPOINT` flushes the previous run's writes,
  so every run writes into a clean state.
- **JIT.** PostgreSQL's own JIT is on by default (`jit_above_cost` 100000), so large scans and joins include LLVM
  compile time (`db_explain_jit_ms`). That is how a stock server behaves; `--pg-options "-c jit=off"` turns it off.

## Notes

- `cpu_signal_ok` in `operator_costs.csv` is based on `wall_exec_ns` and its size (either sign) here.
- Each run opens two connections (Wayang's `JdbcExecutor` and `SqlToStream`; four for the join baseline), each ~2-5 ms:
  in both plans, so mostly cancelled, except for join, whose baseline runs two jobs.
- macOS: no `/proc`, so database CPU and disk stay empty; everything else works.

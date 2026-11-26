# Persistence Benchmark Guide

`src/event_pipeline/bench_persistence.py` measures the telemetry persistence
path — `HalfSyncHalfAsyncPersistenceWorker` → `TelemetryRepository` → SQLite/WAL
— and writes machine-readable artifacts plus a summary table.

It exists because the numbers in
[EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md) §2 were produced by
ad-hoc, uncommitted scripts. This tool reproduces that methodology
(`--checkpoint-mode manual` is the same approach) and records more than the
report originally measured.

- [EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md) — what changed and why
- [EDGE_DEPLOYMENT.md](EDGE_DEPLOYMENT.md) — operating the device
- [RELATIONAL_DB_DESIGN_REVIEW.md](RELATIONAL_DB_DESIGN_REVIEW.md) — schema rationale

---

## 1. Quick start

```bash
make bench                    # edge profile: 5 Hz, 60 s, 10 s flush
make bench-smoke              # 3 s saturating run, quick harness check

# Equivalent, with the full flag list:
python -m event_pipeline.bench_persistence --help
eaa-bench-persistence --rate 5 --duration 60
```

Each run writes into `bench-results/<label>-<UTC timestamp>/` (or `--out DIR`):

| File | Contents |
|---|---|
| `summary.json` | One JSON document with every aggregate below |
| `timeseries.csv` | One row per sample: queue, RSS, DB/WAL sizes, cumulative I/O |

`bench-results/` is gitignored.

---

## 2. What it measures

| Metric | Field(s) | How it is produced |
|---|---|---|
| Bytes written | `io.wchar_bytes`, `io.bytes_per_row` | Δ`wchar` from `/proc/self/io`: bytes the process handed to the filesystem, counted at syscall time |
| WAL volume | `io.wal_logical_bytes` | Monotonic growth accumulation across checkpoints (see §4) |
| DB growth | `io.db_growth_bytes`, `storage.db_*` | File size sampling |
| Write latency | `latency_ms.enqueue`, `.flush`, `.end_to_end` | `submit()` duration; `bulk_insert` duration; per-event submit→durable |
| Read latency | `latency_ms.read_fetch_recent`, `.read_count` | Timed `fetch_recent` / `count` while writes are in flight |
| Maintenance latency | `latency_ms.prune`, `.checkpoint` | Timed `prune` / `PRAGMA wal_checkpoint` |
| Queue depth | `queue.max_depth`, `.mean_depth`, `.p95_depth`, `timeseries.queue_size` | `PersistenceStats.queue_size` sampled every `--sample-interval` |
| Drops / failures | `queue.dropped_queue_full`, `.dropped_low_disk`, `.failed_batches`, `.unflushed_on_shutdown` | Final `PersistenceStats` |
| Flush time | `latency_ms.flush` | Wall time of one `bulk_insert` (includes `BEGIN`/`COMMIT`) |
| Checkpoint time | `latency_ms.checkpoint` | Wall time of an explicit checkpoint; unavailable in `auto` mode (§5) |
| RAM | `memory.rss_start_kb`, `.rss_end_kb`, `.rss_peak_kb`, `.hwm_kb` | `VmRSS` sampling, `VmHWM` |
| DB/WAL over time | `timeseries.db_bytes`, `.wal_bytes`, `.shm_bytes` | File size each sample |

Every latency series reports `n`, `min`, `mean`, `max`, `p50`, `p95`, `p99`, with
percentiles linear-interpolated over the sorted samples.

---

## 3. `summary.json` layout

```
label, started_at, duration_s, interrupted
config        rate_hz, batch_size, flush_interval_s, queue_capacity,
              retention_max_rows, min_free_bytes, checkpoint_mode,
              checkpoint_bytes, checkpoint_truncate, read_interval_s,
              read_limit, sample_interval_s
throughput    submitted, rows_inserted, events_per_s, batches
io            wchar_bytes, write_bytes, read_bytes, bytes_per_row,
              wal_logical_bytes, db_growth_bytes, diskstats_bytes, availability
latency_ms    enqueue, flush, end_to_end, read_fetch_recent, read_count,
              prune, checkpoint          (each: n, min, mean, max, p50, p95, p99)
queue         capacity, max_depth, mean_depth, p95_depth, utilization_pct,
              dropped_queue_full, dropped_low_disk, failed_batches,
              stopped_rejected, unflushed_on_shutdown, unpersisted_at_end,
              evicted_unpersisted, prune_calls, pruned_rows,
              checkpoint_count, checkpoint_errors
memory        rss_start_kb, rss_end_kb, rss_peak_kb, hwm_kb, rss_delta_kb
storage       db_start, db_end, db_peak, wal_start, wal_end, wal_peak,
              page_size, journal_mode
environment   platform, python, sqlite, cpu_count
```

`timeseries.csv` columns: `t_s, queue_size, rss_kb, db_bytes, wal_bytes,
shm_bytes, rows_inserted, batches_flushed, wchar_bytes_total,
write_bytes_total, last_flush_age_s`. The two `*_total` columns are cumulative
process counters (monotonic), not per-sample deltas.

`environment` exists so two runs are comparable: record it alongside results,
since CPU, filesystem and SQLite build all move the absolute numbers.

---

## 4. Reading the numbers correctly

### 4.1 `wchar` is not physical flash wear

`wchar` counts bytes written *through the filesystem*. Filesystem caching, FTL
wear levelling and the card's endurance determine actual NAND writes, so treat
`wchar` as a comparison metric between runs on the same device, not a TBW figure.
For real wear, read the block device's counters (`/proc/diskstats`,
`iostat`, or the vendor's SMART/TBW data) before and after a longer run.

`io.write_bytes` is recorded but usually reported as `null`: writeback is
attributed asynchronously, and on container/overlay filesystems it commonly stays
at 0 for an entire run. The summary says so explicitly in
`io.availability.write_bytes` rather than reporting a misleading zero. Likewise
`io.diskstats_bytes` is `null` when the database's device does not appear in
`/proc/diskstats` (typical in containers).

### 4.2 WAL volume excludes pre-run bytes

`wal_logical_bytes` counts growth of the WAL file during the measured window and
correctly handles truncating checkpoints (a reset adds nothing; growth after it
is counted). It therefore excludes the WAL content left by schema initialisation,
so it can legitimately be **smaller** than `storage.wal_peak`, which is the
absolute file size. Do not expect the two to match.

### 4.3 Queue depth is a sawtooth

With the edge profile (5 Hz, 10 s flush) the queue fills to roughly
`rate × flush_interval` ≈ 50 and then drains on flush. `max_depth` near that
value is expected, not a leak. What matters is whether `max_depth` approaches
`queue_capacity`, and whether `dropped_queue_full` is non-zero.

### 4.4 Read latency includes connection serialisation

Reads and writes share one `aiosqlite` connection, so a timed read can queue
behind an in-flight flush. The measured `read_fetch_recent` values (~1 ms at the
edge profile, several ms under a saturating write rate) reflect a real property
of this design, not measurement overhead.

### 4.5 `end_to_end` exposes the flush window

`latency_ms.end_to_end` is per-event submit→durable. With a 10 s flush interval
it is dominated by the batching delay: a p99 just under 10 s is the documented
at-most-once audit-loss window made visible. Use it to justify (or reject) a
flush-interval change.

---

## 5. Checkpoint modes

| Mode | Behaviour | Checkpoint latency |
|---|---|---|
| `manual` (default) | Sets `PRAGMA wal_autocheckpoint=0` and issues a timed `PRAGMA wal_checkpoint` once the WAL exceeds `--checkpoint-bytes` | Measured (`latency_ms.checkpoint`), `wal_logical_bytes` labelled `exact` |
| `auto` | Leaves production's 1000-page autocheckpoint in place | Not available — SQLite exposes no timer for internal checkpoints; `wal_logical_bytes` labelled `estimated` from observed WAL shrinkage |

Use `manual` when you need checkpoint cost, and `auto` when you want a
production-like I/O pattern. `--checkpoint-truncate` uses `TRUNCATE` instead of
`PASSIVE` (the production clean-shutdown behaviour).

---

## 6. Scenarios

```bash
# Edge profile: what the device actually does
make bench

# Saturating write rate: find the ceiling and tail latency
python -m event_pipeline.bench_persistence --rate 0 --duration 20 --label saturate

# Batching comparison: the report's "2 s vs 10 s flush" question
python -m event_pipeline.bench_persistence --rate 5 --duration 60 \
  --flush-interval 2 --label flush-2s

# Retention cost (prune runs inside the worker's write path)
python -m event_pipeline.bench_persistence --rate 400 --duration 10 \
  --retention-max-rows 5000 --label retention

# Production-like I/O pattern, no explicit checkpoints
python -m event_pipeline.bench_persistence --checkpoint-mode auto --label auto-ckpt

# Keep the database for inspection
python -m event_pipeline.bench_persistence --db-path /tmp/bench.db --keep-db --duration 10
python -c "import sqlite3; print(sqlite3.connect('/tmp/bench.db').execute('SELECT COUNT(*) FROM telemetry_events').fetchone()[0])"
```

`--rate 0` disables pacing: the queue will fill and `dropped_queue_full` will
grow once the writer cannot keep up. That is the intended behaviour of the mode,
not a failure.

---

## 7. Flag reference

| Flag | Default | Purpose |
|---|---|---|
| `--label` | `run` | Run name, used in the output directory |
| `--duration` | `30` | Seconds to run (must be > 0) |
| `--rate` | `5` | Events/second; `0` saturates |
| `--batch-size` | `50` | Worker batch size |
| `--flush-interval` | `10` | Worker flush interval (seconds) |
| `--queue-capacity` | `10000` | Worker queue capacity |
| `--max-retries` | `3` | Worker write retries |
| `--retention-max-rows` | `0` | Enables prune at N rows (`0` = off) |
| `--prune-interval` | `1.0` | Prune interval (seconds) |
| `--min-free-bytes` | `0` | Low-disk drop floor |
| `--checkpoint-mode` | `manual` | `manual` or `auto` (§5) |
| `--checkpoint-bytes` | `4194304` | WAL size that triggers a timed checkpoint |
| `--checkpoint-truncate` | off | Use `TRUNCATE` instead of `PASSIVE` |
| `--read-interval` | `0.5` | Seconds between timed reads; `0` disables |
| `--read-limit` | `100` | `fetch_recent` limit for timed reads |
| `--sample-interval` | `0.5` | Metrics sampling interval |
| `--drain-timeout` | `10` | Worker drain budget at shutdown |
| `--db-path` | temp dir | Database location |
| `--keep-db` | off | Keep the temporary database |
| `--out` | `bench-results/<label>-<ts>` | Output directory |
| `--quiet` | off | Suppress the human-readable table |

---

## 8. Example output

From the development host (not the target device; absolute values will differ).
Byte counts are stable run to run; sub-millisecond latencies vary by a few
hundredths of a millisecond.

```text
benchmark: edge-5hz  (12.0s, manual checkpoint)

throughput
  submitted            60
  rows inserted        60
  events/s             5.00

bytes
  wchar (fs writes)    115,072 B
  per row              1,917.90 B
  WAL logical          65,920 B (exact)
  DB growth            45,056 B

latency (ms)                  p50        p95        p99        n
  enqueue                   0.02       0.03       0.04       60
  flush                     2.52       3.71       3.82        2
  end_to_end            3,904.21   9,214.31   9,686.62       60
  read_fetch_recent         0.98       3.11       4.93       23
  checkpoint                0.34       0.34       0.34        1
```

The `end_to_end` tail is the 10 s flush interval, as expected at this profile.

---

## 9. Troubleshooting

| Symptom | Meaning |
|---|---|
| `io.write_bytes: null` | Writeback attribution; expected in containers. Use `wchar_bytes`. |
| `io.diskstats_bytes: null` | DB device absent from `/proc/diskstats`. Measure the device manually. |
| `latency_ms.checkpoint.n = 0` | `auto` mode, or the WAL never reached `--checkpoint-bytes`. |
| `prune.n = 0` | Retention disabled (`--retention-max-rows 0`). |
| `interrupted: true` | SIGINT during the run; a partial summary is still written. |
| `queue.dropped_queue_full > 0` | Producer outran the writer (expected with `--rate 0`). |
| All `memory`/`io` fields `null` | Not Linux, or `/proc` unavailable. The run still completes. |

---

## 10. Relationship to the CI guard

`test_write_cost_is_neutral_versus_previous_schema` in
`tests/unit_tests/test_persistence.py` compares the current schema against the
previous single-table schema **in CI** and fails above +5 % write cost. That test
is the automation; this benchmark is the deeper, on-demand measurement with
latency, memory and time-series detail. If you change the write path, expect the
guard to catch regressions automatically and use the benchmark to explain them.

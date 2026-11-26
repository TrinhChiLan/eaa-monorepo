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
| Write latency | `latency_ms.enqueue`, `.flush`, `.end_to_end` | See §2.1 — three different spans of the same request |
| Read latency | `latency_ms.read_fetch_recent`, `.read_count` | Timed `fetch_recent` / `count` while writes are in flight |
| Maintenance latency | `latency_ms.prune`, `.checkpoint` | Timed `prune` / `PRAGMA wal_checkpoint` |
| Queue depth | `queue.max_depth`, `.mean_depth`, `.p95_depth`, `timeseries.queue_size` | `PersistenceStats.queue_size` sampled every `--sample-interval` |
| Drops / failures | `queue.dropped_queue_full`, `.dropped_low_disk`, `.failed_batches`, `.unflushed_on_shutdown` | Final `PersistenceStats` |
| RAM | `memory.rss_start_kb`, `.rss_end_kb`, `.rss_peak_kb`, `.hwm_kb` | `VmRSS` sampling, `VmHWM` |
| DB/WAL over time | `timeseries.db_bytes`, `.wal_bytes`, `.shm_bytes` | File size each sample |

Every latency series reports the same eight fields — see §2.2 for the statistic
labels and §2.1 for what each series actually times.

### 2.1 Latency metrics

An event's journey is **produce → `submit()` → queue → batch → `bulk_insert` →
commit**. Each latency series measures one span of that journey. They are not
alternatives to compare against each other; together they say *where* the time
goes.

| Series | What is timed | Span | Unit |
|---|---|---|---|
| `enqueue` | One `await worker.submit(event)` call in the producer | produce → accepted into the in-process queue | ms |
| `flush` | One `await repository.bulk_insert(batch)` call in the worker | batch handed to the DB → commit returns | ms |
| `end_to_end` | `time.perf_counter()` at submit → completion of the flush that contains the event | produce → durable on disk | ms |
| `read_fetch_recent` | One `await repository.fetch_recent(limit=--read-limit)` | call → rows returned | ms |
| `read_count` | One `await repository.count()` | call → count returned | ms |
| `prune` | One worker-side `await repository.prune(...)` | call → deleted rows returned | ms |
| `checkpoint` | One `PRAGMA wal_checkpoint(PASSIVE\|TRUNCATE)` | pragma issued → pragma returns | ms |

- **`enqueue`** — how long `submit()` takes on the producer's side. `submit()`
  never waits for I/O: it does a non-blocking `put_nowait` (`repository.py`)
  and returns immediately, so this is normally tens of microseconds at 5 Hz.
  It is **not** where batching latency lives. It does rise when the producer
  saturates (`--rate 0`) because the producer and worker now contend for the
  same event loop.
- **`flush`** — the work the worker actually does for a batch:
  `BEGIN IMMEDIATE` → `executemany` of the row inserts → `COMMIT`, under the
  repository's write lock (so a concurrent checkpoint or read can be included).
  Its `n` is the number of committed batches, i.e. roughly
  `duration / flush_interval` under the edge profile
  (5–6 for `make bench`), so **`flush` percentiles over a handful of samples
  are statistically weak** — read `mean`/`max`, not `p99`, when `n` is small.
  A failed attempt that is retried is timed separately (each `bulk_insert`
  call appends one sample), so a retried batch contributes 2+ samples and an
  abandoned batch contributes `--max-retries + 1` samples with no row.
- **`end_to_end`** — the user-visible durability latency. This includes the
  batching window (`flush_interval`) plus `flush`. Its `n` counts *persisted
  events* (one sample per row in a successful batch), so it is the only
  latency series weighted by events rather than by calls. Events that were
  dropped, or were still buffered when the run ended, never appear in it.
- **`read_fetch_recent` / `read_count`** — issued by a separate reader task
  every `--read-interval` seconds while writes are in flight, i.e. they
  measure read latency *under concurrent write load*. Both go through the
  same `aiosqlite` connection as the writer, so they include connection
  serialisation (§4.4). `n` ≈ `duration / read_interval`. With
  `--read-interval 0` both are present with `n = 0` and `null` percentiles.
- **`prune`** — recorded only when retention is enabled
  (`--retention-max-rows > 0`); otherwise `n = 0` and everything is `null`.
  Prune runs *inside* the worker's write path, right after a successful flush
  and at most once per `--prune-interval`, so its cost is added to the
  `end_to_end` of the batch that triggered it.
- **`checkpoint`** — recorded only in `--checkpoint-mode manual` (§5), where
  the benchmark triggers checkpoints itself. `n` is one more than the number
  of threshold-triggered checkpoints, because the run performs a final
  checkpoint after the worker drains. In `auto` mode the field is present with
  `n = 0` and all values `null`: SQLite's internal autocheckpoints cannot be
  timed.

### 2.2 Percentile fields and labels

Each `latency_ms.<series>` object in `summary.json` is a summary of that
series' raw samples (`_summarize_ms` in `bench_persistence.py`):

| Field | Meaning |
|---|---|
| `n` | Number of samples: operations for `flush`/`prune`/`checkpoint`/`enqueue`/reads, events for `end_to_end`. `n = 0` means the series was not exercised and every value is `null` |
| `min` | Fastest single sample; mostly a sanity floor |
| `mean` | Arithmetic mean; less skewed by one outlier than `max` |
| `max` | Slowest single sample; with small `n` this *is* the tail |
| `p50` | Median — the typical case. Compare it with `mean` to spot skew |
| `p95` | 95th percentile — the tail to quote for read latency |
| `p99` | 99th percentile — the tail used for the audit-loss argument. Needs `n` in the hundreds to be meaningful (see `flush` above) |

All values are **milliseconds** and rounded to 3 decimals in `summary.json`.
Percentiles are linear-interpolated over the sorted samples: rank
`r = p/100 × (n − 1)`; the result is `x[⌊r⌋] + (x[⌊r⌋+1] − x[⌊r⌋]) × (r − ⌊r⌋)`.
A single sample returns itself for every percentile, and a percentile can never
exceed `max` — so a `p99 ≈ max` in the table means the series simply has few
samples, not that the distribution is pathological. Latencies are measured with
`time.perf_counter()` on the event loop thread and therefore include asyncio
scheduling delay and, during `--rate 0` runs, contention with the producer task.
They do **not** include filesystem writeback that happens after `COMMIT`
returns; that write cost is reported separately as bytes (§4.1).

The human-readable table (§8) prints `p50`, `p95`, `p99` and `n` per series;
`min`, `mean` and `max` are only in `summary.json`. Any `n/a` in that table
means the value is `null`: the series was not exercised in this run (`n = 0`).

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

### 4.3 Queue depth stays at 0 when the writer keeps up — and that is correct

`queue.max_depth = 0` under the default profile is expected, not a bug.

The worker blocks on `queue.get()` until the flush deadline (`_collect_batch`),
so an item is removed from the queue the moment it is handled. The queue
therefore only holds events that arrive *while the worker is busy* — roughly for
the duration of a flush, a few milliseconds every `flush_interval` seconds.

Measured on the development host at 5 Hz with a 10 s flush, sampling at 1 kHz:

```
queue max      : 1
non-empty      : 13 samples of 2,265 (0.57%)
persisted so far: 0     <- nothing written yet, by design
```

So the queue is essentially never observed non-empty by a sampler, even though
events are in flight: they wait on the flush *timer*, not in the queue. The
flush-interval design is what creates latency; it does not show up as queue
depth.

Queue depth therefore only grows persistently under a real bottleneck. For
contrast, an unpaced producer (`--rate 0`) pins it at capacity:

```
max depth            10,000 / 10,000 (100.00%)
mean / p95 depth     8,333.33 / 10,000.00
non-empty samples    5 / 6
dropped (full/disk)  276,506 / 0
```

Use `queue.nonempty_samples / queue.samples` to distinguish the two cases
without relying on a single sample. Signs of an actual bottleneck:

- `max_depth` climbing toward `queue_capacity` over the run
- `dropped_queue_full` or `failed_batches` becoming non-zero
- `queue.unflushed_on_shutdown > 0`

**Where the batching latency actually shows up** is `latency_ms.end_to_end`
(§4.5), which is p50 ≈ 4 s and p99 ≈ 10 s at this profile. If you want to reason
about "how much data is buffered but not yet durable", that is
`rate × flush_interval` ≈ 50 events from arithmetic, not from `queue_size`.

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

### Running on the target hardware

See [UNO_Q_SETUP.md](UNO_Q_SETUP.md) for the Arduino UNO Q (`rora`): offline
wheelhouse install (PyPI is blocked there), the `PYTHONPATH=src` invocation when
the package is not installed, and measured dev-host-vs-board results.
Storage-level findings for that board (eMMC capabilities, mount options,
`--checkpoint-truncate`) are in
[UNO_Q_STORAGE_REPORT.md](UNO_Q_STORAGE_REPORT.md).

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

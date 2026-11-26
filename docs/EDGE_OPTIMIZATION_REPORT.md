# Edge Optimization Report — Flash-Wear and Deployment Changes

Date: 2026-11-18

This report records the changes made to reduce flash write volume and harden the
backend for a constrained Linux edge device. It deliberately excludes event-driven
architecture work: there is no subscription API, dispatcher, producer-idempotency,
correlation metadata, command acknowledgement, or outbox in this change.

> Branch note: this branch is based on `team/orchestrator` after its `src/`
> refactor. The event pipeline lives under `src/event_pipeline/`; the control
> pipeline and `continuous_pipeline.py` are not modified by this change. The two
> edge guides are self-contained and link to each other.

- [EDGE_DEPLOYMENT.md](EDGE_DEPLOYMENT.md) — operating guide
- [deploy/eaa.service](../deploy/eaa.service) — systemd unit
- [deploy/eaa.env](../deploy/eaa.env) — constrained-edge profile
- `src/event_pipeline/mock_data/config.py` — settings and environment variable parsing

---

## 1. Executive summary

The backend was already **wear-aware**: WAL mode, `synchronous=NORMAL`, batched
inserts, CV frames excluded from SQLite by default, bounded retention, and no
`VACUUM`. It was not **wear-minimized**: at 5 Hz the default 2 s flush produced
~10-row batches, so each row carried a large WAL/checkpoint overhead.

The changes:

1. Add a 10 s/50-row edge profile (and systemd/env packaging) that matches
   `batch_size` to the actual 5 Hz event rate.
2. Set SQLite pragmas that avoid temp-file writes, deleted-content overwrites, and
   page-moving auto-vacuum; cap WAL size; checkpoint/truncate on clean shutdown.
3. Replace the `NOT IN` retention query with an id cutoff.
4. Add a free-space floor and a `dropped_low_disk` counter so a full filesystem
   drops batches instead of retrying into failure.
5. Remove the redundant `(source, event_time)` index in schema v2 and route
   `fetch_recent(source=...)` through the 1:1 `event_type` index.
6. Add IPC path-length validation and `0700`/`0600` permissions for runtime files.

Measured result at the recommended 10 s/50-row profile: **~9.1 KB/row and
~4.0 GB/day at 5 Hz**, versus **~16.6 KB/row and ~7.3 GB/day** at the old effective
10-row batch — roughly a **45% reduction** in logical WAL + checkpoint writes.

---

## 2. Measurement method

Synthetic sensor events were inserted into a temporary WAL database in 20,000-row
runs at batch sizes 10, 50, 100, and 200. Auto-checkpointing was disabled and a
manual checkpoint was performed at ~4 MiB of WAL; the returned WAL and checkpoint
frame counts were multiplied by the 4096-byte page size and summed. This captures
logical WAL writes plus the pages copied back to the main database.

Caveats:

- Numbers are from the development host, not the target device.
- Filesystem page cache, FTL wear leveling, and the card's endurance rating affect
  physical writes; logical bytes are a comparison metric, not a TBW measurement.
- The 20k-row run is a steady-state approximation; real traffic includes occasional
  STT/UI rows.

Rows/day at 5 Hz is 432,000 (86,400 × 5), used for the GB/day column.

### Baseline (before this change, four index B-trees)

| Effective batch | WAL + checkpoint | Bytes/row | At 5 Hz |
|---|---:|---:|---:|
| 10 rows (2 s default) | 339.8 MB | 16.6 KB | ~7.3 GB/day |
| 50 rows (10 s) | 198.1 MB | 9.7 KB | ~4.3 GB/day |
| 100 rows (20 s) | 146.9 MB | 7.2 KB | ~3.2 GB/day |
| 200 rows (40 s) | 101.5 MB | 5.0 KB | ~2.2 GB/day |

### After this change (source index removed)

| Effective batch | WAL + checkpoint | Bytes/row | At 5 Hz |
|---|---:|---:|---:|
| 10 rows (2 s default) | 308.0 MB | 15.0 KB | ~6.7 GB/day |
| 50 rows (10 s) | 186.6 MB | 9.1 KB | ~4.0 GB/day |
| 100 rows (20 s) | 139.7 MB | 6.8 KB | ~3.0 GB/day |
| 200 rows (40 s) | 97.6 MB | 4.8 KB | ~2.1 GB/day |

The source-index removal alone accounts for roughly 9% at batch 10 and 5% at batch
50. The much larger change is the effective batch: the edge profile moves the 5 Hz
case from the 10-row to the 50-row row.

---

## 3. Changes by file

### 3.1 `src/event_pipeline/db_client/database.py`

- **Relational schema, no migrations.** The schema is created idempotently with
  `CREATE … IF NOT EXISTS`, including the `event_types`/`event_sources` lookup
  tables and foreign keys. There is no `SCHEMA_VERSION` or `PRAGMA user_version`
  any more: while the system is in development a schema change means deleting
  the (disposable, gitignored) database file. See
  [RELATIONAL_DB_DESIGN_REVIEW.md](RELATIONAL_DB_DESIGN_REVIEW.md).
- **Wear pragmas.**
  - `PRAGMA temp_store=MEMORY` — prune/query temp work stays in RAM instead of
    writing a temp file.
  - `PRAGMA secure_delete=OFF` — deleted payload bytes are not overwritten.
  - `PRAGMA auto_vacuum=NONE` — no page movement and no implicit rewrite.
  - `PRAGMA journal_size_limit` (default 4 MiB, env-configurable) — caps WAL space
    after checkpoint. This is a space control, not a write-volume reduction.
- **Clean shutdown.** A best-effort `PRAGMA wal_checkpoint(TRUNCATE)` runs before
  `PRAGMA optimize`, so the next start begins with a small WAL.
- **Free-space query.** `Database.free_bytes()` returns the DB volume's free bytes
  (or `None` for `:memory:`/`file:` URIs).
- **Permissions.** The DB parent directory is created with mode `0700`; the SQLite
  file is chmod'd to `0600` after connect (best-effort).
- **Writability check.** Startup fails with the resolved path if the DB directory
  is not writable.

### 3.2 `src/event_pipeline/db_client/repository.py`

- **Retention SQL.** The `max_rows` delete is now an id cutoff:
  `DELETE ... WHERE id <= (SELECT id ... ORDER BY id DESC LIMIT 1 OFFSET ?)`.
  It keeps exactly the newest N rows, avoids the large `NOT IN` set, and deletes
  nothing when the table is at or below the limit.
- **Source filter.** `fetch_recent(source=...)` maps source to its 1:1 event type
  and uses the `(event_type, event_time)` index. A conflicting `event_type` returns
  an empty list. The mapping covers every `EventSource`, including `SYSTEM`, which
  now has its own `system.audit` event type (see
  [RELATIONAL_DB_DESIGN_REVIEW.md](RELATIONAL_DB_DESIGN_REVIEW.md) §2).
- **Free space.** `TelemetryRepository.free_bytes()` delegates to the database.
- **Low-disk guard.** The worker accepts `min_free_bytes`; before a batch, if free
  space is below the floor it drops the batch, increments `dropped_low_disk`, and
  logs at most once per minute. This avoids retry storms against a full filesystem.
- **Stats.** `PersistenceStats.dropped_low_disk` exposes the new counter.

### 3.3 `src/event_pipeline/mock_data/config.py`

- New settings:
  - `db_journal_size_limit_bytes` (`EAA_DB_JOURNAL_LIMIT_BYTES`, default 4 MiB)
  - `min_free_bytes` (`EAA_MIN_FREE_BYTES`, default 0 = disabled)
- Validation: journal limit must be positive; free-space floor cannot be negative.
- Existing defaults (`flush_interval=2.0`, `batch_size=50`) are unchanged, so
  non-edge/dev behavior does not silently change. The edge profile opts into 10 s.

### 3.4 `src/event_pipeline/mock_data/schemas/telemetry.py`

- `PersistenceStats.dropped_low_disk` (default 0) for the low-disk path.

### 3.5 `src/event_pipeline/main_nvhn.py`

- Constructs `Database` with `journal_size_limit_bytes`.
- After repository initialization, fails fast if `min_free_bytes > 0` and the
  volume is already below the floor, with available/required bytes in the error.
- Passes `min_free_bytes` to the persistence worker.
- Extends the grouped stats block with `dropped_low_disk`. The existing uncommitted
  stats-formatting work (grouped `Runtime stats`/`Final stats` output) is preserved.

### 3.6 `src/event_pipeline/event_router/event_router.py`

- Rejects IPC socket paths ≥100 characters before bind, with a message pointing at
  the Linux Unix-socket limit.
- Creates IPC directories with mode `0700`; `fchmod`s the router lock file to
  `0600`.

### 3.7 Tests

- `test_config.py`: defaults, env overrides, validation for the new settings.
- `test_persistence.py`: wear pragmas, lookup seeding, foreign-key/CHECK
  enforcement, generated-column readback, idempotent re-open, source-index
  removal, prune-under-limit no-op, source-filter mapping, audit readback by
  source, the source/event-type bijection guard, the write-cost guard, and the
  low-disk drop.
- `test_router.py`: IPC path-length rejection.
- `test_main.py`: formatted stats include `dropped_low_disk`; startup free-space
  guard raises with a clear error.
- `test_mock_data.py`: `FakeRepository.free_bytes` for the low-disk test.

### 3.8 Deployment and documentation

- `deploy/eaa.service`: non-root `eaa` user, journald output, `Restart=on-failure`,
  `StateDirectory`/`RuntimeDirectory`, `ProtectSystem=strict`, `/var/lib/eaa` and
  `/run/eaa` as the only writable paths, `--stats-interval 60`.
- `deploy/eaa.env`: 10 s flush, 50-row batch, 300 s prune interval, 2 MiB WAL cap,
  64 MiB free-space floor, 250k-row/2-day retention, tmpfs IPC path, no bytecode
  writes.
- `docs/EDGE_DEPLOYMENT.md`: install, storage budget, tuning, mount/logging advice,
  verification.
- README: the top-level README is owned by upstream and is not modified by this
  PR; the edge profile and its trade-offs are documented here and in
  `EDGE_DEPLOYMENT.md`.

---

## 4. Reasoning and trade-offs

### Why batching is the dominant lever

At 5 Hz, a 2 s flush produces ~10 rows. SQLite still has to write WAL frames for
each dirty page and then copy checkpointed pages to the main DB; the per-row cost is
much higher than at 50 rows because the same pages are dirtied repeatedly. Raising
the flush interval to 10 s aligns the batch with `batch_size=50` and roughly halves
the per-row overhead. The cost is an at-most-once audit-loss window of up to 10 s on
a crash. Command execution stays in memory; only the SQLite audit copy lags.

### Why not tune `wal_autocheckpoint` for wear

Changing the checkpoint threshold from 1 MiB to 64 MiB did not reduce total measured
WAL+checkpoint bytes (it stayed ~339 MB per 20k rows in the baseline). It changes
WAL space and recovery time, not the total page-write volume. The profile therefore
keeps the existing 1000-page autocheckpoint and caps WAL space separately.

### Why remove the source index

`source` and `event_type` are 1:1 in this contract (`cv`↔`cv.frame`,
`sensor`↔`sensor.snapshot`, `stt`↔`stt.intent`, `ui`↔`ui.command`,
`system`↔`system.audit`). The old `ix_events_source_time` duplicated the
`(event_type, event_time)` index. Removing it reduces B-tree writes and DB size.
That 1:1 claim used to be false at runtime: state-transition audit rows were
written as `ui.command` with `source=system`, which made them unreachable by
source. They now use the dedicated `system.audit` event type, and a test asserts
the mapping stays a bijection. If future sources share event types, the index
decision must be revisited (a source index costs a measured +14–19 % writes).

### Why no `VACUUM`

`VACUUM` rewrites the entire database, which is a large wear event. The current
design reuses freed pages and leaves the file at its high-water mark. If space must
be reclaimed, rotate/archive the database during maintenance rather than adding a
periodic `VACUUM`.

### Why `temp_store=MEMORY` and `secure_delete=OFF`

The prune/query path can otherwise write temp files, and secure-delete was already
off by default on most builds. Making both explicit avoids surprise writes on a
custom SQLite build and keeps prune work in memory. `auto_vacuum=NONE` is also
explicit to prevent page-moving rewrites.

### What was not changed

- `synchronous=NORMAL` stays; `OFF` removes crash safety for no write-volume win.
- WAL stays; a rollback journal would write more for this batching pattern.
- Page size stays 4096; measured WAL bytes were essentially unchanged at 4K/8K/16K.
- No new runtime dependencies.
- No event-driven API changes.

---

## 5. Verification

### Automated

```text
pytest src/event_pipeline/tests -q        73 passed in ~2.4 s
ruff check src/event_pipeline             All checks passed
ruff format --check src/event_pipeline    All files already formatted
```

### Smoke test

A full `main.run()` with temporary IPC/DB directories and `duration=1` produced:

```text
journal_mode wal
source_index None
rows 0
wal_exists False
```

The clean shutdown removes the WAL; the source index is absent, and the
`event_types`/`event_sources` lookups are seeded from the enums.

### systemd unit

`systemd-analyze verify deploy/eaa.service` parsed the unit on this host; the only
reported error was the expected absence of `/opt/eaa/.venv/bin/eaa-orchestrator`
on the development machine. The unit still needs to be verified on the target after
installation.

### Benchmark

The after table in section 2 is the current code's result. At the recommended
10 s/50-row profile: **~9.1 KB/row, ~4.0 GB/day at 5 Hz**, versus the old
**~16.6 KB/row, ~7.3 GB/day** — about a 45% reduction. The 20 s and 40 s profiles
go to ~6.8 KB/row (~3.0 GB/day) and ~4.8 KB/row (~2.1 GB/day) if a larger audit
window is acceptable.

A one-hour soak at the target rate should still be run on the device; this
development environment only ran the synthetic benchmark and a short smoke test.

---

## 6. Residual risks and deferred work

- **Target-device variance.** Measure physical writes with `/proc/diskstats`,
  `iostat`, or the device's SMART/TBW data. The logical-byte reduction is real, but
  FTL behavior and filesystem caching change absolute numbers.
- **10 s audit-loss window.** If command/audit loss of 10 s is unacceptable, use 5 s
  (≈25 rows) or add a per-event-type flush policy. The latter is deferred because it
  is a policy decision and needs separate tests.
- **`temp_store=MEMORY` RAM use.** Small, but on a 128–256 MB device monitor RSS.
- **Source-filter assumption.** The index removal assumes the current 1:1
  source/event-type contract, which is now enforced by a test. Future sources
  sharing an event type need a new index (+14–19 % writes) or a different query plan.
- **High-water mark.** Without `VACUUM`, the DB file does not shrink after a large
  delete. Plan retention and partition size accordingly, or rotate the DB during
  maintenance.
- **Deferred row slimming.** `topic` and `source` are derivable from `event_type`,
  and `payload_json` repeats `"kind"`. Removing them would reduce row bytes further
  but changes the read path. This is now a DB-file-size question only; it saves no
  write bytes, so it is not worth the churn while the payload contract is moving.
- **No long soak or power-loss test** in this environment. The target acceptance
  should include a one-hour soak, a reboot, and a simulated full disk.

---

## 7. How to reproduce the key numbers on the target

1. Install the edge env profile from `deploy/eaa.env` (10 s/50 rows).
2. Run the mock feed or a calibrated producer at 5 Hz sensor rate.
3. Record `persistence.batches_flushed` and `rows_inserted` from the grouped stats
   log; the ratio should be ≈50 rows/batch.
4. Read the block device's written sectors from `/proc/diskstats` before/after a
   30–60 minute run.
5. Repeat with `EAA_DB_FLUSH_INTERVAL=2` to quantify the local difference; the
   expected direction is a roughly 40% reduction, but use the target's own media
   numbers for the final decision.

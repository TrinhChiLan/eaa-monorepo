# Edge Deployment and Flash-Wear Guide

This guide covers a constrained Linux edge deployment of the EAA backend. It
focuses on flash write volume, storage budgeting, and the systemd/runtime settings
that keep the device healthy. It does not change the event-driven architecture or
the at-most-once persistence contract.

- [EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md) — measurements and change rationale
- `src/event_pipeline/mock_data/config.py` — the settings read by `Settings.from_env()`
- `src/event_pipeline/main_nvhn.py` — runtime wiring and stats output

---

## 1. Target assumptions

- Linux/POSIX (systemd), single device, ARM64 or x86-64.
- Roughly 1–4 GB RAM for the standard profile; the example env in
  [deploy/eaa.env](../deploy/eaa.env) is conservative enough for ~256–512 MB.
- Local eMMC or a high-endurance SD card. Avoid low-end SD cards for a
  continuously writing service.
- CV frames stay in the ring buffer; sensor snapshots, STT intents, and UI
  commands are persisted. At 5 Hz the sensor stream dominates write volume.
- At-most-once persistence is acceptable for telemetry/audit. Safety control stays
  on the MCU/RTOS side.

---

## 2. Why batch size matters more than SQLite mode

SQLite already uses WAL and `synchronous=NORMAL`; the code does not fsync every
transaction and does not run `VACUUM`. The remaining cost is the number of pages
dirtied per transaction. At 5 Hz with the default 2 s flush, the worker collects
only about 10 rows even though `batch_size=50`. A 10-row transaction dirties many
index/table pages for very few rows.

Measured logical WAL + checkpoint bytes for 20k synthetic sensor rows (dev host):

| Effective batch | How it happens | Bytes/row | At 5 Hz (432k rows/day) |
|---|---:|---:|---:|
| 10 rows | 2 s flush at 5 Hz (default) | 16.6 KB | ~7.3 GB/day |
| 50 rows | 10 s flush at 5 Hz | 9.7 KB | ~4.3 GB/day |
| 100 rows | 20 s flush at 5 Hz | 7.2 KB | ~3.2 GB/day |
| 200 rows | 40 s flush at 5 Hz | 5.0 KB | ~2.2 GB/day |

The profile in [deploy/eaa.env](../deploy/eaa.env) uses the 10 s/50-row point: it
cuts logical writes by roughly 41% versus the default and keeps the audit-loss
window to 10 s. If the device has a small/low-endurance card and a 20–40 s audit
window is acceptable, use the larger batch values.

These are logical bytes. Filesystem caching, FTL wear leveling, and the card's
endurance rating determine physical wear; measure the target with
`/proc/diskstats`, `iostat`, or the vendor's SMART/TBW data.

The code also sets:

- `PRAGMA temp_store=MEMORY` — avoids temporary-file writes from prune/query work.
- `PRAGMA secure_delete=OFF` — does not overwrite deleted payloads.
- `PRAGMA auto_vacuum=NONE` — no page movement or `VACUUM` rewrite.
- `PRAGMA journal_size_limit=<bytes>` — bounds WAL space after checkpoint.
- A clean-shutdown `PRAGMA wal_checkpoint(TRUNCATE)`.
- A source/event-type index cleanup: `source` and `event_type` are 1:1 (now
  including `system`↔`system.audit`), so the redundant `(source, event_time)`
  index is gone. `fetch_recent(source=...)` uses the `(event_type, event_time)`
  index.
- `event_types` and `event_sources` lookup tables with foreign keys from
  `telemetry_events`, seeded from the enums on connect; unknown values are
  rejected by SQLite rather than by convention.

There is **no schema-version or migration step**. The schema is created with
`CREATE … IF NOT EXISTS`, so a schema change means deleting the database file
first — acceptable while in development, but plan a migration before any real
device carries data worth keeping.

Do **not** add `VACUUM` as a periodic maintenance step; it rewrites the whole
database. The file size stays at its high-water mark, and freed pages are reused.
If space must be reclaimed, rotate/archive the database during a maintenance
window instead.

---

## 3. Storage budget

At the measured ~426 bytes/row for the DB file (plus WAL/checkpoint traffic):

| Sensor rate | Rows/day | 1 M-row cap lasts | 7 days needs |
|---|---:|---:|---:|
| 1 Hz | 86,400 | ~11.6 days | ~605k rows / ~258 MB |
| 5 Hz | 432,000 | ~2.3 days | ~3.0 M rows / ~1.3 GB |
| 10 Hz | 864,000 | ~1.2 days | ~6.0 M rows / ~2.6 GB |

The default `EAA_RETENTION_MAX_ROWS=1000000` and
`EAA_RETENTION_MAX_AGE_DAYS=7` do not agree at 5 Hz: the row cap is the effective
limit (~2.3 days). Choose one deliberately:

- Keep 7 days: set `EAA_RETENTION_MAX_ROWS=3000000` and provision ~1.3 GB.
- Keep ~2 days: set `EAA_RETENTION_MAX_AGE_DAYS=2` and keep the 1 M cap.
- Constrained device: the example env uses 250k rows / 2 days (~106 MB of rows).

Leave headroom for the WAL (up to the `journal_size_limit`), indexes, logs, and
the OS. A practical budget is 50–60% of the data partition for SQLite.

---

## 4. Install

```bash
sudo useradd --system --home /opt/eaa --shell /usr/sbin/nologin eaa
sudo mkdir -p /opt/eaa /etc/eaa /var/lib/eaa /run/eaa
sudo chown -R eaa:eaa /opt/eaa /var/lib/eaa /run/eaa

sudo -u eaa python3 -m venv /opt/eaa/.venv
sudo -u eaa /opt/eaa/.venv/bin/pip install --upgrade pip
sudo -u eaa /opt/eaa/.venv/bin/pip install /path/to/eaa-monorepo
```

Copy [deploy/eaa.env](../deploy/eaa.env) to `/etc/eaa/eaa.env` and adjust paths.
Copy [deploy/eaa.service](../deploy/eaa.service) to
`/etc/systemd/system/eaa.service`.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now eaa
sudo systemctl status eaa
journalctl -u eaa -f
```

The service uses `StateDirectory=eaa` and `RuntimeDirectory=eaa`; systemd creates
`/var/lib/eaa` and `/run/eaa` with the service user and makes only those paths
writable under `ProtectSystem=strict`.

---

## 5. Runtime and filesystem settings

- **IPC on tmpfs:** `EAA_IPC_DIR=/run/eaa`. Socket create/delete is filesystem
  metadata traffic; keep it off the data partition.
- **DB on eMMC:** if the board has eMMC, put `/var/lib/eaa` there instead of a
  low-end SD card.
- **Mount options:** `noatime` (avoid read atime updates). `discard`/periodic
  `fstrim` may help wear leveling but test the device; do not assume it is free.
- **Swap:** avoid swap on flash. Use `vm.swappiness=0` or zram; set a systemd
  `MemoryMax=` if the device has other services.
- **Logging:** the service writes to journald. Set `Storage=volatile` or a small
  `RuntimeMaxUse=` in `journald.conf`, or rotate logs externally. The unit uses
  `--stats-interval 60` instead of the 10 s default.
- **Python bytecode:** `PYTHONDONTWRITEBYTECODE=1` is in the example env; on a
  read-only rootfs, install with precompiled bytecode or keep this setting.
- **Permissions:** the router rejects IPC paths at ~100 characters. Keep
  `EAA_IPC_DIR` short. The code creates its directories with mode `0700`, the
  router lock with `0600`, and the SQLite file with `0600`.
- **Low disk:** `EAA_MIN_FREE_BYTES` fails startup if the volume is already below
  the floor, and the worker drops batches (counting `dropped_low_disk`) if the
  volume falls below it later. Set ~64 MiB on small devices; leave enough room for
  the OS and logs.

---

## 6. Verification

```bash
# Unit/integration tests and style (run from the repository root)
.venv/bin/pytest src/event_pipeline/tests -q
.venv/bin/ruff check src/event_pipeline
.venv/bin/ruff format --check src/event_pipeline

# Database checks
.venv/bin/python - <<'PY'
import sqlite3
con = sqlite3.connect("/var/lib/eaa/edge_system.db")
print("journal_mode:", con.execute("PRAGMA journal_mode").fetchone()[0])
print("rows:", con.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
print("event_types:", con.execute("SELECT COUNT(*) FROM event_types").fetchone()[0])
print("audit rows:", con.execute(
    "SELECT COUNT(*) FROM telemetry_events WHERE event_type='system.audit'"
).fetchone()[0])
print("pruned/dropped counters are in the stats log")
con.close()
PY

# Physical write trend (per block device)
grep -E 'mmcblk|nvme|sd[a-z]' /proc/diskstats
```

Run `iostat -x 60` or read the device's SMART/TBW data for a longer view. A short
smoke test is not enough; run at least a one-hour soak at the target event rate and
confirm that RSS, DB size, and WAL size are stable.

---

## 7. On-target tuning

| Observed problem | Action |
|---|---|
| Write volume still too high | Increase `EAA_DB_FLUSH_INTERVAL` to 20–40 s and raise `EAA_DB_BATCH_SIZE` to match; accept the larger audit-loss window. |
| Audit loss on commands is unacceptable | Defer the deferred per-event-type flush policy, or reduce the interval to 5 s for all events. |
| DB grows faster than expected | Lower `EAA_RETENTION_MAX_ROWS`, raise the prune frequency, or reduce the sensor persistence rate. |
| Disk warnings / `dropped_low_disk` | Increase `EAA_MIN_FREE_BYTES` headroom, reduce retention, rotate the DB, or move to larger media. |
| Startup fails on free space | Free space, lower `EAA_MIN_FREE_BYTES`, or move the DB volume. |
| IPC bind fails with a long path | Shorten `EAA_IPC_DIR` (Linux Unix-socket path limit). |
| Query by `source` is slower | It now uses the `event_type` index; add a source index only if future sources share event types. |

---

## 8. Trade-offs and non-goals

- The 10 s flush is an at-most-once audit window. Command execution in the router
  and handlers is in-memory; only the SQLite copy can lag.
- `temp_store=MEMORY` uses RAM to avoid temp-file writes; on a very small device,
  keep an eye on RSS.
- No `VACUUM`: good for wear, bad for reclaiming disk after a large delete. Plan
  the high-water mark or rotate the file.
- No broker, subscription API, outbox, or producer-idempotency change here; those
  are event-driven architecture work and are intentionally out of scope.

# Testing and Verification Guide

This guide covers the automated suite, manual end-to-end verification, failure
scenarios, and troubleshooting. For design details see
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Prerequisites

- Linux (ZeroMQ IPC and POSIX file locking are used)
- Python 3.12+
- A writable temporary directory for IPC sockets
- No external services required

Install from the monorepo root:

```bash
cd eaa-monorepo
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

or:

```bash
make install
```

Verify:

```bash
python -V
pytest --version
ruff --version
```

---

## 2. Automated tests

### 2.1 Run everything

```bash
cd eaa-monorepo
source .venv/bin/activate
pytest -q
ruff check .
ruff format --check .
```

Expected:

```text
56 passed
All checks passed!
```

The suite is intentionally fast (about 1–2 seconds) and uses temporary SQLite
files and per-test IPC directories, so tests do not collide with a running
development instance.

### 2.2 What each test file verifies

#### [tests/test_events.py](../tests/test_events.py)

- Canonical envelope JSON round-trip.
- `event_type` must match `payload.kind`.
- Confidence bounds.
- Frozen-model mutation rejection.
- Canonical models reject unknown fields; wire models ignore them.
- `NaN`/negative timestamps are rejected.
- Unknown payload discriminators are rejected.

#### [tests/test_router.py](../tests/test_router.py)

- Direct `handle_event` normalization for all four event types.
- Timestamp fallback to the router clock.
- Sequence counters per source.
- Invalid JSON, unknown topics, out-of-range confidence, oversized payloads, and
  non-object JSON are counted and do not raise.
- Handler fan-out and handler-exception isolation.
- Sink-exception isolation.
- Every ZeroMQ protocol: CV/sensor PUB/SUB and STT/UI PUSH/PULL.
- Start/stop idempotency and restart after stop.
- Exclusive single-instance lock.

#### [tests/test_ring_buffer.py](../tests/test_ring_buffer.py)

- Capacity eviction and `evicted` count.
- `get_latest`/`snapshot` ordering and edge counts.
- Concurrent pushes.
- Persistence outcome counters (`QUEUED`, `FILTERED`, `DROPPED`, `STOPPED`).
- Persistence exception isolation.
- Clear semantics.
- Bad capacity and non-envelope rejection.

#### [tests/test_persistence.py](../tests/test_persistence.py)

- WAL mode, `user_version`, health check, idempotent close.
- Batch insert and envelope round-trip.
- Duplicate `event_id` ignored.
- Query filters and chronological ordering.
- Prune by max rows and by max age.
- Refusal to open a newer schema.
- Flush by batch size.
- Flush of a partial batch on interval while the queue is non-empty.
- Retry then success.
- Retry exhaustion drops the batch and keeps the worker alive.
- Drop-oldest queue pressure behavior.
- Event-type allowlist filtering.
- Shutdown drain.
- Submit after stop.
- Retention pruning invocation.
- Argument validation.
- Real repository + worker round-trip.

#### [tests/test_integration.py](../tests/test_integration.py)

- Router → ring buffer → worker → SQLite with the default allowlist.
- CV frames remain in the ring buffer but not in SQLite.
- CV frames persist when explicitly enabled.
- Graceful shutdown flushes all queued events.

#### [tests/test_config.py](../tests/test_config.py)

- Defaults and environment overrides.
- Disabled retention.
- CV persistence toggle.
- Settings validation.

#### [tests/test_mocks.py](../tests/test_mocks.py)

- Mock JSON generators validate against the wire models.
- Feed configuration validation.
- Mock feed runs and stops cleanly.

### 2.3 Run a subset

```bash
pytest tests/test_router.py -q
pytest tests/test_persistence.py -q
pytest -q -k "drop or retry or shutdown"
pytest -q -k "zmq or protocol"
```

### 2.4 Verbose logs during tests

```bash
pytest -q -o log_cli=true --log-cli-level=INFO
```

Some expected warnings appear when tests deliberately inject invalid data; for
example `Rejected ui.command event` or `Dropping batch of 1 events`. They are part
of the failure-path tests.

---

## 3. Manual end-to-end test

### 3.1 Clean state

```bash
cd eaa-monorepo
make clean-db
make clean-ipc
```

`make clean-db` removes the default `data/edge_system.db*`.
`make clean-ipc` removes `/tmp/eaa` socket files and the router lock.

### 3.2 Terminal A — orchestrator

```bash
source .venv/bin/activate
eaa-orchestrator --stats-interval 3 --log-level INFO
```

Expected startup logs:

```text
Migrated SQLite schema from version 0 to 1
EventRouter started: cv=ipc:///tmp/eaa/cv.pub ...
EAA backend running: db=data/edge_system.db ipc=/tmp/eaa ring=1000 persist=['sensor.snapshot', 'stt.intent', 'ui.command']
```

### 3.3 Terminal B — mock feed

```bash
source .venv/bin/activate
eaa-mock-feed --duration 10 --cv-rate 5 --sensor-rate 3 --stt-interval 1 --ui-interval 2 --invalid-every 20
```

Expected mock log:

```text
Mock feed started: cv=ipc:///tmp/eaa/cv.pub ...
Mock feed stopped after N messages
```

Expected orchestrator stats during/after the run:

- `router.received` and `router.emitted` increase.
- `router.invalid` increases when malformed messages are injected.
- `ring.size` increases.
- `ring.persistence_filtered` increases for CV frames.
- `ring.persistence_queued` increases for sensor/STT/UI.
- `persistence.rows_inserted` tracks actual inserts.
- `persistence.failed_batches=0` and `dropped_queue_full=0` under normal load.
- On Ctrl-C, `persistence.unflushed_on_shutdown=0`.

A representative run from this repository (5 CV/s, 3 sensor/s, 1 STT/s, 2 UI/s,
`--invalid-every 20`, 6 seconds) produced:

```text
router.received=57  emitted=55  invalid=2
ring.size=55        persistence_queued=26  persistence_filtered=29
persistence.rows_inserted=26  failed_batches=0
```

The exact numbers vary with timing; the relationships should hold.

### 3.4 Verify SQLite

No `sqlite3` CLI is required:

```bash
.venv/bin/python - <<'PY'
import sqlite3

con = sqlite3.connect("data/edge_system.db")
print("journal_mode:", con.execute("PRAGMA journal_mode").fetchone()[0])
print("schema_version:", con.execute("PRAGMA user_version").fetchone()[0])
print("total:", con.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])

for row in con.execute(
    "SELECT event_type, COUNT(*) FROM telemetry_events GROUP BY event_type ORDER BY event_type"
):
    print(row)

print("latest:")
for row in con.execute(
    "SELECT event_type, source, sequence, event_time, payload_json "
    "FROM telemetry_events ORDER BY id DESC LIMIT 3"
):
    print(row)
con.close()
PY
```

Expected:

- `journal_mode: wal`
- `schema_version: 1`
- `sensor.snapshot`, `stt.intent`, and `ui.command` rows
- no `cv.frame` rows by default
- `payload_json` contains the typed payload object

### 3.5 Test CV persistence opt-in

Stop the orchestrator (Ctrl-C), then:

```bash
make clean-db
eaa-orchestrator --stats-interval 3 --persist-cv
```

In terminal B:

```bash
eaa-mock-feed --duration 5 --cv-rate 5 --sensor-rate 3 --stt-interval 1 --ui-interval 2
```

Verify:

```bash
.venv/bin/python - <<'PY'
import sqlite3

con = sqlite3.connect("data/edge_system.db")
print(con.execute(
    "SELECT event_type, COUNT(*) FROM telemetry_events GROUP BY event_type"
).fetchall())
con.close()
PY
```

You should now see `cv.frame` rows. The ring buffer contains CV frames in both
cases; the difference is only persistence policy.

### 3.6 Test invalid input handling

Run the mock with aggressive invalid injection:

```bash
eaa-mock-feed --duration 10 --invalid-every 3
```

Expected:

- The orchestrator continues running.
- `router.invalid` increases.
- Logs show rate-limited `Rejected ... event` warnings.
- No traceback and no listener exit.
- Valid events keep appearing in SQLite.

### 3.7 Test graceful shutdown / drain

1. Start the orchestrator and mock feed.
2. Let it run for a few seconds.
3. Press Ctrl-C in the orchestrator terminal.

Expected final log:

```text
Final stats: router=RouterStats(...) ring=RingBufferStats(...) persistence=PersistenceStats(... unflushed_on_shutdown=0 ... running=False)
```

Then verify the DB is readable and has the expected rows. If
`unflushed_on_shutdown` is greater than zero, the worker hit the drain timeout;
see the troubleshooting table.

### 3.8 Test retention

Stop the orchestrator, then run with a small row cap and a short prune interval:

```bash
make clean-db
EAA_RETENTION_MAX_ROWS=10 \
EAA_PRUNE_INTERVAL_SECONDS=0.1 \
eaa-orchestrator --stats-interval 2
```

Run the mock for 10 seconds:

```bash
eaa-mock-feed --duration 10
```

After shutdown:

```bash
.venv/bin/python - <<'PY'
import sqlite3

con = sqlite3.connect("data/edge_system.db")
print("rows:", con.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
print("max id:", con.execute("SELECT MAX(id) FROM telemetry_events").fetchone()[0])
con.close()
PY
```

`rows` should be bounded near the configured maximum (10), and the newest rows
should be retained. Pruning is periodic, so the count can briefly exceed the cap
between prune runs.

To test age-based retention instead:

```bash
EAA_RETENTION_MAX_ROWS=0 \
EAA_RETENTION_MAX_AGE_DAYS=0.0001 \
EAA_PRUNE_INTERVAL_SECONDS=0.1 \
eaa-orchestrator --stats-interval 2
```

The age cutoff is about 8.6 seconds, so older rows should be removed. Age is based
on `event_time`.

### 3.9 Test single-instance protection

Start one orchestrator, then in a second terminal run another with the same IPC
directory:

```bash
eaa-orchestrator --ipc-dir /tmp/eaa
```

Expected:

```text
Another EAA router is already running for /tmp/eaa/.router.lock.
```

The second process should exit non-zero rather than steal the IPC sockets. This
lock is necessary because libzmq silently unlinks duplicate IPC binds.

### 3.10 Test a custom database and IPC directory

```bash
eaa-orchestrator --db-path data/custom.db --ipc-dir /tmp/eaa-custom
eaa-mock-feed --ipc-dir /tmp/eaa-custom --duration 5
```

Verify `data/custom.db` using the Python snippet with the custom path. This is the
recommended way to run multiple manual scenarios without touching the default DB.

### 3.11 Queue-pressure observation

Deterministic queue-drop behavior is covered by
`tests/test_persistence.py::test_worker_queue_drops_oldest_under_pressure`. A manual
stress version is timing-dependent: run at high rates with a small queue and short
flush interval, for example:

```bash
EAA_DB_QUEUE_CAPACITY=2 \
EAA_DB_BATCH_SIZE=1000 \
EAA_DB_FLUSH_INTERVAL=10 \
eaa-orchestrator --stats-interval 1
```

Then run:

```bash
eaa-mock-feed --cv-rate 50 --sensor-rate 50 --stt-interval 0.01 --ui-interval 0.01 --duration 10
```

If SQLite cannot keep up, `persistence.dropped_queue_full` becomes non-zero. The
router and ring buffer keep running; CV frames still count as `filtered`, not
`dropped`. For a guaranteed assertion, rely on the automated test rather than the
manual stress run.

### 3.12 Debug logging

```bash
eaa-orchestrator --log-level DEBUG --stats-interval 2
eaa-mock-feed --log-level DEBUG --duration 5
```

Useful for inspecting rejected messages, socket errors, and per-batch flush logs.

---

## 4. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No SQLite rows | Mock feed ran before router, or no overlap | Start orchestrator first; allow at least 1 s of overlap |
| `Another EAA router is already running` | A previous router still holds `.router.lock` | Stop the other process; check `ps`; or remove `/tmp/eaa/.router.lock` after confirming no router is running |
| `Unable to bind router endpoint ...` | TCP endpoint conflict, or an IPC path problem | Use a different `--ipc-dir`; remove stale socket files |
| Mock feed fails to bind `cv.pub` | Another mock feed is already running | Stop the other feed or use another `--ipc-dir` |
| `No such file or directory for ipc path` | Missing IPC directory | The router creates it; for custom scripts, create the parent directory or use `MockFeed.run()`, which creates it |
| IPC path too long | Linux UNIX socket path limit (~107 bytes) | Use a shorter `EAA_IPC_DIR`, e.g. `/tmp/eaa` |
| `database is locked` | Another process holds a long write, or the DB is on a network FS | Use one writer; keep SQLite on a local disk; `busy_timeout` is 5 s |
| CV rows missing | Default allowlist excludes CV frames | Use `--persist-cv` or `EAA_PERSIST_CV_FRAMES=1` |
| `unflushed_on_shutdown > 0` | Worker hit the drain timeout | Check SQLite health/latency; increase `batch_size`/drain timeout or reduce load |
| `failed_batches > 0` | SQLite write failures after retries | Check disk space, permissions, locking, and DB file integrity |
| `invalid` grows quickly | Producer is sending malformed JSON or out-of-range values | Inspect rate-limited warnings; validate the producer payload |
| `unknown_topic` grows | Producer topic does not match `cv.frame`, `sensor.snapshot`, `stt.intent`, or `ui.command` | Fix the topic frame |
| Tests cannot import `shared`/`eaa_orchestrator` | Package not installed editable | `pip install -e ".[dev]"` |
| `pytest` reports no tests | Wrong working directory | Run from `eaa-monorepo` |
| `ADDRINUSE`/stale IPC after a crash | Socket file remains | `make clean-ipc` after stopping all routers |

---

## 5. Acceptance matrix

| Requirement | Automated coverage | Manual check |
|---|---|---|
| Canonical event format across modules | `test_events.py`, `test_integration.py` | Inspect `payload_json` and logs |
| CV, sensor, STT, UI ingress | `test_router.py` | Run mock feed |
| Invalid input does not crash | `test_router.py`, `test_mocks.py` | `--invalid-every 3` |
| Ring buffer capacity/eviction | `test_ring_buffer.py` | Small `EAA_RING_CAPACITY` |
| Persistence batching | `test_persistence.py` | Stats `batches_flushed` |
| Queue overflow behavior | `test_persistence.py` | Section 3.11 |
| SQLite WAL + schema | `test_persistence.py` | `PRAGMA journal_mode` |
| Idempotent event IDs | `test_persistence.py` | Duplicate event insertion |
| CV excluded by default | `test_integration.py` | Section 3.4 |
| CV persistence opt-in | `test_integration.py` | Section 3.5 |
| Retention | `test_persistence.py` | Section 3.8 |
| Graceful shutdown flush | `test_persistence.py`, `test_integration.py` | Section 3.7 |
| Single-instance protection | `test_router.py` | Section 3.9 |
| Config overrides | `test_config.py` | Custom DB/IPC run |

---

## 6. Cleanup

```bash
make clean-db
make clean-ipc
```

`make clean-db` removes only the default database files. Custom `--db-path` files
must be removed separately. `make clean-ipc` removes the default `/tmp/eaa`
sockets and lock; custom IPC directories must be removed separately.

---

## 7. Adding tests

Guidelines:

- Use `tests/support.py` factories (`make_sensor_event`, `make_cv_event`, etc.)
  instead of hand-building envelopes.
- Use `FakeSink`, `FakePersistence`, and `FakeRepository` for module-level tests.
- Use `Database(tmp_path / "test.db")` for real SQLite tests; do not use
  `:memory:` if you are asserting WAL behavior.
- Use unique `tmp_path` IPC directories; never reuse `/tmp/eaa` in tests.
- Use `wait_until` instead of fixed sleeps for asynchronous conditions.
- Keep tests deterministic: avoid asserting exact counts for PUB/SUB tests where
  slow-joiner retries can create duplicates.
- Add a regression test for every fixed pseudocode bug.

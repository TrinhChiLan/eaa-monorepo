# Architecture and Behavior

This document explains how the implemented EAA backend slice works, why each
decision was made, and how it maps back to the low-level design and pseudocode.

- [README](../README.md) — quick start and configuration
- [TESTING.md](TESTING.md) — automated and manual verification

---

## 1. Scope

The slice contains exactly three production modules plus shared contracts:

1. `EventRouter`
2. `TelemetryRingBuffer`
3. `SQLite` database/repository and the `HalfSyncHalfAsyncPersistenceWorker`

The FSM, safety arbiter, multimodal context builder, SLM agent, CV/STT/TTS model
engines, OpenAMP/RPMsg bridge, FastAPI gateway, and cloud egress are out of scope.
The router exposes handler hooks so those modules can be attached later without
changing the event contract.

---

## 2. Component map

```text
eaa-monorepo/
├── container-orchestrator/src/eaa_orchestrator/
│   ├── main.py                    startup, signals, stats, shutdown
│   ├── router/event_router.py     external protocols -> EventEnvelope
│   ├── telemetry/ring_buffer.py   bounded recent-event store
│   └── persistence/
│       ├── database.py            connection, WAL, schema version
│       └── repository.py          SQL, batch worker, retention
│
└── nvhn/
    ├── shared/
    │   ├── schemas/events.py      EventEnvelope + wire models
    │   ├── schemas/telemetry.py   stats and retention dataclasses
    │   ├── contracts.py           EventSink / PersistenceSink protocols
    │   ├── constants/zmq_topics.py topics and endpoints
    │   └── config.py              environment-backed Settings
    ├── mocks/feed.py              mock external producers
    ├── tests/                     unit and integration tests
    ├── docs/                      architecture and testing guides
    ├── reference/                 PDF, architecture XML, pseudocode
    ├── pyproject.toml
    ├── requirements*.txt
    └── Makefile
```

At runtime the object graph is:

```text
Database ◀── TelemetryRepository ◀── HalfSyncHalfAsyncPersistenceWorker
                                              ▲
                                              │ PersistenceSink.submit()
EventRouter ──EventSink.push()──▶ TelemetryRingBuffer
```

`main.py` constructs the graph in that order, starts the worker before the router,
and shuts down in reverse order.

---

## 3. Runtime lifecycle

`main.run()` performs:

1. Load `Settings` from environment/CLI.
2. Open SQLite, apply pragmas, migrate the schema.
3. Start the persistence worker task.
4. Create the ring buffer with the worker as its persistence sink.
5. Create and start the router.
6. Install `SIGINT`/`SIGTERM` handlers and wait for stop or `--duration`.
7. Shutdown:
   - stop the router (cancel listeners, close sockets, release the instance lock),
   - stop the worker (sentinel, drain, final flush),
   - close the repository/database,
   - log final stats.

If database initialization fails, the process logs the error and exits non-zero
without starting ingestion.

---

## 4. Canonical event contract

Defined in [events.py](../shared/schemas/events.py). The canonical envelope is a
frozen Pydantic v2 model with `extra="forbid"`.

### 4.1 Envelope fields

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | `int >= 1` | Contract version; currently `1` |
| `event_id` | `str` | UUID4 hex; DB uniqueness/idempotency key |
| `event_type` | enum | `cv.frame`, `stt.intent`, `sensor.snapshot`, `ui.command` |
| `source` | enum | `cv`, `stt`, `sensor`, `ui`, `system` |
| `topic` | `str` | Routing topic |
| `sequence` | `int >= 0` | Per-source, per-process counter |
| `event_time` | `float > 0` | Source event time, epoch seconds UTC |
| `ingested_at` | `float > 0` | Router receive time, epoch seconds UTC |
| `payload` | discriminated union | Typed payload; `payload.kind == event_type` |

### 4.2 Payloads

**`cv.frame`**

```python
CVFramePayload(
    kind="cv.frame",
    camera_id="cam0",
    detected_objects=["carton", "jam"],
    confidence=0.93,  # 0..1
    jam_suspected=True,
)
```

**`stt.intent`**

```python
IntentPayload(
    kind="stt.intent",
    transcript="clear the jam and resume",
    intent="clear_jam",
    confidence=0.88,  # 0..1
)
```

**`sensor.snapshot`**

```python
SensorSnapshotPayload(
    kind="sensor.snapshot",
    motor_current_amp=1.84,
    infrared_temp_celsius=41.2,
    conveyor_speed_mms=320.0,
    optical_curtain_clear=True,
    estop_circuit_closed=True,
    sensor_timestamp_ms=1762771829123,
)
```

**`ui.command`**

```python
UICommandPayload(
    kind="ui.command",
    action="pause",
    parameters={"source": "mock-ui"},  # flat JSON scalars
)
```

### 4.3 Construction and validation

`EventEnvelope.create(...)` centralizes ID and timestamp handling:

- `event_id` defaults to `uuid.uuid4().hex`;
- `event_time` defaults to the router clock;
- `ingested_at` defaults to the router clock;
- `event_type` is derived from `payload.kind`;
- a model validator rejects `event_type`/`payload.kind` mismatches.

Wire models (`CVFrameWire`, `IntentWire`, `SensorSnapshotWire`,
`UICommandWire`) use `extra="ignore"` so a future producer field does not break an
older edge device. The canonical envelope uses `extra="forbid"`.

### 4.4 Time and ordering

- Event time is epoch seconds as a float, not an ISO string. This matches the
  pseudocode and SQLite `REAL` storage.
- If the external message has no timestamp, the router uses the current time.
- Sensor messages use `timestamp_ms / 1000` when `timestamp_ms` is present.
- `sequence` starts at 0 per source and resets on router restart.
- `event_id` and `ingested_at` are the durable ordering keys. Out-of-order source
  timestamps are allowed.

---

## 5. External protocols

### 5.1 Endpoint topology

| Protocol | Router socket | Producer socket | Endpoint |
|---|---|---|---|
| CV | `SUB` connects | `PUB` binds | `ipc://<ipc_dir>/cv.pub` |
| Sensor | `SUB` connects | `PUB` binds | `ipc://<ipc_dir>/sensor.pub` |
| STT | `PULL` binds | `PUSH` connects | `ipc://<ipc_dir>/stt.pull` |
| UI | `PULL` binds | `PUSH` connects | `ipc://<ipc_dir>/ui.pull` |
| REST/WS future | direct method | in-process caller | n/a |

The router subscribes only to `cv.` and `sensor.` prefixes; unknown topics are
rejected in code. PUB/SUB may drop messages when no subscriber is connected; this is
normal for perception streams. PUSH/PULL queues commands until the router is
available.

### 5.2 Multipart format

```text
[topic_frame, json_payload]
```

- PUB/SUB requires the topic frame.
- PULL sockets accept one JSON frame and use the socket's default topic
  (`stt.intent` or `ui.command`).
- Multiple payload frames after the topic are concatenated before JSON parsing.
- Non-UTF-8 topics, empty topics, oversized messages, and non-object JSON are
  rejected.

### 5.3 Wire payloads

```json
{"timestamp": 1762771829.1, "camera_id": "cam0", "detected_objects": ["carton"], "confidence": 0.93, "jam_suspected": false}
{"timestamp_ms": 1762771829123, "motor_current_amp": 1.84, "infrared_temp_celsius": 41.2, "conveyor_speed_mms": 320.0, "optical_curtain_clear": true, "estop_circuit_closed": true}
{"timestamp": 1762771829.1, "transcript": "clear the jam and resume", "intent": "clear_jam", "confidence": 0.88}
{"action": "pause", "parameters": {"source": "mock-ui"}}
```

`NaN`/`Infinity` JSON constants are rejected before Pydantic validation.

---

## 6. Router internals

File: [event_router.py](../../container-orchestrator/src/eaa_orchestrator/router/event_router.py)

### 6.1 Start sequence

1. Resolve `RouterEndpoints` (defaults or explicit).
2. Create IPC parent directories.
3. Acquire the single-instance POSIX lock (`.router.lock` in the IPC directory).
4. Create a `zmq.asyncio.Context`.
5. Create CV/sensor `SUB` sockets and connect; set `SUBSCRIBE` prefixes.
6. Create STT/UI `PULL` sockets and bind.
7. Set `LINGER=0`, `RCVHWM=1000`, `MAXMSGSIZE=max_message_bytes`.
8. Start one listener task per socket.

If any step fails, sockets are closed, the context is destroyed, and the lock is
released. A second router on the same endpoints fails with a clear "already running"
error. libzmq silently unlinks duplicate IPC binds, so the advisory lock is the
actual single-instance guarantee.

### 6.2 Listener supervision

Each listener loops:

```text
recv_multipart()
  -> size check
  -> topic extraction
  -> handle_event(topic, payload)
```

- A per-message exception is counted as invalid and logged; the listener continues.
- A `ZMQError` increments `socket_errors`, backs off exponentially (100 ms → 5 s),
  and retries until shutdown.
- `CancelledError` and `ContextTerminated` end the listener.

### 6.3 Normalization pipeline

`handle_event(topic, raw)` is the single ingress path for sockets and the future
REST/WS adapter:

1. Count `received`.
2. Enforce the message size limit for bytes/str.
3. Select the wire model by topic; unknown topics increment `unknown_topic`.
4. Decode JSON (strict UTF-8, reject `NaN`/`Infinity`, require an object).
5. Validate the wire model.
6. Determine `event_time` from the wire timestamp or the router clock.
7. Build the canonical payload.
8. Assign the next per-source `sequence`.
9. `EventEnvelope.create(...)`.
10. `await sink.push(envelope)`.
11. Increment `emitted`, update `last_event_at`.
12. Dispatch registered handlers.

If the sink fails, the router increments `sink_errors`, logs, and does not count the
event as emitted. Handlers are not called for an event the sink rejected.

### 6.4 Handler registry

```python
router.register_handler("sensor.", handler)
```

- Matching is by topic prefix; `""` matches everything.
- Handlers run sequentially after the ring buffer receives the event.
- A handler exception increments `handler_errors`, logs, and does not affect other
  handlers or the sink.
- Handlers must be fast or enqueue internally; the router does not create unbounded
  background tasks.

This is the extension point for the FSM, context builder, safety arbiter, and future
WebSocket fan-out.

### 6.5 Statistics

`RouterStats`:

| Field | Meaning |
|---|---|
| `received` | Socket messages + direct calls observed |
| `emitted` | Canonical events accepted by the sink |
| `invalid` | Decode/validation failures and unexpected per-message errors |
| `oversized` | Messages over the configured byte limit |
| `unknown_topic` | Topic not present in the wire-spec table |
| `handler_errors` | Downstream handler exceptions |
| `socket_errors` | ZeroMQ receive errors |
| `sink_errors` | Ring buffer/sink push failures |
| `last_event_at` | `ingested_at` of the last emitted event |

Invalid-message logs are rate-limited to 10 per 60 seconds per router.

---

## 7. Ring buffer internals

File: [ring_buffer.py](../../container-orchestrator/src/eaa_orchestrator/telemetry/ring_buffer.py)

### 7.1 Storage

- `collections.deque(maxlen=capacity)`.
- `capacity > 0` is required.
- An `asyncio.Lock` protects the critical section.
- The buffer is event-loop safe, not thread-safe.

### 7.2 Push path

```text
lock:
  if size == capacity: evicted += 1
  append(event)
  total_pushed += 1
unlock:
  if persistence exists:
      outcome = await persistence.submit(event)
      update queued/filtered/dropped counters
```

`submit()` is non-blocking, so the ring buffer never waits for SQLite. If the sink
raises, the buffer increments `persistence_errors` and keeps the event in memory.

### 7.3 Read ordering

- `get_latest(n)` returns up to `n` newest events in **chronological ascending**
  order (oldest of the returned window first).
- `snapshot(limit)` returns the buffer, or its newest `limit` entries, in the same
  ordering.
- `count <= 0` returns `[]`.
- `clear()` empties the buffer but keeps cumulative counters.

### 7.4 Eviction and TTL

- Capacity is the only eviction rule. `evicted` counts overwritten entries.
- There is no TTL in this slice. TTL belongs to the future context builder, which
  will consume the ring buffer through `get_latest`/`snapshot`.
- The buffer is a live/recent-data store, not a durable history store.

### 7.5 Statistics

`RingBufferStats` includes `capacity`, `size`, `total_pushed`, `evicted`,
`persistence_queued`, `persistence_filtered`, `persistence_dropped`,
`persistence_errors`, `oldest_event_time`, and `newest_event_time`.

---

## 8. Persistence worker internals

File: [repository.py](../../container-orchestrator/src/eaa_orchestrator/persistence/repository.py)

### 8.1 Queue policy

`asyncio.Queue(maxsize=queue_capacity)`, `SubmitOutcome` values:

| Outcome | Meaning |
|---|---|
| `QUEUED` | Accepted into the queue |
| `FILTERED` | Deliberately not persisted by the event-type allowlist |
| `DROPPED` | Could not enqueue after the drop-oldest attempt |
| `STOPPED` | Worker is not accepting new events |

When the queue is full:

1. Pop the oldest item.
2. Call `task_done()` for it.
3. Increment `dropped_queue_full`.
4. Enqueue the new event.

This favors recent telemetry and keeps ingestion non-blocking. The default
allowlist excludes CV frames.

### 8.2 Batching algorithm

The worker waits for the first item of a batch. The flush deadline starts at that
moment. It then collects until:

- `batch_size` is reached, or
- `flush_interval` elapses since the first item, or
- the stop sentinel arrives.

A partial batch is flushed when the interval elapses even if the queue is still
non-empty. This fixes the pseudocode bug where the timeout path only ran when the
queue was empty.

### 8.3 Persistence and retry

```text
for attempt in 0..max_retries:
    try repository.bulk_insert(batch)
    on success: batches_flushed++, rows_inserted += n, last_flush_at = now
    on failure: sleep min(retry_backoff * 2**attempt, 5s) and retry
after retries: failed_batches++, log, drop the batch
```

`task_done()` runs after the persistence attempt, not before.

### 8.4 Retention pruning

After a successful flush, if a `RetentionPolicy` is configured and the prune
interval has elapsed:

- delete rows with `event_time < now - max_age_seconds`;
- delete rows outside the newest `max_rows` by insertion id;
- add the deleted count to `pruned_rows`.

Prune failures are logged and do not fail the batch.

### 8.5 Shutdown drain

`stop(drain_timeout)`:

1. Set `accepting = False`.
2. Insert a sentinel, waiting up to `drain_timeout` for room.
3. If still full, drop oldest until the sentinel fits.
4. Wait up to `drain_timeout` for the worker to flush the current batch and drain
   the queue.
5. On timeout, cancel the worker and record `unflushed_on_shutdown = queue.qsize()`.

The worker does not close the database; `main` owns repository/database shutdown.

### 8.6 Statistics

`PersistenceStats`: `queue_capacity`, `queue_size`, `submitted`, `queued`,
`filtered`, `dropped_queue_full`, `stopped_rejected`, `batches_flushed`,
`rows_inserted`, `failed_batches`, `pruned_rows`, `unflushed_on_shutdown`,
`last_flush_at`, `running`.

---

## 9. Database and repository internals

File: [database.py](../../container-orchestrator/src/eaa_orchestrator/persistence/database.py)

### 9.1 Connection and pragmas

- `aiosqlite.connect(path, isolation_level=None)` for explicit transactions.
- Parent directories are created at connect time.
- `journal_mode=WAL`
- `synchronous=NORMAL`
- `busy_timeout=5000`
- `foreign_keys=ON`
- `wal_autocheckpoint=1000`

`:memory:` databases cannot use WAL; the code logs a warning and continues. Tests
use temporary files so WAL behavior is exercised.

### 9.2 Schema versioning

`SCHEMA_VERSION = 1`, tracked with `PRAGMA user_version`.

- `0` → create table/indexes, set `1`.
- `1` → re-run idempotent `CREATE IF NOT EXISTS` statements.
- `> 1` → raise `RuntimeError`; an older binary must not open a newer database.

### 9.3 Schema

```sql
CREATE TABLE IF NOT EXISTS telemetry_events (
    id              INTEGER PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    schema_version  INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    source          TEXT NOT NULL,
    topic           TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    event_time      REAL NOT NULL,
    ingested_at     REAL NOT NULL,
    payload_json    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_events_event_time
    ON telemetry_events(event_time);
CREATE INDEX IF NOT EXISTS ix_events_type_time
    ON telemetry_events(event_type, event_time);
CREATE INDEX IF NOT EXISTS ix_events_source_time
    ON telemetry_events(source, event_time);
```

`payload_json` stores only the payload; the repository reconstructs the envelope
from the promoted columns. `event_id UNIQUE` plus `INSERT OR IGNORE` makes retries
idempotent.

### 9.4 Insert path

`TelemetryRepository.bulk_insert(events)`:

1. Map envelopes to row tuples.
2. Take the repository write lock.
3. `BEGIN IMMEDIATE`.
4. `executemany(INSERT OR IGNORE ...)`.
5. Commit, or roll back on any exception.
6. Return the number of rows actually inserted (`total_changes` delta).

### 9.5 Query path

`fetch_recent(limit, event_type, source, since, until)`:

- parameterized `WHERE` clauses;
- newest-first query for an efficient `LIMIT`;
- reversed before returning, so the public order is chronological ascending.

`count(event_type)` returns row counts. `health_check()` runs `SELECT 1`.
`close()` is idempotent and runs `PRAGMA optimize` before closing.

### 9.6 Retention SQL

- Age: `DELETE FROM telemetry_events WHERE event_time < cutoff`.
- Rows: delete ids not in `SELECT id ... ORDER BY id DESC LIMIT max_rows`.

Both may be combined. Pruning runs in a write transaction and returns the deleted
count.

### 9.7 Concurrency

One connection. Writes are serialized by an `asyncio.Lock`; reads are autocommit
queries. WAL allows external readers while the writer is active. SQLite is assumed
to be on a local disk, not a network filesystem.

---

## 10. Failure modes and observability

| Situation | Behavior | Counter/log |
|---|---|---|
| Bad JSON/UTF-8 | Drop, continue | `invalid` |
| Unknown topic | Drop, continue | `unknown_topic` |
| Oversized message | Drop, continue | `oversized` |
| Handler exception | Log, continue | `handler_errors` |
| Sink exception | Log, do not emit | `sink_errors` |
| Ring full | Evict oldest | `evicted` |
| Persistence queue full | Drop oldest queued event | `dropped_queue_full` |
| SQLite transient failure | Retry with backoff | `failed_batches` after exhaustion |
| SQLite permanent failure | Drop batch, worker continues | `failed_batches` |
| Shutdown timeout | Cancel worker | `unflushed_on_shutdown` |
| Duplicate event ID | Ignored | `rows_inserted` reflects actual inserts |
| Newer schema | Refuse to open | runtime error / non-zero exit |
| Second router | Refuse to start | "already running" error |
| Stale IPC path | libzmq may unlink; lock still prevents a second router | README cleanup |

The system exposes in-process stats through the three `stats()` methods and periodic
INFO logs. There is no Prometheus endpoint in this slice.

---

## 11. Extension points

### 11.1 FSM / context builder / safety arbiter

```python
router.register_handler("sensor.", context_builder.push_telemetry)
router.register_handler("cv.", context_builder.push_vision_frame)
router.register_handler("stt.", fsm.handle_intent)
```

Handlers receive the canonical envelope. They should be fast or push into their own
bounded queues.

### 11.2 REST/WebSocket gateway

The future FastAPI layer can call:

```python
await router.handle_event("ui.command", payload)
```

No socket changes are needed. The same validation, sequence assignment, ring buffer
write, and handler fan-out apply.

### 11.3 gRPC sensor stream

A future gRPC adapter maps `SensorSnapshot` protobuf fields to the `sensor.snapshot`
wire dict and calls `handle_event`. The router contract does not change; only the
transport adapter is replaced. v1 mocks sensors over ZeroMQ.

### 11.4 Live telemetry fan-out

The ring buffer currently exposes `get_latest`/`snapshot`. A WebSocket gateway can
poll these or a future `subscribe()` async iterator can be added without changing
the persistence path.

### 11.5 Durable replay

If at-most-once is not sufficient, add a bounded on-disk spool between the ring
buffer and the worker. The `PersistenceSink` protocol is the seam; the router and
ring buffer do not need to change.

---

## 12. Mapping to the source design

### 12.1 Low-level design PDF

| PDF element | Implementation location |
|---|---|
| `container-orchestrator/src/router/event_router.py` | `container-orchestrator/src/eaa_orchestrator/router/event_router.py` |
| `container-orchestrator/src/persistence/database.py` | `container-orchestrator/src/eaa_orchestrator/persistence/database.py` |
| `container-orchestrator/src/persistence/repository.py` | `container-orchestrator/src/eaa_orchestrator/persistence/repository.py` |
| `shared/schemas/events.py` | `nvhn/shared/schemas/events.py` |
| `shared/constants/zmq_topics.py` | `nvhn/shared/constants/zmq_topics.py` |
| `shared/schemas/telemetry.py` | `nvhn/shared/schemas/telemetry.py` |
| WAL-mode SQLite | Implemented |
| ZeroMQ SUB/PULL | Implemented, with explicit bind/connect topology |
| Pydantic v2 normalization | Implemented |
| Half-sync/half-async persistence | Implemented |
| Telemetry ring buffer | Added under `telemetry/` because the PDF omitted its path |
| gRPC sensor stream | Deferred; `handle_event` is the adapter seam |
| FSM/safety/context/SLM | Out of scope; handler extension points provided |

### 12.2 Pseudocode fixes

| Pseudocode | Problem | Fix |
|---|---|---|
| `pseudo-router.txt` | CV SUB connects but no publisher binds | Mock binds `cv.pub`; topology documented |
| `pseudo-router.txt` | STT transport contradicts the diagram | STT/UI PUSH/PULL with router PULL binder |
| `pseudo-router.txt` | `recv_json()` loses topic | Multipart parsing |
| `pseudo-router.txt` | Print-only errors | Counters, rate-limited logs, listener isolation |
| `pseudo-router.txt` | No lifecycle | `start`/`stop`, lock, task supervision |
| `pseudo-buffer.txt` | Unbounded DB queue | Bounded drop-oldest queue |
| `pseudo-buffer.txt` | Timeout flush only when queue empty | Interval flush of partial batches |
| `pseudo-buffer.txt` | `task_done()` before DB write | `task_done()` after persistence attempt |
| `pseudo-buffer.txt` | No shutdown drain | Sentinel + drain timeout + final flush |
| `pseudo-buffer.txt` | No drop metrics | Queue/ring/persistence counters |
| `pseudo-database.txt` | Only timestamp/payload columns | Promoted typed columns |
| `pseudo-database.txt` | No idempotency | Unique `event_id`, `INSERT OR IGNORE` |
| `pseudo-database.txt` | No indices | Three query indices |
| `pseudo-database.txt` | No migrations | `PRAGMA user_version` migration |
| `pseudo-database.txt` | No retention | Max-rows/max-age pruning |
| `pseudo-database.txt` | No read API | `fetch_recent`, `count`, `health_check` |

---

## 13. Performance and limits

- **Ingestion:** validation and ring-buffer append are in-memory; the router never
  awaits disk I/O. The persistence handoff is `put_nowait`.
- **Write amplification:** batching amortizes SQLite transactions. CV frames are
  excluded by default because they dominate volume.
- **Backpressure:** intentionally avoided at the router. On pressure, the ring
  buffer evicts oldest and the persistence queue drops oldest; both are counted.
- **Ordering:** per-source `sequence` is process-local. Cross-source ordering is by
  `ingested_at`, then DB id.
- **IPC path length:** Linux IPC socket paths are limited (roughly 107 bytes).
  Keep `EAA_IPC_DIR` short.
- **Single instance:** one router per IPC directory; the `.router.lock` file
  enforces this.
- **No authentication:** local IPC and local SQLite are trusted.
- **No disk spool:** a process crash can lose queued events. This is deliberate for
  v1; see `PersistenceSink` for the future extension point.

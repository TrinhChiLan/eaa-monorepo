# EAA Backend Modules — Router, Telemetry Ring Buffer, SQLite Persistence

A runnable Python 3.12 slice of the Edge Agentic Assistant (EAA) backend described in
[EAA-Output2-Backend-LowLevel-Design.docx.pdf](reference/EAA-Output2-Backend-LowLevel-Design.docx.pdf).
It implements the three scoped modules end to end and mocks every external producer:

1. **Multi-protocol event router** — ZeroMQ PUB/SUB, ZeroMQ PUSH/PULL, and an
   in-process REST/WebSocket seam, all normalized into one typed event.
2. **Telemetry ring buffer** — bounded in-memory recent-event store with a
   non-blocking persistence handoff.
3. **SQLite persistence** — WAL-mode database, half-sync/half-async batch worker,
   idempotent inserts, retention, and query APIs.

The current automated suite is **56 tests**, and `ruff check` is clean.

- Detailed design and behavior: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Detailed test and verification guide: [docs/TESTING.md](docs/TESTING.md)

## Repository layout

The runnable code stays in `container-orchestrator/`; all supporting material lives in
`nvhn/` so the repo root has only the main application directory and `nvhn/`:

```text
eaa-monorepo/
├── container-orchestrator/     # main runtime code
│   └── src/eaa_orchestrator/
└── nvhn/                       # everything that supports it
    ├── shared/                 # canonical schemas and contracts
    ├── mocks/                  # mock external producers
    ├── tests/                  # unit and integration tests
    ├── docs/                   # architecture and testing guides
    ├── reference/              # PDF, architecture XML, pseudocode
    ├── pyproject.toml
    ├── requirements*.txt
    ├── Makefile
    └── README.md
```

---

## 1. What is implemented

| Module | Main file | Responsibility |
|---|---|---|
| Shared contract | [events.py](shared/schemas/events.py) | `EventEnvelope`, typed payloads, external wire models |
| Shared contracts | [contracts.py](shared/contracts.py) | `EventSink`, `PersistenceSink`, `SubmitOutcome` |
| Shared constants | [zmq_topics.py](shared/constants/zmq_topics.py) | Topics, endpoints, bind/connect topology |
| Router | [event_router.py](../container-orchestrator/src/eaa_orchestrator/router/event_router.py) | Ingest, validate, normalize, fan out |
| Ring buffer | [ring_buffer.py](../container-orchestrator/src/eaa_orchestrator/telemetry/ring_buffer.py) | Bounded recent-event store |
| Database | [database.py](../container-orchestrator/src/eaa_orchestrator/persistence/database.py) | Connection, WAL pragmas, schema version |
| Repository | [repository.py](../container-orchestrator/src/eaa_orchestrator/persistence/repository.py) | SQL, batch worker, retry, retention |
| Runtime | [main.py](../container-orchestrator/src/eaa_orchestrator/main.py) | Wiring, signals, stats logs |
| Mocks | [feed.py](mocks/feed.py) | Mock CV, sensor, STT, UI producers |

---

## 2. Data flow

```text
CV PUB ───────────────┐
Sensor PUB ───────────┤      ┌──────────────────┐      ┌──────────────────┐
STT PUSH ─────────────┼─────▶│   EventRouter    │─────▶│ TelemetryRing    │
UI PUSH ──────────────┤      │ normalize/validate│      │ Buffer (bounded) │
REST/WS direct call ──┘      └──────────────────┘      └────────┬─────────┘
                                                                 │ non-blocking
                                                                 ▼
                                                       ┌──────────────────┐
                                                       │ Persistence      │
                                                       │ Worker (bounded  │
                                                       │ queue + batching)│
                                                       └────────┬─────────┘
                                                                ▼
                                                       ┌──────────────────┐
                                                       │ SQLite (WAL)     │
                                                       └──────────────────┘
```

Only one object crosses module boundaries: the frozen, validated `EventEnvelope`.
Raw JSON is confined to the router's external adapters.

---

## 3. Quick start

### 3.1 Install

```bash
cd eaa-monorepo/nvhn
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Alternatively:

```bash
make install
```

### 3.2 Run the orchestrator

In terminal A:

```bash
source .venv/bin/activate
eaa-orchestrator --stats-interval 5
```

or:

```bash
make run
```

The orchestrator binds the STT/UI PULL sockets and connects to the CV/sensor PUB
sockets. Defaults:

- IPC directory: `/tmp/eaa`
- SQLite file: `data/edge_system.db`
- Ring capacity: 1000
- Persisted event types: sensor snapshots, STT intents, UI commands
- CV frames: ring-buffer only unless explicitly enabled

### 3.3 Run the mock feed

In terminal B:

```bash
source .venv/bin/activate
eaa-mock-feed
```

or:

```bash
make mock
```

The mock emits CV at 10 Hz, sensor at 5 Hz, STT every 2 s, UI every 5 s, and
injects malformed JSON every 50 messages. Start the orchestrator first, or ignore
the first second of output; ZeroMQ PUB/SUB drops messages sent before a
subscription is established.

### 3.4 Inspect the database

The `sqlite3` CLI is optional. The repository uses Python:

```bash
.venv/bin/python - <<'PY'
import sqlite3

con = sqlite3.connect("data/edge_system.db")
print("journal_mode:", con.execute("PRAGMA journal_mode").fetchone()[0])
print("schema_version:", con.execute("PRAGMA user_version").fetchone()[0])
print("total:", con.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
for row in con.execute(
    "SELECT event_type, COUNT(*) FROM telemetry_events GROUP BY event_type"
):
    print(row)
con.close()
PY
```

With the default allowlist, CV frames are present in the ring buffer but absent
from SQLite.

### 3.5 Stop cleanly

Press `Ctrl-C` in the orchestrator terminal. It stops the router, drains the
persistence queue up to the drain timeout, flushes the final batch, and closes the
database. The final stats line shows `unflushed_on_shutdown=0` when everything was
written.

---

## 4. Canonical internal event

Every module accepts and returns this shape:

```json
{
  "schema_version": 1,
  "event_id": "7c1d6d0f4e3b4f6c9a2b8e1d0c3f5a7b",
  "event_type": "sensor.snapshot",
  "source": "sensor",
  "topic": "sensor.snapshot",
  "sequence": 42,
  "event_time": 1762771829.123,
  "ingested_at": 1762771829.130,
  "payload": {
    "kind": "sensor.snapshot",
    "motor_current_amp": 1.84,
    "infrared_temp_celsius": 41.2,
    "conveyor_speed_mms": 320.0,
    "optical_curtain_clear": true,
    "estop_circuit_closed": true,
    "sensor_timestamp_ms": 1762771829123
  }
}
```

Key rules:

- `event_type` must equal `payload.kind`; Pydantic enforces this.
- `event_id` is UUID4 hex, generated by the router; the DB unique constraint makes
  retries idempotent.
- `event_time` is source epoch seconds (UTC); `ingested_at` is router receive time.
- `sequence` is a per-source counter that resets when the router restarts.
- Canonical models are frozen and reject unknown fields.
- External wire models ignore unknown fields for forward compatibility.

Payload types are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#4-canonical-event-contract).

---

## 5. External ingress

| Source | Socket topology | Topic | Default endpoint |
|---|---|---|---|
| CV | router `SUB` connects; producer `PUB` binds | `cv.frame` | `ipc:///tmp/eaa/cv.pub` |
| Sensor | router `SUB` connects; producer `PUB` binds | `sensor.snapshot` | `ipc:///tmp/eaa/sensor.pub` |
| STT | router `PULL` binds; producer `PUSH` connects | `stt.intent` | `ipc:///tmp/eaa/stt.pull` |
| UI | router `PULL` binds; producer `PUSH` connects | `ui.command` | `ipc:///tmp/eaa/ui.pull` |
| REST/WS future | direct `EventRouter.handle_event(topic, payload)` | any supported | in-process |

Wire formats:

```json
{"timestamp": 1762771829.1, "camera_id": "cam0", "detected_objects": ["carton"], "confidence": 0.93, "jam_suspected": false}
{"timestamp_ms": 1762771829123, "motor_current_amp": 1.84, "infrared_temp_celsius": 41.2, "conveyor_speed_mms": 320.0, "optical_curtain_clear": true, "estop_circuit_closed": true}
{"timestamp": 1762771829.1, "transcript": "clear the jam and resume", "intent": "clear_jam", "confidence": 0.88}
{"action": "pause", "parameters": {"source": "mock-ui"}}
```

ZeroMQ messages are multipart: `[topic_bytes, json_bytes]`. A single JSON frame is
accepted on PULL sockets and assigned the socket's default topic. Messages over
1 MiB, unknown topics, invalid UTF-8/JSON, and schema violations are counted and
dropped without stopping the listener.

---

## 6. Persistence policy

By default:

| Event type | Ring buffer | SQLite |
|---|---|---|
| `sensor.snapshot` | yes | yes |
| `stt.intent` | yes | yes |
| `ui.command` | yes | yes |
| `cv.frame` | yes | **no** |

Enable CV persistence with either:

```bash
EAA_PERSIST_CV_FRAMES=1 eaa-orchestrator
# or
eaa-orchestrator --persist-cv
```

The persistence queue is bounded. If SQLite is slower than ingestion, the oldest
queued event is dropped and `dropped_queue_full` increments. Ingestion itself never
waits for disk I/O. This is an explicit at-most-once design for the edge; there is
no disk spool in this slice.

---

## 7. Configuration

Environment variables are read by `Settings.from_env()` in [config.py](shared/config.py):

| Variable | Default | Meaning |
|---|---|---|
| `EAA_IPC_DIR` | `/tmp/eaa` | ZeroMQ IPC directory |
| `EAA_DB_PATH` | `data/edge_system.db` | SQLite file |
| `EAA_RING_CAPACITY` | `1000` | Ring buffer capacity |
| `EAA_DB_QUEUE_CAPACITY` | `10000` | Persistence queue bound |
| `EAA_DB_BATCH_SIZE` | `50` | Rows per batch |
| `EAA_DB_FLUSH_INTERVAL` | `2.0` | Max seconds before a partial batch flushes |
| `EAA_DB_MAX_RETRIES` | `3` | SQLite retry attempts |
| `EAA_RETENTION_MAX_ROWS` | `1000000` | Row cap; `0` disables |
| `EAA_RETENTION_MAX_AGE_DAYS` | `7` | Age cap; `0` disables |
| `EAA_PRUNE_INTERVAL_SECONDS` | `60` | Minimum time between prunes |
| `EAA_PERSIST_CV_FRAMES` | `0` | Set `1` to persist CV frames |
| `EAA_MAX_MESSAGE_BYTES` | `1048576` | Inbound message limit |
| `EAA_LOG_LEVEL` | `INFO` | Logging level |

CLI options:

```text
eaa-orchestrator --help
  --db-path PATH
  --ipc-dir PATH
  --duration SECONDS       (0 = run until SIGINT/SIGTERM)
  --stats-interval SECONDS (0 = disable periodic stats)
  --log-level LEVEL
  --persist-cv
```

---

## 8. How to test

### Automated

```bash
cd eaa-monorepo/nvhn
source .venv/bin/activate
pytest -q
ruff check . ../container-orchestrator
ruff format --check . ../container-orchestrator
```

Current expected result:

```text
56 passed
All checks passed!
```

### Test coverage summary

| Test file | Focus |
|---|---|
| [test_events.py](tests/test_events.py) | Contract validation, immutability, JSON round-trip |
| [test_router.py](tests/test_router.py) | Direct normalization, all four ZeroMQ protocols, errors, handlers, lock |
| [test_ring_buffer.py](tests/test_ring_buffer.py) | Capacity, eviction, ordering, concurrency, persistence outcomes |
| [test_persistence.py](tests/test_persistence.py) | WAL/schema, inserts, queries, batching, retry, drops, retention, shutdown |
| [test_integration.py](tests/test_integration.py) | Router → ring → worker → SQLite with default and CV-enabled policies |
| [test_config.py](tests/test_config.py) | Environment parsing and validation |
| [test_mocks.py](tests/test_mocks.py) | Mock payload generators and feed lifecycle |

### Manual end-to-end

The full checklist, SQL checks, pressure/retention scenarios, and troubleshooting
table are in [docs/TESTING.md](docs/TESTING.md). The shortest version:

```bash
make clean-db
make clean-ipc

# terminal A
eaa-orchestrator --stats-interval 3

# terminal B
eaa-mock-feed --duration 10
```

After the run, verify rows with the Python snippet in section 3.4. You should see
`sensor.snapshot`, `stt.intent`, and `ui.command` rows, but no `cv.frame` rows
unless `--persist-cv` was used.

---

## 9. Design decisions and limitations

- **At-most-once persistence.** Queue pressure drops the oldest queued event; stats
  and logs expose it. There is no replay log or disk spool.
- **CV frames are not persisted by default.** At 10–30 fps they dominate write
  volume and SD-card wear; the ring buffer still keeps them for live consumers.
- **Single router per device.** A POSIX advisory lock in the IPC directory prevents
  two routers from sharing endpoints. libzmq silently unlinks duplicate IPC binds,
  so relying on `EADDRINUSE` alone would be unsafe.
- **No gRPC yet.** Sensor ingest is mocked over ZeroMQ.
  `EventRouter.handle_event("sensor.snapshot", payload)` is the adapter seam for a
  future gRPC source.
- **No FastAPI/WebSocket server.** `handle_event` is the in-process REST/WS seam.
- **No authentication/encryption.** IPC and SQLite are treated as local, trusted
  resources.
- **SQLite is local-disk only.** WAL mode is enabled; network filesystems are not
  supported.
- **Ring buffer has no TTL.** Capacity is the only eviction rule. TTL belongs in the
  future multimodal context builder.
- **Sequence numbers are per process.** `event_id` and `ingested_at` are the durable
  ordering keys.

---

## 10. What changed from the pseudocode

| Pseudocode issue | Fix in this implementation |
|---|---|
| CV SUB connects to an endpoint no producer binds | Mock feed binds `cv.pub`; documented bind/connect table |
| STT transport contradicts the architecture diagram | STT and UI use PUSH/PULL with the router as PULL binder |
| `recv_json()` discards the topic | Multipart `[topic, json]` parsing |
| Invalid messages only printed | Counted, rate-limited logs, listener survives |
| Unbounded DB queue | Bounded drop-oldest queue with counters |
| Timeout flush only when queue empty | Partial batch flushes on interval even under load |
| `task_done()` before DB write | `task_done()` after persistence attempt |
| No shutdown drain | Sentinel + drain timeout + flushed final batch |
| Schema had only timestamp/payload | Promoted typed columns, unique `event_id`, indices, schema version |
| No retention | Configurable max rows/age, periodic pruning |
| No read API | `fetch_recent`, `count`, `health_check` |
| No single-instance protection | POSIX lock file in the IPC directory |

For the full mapping to the PDF and original pseudo files, see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#10-mapping-to-the-source-design).

# Edge Agentic Assistant (EAA) — Backend Orchestrator

This repository hosts the core backend orchestration runtime for the **Edge Agentic Assistant (EAA)** packaging workstation.

All code lives under the encompassing [`src/`](file:///E:/edge/src/) directory, organized into two decoupled pipelines that can be developed and tested in isolation or executed end-to-end as a unified continuous pipeline.

```
                                    EAA Backend Monorepo
                                              |
                     +------------------------+------------------------+
                     |                                                 |
             [ event_pipeline ]                               [ control_pipeline ]
    (Ingress Router, Ring Buffer, DB)                (FSM Engine, Safety Arbiter, MCU Bridge)
                     |                                                 |
                     +------------------------+------------------------+
                                              |
                                  [ continuous_pipeline ]
                                (End-to-End Unified Flow)
```

---

## 1. Directory Structure

```text
E:\edge\
├── src/
│   ├── event_pipeline/            # Pipeline 1: Router -> Ring Buffer -> SQLite Persistence
│   │   ├── event_router/          # Multi-protocol ingress router (ZeroMQ / in-process)
│   │   ├── telemetry_ring/        # Bounded in-memory circular ring buffer
│   │   ├── db_client/             # WAL-mode SQLite database connection & async worker
│   │   ├── mock_data/             # Schemas, event generators, and constants
│   │   ├── tests/                 # Dedicated unit tests for Pipeline 1
│   │   └── main_nvhn.py           # Independent runner for Pipeline 1
│   │
│   ├── control_pipeline/          # Pipeline 2: FSM -> Safety Arbiter -> MCU Bridge
│   │   ├── fsm/                   # Table-driven state machine & anti-flap lockout
│   │   ├── safety/                # Hardware safety invariant arbiter (I-1 to I-4)
│   │   ├── bridge/                # MCU communication bridge, B-03 codec, Mock MCU
│   │   ├── schemas/               # FSM, Arbiter, and Bridge Pydantic models
│   │   ├── tests/                 # Dedicated test suite (42 unit & integration tests)
│   │   └── main.py                # Independent runner / scenario demo for Pipeline 2
│   │
│   ├── continuous_pipeline.py     # Non-invasive linking layer uniting both pipelines
│   └── run_continuous_demo.py     # Standalone interactive demo of the continuous pipeline
│
├── pyproject.toml                 # Discovery paths, dependencies & test configuration
└── README.md                      # This documentation
```

---

## 2. Independent Development & Testing

Each pipeline operates as an independent module with its own models, lifecycle, and tests.

### A. Testing & Running Pipeline 1 (`event_pipeline`)
Focuses on event ingress validation, circular ring buffer eviction, and background SQLite WAL writes:

```bash
# Run standalone unit tests
pytest src/event_pipeline/tests/unit_tests -v

# Run Pipeline 1 standalone
python src/event_pipeline/main_nvhn.py --log-level INFO
```

### B. Testing & Running Pipeline 2 (`control_pipeline`)
Focuses on FSM state transitions, safety invariant gates ("MPU proposes, MCU decides"), B-03 binary framing, and 20Hz telemetry ingest:

```bash
# Run standalone test runner (42 tests)
python src/control_pipeline/run_all_tests.py

# Or via pytest directly
pytest src/control_pipeline/tests -v

# Run Pipeline 2 standalone scenario demo against MockMcu
python -m control_pipeline.main --profile mock
```

---

## 3. The Unified Continuous Pipeline (`continuous_pipeline.py`)

The continuous pipeline connects the two subsystems through clear, non-invasive interfaces without modifying the internal code of either pipeline:

1. **Inbound Ingress Routing (`event_pipeline` $\rightarrow$ `control_pipeline`):**
   * Implements the `EventRouter` sink protocol (`await push(envelope)`).
   * Maps incoming `UICommandPayload`, `IntentPayload` (voice STT), and `CVFramePayload` (vision jam detection) directly into `control_pipeline.submit_event()`.
2. **Actuation & Hardware Safety (`control_pipeline`):**
   * Validates state legality in `FSMEngine`.
   * Enforces hardware safety invariants in `SafetyInvariantArbiter` before dispatching to `McuBridgeWorker`.
3. **Outbound Telemetry Streaming (`control_pipeline` $\rightarrow$ `event_pipeline`):**
   * Listens to 20Hz `TelemetryFrame` data from the bridge.
   * Converts frames into canonical `SensorSnapshotPayload` envelopes and pushes them into `TelemetryRingBuffer` for real-time HMI WebSocket queries.
4. **Outbound State Audit & Persistence (`control_pipeline` $\rightarrow$ `event_pipeline`):**
   * Listens to FSM state changes (`on_state_changed`).
   * Queues audit records into the `HalfSyncHalfAsyncPersistenceWorker` for durable SQLite logging.

### Running the Continuous Pipeline Demo:
```bash
python src/run_continuous_demo.py
```
This executes an interactive 6-step lifecycle:
1. Bootstraps SQLite in WAL mode and launches `TelemetryRingBuffer`.
2. Performs HELLO handshake with Mock MCU over the bridge.
3. Injects UI `start` command via `EventRouter` $\rightarrow$ FSM transitions to `RUNNING`.
4. Streams 20Hz telemetry from MCU into `TelemetryRingBuffer`.
5. Injects voice `pause` intent via STT ingress (confidence 0.94) $\rightarrow$ FSM transitions to `PAUSED`.
6. Issues `resume` and graceful `stop` shutdown.

---

## 4. Requirements & Installation

- Python 3.12+ (tested on Python 3.12 and 3.13)
- Required packages:
  ```bash
  pip install pydantic>=2.10 pyzmq>=26 aiosqlite>=0.20 pyyaml>=6.0 pytest>=8.3 pytest-asyncio>=0.24
  ```

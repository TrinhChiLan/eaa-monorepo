# EAA Conveyor Orchestrator

Standalone core orchestrator block for the Edge AI Automated conveyor system, implementing:
1. **FSMEngine**: Pure table-driven state machine enforcing valid transitions and anti-flap rate limiting.
2. **SafetyInvariantArbiter**: "MPU proposes, MCU decides" hardware invariant gate (I-1 through I-4, speed limits, voice confidence).
3. **McuBridgeWorker**: Asynchronous B-03 protocol bridge managing STM32 / MCU UART communication, frame codec, CRC16-CCITT, HELLO handshake, and keep-alive watchdog.

This block is strictly isolated from the legacy `src/` directory and can run fully standalone or be embedded into the larger edge runtime.

---

## Architecture Overview

```
                      +-----------------------------+
                      | Inbound Events (UI, STT, CV)|
                      +--------------+--------------+
                                     |
                                     v
                           [ submit_event() ]
                                     |
                                     v
                            +-----------------+
                            |    FSMEngine    | (Validates state legality & anti-flap)
                            +--------+--------+
                                     | (FsmStateChanged)
                                     v
                     +-------------------------------+
                     |    SafetyInvariantArbiter     |<----+ (Sensor snapshots)
                     +---------------+---------------+     |
                                     | (ArbiterDecision)   |
                                     v                     |
                           [ send_action() ]               |
                                     |                     |
                                     v                     |
                           +-------------------+           |
                           |  McuBridgeWorker  |-----------+
                           +---------+---------+ (Telemetry at 20Hz)
                                     |
             +-----------------------+-----------------------+
             | (Mock transport)      | (COBS Unix socket)    | (Direct Serial UART)
             v                       v                       v
        [ MockMcu ]       [ /var/run/arduino-router.sock ]  [ /dev/ttyS0 ]
```

---

## 1. Setup and Prerequisites

### Requirements
- Python 3.12+ (tested on Python 3.12 and 3.13)
- Dependencies: `pydantic>=2.10`, `pytest>=8.3`, `pytest-asyncio>=0.24`, `pyyaml>=6.0`

Install dependencies:
```bash
pip install -r src/control_pipeline/requirements.txt
```

---

## 2. Running the Pipeline Demo

To run the interactive standalone simulation against the built-in `MockMcu`:

```bash
# Run default mock scenario
python -m control_pipeline.main --profile mock

# Run with custom YAML settings
python -m control_pipeline.main --config src/control_pipeline/config_example.yaml --log-level INFO
```

The demo executes an 8-step lifecycle:
1. Establishes HELLO / HELLO_ACK handshake (session negotiation).
2. Issues `START_CMD` from UI (transitions `IDLE` -> `RUNNING`).
3. Streams 20Hz telemetry and verifies conveyor speed ramp.
4. Issues `SET_SPEED_CMD` (setpoint adjustment to 250 mm/s).
5. Issues voice STT `PAUSE_CMD` (confidence check pass).
6. Issues `RESUME_CMD` back to `RUNNING`.
7. Injects a hardware Emergency Stop fault (trips `SAFETY_FAULT` and dispatches `HALT`).
8. Verifies recovery cycle back to `IDLE`.

---

## 3. Running the Test Suite

The test suite covers 100% of the B-03 codec specification, all FSM transition matrices, all 4 safety invariants, transport link degradation, and end-to-end pipeline lifecycles.

```bash
# Run with the standalone test runner:
python src/control_pipeline/run_all_tests.py

# Or directly with pytest:
pytest src/control_pipeline/tests -v
```

### Test Suite Structure
- `test_codec.py`: Validates B-03 CRC16-CCITT, modular sequence comparison, frame framing, and Section 9 spec test vectors.
- `test_fsm.py`: Tests the 28-entry state transition matrix, anti-flap lockout, illegal transition rejection, and observer notifications.
- `test_arbiter.py`: Validates Invariant I-1 (E-Stop), I-2 (Photoelectric beam on recovery), I-3 (motor overcurrent and temperature ceilings), I-4 (link staleness and loss), speed limits, and STT voice confidence filtering.
- `test_bridge.py`: Tests HELLO handshake, 20Hz telemetry streaming, command ACK correlation, asynchronous MCU event handling, and link-loss watchdog triggers.
- `test_pipeline.py`: End-to-end integration tests connecting FSM, Arbiter, and Bridge into real-world operational flows.

---

## 4. Integration Touchpoints: How to Link Other Parts

The orchestrator defines clear, decoupled input/output protocols:

### A. Feeding Inbound Events (UI, Voice STT, Computer Vision, MQTT, ZeroMQ)
External event routers or WebSocket handlers drive the orchestrator using `submit_event()`:

```python
from control_pipeline.pipeline import OrchestratorPipeline
from control_pipeline.config import OrchestratorSettings
from control_pipeline.schemas.fsm import FsmTrigger

pipeline = OrchestratorPipeline(OrchestratorSettings())
await pipeline.start()

# 1. Start command from Web UI
response = await pipeline.submit_event(
    trigger=FsmTrigger.START_CMD,
    origin="ui",
    payload={"setpoint_mms": 150}
)
# Returns: {"accepted": True, "state": "running", "arbiter_decision": {...}, "detail": "..."}

# 2. Voice command from STT service (e.g. Whisper / Web Speech API)
response = await pipeline.submit_event(
    trigger=FsmTrigger.PAUSE_CMD,
    origin="stt",
    payload={"confidence": 0.94, "intent_age_ms": 120}
)

# 3. Vision service detecting a jam
response = await pipeline.submit_event(
    trigger=FsmTrigger.JAM_DETECTED,
    origin="cv",
    payload={"camera_id": "cam_conveyor_1"}
)
```

### B. Connecting Real Hardware (UART / Arduino Router Socket)
To link to physical STM32 / MCU hardware instead of `MockMcu`:

1. **Profile B (Arduino Router Socket with COBS framing)**:
   In `config.yaml`:
   ```yaml
   transport_profile: "pipe"
   socket_path: "/var/run/arduino-router.sock"
   ```
   Or in Python code:
   ```python
   from control_pipeline.bridge.transport import PipeTransport
   
   transport = PipeTransport("/var/run/arduino-router.sock", use_cobs=True)
   pipeline = OrchestratorPipeline(settings, transport=transport, mock_mcu=None)
   await pipeline.start()
   ```

2. **Profile A (Raw Serial UART via PySerial)**:
   Implement a minimal 15-line `ByteTransport` wrapper around `pyserial_asyncio`:
   ```python
   class SerialTransport:
       async def send(self, data: bytes) -> None:
           self.writer.write(cobs.encode(data) + b"\x00")
           await self.writer.drain()
       async def recv(self) -> bytes:
           raw = await self.reader.readuntil(b"\x00")
           return cobs.decode(raw[:-1])
   ```

### C. Listening to Outbound Telemetry and Events (Web HMI / SQLite WAL)
To publish telemetry out to WebSockets or an external database:

```python
from control_pipeline.schemas.bridge import TelemetryFrame, McuEventFrame

async def broadcast_to_websocket(frame: TelemetryFrame) -> None:
    # Forward to container-web or EventRouter
    ws_payload = {
        "speed_mms": frame.speed_mms,
        "current_ma": frame.current_ma,
        "mcu_state": frame.mcu_state.name,
        "fault_reason": frame.fault_reason,
    }
    # await websocket_manager.broadcast(ws_payload)

pipeline.bridge.set_telemetry_listener(broadcast_to_websocket)
```

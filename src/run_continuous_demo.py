"""run_continuous_demo.py — End-to-end interactive demo of the linked continuous pipeline.

Simulates the full architecture:
  EventRouter Ingress -> FSM & Safety Arbiter -> McuBridge Actuation -> 20Hz Telemetry -> Ring Buffer -> SQLite WAL.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent
_EVENT_PIPELINE = _SRC_DIR / "event_pipeline"
_CONTROL_PIPELINE = _SRC_DIR / "control_pipeline"

for _p in (str(_SRC_DIR), str(_EVENT_PIPELINE), str(_CONTROL_PIPELINE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import asyncio
import logging
import time
import uuid

from continuous_pipeline import ContinuousPipeline
from mock_data.constants.zmq_topics import TOPIC_STT_INTENT, TOPIC_UI_COMMAND
from mock_data.schemas.events import EventEnvelope, EventSource, EventType, IntentPayload, UICommandPayload


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("\n" + "=" * 65)
    print("       EAA CONTINUOUS PIPELINE: LINKED ARCHITECTURE DEMO      ")
    print("=" * 65)
    print("Flow: EventRouter -> FSM -> Arbiter -> Bridge -> RingBuffer -> DB")
    print("-" * 65)

    pipeline = ContinuousPipeline(db_path=":memory:")
    await pipeline.start()

    try:
        print("\n[Step 1] Handshake & Initialization...")
        await asyncio.sleep(1.0)
        print(f"  FSM State         : {pipeline.control.fsm_state.value}")
        print(f"  MCU Link State    : {pipeline.control.bridge_health.link_state.value}")
        print(f"  Ring Buffer Size  : {(await pipeline.ring_buffer.stats()).size}")

        print("\n[Step 2] Sending UI START Command via Ingress Sink...")
        start_env = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=1,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=UICommandPayload(action="start", parameters={"setpoint_mms": 150}),
        )
        await pipeline.push(start_env)
        print(f"  FSM State         : {pipeline.control.fsm_state.value}")

        print("\n[Step 3] Conveyor Running — streaming 20Hz telemetry into ring buffer...")
        await asyncio.sleep(2.0)
        stats = await pipeline.ring_buffer.stats()
        latest = await pipeline.ring_buffer.get_latest(count=1)
        print(f"  Total Pushed to Ring Buffer : {stats.total_pushed}")
        if latest:
            p = latest[0].payload
            print(f"  Latest Snapshot   : speed={p.conveyor_speed_mms:.1f} mm/s, current={p.motor_current_amp:.2f} A, temp={p.infrared_temp_celsius:.1f} C")

        print("\n[Step 4] Sending Voice 'PAUSE' Intent via STT Ingress (confidence=0.94)...")
        pause_env = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.STT_INTENT,
            source=EventSource.STT,
            topic=TOPIC_STT_INTENT,
            sequence=2,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=IntentPayload(intent="pause", transcript="hold line", confidence=0.94),
        )
        await pipeline.push(pause_env)
        print(f"  FSM State         : {pipeline.control.fsm_state.value}")

        print("\n[Step 5] Sending UI RESUME Command...")
        resume_env = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=3,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=UICommandPayload(action="resume"),
        )
        await pipeline.push(resume_env)
        print(f"  FSM State         : {pipeline.control.fsm_state.value}")

        print("\n[Step 6] Shutting Down...")
        stop_env = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=4,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=UICommandPayload(action="stop"),
        )
        await pipeline.push(stop_env)
        print(f"  Final FSM State   : {pipeline.control.fsm_state.value}")

    finally:
        await pipeline.stop()
        print("\n[Done] Continuous pipeline closed cleanly.")


if __name__ == "__main__":
    asyncio.run(main())

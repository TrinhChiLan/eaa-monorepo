"""main.py — Standalone entry point and demonstration runner for EAA control_pipeline.

Runs the FSMEngine + SafetyInvariantArbiter + McuBridgeWorker pipeline
against a MockMcu in-process, stepping through a complete operational scenario.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent.parent
_THIS_DIR = Path(__file__).resolve().parent
for _p in (str(_SRC_DIR), str(_THIS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from control_pipeline.config import OrchestratorSettings
from control_pipeline.pipeline import OrchestratorPipeline
from control_pipeline.schemas.fsm import FsmTrigger


async def run_demo(settings: OrchestratorSettings, duration: float = 15.0) -> None:
    """Run an automated scenario demonstrating all three modules."""
    pipeline = OrchestratorPipeline(settings)
    mock = pipeline.mock_mcu

    print("\n=======================================================")
    print("      EAA Orchestrator Standalone Pipeline Demo        ")
    print("=======================================================")
    print(f"Initial FSM State   : {pipeline.fsm_state.value}")
    print(f"Transport Profile   : {settings.transport_profile}")
    print("-------------------------------------------------------\n")

    await pipeline.start()

    try:
        # 1. Handshake
        print("[Step 1] Initialising and awaiting HELLO handshake...")
        await asyncio.sleep(1.0)
        print(f"         Link State: {pipeline.bridge_health.link_state.value}")

        # 2. START command
        print("\n[Step 2] Sending START_CMD from UI...")
        res = await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
        print(f"         Outcome   : {res['detail']}")
        print(f"         FSM State : {pipeline.fsm_state.value}")

        # Wait and let telemetry stream
        await asyncio.sleep(2.0)
        telem = pipeline._bridge.latest_telemetry
        if telem:
            print(f"         Telemetry : speed={telem.speed_mms} mm/s, current={telem.current_ma} mA, state={telem.mcu_state.name}")

        # 3. SET_SPEED command
        print("\n[Step 3] Sending SET_SPEED_CMD (setpoint=250 mm/s)...")
        res = await pipeline.submit_event(
            FsmTrigger.SET_SPEED_CMD,
            origin="ui",
            payload={"setpoint_mms": 250},
        )
        print(f"         Outcome   : {res['detail']}")

        await asyncio.sleep(1.5)
        telem = pipeline._bridge.latest_telemetry
        if telem:
            print(f"         Telemetry : speed={telem.speed_mms} mm/s, current={telem.current_ma} mA")

        # 4. PAUSE via voice STT
        print("\n[Step 4] Sending PAUSE_CMD from STT (voice intent, confidence=0.92)...")
        res = await pipeline.submit_event(
            FsmTrigger.PAUSE_CMD,
            origin="stt",
            payload={"confidence": 0.92},
        )
        print(f"         Outcome   : {res['detail']}")
        print(f"         FSM State : {pipeline.fsm_state.value}")

        await asyncio.sleep(1.5)

        # 5. RESUME
        print("\n[Step 5] Sending RESUME_CMD from UI...")
        res = await pipeline.submit_event(FsmTrigger.RESUME_CMD, origin="ui")
        print(f"         Outcome   : {res['detail']}")
        print(f"         FSM State : {pipeline.fsm_state.value}")

        await asyncio.sleep(1.5)

        # 6. Fault injection: Hardware E-Stop trip
        if mock is not None:
            print("\n[Step 6] INJECTING HARDWARE FAULT: E-Stop circuit open...")
            mock.inject_estop()
            # Wait for event frame to arrive and trip safety
            await pipeline.submit_event(FsmTrigger.SAFETY_EVENT, origin="mcu")
            await asyncio.sleep(1.0)
            print(f"         FSM State : {pipeline.fsm_state.value}")

            # Try to start while E-Stop is active (should be blocked by Arbiter)
            print("\n[Step 7] Testing Invariant I-1: Attempting START while E-Stop active...")
            res = await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
            print(f"         Outcome   : {res['detail']}")

            # 8. Restore and clear fault
            print("\n[Step 8] Restoring E-Stop and clearing fault...")
            mock.restore()
            res = await pipeline.submit_event(FsmTrigger.FAULT_CLEARED, origin="mcu")
            print(f"         Outcome   : {res['detail']}")
            print(f"         FSM State : {pipeline.fsm_state.value} (Restored to IDLE, ready for next run)")

        print("\n=======================================================")
        print("Pipeline Summary:")
        print(f"  Arbiter Stats : {pipeline.arbiter_stats}")
        print(f"  Bridge Health : telemetry_rx={pipeline.bridge_health.telemetry_frames_rx}, "
              f"bad_crc={pipeline.bridge_health.bad_crc}")
        print("=======================================================\n")

    finally:
        await pipeline.stop()


def cli() -> None:
    parser = argparse.ArgumentParser(description="EAA Standalone Orchestrator Block")
    parser.add_argument("--config", help="Path to YAML configuration file")
    parser.add_argument("--profile", default="mock", choices=["mock", "pipe", "socket"], help="Transport profile")
    parser.add_argument("--duration", type=float, default=15.0, help="Run duration in seconds")
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.config:
        settings = OrchestratorSettings.from_yaml(args.config)
    else:
        settings = OrchestratorSettings(transport_profile=args.profile)

    try:
        asyncio.run(run_demo(settings, duration=args.duration))
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    cli()

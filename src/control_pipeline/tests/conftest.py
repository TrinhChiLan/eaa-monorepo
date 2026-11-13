"""conftest.py — Pytest fixtures for EAA orchestrator tests."""

from __future__ import annotations

import pytest

from control_pipeline.bridge.mock_mcu import MockMcu
from control_pipeline.bridge.transport import MockMcuTransport
from control_pipeline.bridge.worker import McuBridgeWorker
from control_pipeline.schemas.arbiter import SensorReading


@pytest.fixture
def default_sensor_reading() -> SensorReading:
    """Return a healthy, nominal sensor reading."""
    return SensorReading(
        estop_closed=True,
        pe_clear=True,
        hb_valid=True,
        fault_latched=False,
        motor_enabled=False,
        sensors_ok=True,
        motor_current_ma=850.0,
        conveyor_speed_mms=0.0,
        infrared_temp_celsius=34.2,
        sensor_timestamp_ms=1000,
        link_state="up",
        last_frame_age_ms=50.0,
    )


class FakeSensorSource:
    """In-memory SensorSource that can be mutated in tests."""

    def __init__(self, reading: SensorReading) -> None:
        self._reading = reading

    def set(self, reading: SensorReading) -> None:
        self._reading = reading

    async def latest(self) -> SensorReading:
        return self._reading


@pytest.fixture
def fake_sensor_source(default_sensor_reading: SensorReading) -> FakeSensorSource:
    return FakeSensorSource(default_sensor_reading)


@pytest.fixture
def mock_mcu_pair() -> tuple[McuBridgeWorker, MockMcu]:
    """Provide a linked (McuBridgeWorker, MockMcu) pair over MockMcuTransport."""
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    worker = McuBridgeWorker(mpu_tr)
    return worker, mock

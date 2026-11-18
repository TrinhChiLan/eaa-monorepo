"""mock_mcu.py — Simulated STM32U585 MCU for the EAA B-03 protocol.

Runs in-process to simulate the Zephyr RTOS firmware on the MCU side of the
B-03 v1.0 link.  Supports:
- Full HELLO/HELLO_ACK handshake
- Periodic HEARTBEAT reception and 300 ms watchdog
- 20 Hz TELEMETRY streaming
- Command execution (RUN, SET_SPEED, STOP, HALT, CLEAR_FAULT) and CMD_ACK
- Event emission and retransmission until EVENT_ACK
- Fault injection (E-stop trip, jam detection, motor overcurrent)
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time
from typing import Final

from control_pipeline import codec
from control_pipeline.bridge.transport import ByteTransport
from control_pipeline.schemas.arbiter import ReasonCode
from control_pipeline.schemas.bridge import McuEventKind, McuState

# Safety flags bitmask definitions (MCU -> MPU)
FLAG_ESTOP_CLOSED: Final[int] = 1 << 0
FLAG_PE_CLEAR: Final[int] = 1 << 1
FLAG_HB_VALID: Final[int] = 1 << 2
FLAG_FAULT_LATCHED: Final[int] = 1 << 3
FLAG_MOTOR_ENABLED: Final[int] = 1 << 4
FLAG_SENSORS_OK: Final[int] = 1 << 5
FLAG_VOICE_ACTIVE: Final[int] = 1 << 6


class MockMcu:
    """Simulates the STM32 MCU for local testing and demonstration."""

    def __init__(
        self,
        transport: ByteTransport,
        *,
        boot_id: int = 0x1234,
        fw_ver: int = 0x0100,
        logger: logging.Logger | None = None,
    ) -> None:
        self._transport = transport
        self._boot_id = boot_id
        self._fw_ver = fw_ver
        self._logger = logger or logging.getLogger(__name__)

        self._running = False
        self._tasks: list[asyncio.Task[None]] = []

        # Internal state
        self._mcu_state = McuState.STOPPED
        self._fault_reason = 0
        self._setpoint_mms = 100
        self._current_ma = 850
        self._temp_c10 = 342  # 34.2 °C
        self._flags = (
            FLAG_ESTOP_CLOSED
            | FLAG_PE_CLEAR
            | FLAG_HB_VALID
            | FLAG_SENSORS_OK
        )

        # Sequence and timing
        self._seq = 0
        self._last_rx_seq = 0
        self._start_time = time.monotonic()
        self._last_heartbeat_at = time.monotonic()
        self._watchdog_enabled = True

        # Event retransmission
        self._event_seq = 0
        self._pending_events: dict[int, bytes] = {}

    @property
    def mcu_state(self) -> McuState:
        return self._mcu_state

    @property
    def setpoint_mms(self) -> int:
        return self._setpoint_mms

    @property
    def current_ma(self) -> int:
        return self._current_ma

    def _tick_ms(self) -> int:
        return int((time.monotonic() - self._start_time) * 1000) & 0xFFFFFFFF

    def _next_seq(self) -> int:
        s = self._seq
        self._seq = (self._seq + 1) & 0xFFFF
        return s

    async def start(self) -> None:
        """Start the MockMcu processing loops."""
        if self._running:
            return
        self._running = True
        self._start_time = time.monotonic()
        self._last_heartbeat_at = time.monotonic()

        self._tasks = [
            asyncio.create_task(self._rx_loop(), name="mock-mcu-rx"),
            asyncio.create_task(self._telemetry_loop(), name="mock-mcu-telem"),
            asyncio.create_task(self._watchdog_loop(), name="mock-mcu-watchdog"),
        ]

    async def stop(self) -> None:
        """Stop the MockMcu gracefully."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # ------------------------------------------------------------------
    # Fault injection methods
    # ------------------------------------------------------------------

    def inject_estop(self) -> None:
        """Simulate hardware emergency stop activation."""
        self._flags &= ~FLAG_ESTOP_CLOSED
        self._flags |= FLAG_FAULT_LATCHED
        self._flags &= ~FLAG_MOTOR_ENABLED
        self._mcu_state = McuState.SAFE_STOP
        self._fault_reason = ReasonCode.ERR_ESTOP_ACTIVE
        self.emit_event(
            McuEventKind.HARDWARE_ESTOP,
            reason=ReasonCode.ERR_ESTOP_ACTIVE,
            observed=0,
            threshold=1,
        )

    def inject_jam(self) -> None:
        """Simulate jam detected by photoelectric beam."""
        self._flags &= ~FLAG_PE_CLEAR
        self.emit_event(
            McuEventKind.JAM_DETECTED,
            reason=ReasonCode.ERR_PHOTOELECTRIC_BLOCKED,
            observed=1,
            threshold=0,
        )

    def inject_overcurrent(self, current_ma: int = 3000) -> None:
        """Simulate motor overcurrent."""
        self._current_ma = current_ma
        if current_ma > 2500:
            self._mcu_state = McuState.SAFE_STOP
            self._fault_reason = ReasonCode.ERR_MOTOR_OVERCURRENT
            self._flags |= FLAG_FAULT_LATCHED
            self._flags &= ~FLAG_MOTOR_ENABLED
            self.emit_event(
                McuEventKind.MOTOR_OVERLOAD,
                reason=ReasonCode.ERR_MOTOR_OVERCURRENT,
                observed=current_ma,
                threshold=2500,
            )

    def restore(self) -> None:
        """Restore all sensor flags to healthy status."""
        self._flags = (
            FLAG_ESTOP_CLOSED
            | FLAG_PE_CLEAR
            | FLAG_HB_VALID
            | FLAG_SENSORS_OK
        )
        self._current_ma = 850
        self._fault_reason = 0
        self._last_heartbeat_at = time.monotonic()

    def emit_event(
        self,
        event_kind: McuEventKind,
        reason: int = 0,
        observed: int = 0,
        threshold: int = 0,
    ) -> None:
        """Queue and transmit an unsolicited MCU event."""
        self._event_seq = (self._event_seq + 1) & 0xFFFF
        # Payload layout: event_kind u8, reason u8, event_seq u16, mcu_state u8, observed i16, threshold i16
        payload = struct.pack(
            "<BBHBhh",
            int(event_kind),
            reason,
            self._event_seq,
            int(self._mcu_state),
            observed,
            threshold,
        )
        frame = codec.encode_frame(
            msg_type=codec.MT_MCU_EVENT,
            seq=self._next_seq(),
            ack=self._last_rx_seq,
            tick_ms=self._tick_ms(),
            flags=self._flags,
            payload=payload,
        )
        self._pending_events[self._event_seq] = frame
        asyncio.create_task(self._send_raw(frame))

    def emit_voice_event(
        self,
        kind: int,
        confidence_pct: int = 95,
        model_ver: int = 0x0100,
    ) -> None:
        """Queue and transmit a VOICE_EVENT frame (0x93).

        For kind == 1 (STOP), the MCU stops the belt immediately (McuState.STOPPED)
        before dispatching the frame, per v1.1 Section 2.4 / 3.3.
        For kind == 0 (WAKE), sets FLAG_VOICE_ACTIVE in the flags bitmask.
        """
        if kind == 1:
            self._mcu_state = McuState.STOPPED
            self._flags &= ~FLAG_MOTOR_ENABLED
            self._flags &= ~FLAG_VOICE_ACTIVE
        elif kind == 0:
            self._flags |= FLAG_VOICE_ACTIVE

        self._event_seq = (self._event_seq + 1) & 0xFFFF
        frame = codec.encode_voice_event(
            seq=self._next_seq(),
            ack=self._last_rx_seq,
            tick_ms=self._tick_ms(),
            flags=self._flags,
            kind=kind,
            confidence_pct=confidence_pct,
            event_seq=self._event_seq,
            mcu_state=int(self._mcu_state),
            model_ver=model_ver,
        )
        self._pending_events[self._event_seq] = frame
        asyncio.create_task(self._send_raw(frame))

    # ------------------------------------------------------------------
    # Frame reception loop
    # ------------------------------------------------------------------

    async def _send_raw(self, frame: bytes) -> None:
        try:
            await self._transport.send(frame)
        except Exception:
            self._logger.exception("MockMcu failed to send frame")

    async def _rx_loop(self) -> None:
        while self._running:
            try:
                data = await self._transport.recv()
            except asyncio.CancelledError:
                break
            except Exception:
                self._logger.exception("MockMcu recv error")
                await asyncio.sleep(0.05)
                continue

            dec = codec.decode_frame(data)
            if dec is None:
                continue

            msg_type, seq, ack, tick_ms, flags, payload = dec
            self._last_rx_seq = seq

            await self._handle_frame(msg_type, seq, payload)

    async def _handle_frame(self, msg_type: int, seq: int, payload: bytes) -> None:
        if msg_type == codec.MT_HELLO:
            # Payload: proto_ver u8, session_id u32
            proto_ver, session_id = struct.unpack_from("<BI", payload)
            # HELLO_ACK payload (v1.1): proto_ver u8, mcu_boot_id u16, fw_ver u16, mcu_state u8, fault_reason u8
            ack_payload = struct.pack(
                "<BHHBB",
                codec.PROTO_VER,
                self._boot_id & 0xFFFF,
                self._fw_ver,
                int(self._mcu_state),
                self._fault_reason,
            )
            ack_frame = codec.encode_frame(
                msg_type=codec.MT_HELLO_ACK,
                seq=self._next_seq(),
                ack=self._last_rx_seq,
                tick_ms=self._tick_ms(),
                flags=self._flags,
                payload=ack_payload,
            )
            await self._send_raw(ack_frame)

        elif msg_type == codec.MT_HEARTBEAT:
            self._last_heartbeat_at = time.monotonic()
            self._flags |= FLAG_HB_VALID

        elif msg_type == codec.MT_EVENT_ACK:
            # Payload: event_seq u16
            if len(payload) >= 2:
                event_seq = struct.unpack_from("<H", payload)[0]
                self._pending_events.pop(event_seq, None)

        elif msg_type == codec.MT_RUN:
            # Payload: cmd_id u16, ttl_ms u16, kind u8, setpoint_mms u16
            cmd_id, ttl_ms, kind, setpoint_mms = struct.unpack_from("<HHBH", payload)
            if self._mcu_state in (McuState.STOPPED, McuState.RUNNING):
                self._mcu_state = McuState.RUNNING
                self._setpoint_mms = setpoint_mms
                self._flags |= FLAG_MOTOR_ENABLED
                result = 0  # ACCEPTED
                reason = ReasonCode.OK_NOMINAL
            else:
                result = 1  # REJECTED
                reason = ReasonCode.ERR_STATE_NOT_PERMITTED

            await self._send_cmd_ack(cmd_id, result, reason)

        elif msg_type == codec.MT_SET_SPEED:
            # Payload: cmd_id u16, ttl_ms u16, setpoint_mms u16
            cmd_id, ttl_ms, setpoint_mms = struct.unpack_from("<HHH", payload)
            self._setpoint_mms = setpoint_mms
            await self._send_cmd_ack(cmd_id, 0, ReasonCode.OK_NOMINAL)

        elif msg_type == codec.MT_STOP:
            # Payload: cmd_id u16, ttl_ms u16, kind u8 (0 STOP, 1 PAUSE)
            cmd_id, ttl_ms, kind = struct.unpack_from("<HHB", payload)
            self._mcu_state = McuState.STOPPED
            self._flags &= ~FLAG_MOTOR_ENABLED
            await self._send_cmd_ack(cmd_id, 0, ReasonCode.OK_NOMINAL)

        elif msg_type == codec.MT_HALT:
            # Payload: cmd_id u16, ttl_ms u16
            cmd_id, ttl_ms = struct.unpack_from("<HH", payload)
            self._mcu_state = McuState.SAFE_STOP
            self._fault_reason = ReasonCode.ERR_MPU_HALT_REQUEST
            self._flags |= FLAG_FAULT_LATCHED
            self._flags &= ~FLAG_MOTOR_ENABLED
            await self._send_cmd_ack(cmd_id, 0, ReasonCode.OK_NOMINAL)

        elif msg_type == codec.MT_CLEAR_FAULT:
            # Payload: cmd_id u16, ttl_ms u16
            cmd_id, ttl_ms = struct.unpack_from("<HH", payload)
            if self._mcu_state == McuState.SAFE_STOP:
                self._mcu_state = McuState.STOPPED
                self._fault_reason = 0
                self._flags &= ~FLAG_FAULT_LATCHED
                result = 0
                reason = ReasonCode.OK_NOMINAL
            else:
                result = 0
                reason = ReasonCode.OK_NOMINAL
            await self._send_cmd_ack(cmd_id, result, reason)

    async def _send_cmd_ack(self, cmd_id: int, result: int, reason: int) -> None:
        payload = struct.pack("<HBBB", cmd_id, result, reason, int(self._mcu_state))
        frame = codec.encode_frame(
            msg_type=codec.MT_CMD_ACK,
            seq=self._next_seq(),
            ack=self._last_rx_seq,
            tick_ms=self._tick_ms(),
            flags=self._flags,
            payload=payload,
        )
        await self._send_raw(frame)

    # ------------------------------------------------------------------
    # Telemetry and Watchdog loops
    # ------------------------------------------------------------------

    async def _telemetry_loop(self) -> None:
        """Transmit 20 Hz (50 ms period) TELEMETRY frames."""
        while self._running:
            speed = self._setpoint_mms if self._mcu_state == McuState.RUNNING else 0
            pwm = self._setpoint_mms if self._mcu_state == McuState.RUNNING else 0

            # 13-byte TELEMETRY payload:
            # mcu_state u8, fault_reason u8, setpoint_mms u16, speed_mms u16,
            # current_ma u16, temp_c10 i16, sensor_status u8, pwm_pm u16
            payload = struct.pack(
                "<BBHHHhBH",
                int(self._mcu_state),
                self._fault_reason,
                self._setpoint_mms,
                speed,
                self._current_ma,
                self._temp_c10,
                0x00,  # all sensors OK
                pwm,
            )

            frame = codec.encode_frame(
                msg_type=codec.MT_TELEMETRY,
                seq=self._next_seq(),
                ack=self._last_rx_seq,
                tick_ms=self._tick_ms(),
                flags=self._flags,
                payload=payload,
            )
            await self._send_raw(frame)
            await asyncio.sleep(0.05)  # 20 Hz

    async def _watchdog_loop(self) -> None:
        """MCU-side heartbeat watchdog (300 ms timeout)."""
        while self._running:
            await asyncio.sleep(0.05)
            if not self._watchdog_enabled:
                continue

            elapsed = time.monotonic() - self._last_heartbeat_at
            if elapsed > 0.300 and self._mcu_state == McuState.RUNNING:
                self._flags &= ~FLAG_HB_VALID
                self._flags &= ~FLAG_MOTOR_ENABLED
                self._flags |= FLAG_FAULT_LATCHED
                self._mcu_state = McuState.SAFE_STOP
                self._fault_reason = ReasonCode.ERR_HEARTBEAT_TIMEOUT
                self.emit_event(
                    McuEventKind.HEARTBEAT_TIMEOUT,
                    reason=ReasonCode.ERR_HEARTBEAT_TIMEOUT,
                    observed=int(elapsed * 1000),
                    threshold=300,
                )

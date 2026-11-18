"""worker.py — McuBridgeWorker for B-03 v1.1 protocol.

Manages MPU-side communications:
- Handshake with MCU via HELLO / HELLO_ACK
- Heartbeat loop at 100 ms while link is healthy
- Link state tracking (DOWN -> SYNC -> UP -> STALE -> LOST)
- Ingestion of 20 Hz TELEMETRY frames and dispatch to telemetry_listener
- Ingestion of unsolicited MCU events (0x91 EVENT, 0x93 VOICE_EVENT), sending EVENT_ACK
- Reliable command transmission with cmd_id, ttl_ms, and CMD_ACK confirmation
- Conversion of live telemetry into a SensorReading for the SafetyInvariantArbiter
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable, Coroutine
from typing import Any

from control_pipeline import codec
from control_pipeline.bridge.transport import ByteTransport
from control_pipeline.schemas.arbiter import SensorReading
from control_pipeline.schemas.bridge import (
    BridgeHealth,
    CmdAckFrame,
    HelloAckFrame,
    LinkState,
    McuEventFrame,
    McuState,
    TelemetryFrame,
    VoiceEventFrame,
)
from control_pipeline.schemas.fsm import Action


class McuBridgeWorker:
    """Manages the MPU side of the B-03 v1.0 MPU-MCU communication link."""

    def __init__(
        self,
        transport: ByteTransport,
        *,
        telemetry_listener: Callable[[TelemetryFrame], Coroutine[Any, Any, None]] | None = None,
        mcu_event_listener: Callable[[McuEventFrame], Coroutine[Any, Any, None]] | None = None,
        voice_event_listener: Callable[[VoiceEventFrame], Coroutine[Any, Any, None]] | None = None,
        link_lost_callback: Callable[[], Coroutine[Any, Any, None]] | None = None,
        heartbeat_interval_s: float = 0.1,
        hello_retry_interval_s: float = 0.5,
        stale_threshold_ms: float = 250.0,
        lost_threshold_ms: float = 500.0,
        default_ttl_ms: int = 500,
        clock: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        self._transport = transport
        self._telemetry_listener = telemetry_listener
        self._mcu_event_listener = mcu_event_listener
        self._voice_event_listener = voice_event_listener
        self._link_lost_callback = link_lost_callback

        self._heartbeat_interval_s = heartbeat_interval_s
        self._hello_retry_interval_s = hello_retry_interval_s
        self._stale_threshold_ms = stale_threshold_ms
        self._lost_threshold_ms = lost_threshold_ms
        self._default_ttl_ms = default_ttl_ms
        self._clock = clock
        self._logger = logger or logging.getLogger(__name__)

        self._running = False
        self._tasks: list[asyncio.Task[None]] = []

        # Sequence and transaction tracking
        self._mpu_seq: int = 0
        self._last_mcu_seq: int = 0
        self._session_id: int = random.randint(1, 0xFFFF)
        self._start_time: float = self._clock()
        self._last_frame_rx_at: float | None = None

        # Health and stats
        self._health = BridgeHealth(
            link_state=LinkState.DOWN,
            last_valid_frame_at=None,
            hello_sent_at=None,
            session_id=self._session_id,
            mcu_boot_id=None,
            fw_ver=None,
        )

        # Pending command acknowledgements: cmd_id -> asyncio.Future[CmdAckFrame]
        self._pending_cmd_acks: dict[int, asyncio.Future[CmdAckFrame]] = {}
        self._cmd_id_counter: int = 0

        # Latest state snapshot
        self._latest_telemetry: TelemetryFrame | None = None
        self._mpu_state_for_heartbeat: int = 1  # STOPPED / IDLE by default

    @property
    def health(self) -> BridgeHealth:
        return self._health

    @property
    def link_state(self) -> LinkState:
        return self._health.link_state

    @property
    def latest_telemetry(self) -> TelemetryFrame | None:
        return self._latest_telemetry

    def set_telemetry_listener(self, listener: TelemetryListener | None) -> None:
        """Register or replace the telemetry frame listener callback."""
        self._telemetry_listener = listener

    def set_fsm_state_for_heartbeat(self, state_val: int) -> None:
        self._mpu_state_for_heartbeat = state_val

    def _tick_ms(self) -> int:
        return int((self._clock() - self._start_time) * 1000) & 0xFFFFFFFF

    def _next_mpu_seq(self) -> int:
        s = self._mpu_seq
        self._mpu_seq = (self._mpu_seq + 1) & 0xFFFF
        return s

    def _next_cmd_id(self) -> int:
        self._cmd_id_counter = (self._cmd_id_counter + 1) & 0xFFFF
        return self._cmd_id_counter or 1

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the worker and begin link handshake."""
        if self._running:
            return
        self._running = True
        self._start_time = self._clock()
        await self._transport.open()

        self._tasks = [
            asyncio.create_task(self._rx_loop(), name="bridge-rx"),
            asyncio.create_task(self._hello_handshake_loop(), name="bridge-hello"),
            asyncio.create_task(self._heartbeat_loop(), name="bridge-hb"),
            asyncio.create_task(self._health_monitor_loop(), name="bridge-health"),
        ]

    async def stop(self) -> None:
        """Stop worker tasks and close transport."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

        # Cancel any pending command futures
        for fut in self._pending_cmd_acks.values():
            if not fut.done():
                fut.cancel()
        self._pending_cmd_acks.clear()

        await self._transport.close()
        self._health.link_state = LinkState.DOWN

    # ------------------------------------------------------------------
    # Command transmission
    # ------------------------------------------------------------------

    async def send_action(
        self,
        action: Action,
        setpoint_mms: int | None = None,
        cmd_id: int | None = None,
        ttl_ms: int | None = None,
    ) -> CmdAckFrame | None:
        """Translate Action to B-03 frame, send, and await CMD_ACK."""
        cid = cmd_id if cmd_id is not None else self._next_cmd_id()
        ttl = ttl_ms or self._default_ttl_ms
        sp = setpoint_mms or 100

        seq = self._next_mpu_seq()
        ack = self._last_mcu_seq
        tick = self._tick_ms()

        if action == Action.START:
            frame = codec.encode_run(seq, ack, tick, cmd_id=cid, ttl_ms=ttl, kind=0, setpoint_mms=sp)
        elif action == Action.RESUME:
            frame = codec.encode_run(seq, ack, tick, cmd_id=cid, ttl_ms=ttl, kind=1, setpoint_mms=sp)
        elif action == Action.SET_SPEED:
            frame = codec.encode_set_speed(seq, ack, tick, cmd_id=cid, ttl_ms=ttl, setpoint_mms=sp)
        elif action == Action.STOP:
            frame = codec.encode_stop(seq, ack, tick, cmd_id=cid, ttl_ms=ttl, kind=0)
        elif action == Action.PAUSE:
            frame = codec.encode_stop(seq, ack, tick, cmd_id=cid, ttl_ms=ttl, kind=1)
        elif action == Action.HALT:
            frame = codec.encode_halt(seq, ack, tick, cmd_id=cid, ttl_ms=ttl)
        elif action == Action.RECOVER:
            frame = codec.encode_clear_fault(seq, ack, tick, cmd_id=cid, ttl_ms=ttl)
        else:
            raise ValueError(f"Unsupported action: {action}")

        fut: asyncio.Future[CmdAckFrame] = asyncio.get_running_loop().create_future()
        self._pending_cmd_acks[cid] = fut

        timeout = min(ttl / 1000.0, 1.0)
        try:
            await self._transport.send(frame)
            ack_frame = await asyncio.wait_for(fut, timeout=timeout)
            return ack_frame
        except TimeoutError:
            self._logger.warning("Command %s (id=%d) timed out waiting for CMD_ACK", action, cid)
            return None
        except asyncio.CancelledError:
            raise
        finally:
            self._pending_cmd_acks.pop(cid, None)

    # ------------------------------------------------------------------
    # Sensor Reading conversion for Safety Arbiter
    # ------------------------------------------------------------------

    def latest_sensor_reading(self) -> SensorReading:
        """Produce a SensorReading snapshot for SafetyInvariantArbiter."""
        tf = self._latest_telemetry
        now = self._clock()
        last_age_ms = (now - self._last_frame_rx_at) * 1000.0 if self._last_frame_rx_at else 999999.0

        if tf is None:
            return SensorReading(
                estop_closed=False,
                pe_clear=False,
                hb_valid=False,
                fault_latched=True,
                motor_enabled=False,
                sensors_ok=False,
                motor_current_ma=0.0,
                conveyor_speed_mms=0.0,
                infrared_temp_celsius=0.0,
                sensor_timestamp_ms=None,
                link_state=self._health.link_state.value,
                last_frame_age_ms=last_age_ms,
            )

        flags = tf.flags
        return SensorReading(
            estop_closed=bool(flags & 1),
            pe_clear=bool(flags & 2),
            hb_valid=bool(flags & 4),
            fault_latched=bool(flags & 8),
            motor_enabled=bool(flags & 16),
            sensors_ok=bool(flags & 32),
            motor_current_ma=float(tf.current_ma),
            conveyor_speed_mms=float(tf.speed_mms),
            infrared_temp_celsius=float(tf.temp_c10) / 10.0,
            sensor_timestamp_ms=tf.tick_ms,
            link_state=self._health.link_state.value,
            last_frame_age_ms=last_age_ms,
        )

    # ------------------------------------------------------------------
    # Internal Loops
    # ------------------------------------------------------------------

    async def _rx_loop(self) -> None:
        """Continuous frame reception loop."""
        while self._running:
            try:
                data = await self._transport.recv()
            except asyncio.CancelledError:
                break
            except Exception:
                self._logger.exception("Transport receive error in McuBridgeWorker")
                await asyncio.sleep(0.05)
                continue

            dec = codec.decode_frame(data)
            if dec is None:
                self._health.bad_crc += 1
                continue

            msg_type, seq, ack, tick_ms, flags, payload = dec
            now = self._clock()
            self._last_frame_rx_at = now
            self._health.last_valid_frame_at = now

            # If we were STALE and get valid frames again, restore UP
            if self._health.link_state == LinkState.STALE:
                self._health.link_state = LinkState.UP

            # Modular sequence check
            if not codec.newer_seq(seq, self._last_mcu_seq) and seq != 0 and self._last_mcu_seq != 0:
                if seq == self._last_mcu_seq:
                    self._health.duplicate_frames += 1
                else:
                    self._health.out_of_seq += 1
            self._last_mcu_seq = seq

            # Route by message type
            if msg_type == codec.MT_HELLO_ACK:
                ha = codec.decode_hello_ack(msg_type, seq, ack, tick_ms, flags, payload)
                if ha:
                    self._health.link_state = LinkState.UP
                    self._health.mcu_boot_id = ha.mcu_boot_id
                    self._health.fw_ver = ha.fw_ver

            elif msg_type == codec.MT_TELEMETRY:
                tf = codec.decode_telemetry(msg_type, seq, ack, tick_ms, flags, payload)
                if tf:
                    self._latest_telemetry = tf
                    self._health.telemetry_frames_rx += 1
                    if self._telemetry_listener:
                        try:
                            await self._telemetry_listener(tf)
                        except Exception:
                            self._logger.exception("Telemetry listener error")

            elif msg_type == codec.MT_CMD_ACK:
                ca = codec.decode_cmd_ack(msg_type, seq, ack, tick_ms, flags, payload)
                if ca and ca.cmd_id in self._pending_cmd_acks:
                    fut = self._pending_cmd_acks[ca.cmd_id]
                    if not fut.done():
                        fut.set_result(ca)

            elif msg_type == codec.MT_MCU_EVENT:
                ev = codec.decode_mcu_event(msg_type, seq, ack, tick_ms, flags, payload)
                if ev:
                    # Immediately transmit EVENT_ACK
                    ack_frame = codec.encode_event_ack(
                        self._next_mpu_seq(),
                        self._last_mcu_seq,
                        self._tick_ms(),
                        event_seq=ev.event_seq,
                    )
                    await self._transport.send(ack_frame)
                    self._health.event_acks_sent += 1

                    if self._mcu_event_listener:
                        try:
                            await self._mcu_event_listener(ev)
                        except Exception:
                            self._logger.exception("MCU event listener error")

            elif msg_type == codec.MT_VOICE_EVENT:
                ve = codec.decode_voice_event(msg_type, seq, ack, tick_ms, flags, payload)
                if ve:
                    # Retransmitted until EVENT_ACK, like EVENT (v1.1 §3.3)
                    ack_frame = codec.encode_event_ack(
                        self._next_mpu_seq(),
                        self._last_mcu_seq,
                        self._tick_ms(),
                        event_seq=ve.event_seq,
                    )
                    await self._transport.send(ack_frame)
                    self._health.event_acks_sent += 1

                    if self._voice_event_listener:
                        try:
                            await self._voice_event_listener(ve)
                        except Exception:
                            self._logger.exception("Voice event listener error")

    async def _hello_handshake_loop(self) -> None:
        """Retry sending HELLO until HELLO_ACK received."""
        while self._running and self._health.link_state in (LinkState.DOWN, LinkState.SYNC):
            frame = codec.encode_hello(
                seq=self._next_mpu_seq(),
                ack=self._last_mcu_seq,
                tick_ms=self._tick_ms(),
                session_id=self._session_id,
            )
            self._health.hello_sent_at = self._clock()
            self._health.link_state = LinkState.SYNC
            await self._transport.send(frame)
            await asyncio.sleep(self._hello_retry_interval_s)

    async def _heartbeat_loop(self) -> None:
        """Transmit HEARTBEAT frame at 100 ms intervals while link is not LOST."""
        while self._running:
            await asyncio.sleep(self._heartbeat_interval_s)
            if self._health.link_state == LinkState.LOST:
                continue

            frame = codec.encode_heartbeat(
                seq=self._next_mpu_seq(),
                ack=self._last_mcu_seq,
                tick_ms=self._tick_ms(),
                mpu_state=self._mpu_state_for_heartbeat,
            )
            try:
                await self._transport.send(frame)
            except Exception:
                self._logger.debug("Failed to send heartbeat frame", exc_info=True)

    async def _health_monitor_loop(self) -> None:
        """Monitor telemetry age and transition UP -> STALE -> LOST."""
        while self._running:
            await asyncio.sleep(0.05)
            if self._last_frame_rx_at is None:
                continue

            age_ms = (self._clock() - self._last_frame_rx_at) * 1000.0
            prev_state = self._health.link_state

            if age_ms >= self._lost_threshold_ms:
                if prev_state != LinkState.LOST:
                    self._health.link_state = LinkState.LOST
                    self._logger.warning("MCU link LOST (no frames for %.1f ms)", age_ms)
                    # Send best-effort HALT on link loss
                    try:
                        halt_frame = codec.encode_halt(
                            self._next_mpu_seq(),
                            self._last_mcu_seq,
                            self._tick_ms(),
                            cmd_id=self._next_cmd_id(),
                            ttl_ms=200,
                        )
                        await self._transport.send(halt_frame)
                    except Exception:
                        pass
                    if self._link_lost_callback:
                        try:
                            await self._link_lost_callback()
                        except Exception:
                            self._logger.exception("Link lost callback error")

            elif age_ms >= self._stale_threshold_ms:
                if prev_state == LinkState.UP:
                    self._health.link_state = LinkState.STALE
                    self._logger.warning("MCU link STALE (no frames for %.1f ms)", age_ms)

            else:
                if prev_state in (LinkState.STALE, LinkState.DOWN, LinkState.SYNC):
                    self._health.link_state = LinkState.UP

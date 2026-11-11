"""Multi-protocol event router.

Ingress protocols in this slice:
  * ZeroMQ PUB/SUB for CV frames and sensor snapshots (router SUB connects).
  * ZeroMQ PUSH/PULL for STT intents and UI commands (router PULL binds).
  * An in-process ``handle_event`` method for the future FastAPI REST/WebSocket
    gateway and for tests.

The router is the only place where external JSON becomes a canonical
``EventEnvelope``.  Downstream modules never see raw dictionaries.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, NamedTuple

import zmq
import zmq.asyncio
from pydantic import BaseModel, ValidationError

try:  # POSIX-only; the target is a Linux edge device.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX development host
    fcntl = None  # type: ignore[assignment]


from shared.constants.zmq_topics import (
    CV_SUBSCRIPTION_PREFIX,
    SENSOR_SUBSCRIPTION_PREFIX,
    TOPIC_CV_FRAME,
    TOPIC_SENSOR_SNAPSHOT,
    TOPIC_STT_INTENT,
    TOPIC_UI_COMMAND,
    RouterEndpoints,
)
from shared.contracts import EventSink
from shared.schemas.events import (
    CVFramePayload,
    CVFrameWire,
    EventEnvelope,
    EventPayload,
    EventSource,
    IntentPayload,
    IntentWire,
    SensorSnapshotPayload,
    SensorSnapshotWire,
    UICommandPayload,
    UICommandWire,
)
from shared.schemas.telemetry import RouterStats


class _WireSpec(NamedTuple):
    model: type[BaseModel]
    source: EventSource
    payload_factory: Callable[[Any], EventPayload]
    event_time: Callable[[Any], float | None]


def _cv_payload(wire: CVFrameWire) -> CVFramePayload:
    return CVFramePayload(
        camera_id=wire.camera_id,
        detected_objects=wire.detected_objects,
        confidence=wire.confidence,
        jam_suspected=wire.jam_suspected,
    )


def _intent_payload(wire: IntentWire) -> IntentPayload:
    return IntentPayload(
        transcript=wire.transcript,
        intent=wire.intent,
        confidence=wire.confidence,
    )


def _sensor_payload(wire: SensorSnapshotWire) -> SensorSnapshotPayload:
    return SensorSnapshotPayload(
        motor_current_amp=wire.motor_current_amp,
        infrared_temp_celsius=wire.infrared_temp_celsius,
        conveyor_speed_mms=wire.conveyor_speed_mms,
        optical_curtain_clear=wire.optical_curtain_clear,
        estop_circuit_closed=wire.estop_circuit_closed,
        sensor_timestamp_ms=wire.timestamp_ms,
    )


def _ui_payload(wire: UICommandWire) -> UICommandPayload:
    return UICommandPayload(action=wire.action, parameters=wire.parameters)


def _timestamp_or_none(wire: Any) -> float | None:
    timestamp = getattr(wire, "timestamp", None)
    return timestamp if isinstance(timestamp, (int, float)) else None


def _sensor_timestamp_or_none(wire: SensorSnapshotWire) -> float | None:
    return None if wire.timestamp_ms is None else wire.timestamp_ms / 1000.0


_WIRE_SPECS: dict[str, _WireSpec] = {
    TOPIC_CV_FRAME: _WireSpec(CVFrameWire, EventSource.CV, _cv_payload, _timestamp_or_none),
    TOPIC_STT_INTENT: _WireSpec(IntentWire, EventSource.STT, _intent_payload, _timestamp_or_none),
    TOPIC_SENSOR_SNAPSHOT: _WireSpec(
        SensorSnapshotWire, EventSource.SENSOR, _sensor_payload, _sensor_timestamp_or_none
    ),
    TOPIC_UI_COMMAND: _WireSpec(UICommandWire, EventSource.UI, _ui_payload, _timestamp_or_none),
}


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant {value!r}")


class EventRouter:
    """Normalizes external events and fans them out to sinks and handlers."""

    def __init__(
        self,
        sink: EventSink,
        *,
        endpoints: RouterEndpoints | None = None,
        max_message_bytes: int = 1_048_576,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive")
        self._sink = sink
        self._endpoints = endpoints
        self._max_message_bytes = max_message_bytes
        self._clock = clock
        self._logger = logger or logging.getLogger(__name__)

        self._context: zmq.asyncio.Context | None = None
        self._sockets: list[zmq.asyncio.Socket] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        self._lock_fd: int | None = None

        self._handlers: dict[str, list[Callable[[EventEnvelope], Awaitable[None]]]] = {}
        self._sequences: dict[EventSource, int] = {}

        self._received = 0
        self._emitted = 0
        self._invalid = 0
        self._oversized = 0
        self._unknown_topic = 0
        self._handler_errors = 0
        self._socket_errors = 0
        self._sink_errors = 0
        self._last_event_at: float | None = None
        self._invalid_log_start = 0.0
        self._invalid_log_count = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return

        endpoints = self._endpoints or RouterEndpoints.default()
        self._endpoints = endpoints
        self._prepare_ipc_directories(endpoints)
        self._acquire_instance_lock(endpoints)

        sockets: list[zmq.asyncio.Socket] = []
        try:
            context = zmq.asyncio.Context()
            self._context = context
            cv_socket = self._sub_socket(endpoints.cv_sub, CV_SUBSCRIPTION_PREFIX)
            sockets.append(cv_socket)
            sensor_socket = self._sub_socket(endpoints.sensor_sub, SENSOR_SUBSCRIPTION_PREFIX)
            sockets.append(sensor_socket)
            stt_socket = self._pull_socket(endpoints.stt_pull)
            sockets.append(stt_socket)
            ui_socket = self._pull_socket(endpoints.ui_pull)
            sockets.append(ui_socket)
            self._sockets = sockets

            self._running = True
            self._tasks = [
                asyncio.create_task(self._listen_sub(cv_socket, "cv"), name="eaa-router-cv"),
                asyncio.create_task(
                    self._listen_sub(sensor_socket, "sensor"), name="eaa-router-sensor"
                ),
                asyncio.create_task(
                    self._listen_pull(stt_socket, TOPIC_STT_INTENT), name="eaa-router-stt"
                ),
                asyncio.create_task(
                    self._listen_pull(ui_socket, TOPIC_UI_COMMAND), name="eaa-router-ui"
                ),
            ]
        except BaseException:
            self._running = False
            await self._close_sockets()
            raise

        self._logger.info(
            "EventRouter started: cv=%s sensor=%s stt=%s ui=%s",
            endpoints.cv_sub,
            endpoints.sensor_sub,
            endpoints.stt_pull,
            endpoints.ui_pull,
        )

    async def stop(self) -> None:
        if (
            not self._running
            and self._context is None
            and not self._tasks
            and self._lock_fd is None
        ):
            return

        self._running = False
        tasks = self._tasks
        self._tasks = []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._close_sockets()
        self._logger.info("EventRouter stopped")

    async def handle_event(
        self, topic: str, raw: Mapping[str, Any] | bytes | str
    ) -> EventEnvelope | None:
        """Normalize and fan out one external event.

        This is the adapter seam for the future REST/WebSocket gateway.  Socket
        listeners use the same path, so normalization and error handling are
        identical across protocols.
        """
        self._received += 1

        if isinstance(raw, (bytes, bytearray, memoryview)):
            if len(raw) > self._max_message_bytes:
                self._oversized += 1
                self._logger.warning("Oversized message on topic %s dropped", topic)
                return None
        elif isinstance(raw, str):
            if len(raw.encode("utf-8")) > self._max_message_bytes:
                self._oversized += 1
                self._logger.warning("Oversized message on topic %s dropped", topic)
                return None

        envelope = self._normalize(topic, raw)
        if envelope is None:
            return None

        try:
            await self._sink.push(envelope)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._sink_errors += 1
            self._logger.exception("Event sink failed for event %s", envelope.event_id)
            return None

        self._emitted += 1
        self._last_event_at = envelope.ingested_at
        await self._dispatch_handlers(envelope)
        return envelope

    def register_handler(
        self, topic_prefix: str, handler: Callable[[EventEnvelope], Awaitable[None]]
    ) -> None:
        """Register a downstream consumer for topics matching ``topic_prefix``."""
        if not isinstance(topic_prefix, str):
            raise TypeError("topic_prefix must be a string")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._handlers.setdefault(topic_prefix, []).append(handler)

    def stats(self) -> RouterStats:
        return RouterStats(
            received=self._received,
            emitted=self._emitted,
            invalid=self._invalid,
            oversized=self._oversized,
            unknown_topic=self._unknown_topic,
            handler_errors=self._handler_errors,
            socket_errors=self._socket_errors,
            sink_errors=self._sink_errors,
            last_event_at=self._last_event_at,
        )

    # ------------------------------------------------------------------
    # Socket setup
    # ------------------------------------------------------------------

    def _prepare_ipc_directories(self, endpoints: RouterEndpoints) -> None:
        for endpoint in endpoints.bind_endpoints + endpoints.connect_endpoints:
            if endpoint.startswith("ipc://"):
                path = Path(endpoint[len("ipc://") :])
                path.parent.mkdir(parents=True, exist_ok=True)

    def _acquire_instance_lock(self, endpoints: RouterEndpoints) -> None:
        """Prevent two routers from sharing the same IPC directory.

        libzmq silently unlinks an existing IPC path on ``bind``, so relying on
        ``EADDRINUSE`` is not enough.  A POSIX advisory lock gives one clear
        owner of the router endpoints.
        """
        if fcntl is None:
            return

        lock_path: Path | None = None
        for endpoint in endpoints.bind_endpoints:
            if endpoint.startswith("ipc://"):
                lock_path = Path(endpoint[len("ipc://") :]).parent / ".router.lock"
                break
        if lock_path is None:
            return

        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise RuntimeError(
                f"Another EAA router is already running for {lock_path}. "
                "Stop it, or wait for the lock to be released."
            ) from exc

        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        self._lock_fd = fd

    def _release_instance_lock(self) -> None:
        fd = self._lock_fd
        self._lock_fd = None
        if fd is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:  # pragma: no cover - close still releases the lock
            self._logger.debug("Could not explicitly unlock router lock", exc_info=True)
        finally:
            os.close(fd)

    def _base_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        assert self._context is not None
        socket = self._context.socket(socket_type)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVHWM, 1000)
        socket.setsockopt(zmq.MAXMSGSIZE, self._max_message_bytes)
        return socket

    def _sub_socket(self, endpoint: str, topic_prefix: str) -> zmq.asyncio.Socket:
        socket = self._base_socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, topic_prefix)
        socket.connect(endpoint)
        return socket

    def _pull_socket(self, endpoint: str) -> zmq.asyncio.Socket:
        socket = self._base_socket(zmq.PULL)
        try:
            socket.bind(endpoint)
        except zmq.ZMQError as exc:
            socket.close(linger=0)
            raise RuntimeError(
                f"Unable to bind router endpoint {endpoint!r}: {exc}. "
                "If a previous router crashed, remove the stale IPC file and retry."
            ) from exc
        return socket

    async def _close_sockets(self) -> None:
        for socket in self._sockets:
            try:
                socket.close(linger=0)
            except Exception:
                self._logger.debug("Error closing router socket", exc_info=True)
        self._sockets = []

        context = self._context
        self._context = None
        if context is not None:
            try:
                context.destroy(linger=0)
            except Exception:
                self._logger.debug("Error destroying router context", exc_info=True)
        self._release_instance_lock()

    # ------------------------------------------------------------------
    # Listener loops
    # ------------------------------------------------------------------

    async def _listen_sub(self, socket: zmq.asyncio.Socket, label: str) -> None:
        await self._listen(socket, label, default_topic=None)

    async def _listen_pull(self, socket: zmq.asyncio.Socket, default_topic: str) -> None:
        await self._listen(socket, label=default_topic, default_topic=default_topic)

    async def _listen(
        self,
        socket: zmq.asyncio.Socket,
        label: str,
        *,
        default_topic: str | None,
    ) -> None:
        backoff = 0.1
        while self._running:
            try:
                parts = await socket.recv_multipart()
            except asyncio.CancelledError:
                raise
            except zmq.ContextTerminated:
                return
            except zmq.ZMQError as exc:
                if not self._running:
                    return
                self._socket_errors += 1
                self._logger.warning(
                    "ZeroMQ receive error on %s (%s); retrying in %.2fs",
                    label,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue

            backoff = 0.1
            try:
                await self._process_parts(parts, default_topic)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A single malformed message must never kill the listener.
                self._invalid += 1
                self._logger.exception("Unhandled error while processing a router message")

    async def _process_parts(self, parts: list[bytes], default_topic: str | None) -> None:
        if not parts:
            self._received += 1
            self._invalid += 1
            self._logger.warning("Empty ZeroMQ message dropped")
            return

        total_bytes = sum(len(part) for part in parts)
        if total_bytes > self._max_message_bytes:
            self._received += 1
            self._oversized += 1
            self._logger.warning("Oversized ZeroMQ message (%d bytes) dropped", total_bytes)
            return

        if len(parts) == 1:
            if default_topic is None:
                self._received += 1
                self._unknown_topic += 1
                self._logger.warning("SUB message had no topic frame; cannot route it")
                return
            topic = default_topic
            payload = parts[0]
        else:
            try:
                topic = parts[0].decode("utf-8").strip()
            except UnicodeDecodeError:
                self._received += 1
                self._invalid += 1
                self._logger.warning("Non-UTF-8 topic frame dropped")
                return
            if not topic:
                self._received += 1
                self._invalid += 1
                self._logger.warning("Empty topic frame dropped")
                return
            payload = b"".join(parts[1:])

        await self.handle_event(topic, payload)

    # ------------------------------------------------------------------
    # Normalization and fan-out
    # ------------------------------------------------------------------

    def _normalize(self, topic: str, raw: Mapping[str, Any] | bytes | str) -> EventEnvelope | None:
        spec = _WIRE_SPECS.get(topic)
        if spec is None:
            self._unknown_topic += 1
            self._logger.debug("Unknown topic %r dropped", topic)
            return None

        try:
            data = self._decode(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            self._record_invalid(topic, exc)
            return None

        try:
            wire = spec.model.model_validate(data)
        except ValidationError as exc:
            self._record_invalid(topic, exc)
            return None

        now = self._clock()
        try:
            source_time = spec.event_time(wire)
        except Exception as exc:
            self._record_invalid(topic, exc)
            return None
        event_time = now if source_time is None else source_time
        if not math.isfinite(event_time) or event_time <= 0:
            self._record_invalid(topic, ValueError(f"invalid event_time {event_time!r}"))
            return None

        try:
            payload = spec.payload_factory(wire)
            envelope = EventEnvelope.create(
                payload=payload,
                source=spec.source,
                topic=topic,
                sequence=self._next_sequence(spec.source),
                event_time=event_time,
                ingested_at=now,
            )
        except (ValidationError, TypeError, ValueError, AttributeError) as exc:
            self._record_invalid(topic, exc)
            return None

        return envelope

    @staticmethod
    def _decode(raw: Mapping[str, Any] | bytes | str) -> dict[str, Any]:
        if isinstance(raw, Mapping):
            return dict(raw)
        if isinstance(raw, (bytes, bytearray, memoryview)):
            text = bytes(raw).decode("utf-8")
        elif isinstance(raw, str):
            text = raw
        else:
            raise TypeError(f"unsupported raw payload type {type(raw).__name__}")

        data = json.loads(text, parse_constant=_reject_json_constant)
        if not isinstance(data, dict):
            raise ValueError("event payload must be a JSON object")
        return data

    def _next_sequence(self, source: EventSource) -> int:
        sequence = self._sequences.get(source, -1) + 1
        self._sequences[source] = sequence
        return sequence

    async def _dispatch_handlers(self, envelope: EventEnvelope) -> None:
        for prefix, handlers in self._handlers.items():
            if not envelope.topic.startswith(prefix):
                continue
            for handler in handlers:
                try:
                    await handler(envelope)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._handler_errors += 1
                    self._logger.exception(
                        "Router handler %r failed for event %s",
                        handler,
                        envelope.event_id,
                    )

    def _record_invalid(self, topic: str, exc: BaseException) -> None:
        self._invalid += 1
        now = self._clock()
        if now - self._invalid_log_start >= 60.0:
            self._invalid_log_start = now
            self._invalid_log_count = 0
        self._invalid_log_count += 1
        if self._invalid_log_count <= 10:
            self._logger.warning("Rejected %s event: %s", topic, exc)
        elif self._invalid_log_count == 11:
            self._logger.warning("Further invalid-event warnings suppressed for 60 seconds")


__all__ = ["EventRouter", "RouterStats"]

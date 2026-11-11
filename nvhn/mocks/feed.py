"""Mock external producers for local development and smoke tests.

CV and sensor are PUB sockets (the router SUBSCRIBEs).  STT and UI are PUSH
sockets (the router PULL-binds).
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import zmq

from shared.constants.zmq_topics import (
    TOPIC_CV_FRAME,
    TOPIC_SENSOR_SNAPSHOT,
    TOPIC_STT_INTENT,
    TOPIC_UI_COMMAND,
    RouterEndpoints,
    default_ipc_dir,
)

LOGGER = logging.getLogger("eaa.mock-feed")

_TRANSCRIPTS = (
    "clear the jam and resume",
    "pause the conveyor",
    "what is the current speed",
    "show me the last inspection",
    "resume the line",
)
_INTENTS = ("clear_jam", "pause", "status", "inspection", "resume")
_ACTIONS = ("pause", "resume", "status", "clear_jam")


@dataclass(frozen=True, slots=True)
class FeedConfig:
    ipc_dir: Path
    duration: float = 0.0
    cv_rate: float = 10.0
    sensor_rate: float = 5.0
    stt_interval: float = 2.0
    ui_interval: float = 5.0
    invalid_every: int = 50
    seed: int = 7
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        for name in ("cv_rate", "sensor_rate", "stt_interval", "ui_interval"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.duration < 0:
            raise ValueError("duration cannot be negative")
        if self.invalid_every < 0:
            raise ValueError("invalid_every cannot be negative")


def cv_json(rng: random.Random, *, timestamp: float) -> bytes:
    jam = rng.random() < 0.08
    payload = {
        "timestamp": timestamp,
        "camera_id": "cam0",
        "detected_objects": ["carton", "jam"] if jam else ["carton"],
        "confidence": round(rng.uniform(0.72, 0.99), 3),
        "jam_suspected": jam,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def sensor_json(rng: random.Random, *, timestamp: float) -> bytes:
    payload = {
        "timestamp_ms": int(timestamp * 1000),
        "motor_current_amp": round(rng.uniform(1.1, 2.65), 3),
        "infrared_temp_celsius": round(rng.uniform(34.0, 58.0), 2),
        "conveyor_speed_mms": round(rng.uniform(0.0, 480.0), 1),
        "optical_curtain_clear": rng.random() > 0.03,
        "estop_circuit_closed": rng.random() > 0.01,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def stt_json(rng: random.Random, *, timestamp: float) -> bytes:
    payload = {
        "timestamp": timestamp,
        "transcript": rng.choice(_TRANSCRIPTS),
        "intent": rng.choice(_INTENTS),
        "confidence": round(rng.uniform(0.68, 0.98), 3),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def ui_json(rng: random.Random) -> bytes:
    action = rng.choice(_ACTIONS)
    payload = {"action": action, "parameters": {"source": "mock-ui"}}
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class MockFeed:
    """Single-threaded periodic mock producer."""

    def __init__(self, config: FeedConfig) -> None:
        self._config = config
        self._rng = random.Random(config.seed)
        self._stop = threading.Event()
        self._sent = 0

    def request_stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        config = self._config
        config.ipc_dir.mkdir(parents=True, exist_ok=True)
        endpoints = RouterEndpoints.default(config.ipc_dir)
        context = zmq.Context()
        cv_socket = context.socket(zmq.PUB)
        sensor_socket = context.socket(zmq.PUB)
        stt_socket = context.socket(zmq.PUSH)
        ui_socket = context.socket(zmq.PUSH)

        for socket in (cv_socket, sensor_socket, stt_socket, ui_socket):
            socket.setsockopt(zmq.LINGER, 0)

        try:
            cv_socket.bind(endpoints.cv_sub)
            sensor_socket.bind(endpoints.sensor_sub)
            stt_socket.connect(endpoints.stt_pull)
            ui_socket.connect(endpoints.ui_pull)

            LOGGER.info(
                "Mock feed started: cv=%s sensor=%s stt=%s ui=%s",
                endpoints.cv_sub,
                endpoints.sensor_sub,
                endpoints.stt_pull,
                endpoints.ui_pull,
            )
            # Give PUB/SUB subscriptions a moment to propagate.  Early messages
            # may still be dropped by ZeroMQ; that is normal PUB/SUB behavior.
            time.sleep(0.3)

            started = time.monotonic()
            next_cv = started
            next_sensor = started
            next_stt = started
            next_ui = started

            while not self._stop.is_set():
                now = time.monotonic()
                if config.duration > 0 and now - started >= config.duration:
                    break

                if now >= next_cv:
                    self._send(cv_socket, TOPIC_CV_FRAME, "cv")
                    next_cv = now + 1.0 / config.cv_rate
                if now >= next_sensor:
                    self._send(sensor_socket, TOPIC_SENSOR_SNAPSHOT, "sensor")
                    next_sensor = now + 1.0 / config.sensor_rate
                if now >= next_stt:
                    self._send(stt_socket, TOPIC_STT_INTENT, "stt")
                    next_stt = now + config.stt_interval
                if now >= next_ui:
                    self._send(ui_socket, TOPIC_UI_COMMAND, "ui")
                    next_ui = now + config.ui_interval

                self._stop.wait(0.02)
        finally:
            for socket in (cv_socket, sensor_socket, stt_socket, ui_socket):
                socket.close(linger=0)
            context.term()
            LOGGER.info("Mock feed stopped after %d messages", self._sent)

    def _send(self, socket: zmq.Socket, topic: str, kind: str) -> None:
        self._sent += 1
        if self._config.invalid_every and self._sent % self._config.invalid_every == 0:
            self._send_invalid(socket, topic)
            return

        timestamp = time.time()
        if kind == "cv":
            payload = cv_json(self._rng, timestamp=timestamp)
        elif kind == "sensor":
            payload = sensor_json(self._rng, timestamp=timestamp)
        elif kind == "stt":
            payload = stt_json(self._rng, timestamp=timestamp)
        else:
            payload = ui_json(self._rng)
        socket.send_multipart([topic.encode("utf-8"), payload])

    @staticmethod
    def _send_invalid(socket: zmq.Socket, topic: str) -> None:
        LOGGER.debug("Injecting malformed %s event", topic)
        socket.send_multipart([topic.encode("utf-8"), b"{this is not valid json"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eaa-mock-feed",
        description="Publish mock CV, sensor, STT and UI events to the EAA router.",
    )
    parser.add_argument("--ipc-dir", help="ZeroMQ IPC directory (default: /tmp/eaa)")
    parser.add_argument("--duration", type=float, default=0.0, help="Stop after N seconds")
    parser.add_argument("--cv-rate", type=float, default=10.0, help="CV frames per second")
    parser.add_argument(
        "--sensor-rate",
        type=float,
        default=5.0,
        help="Sensor snapshots per second",
    )
    parser.add_argument(
        "--stt-interval",
        type=float,
        default=2.0,
        help="Seconds between STT events",
    )
    parser.add_argument(
        "--ui-interval",
        type=float,
        default=5.0,
        help="Seconds between UI commands",
    )
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=50,
        help="Inject malformed JSON every N messages (0 = never)",
    )
    parser.add_argument("--seed", type=int, default=7, help="Random seed")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return parser


def cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    ipc_dir = Path(args.ipc_dir).expanduser() if args.ipc_dir else default_ipc_dir()
    config = FeedConfig(
        ipc_dir=ipc_dir,
        duration=args.duration,
        cv_rate=args.cv_rate,
        sensor_rate=args.sensor_rate,
        stt_interval=args.stt_interval,
        ui_interval=args.ui_interval,
        invalid_every=args.invalid_every,
        seed=args.seed,
    )
    feed = MockFeed(config)

    def _stop(_signum: int, _frame: object) -> None:
        feed.request_stop()

    previous_handlers: dict[int, object] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, _stop)

    try:
        feed.run()
    except KeyboardInterrupt:
        return 130
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
    return 0


if __name__ == "__main__":
    sys.exit(cli())

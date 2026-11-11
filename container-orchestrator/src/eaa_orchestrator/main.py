"""Runnable wiring for the router -> ring buffer -> persistence slice."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from dataclasses import replace
from pathlib import Path

from eaa_orchestrator.persistence.database import Database
from eaa_orchestrator.persistence.repository import (
    HalfSyncHalfAsyncPersistenceWorker,
    TelemetryRepository,
)
from eaa_orchestrator.router.event_router import EventRouter
from eaa_orchestrator.telemetry.ring_buffer import TelemetryRingBuffer
from shared.config import Settings
from shared.constants.zmq_topics import RouterEndpoints
from shared.schemas.events import EventType

LOGGER = logging.getLogger("eaa.main")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _install_signal_handlers(stop_event: asyncio.Event) -> list[signal.Signals]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Not on the main thread (for example an embedded test runner).
            continue
        installed.append(sig)
    return installed


def _remove_signal_handlers(installed: list[signal.Signals]) -> None:
    loop = asyncio.get_running_loop()
    for sig in installed:
        try:
            loop.remove_signal_handler(sig)
        except (NotImplementedError, RuntimeError):
            pass


async def _log_stats_loop(
    router: EventRouter,
    buffer: TelemetryRingBuffer,
    worker: HalfSyncHalfAsyncPersistenceWorker,
    interval: float,
) -> None:
    while True:
        await asyncio.sleep(interval)
        LOGGER.info(
            "router=%s ring=%s persistence=%s",
            router.stats(),
            await buffer.stats(),
            worker.stats(),
        )


async def run(
    settings: Settings,
    *,
    duration: float = 0.0,
    stats_interval: float = 10.0,
) -> None:
    """Run the three modules until ``duration`` elapses or a signal arrives."""
    database = Database(settings.db_path)
    repository = TelemetryRepository(database)
    worker: HalfSyncHalfAsyncPersistenceWorker | None = None
    buffer: TelemetryRingBuffer | None = None
    router: EventRouter | None = None
    stats_task: asyncio.Task[None] | None = None
    installed_signals: list[signal.Signals] = []

    try:
        await repository.initialize()
        worker = HalfSyncHalfAsyncPersistenceWorker(
            repository,
            queue_capacity=settings.persistence_queue_capacity,
            batch_size=settings.batch_size,
            flush_interval=settings.flush_interval,
            max_retries=settings.max_retries,
            persist_event_types=settings.persist_event_types,
            retention=settings.retention,
        )
        await worker.start()

        buffer = TelemetryRingBuffer(settings.ring_capacity, persistence=worker)
        router = EventRouter(
            buffer,
            endpoints=RouterEndpoints.default(settings.ipc_dir),
            max_message_bytes=settings.max_message_bytes,
        )
        await router.start()

        LOGGER.info(
            "EAA backend running: db=%s ipc=%s ring=%d persist=%s",
            settings.db_path,
            settings.ipc_dir,
            settings.ring_capacity,
            sorted(event.value for event in settings.persist_event_types),
        )

        stop_event = asyncio.Event()
        installed_signals = _install_signal_handlers(stop_event)
        if stats_interval > 0:
            stats_task = asyncio.create_task(
                _log_stats_loop(router, buffer, worker, stats_interval),
                name="eaa-stats",
            )

        if duration > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=duration)
            except TimeoutError:
                LOGGER.info("Run duration %.1fs elapsed; shutting down", duration)
        else:
            await stop_event.wait()
        LOGGER.info("Shutdown requested")
    finally:
        if stats_task is not None:
            stats_task.cancel()
            await asyncio.gather(stats_task, return_exceptions=True)
        if router is not None:
            await router.stop()
        if worker is not None:
            await worker.stop()
        await repository.close()
        _remove_signal_handlers(installed_signals)

        if router is not None and buffer is not None and worker is not None:
            LOGGER.info(
                "Final stats: router=%s ring=%s persistence=%s",
                router.stats(),
                await buffer.stats(),
                worker.stats(),
            )


def _build_settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env()
    updates: dict[str, object] = {}
    if args.db_path:
        updates["db_path"] = Path(args.db_path).expanduser()
    if args.ipc_dir:
        updates["ipc_dir"] = Path(args.ipc_dir).expanduser()
    if args.log_level:
        updates["log_level"] = args.log_level.upper()
    if getattr(args, "persist_cv", False):
        updates["persist_event_types"] = frozenset(
            set(settings.persist_event_types) | {EventType.CV_FRAME}
        )
    return replace(settings, **updates)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eaa-orchestrator",
        description="Run the EAA router, telemetry ring buffer and SQLite persistence.",
    )
    parser.add_argument("--db-path", help="SQLite database path (default: data/edge_system.db)")
    parser.add_argument("--ipc-dir", help="ZeroMQ IPC directory (default: /tmp/eaa)")
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Stop automatically after N seconds (0 = run until SIGINT/SIGTERM)",
    )
    parser.add_argument(
        "--stats-interval",
        type=float,
        default=10.0,
        help="Seconds between stats log lines (0 = disable)",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING, ERROR")
    parser.add_argument(
        "--persist-cv",
        action="store_true",
        help="Also persist CV frames (default: ring-buffer only)",
    )
    return parser


def cli(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = _build_settings(args)
    _configure_logging(settings.log_level)
    try:
        asyncio.run(run(settings, duration=args.duration, stats_interval=args.stats_interval))
    except KeyboardInterrupt:
        return 130
    except Exception:
        LOGGER.exception("EAA backend terminated with an error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())

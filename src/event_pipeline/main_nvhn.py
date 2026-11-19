"""Runnable wiring for the router -> ring buffer -> persistence slice."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from db_client.database import Database
from db_client.repository import (
    HalfSyncHalfAsyncPersistenceWorker,
    TelemetryRepository,
)
from event_router.event_router import EventRouter
from mock_data.config import Settings
from mock_data.constants.zmq_topics import RouterEndpoints
from mock_data.schemas.events import EventType
from mock_data.schemas.telemetry import PersistenceStats, RingBufferStats, RouterStats
from telemetry_ring.ring_buffer import TelemetryRingBuffer

LOGGER = logging.getLogger("eaa.main")


def _format_event_time(value: float | None) -> str:
    """Render an event timestamp as UTC, or ``n/a`` when no event has arrived."""
    if value is None:
        return "n/a"
    rendered = datetime.fromtimestamp(value, tz=UTC).isoformat(timespec="milliseconds")
    return rendered.replace("+00:00", "Z")


def _format_stats(
    title: str,
    router_stats: RouterStats,
    ring_stats: RingBufferStats,
    persistence_stats: PersistenceStats,
) -> str:
    """Format the three module stat snapshots as one readable, grouped block."""
    return "\n".join(
        (
            title,
            (
                "  router       "
                f"received={router_stats.received}  emitted={router_stats.emitted}  "
                f"invalid={router_stats.invalid}  oversized={router_stats.oversized}  "
                f"unknown_topic={router_stats.unknown_topic}"
            ),
            (
                "               "
                f"handler_errors={router_stats.handler_errors}  "
                f"socket_errors={router_stats.socket_errors}  "
                f"sink_errors={router_stats.sink_errors}  "
                f"last_event_at={_format_event_time(router_stats.last_event_at)}"
            ),
            (
                "  ring         "
                f"size={ring_stats.size}/{ring_stats.capacity}  "
                f"total_pushed={ring_stats.total_pushed}  evicted={ring_stats.evicted}"
            ),
            (
                "               "
                f"persistence_queued={ring_stats.persistence_queued}  "
                f"persistence_filtered={ring_stats.persistence_filtered}"
            ),
            (
                "               "
                f"persistence_dropped={ring_stats.persistence_dropped}  "
                f"persistence_errors={ring_stats.persistence_errors}"
            ),
            (
                "               "
                f"oldest_event_at={_format_event_time(ring_stats.oldest_event_time)}  "
                f"newest_event_at={_format_event_time(ring_stats.newest_event_time)}"
            ),
            (
                "  persistence  "
                f"queue={persistence_stats.queue_size}/{persistence_stats.queue_capacity}  "
                f"submitted={persistence_stats.submitted}  "
                f"queued={persistence_stats.queued}  "
                f"filtered={persistence_stats.filtered}"
            ),
            (
                "               "
                f"dropped_queue_full={persistence_stats.dropped_queue_full}  "
                f"stopped_rejected={persistence_stats.stopped_rejected}  "
                f"batches_flushed={persistence_stats.batches_flushed}"
            ),
            (
                "               "
                f"dropped_low_disk={persistence_stats.dropped_low_disk}  "
                f"rows_inserted={persistence_stats.rows_inserted}  "
                f"failed_batches={persistence_stats.failed_batches}  "
                f"pruned_rows={persistence_stats.pruned_rows}"
            ),
            (
                "               "
                f"last_flush_at={_format_event_time(persistence_stats.last_flush_at)}  "
                f"unflushed_on_shutdown={persistence_stats.unflushed_on_shutdown}  "
                f"running={str(persistence_stats.running).lower()}"
            ),
        )
    )


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
            "%s",
            _format_stats(
                "Runtime stats",
                router.stats(),
                await buffer.stats(),
                worker.stats(),
            ),
        )


async def run(
    settings: Settings,
    *,
    duration: float = 0.0,
    stats_interval: float = 10.0,
) -> None:
    """Run the three modules until ``duration`` elapses or a signal arrives."""
    database = Database(
        settings.db_path,
        journal_size_limit_bytes=settings.db_journal_size_limit_bytes,
    )
    repository = TelemetryRepository(database)
    worker: HalfSyncHalfAsyncPersistenceWorker | None = None
    buffer: TelemetryRingBuffer | None = None
    router: EventRouter | None = None
    stats_task: asyncio.Task[None] | None = None
    installed_signals: list[signal.Signals] = []

    try:
        await repository.initialize()
        free_bytes = repository.free_bytes()
        if (
            settings.min_free_bytes > 0
            and free_bytes is not None
            and free_bytes < settings.min_free_bytes
        ):
            raise RuntimeError(
                f"Insufficient free space for {settings.db_path}: "
                f"{free_bytes} bytes available, {settings.min_free_bytes} required"
            )

        worker = HalfSyncHalfAsyncPersistenceWorker(
            repository,
            queue_capacity=settings.persistence_queue_capacity,
            batch_size=settings.batch_size,
            flush_interval=settings.flush_interval,
            max_retries=settings.max_retries,
            persist_event_types=settings.persist_event_types,
            retention=settings.retention,
            min_free_bytes=settings.min_free_bytes,
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
                "%s",
                _format_stats(
                    "Final stats",
                    router.stats(),
                    await buffer.stats(),
                    worker.stats(),
                ),
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
    parser.add_argument(
        "--db-path",
        help="SQLite database path (default: src/tests/db_test/edge_system.db)",
    )
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

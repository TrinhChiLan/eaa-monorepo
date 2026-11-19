from __future__ import annotations

from dataclasses import replace

import pytest
from main_nvhn import _format_event_time, _format_stats, run
from mock_data.config import Settings
from mock_data.schemas.telemetry import PersistenceStats, RingBufferStats, RouterStats


def _sample_stats() -> tuple[RouterStats, RingBufferStats, PersistenceStats]:
    return (
        RouterStats(
            received=57,
            emitted=55,
            invalid=2,
            oversized=0,
            unknown_topic=0,
            handler_errors=0,
            socket_errors=0,
            sink_errors=0,
            last_event_at=1_763_000_000.0,
        ),
        RingBufferStats(
            capacity=1000,
            size=55,
            total_pushed=57,
            evicted=0,
            persistence_queued=26,
            persistence_filtered=29,
            persistence_dropped=0,
            persistence_errors=0,
            oldest_event_time=1_762_999_999.5,
            newest_event_time=1_763_000_000.0,
        ),
        PersistenceStats(
            queue_capacity=10_000,
            queue_size=26,
            submitted=57,
            queued=26,
            filtered=29,
            dropped_queue_full=0,
            stopped_rejected=0,
            batches_flushed=1,
            rows_inserted=26,
            failed_batches=0,
            pruned_rows=0,
            unflushed_on_shutdown=0,
            last_flush_at=1_763_000_000.0,
            running=True,
            dropped_low_disk=0,
        ),
    )


def test_format_stats_groups_each_module_on_its_own_lines() -> None:
    output = _format_stats("Runtime stats", *_sample_stats())
    lines = output.splitlines()

    assert lines[0] == "Runtime stats"
    assert lines[1].startswith("  router ")
    assert any(line.startswith("  ring ") for line in lines)
    assert any(line.startswith("  persistence ") for line in lines)
    assert "RouterStats(" not in output
    assert "RingBufferStats(" not in output
    assert "PersistenceStats(" not in output


def test_format_stats_includes_all_counters() -> None:
    output = _format_stats("Final stats", *_sample_stats())

    for expected in (
        "received=57",
        "emitted=55",
        "invalid=2",
        "last_event_at=2025-11-13T02:13:20.000Z",
        "size=55/1000",
        "total_pushed=57",
        "persistence_queued=26",
        "persistence_filtered=29",
        "queue=26/10000",
        "batches_flushed=1",
        "dropped_low_disk=0",
        "rows_inserted=26",
        "unflushed_on_shutdown=0",
        "running=true",
    ):
        assert expected in output


def test_format_event_time_handles_missing_and_present_values() -> None:
    assert _format_event_time(None) == "n/a"
    assert _format_event_time(0.0) == "1970-01-01T00:00:00.000Z"


async def test_run_fails_fast_when_free_space_is_below_threshold(tmp_path) -> None:
    settings = replace(
        Settings.from_env({}),
        ipc_dir=tmp_path / "ipc",
        db_path=tmp_path / "edge.db",
        min_free_bytes=10**18,
    )

    with pytest.raises(RuntimeError, match="Insufficient free space"):
        await run(settings)

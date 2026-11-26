from __future__ import annotations

import json
from pathlib import Path

import pytest
from bench_persistence import (
    TimedRepository,
    _observe_wal,
    _percentile,
    _read_diskstats,
    _read_proc_io,
    _read_proc_status,
    _sizes,
    _State,
    _validate,
    _wal_logical_bytes,
    build_parser,
    run_benchmark,
    summarize,
    write_artifacts,
)
from tests.test_mock_data import make_sensor_event

_PROC_IO_AVAILABLE = Path("/proc/self/io").exists()


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def test_summarize_empty() -> None:
    assert summarize([]) == {
        "n": 0,
        "min": None,
        "mean": None,
        "max": None,
        "p50": None,
        "p95": None,
        "p99": None,
    }


def test_summarize_single_value_has_no_spread() -> None:
    summary = summarize([7.5])
    assert summary == {
        "n": 1,
        "min": 7.5,
        "mean": 7.5,
        "max": 7.5,
        "p50": 7.5,
        "p95": 7.5,
        "p99": 7.5,
    }


def test_percentiles_interpolate_linearly() -> None:
    ordered = [1.0, 2.0, 3.0, 4.0]
    assert _percentile(ordered, 50.0) == pytest.approx(2.5)
    assert _percentile(ordered, 95.0) == pytest.approx(3.85)
    assert _percentile(ordered, 99.0) == pytest.approx(3.97)
    assert _percentile(ordered, 0.0) == 1.0
    assert _percentile(ordered, 100.0) == 4.0


def test_percentiles_are_monotonic() -> None:
    summary = summarize([5, 1, 9, 3, 7, 2, 8])
    assert summary["p50"] <= summary["p95"] <= summary["p99"]
    assert summary["min"] <= summary["p50"]
    assert summary["p99"] <= summary["max"]


def test_percentile_of_empty_is_none() -> None:
    assert _percentile([], 50.0) is None


# ---------------------------------------------------------------------------
# /proc parsers
# ---------------------------------------------------------------------------


def test_read_proc_io_parses_counters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "io"
    target.write_text(
        "rchar: 10\nwchar: 2048\nread_bytes: 0\nwrite_bytes: 4096\n", encoding="utf-8"
    )
    monkeypatch.setattr("bench_persistence._PROC_IO", target)
    counters = _read_proc_io()
    assert counters is not None
    assert counters["wchar"] == 2048
    assert counters["write_bytes"] == 4096


def test_read_proc_io_missing_file_is_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bench_persistence._PROC_IO", tmp_path / "absent")
    assert _read_proc_io() is None


def test_read_proc_io_ignores_malformed_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "io"
    target.write_text("rchar: 10\nbroken line\nwchar: 7\n", encoding="utf-8")
    monkeypatch.setattr("bench_persistence._PROC_IO", target)
    counters = _read_proc_io()
    assert counters == {"rchar": 10, "wchar": 7}


def test_read_proc_status_returns_memory_kb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "status"
    target.write_text("Name:\tx\nVmRSS:\t  1234 kB\nVmHWM:\t  5678 kB\n", encoding="utf-8")
    monkeypatch.setattr("bench_persistence._PROC_STATUS", target)
    status = _read_proc_status()
    assert status == {"VmRSS": 1234, "VmHWM": 5678}


def test_read_diskstats_matches_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "diskstats"
    target.write_text(
        " 179       0 mmcblk0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16\n"
        " 179       1 mmcblk0p1 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("bench_persistence._PROC_DISKSTATS", target)
    # Field 9 is sectors written: major minor name ... writes_merged sectors_written
    assert _read_diskstats(179, 1) == {"sectors_written": 7}
    assert _read_diskstats(179, 99) is None


# ---------------------------------------------------------------------------
# Sizes
# ---------------------------------------------------------------------------


def test_sizes_treats_missing_wal_and_shm_as_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "x.db"
    db_path.write_bytes(b"abc")
    sizes = _sizes(db_path)
    assert sizes == {"db_bytes": 3, "wal_bytes": 0, "shm_bytes": 0}


# ---------------------------------------------------------------------------
# WAL accounting
# ---------------------------------------------------------------------------


def test_observe_wal_accumulates_growth_only() -> None:
    state = _State()
    state.last_wal = 100  # bytes that pre-date the measured window
    _observe_wal(state, 200)
    assert state.wal_appended == 100
    _observe_wal(state, 250)
    assert state.wal_appended == 150
    # A truncating checkpoint adds nothing for the reset itself.
    _observe_wal(state, 0)
    assert state.wal_appended == 150
    _observe_wal(state, 50)
    assert state.wal_appended == 200


def test_wal_logical_is_appended_volume() -> None:
    state = _State()
    state.wal_appended = 4096
    assert _wal_logical_bytes(state) == 4096


# ---------------------------------------------------------------------------
# Instrumented repository
# ---------------------------------------------------------------------------


class _FakeInner:
    def __init__(self, prune_result: int = 2) -> None:
        self.events = []
        self.prune_result = prune_result
        self.prune_kwargs: list[dict] = []

    def free_bytes(self) -> int | None:
        return 1024

    async def bulk_insert(self, events) -> int:
        self.events.extend(events)
        return len(events)

    async def prune(self, **kwargs) -> int:
        self.prune_kwargs.append(kwargs)
        return self.prune_result


async def test_timed_repository_records_flush_and_end_to_end() -> None:
    inner = _FakeInner()
    timed = TimedRepository(inner, sidecar_limit=10)
    event = make_sensor_event(sequence=0)

    timed.note_submitted(event.event_id)
    assert timed.pending == 1
    inserted = await timed.bulk_insert([event])

    assert inserted == 1
    assert inner.events == [event]
    assert len(timed.flush_ms) == 1 and timed.flush_ms[0] >= 0.0
    assert len(timed.end_to_end_ms) == 1 and timed.end_to_end_ms[0] >= 0.0
    assert timed.pending == 0


async def test_timed_repository_counts_evicted_sidecar_entries() -> None:
    timed = TimedRepository(_FakeInner(), sidecar_limit=2)
    for sequence in range(3):
        timed.note_submitted(make_sensor_event(sequence=sequence).event_id)
    assert timed.pending == 2
    assert timed.evicted_unpersisted == 1


async def test_timed_repository_records_prune() -> None:
    inner = _FakeInner(prune_result=3)
    timed = TimedRepository(inner, sidecar_limit=4)
    deleted = await timed.prune(max_rows=10, now=1.0)
    assert deleted == 3
    assert timed.prune_calls == 1
    assert timed.prune_rows == 3
    assert len(timed.prune_ms) == 1
    assert inner.prune_kwargs == [{"max_rows": 10, "now": 1.0}]


def test_timed_repository_delegates_free_bytes() -> None:
    timed = TimedRepository(_FakeInner(), sidecar_limit=1)
    assert timed.free_bytes() == 1024


# ---------------------------------------------------------------------------
# CLI validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["--duration", "0"],
        ["--rate", "-1"],
        ["--batch-size", "0"],
        ["--queue-capacity", "0"],
        ["--flush-interval", "0"],
        ["--checkpoint-bytes", "0"],
    ],
)
def test_invalid_arguments_are_rejected(argv: list[str]) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    with pytest.raises(SystemExit):
        _validate(parser, args)


def test_valid_defaults_pass_validation() -> None:
    parser = build_parser()
    _validate(parser, parser.parse_args([]))


# ---------------------------------------------------------------------------
# Artifacts
# ---------------------------------------------------------------------------


def test_write_artifacts_creates_both_files(tmp_path: Path) -> None:
    summary = {"label": "unit", "io": {"wchar_bytes": 10}}
    timeseries = [{"t_s": 0.5, "queue_size": 1, "unknown": "ignored"}]
    summary_path, series_path = write_artifacts(summary, timeseries, tmp_path / "out")

    assert json.loads(summary_path.read_text(encoding="utf-8")) == summary
    lines = series_path.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("t_s,queue_size,rss_kb")
    assert "unknown" not in lines[0]
    assert lines[1].startswith("0.5,1,")


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _PROC_IO_AVAILABLE, reason="requires /proc/self/io (Linux)")
async def test_benchmark_run_produces_complete_summary() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--label",
            "integration",
            "--duration",
            "0.6",
            "--rate",
            "300",
            "--batch-size",
            "25",
            "--flush-interval",
            "0.2",
            "--sample-interval",
            "0.1",
            "--read-interval",
            "0.1",
            "--checkpoint-mode",
            "auto",
        ]
    )
    _validate(parser, args)
    summary, timeseries = await run_benchmark(args)

    assert summary["label"] == "integration"
    assert summary["interrupted"] is False
    assert summary["throughput"]["rows_inserted"] > 0
    assert summary["throughput"]["submitted"] > 0
    assert summary["io"]["wchar_bytes"] is not None
    assert summary["io"]["bytes_per_row"] is not None
    assert summary["io"]["bytes_per_row"] > 0
    assert summary["io"]["availability"]["proc_io"] == "available"
    assert summary["storage"]["journal_mode"] == "wal"
    assert timeseries and "queue_size" in timeseries[0]

    for series in summary["latency_ms"].values():
        assert series["p50"] is None or series["p50"] <= series["p95"] <= series["p99"]

    # auto mode cannot time SQLite's internal checkpoint.
    assert summary["latency_ms"]["checkpoint"]["n"] == 0

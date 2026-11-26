from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest
from db_client import database as database_module
from db_client.database import Database
from db_client.repository import (
    _SOURCE_EVENT_TYPES,
    HalfSyncHalfAsyncPersistenceWorker,
    TelemetryRepository,
)
from mock_data.contracts import SubmitOutcome
from mock_data.schemas.events import EventSource, EventType
from mock_data.schemas.telemetry import RetentionPolicy
from tests.test_mock_data import (
    FakeRepository,
    epoch,
    make_audit_event,
    make_cv_event,
    make_intent_event,
    make_sensor_event,
    make_ui_event,
    wait_until,
)


async def test_wal_mode_and_health(db_path) -> None:
    database = Database(db_path)
    await database.connect()
    try:
        cursor = await database.connection.execute("PRAGMA journal_mode")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and str(row[0]).lower() == "wal"

        assert await database.health_check() is True
    finally:
        await database.close()
        await database.close()  # idempotent


async def test_wear_pragmas_and_index_set(db_path) -> None:
    database = Database(db_path, journal_size_limit_bytes=123_456)
    await database.connect()
    try:
        cursor = await database.connection.execute("PRAGMA temp_store")
        assert (await cursor.fetchone())[0] == 2
        await cursor.close()
        cursor = await database.connection.execute("PRAGMA secure_delete")
        assert (await cursor.fetchone())[0] == 0
        await cursor.close()
        cursor = await database.connection.execute("PRAGMA auto_vacuum")
        assert (await cursor.fetchone())[0] == 0
        await cursor.close()
        cursor = await database.connection.execute("PRAGMA journal_size_limit")
        assert (await cursor.fetchone())[0] == 123_456
        await cursor.close()

        cursor = await database.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'ix_%'"
        )
        indexes = {str(row[0]) for row in await cursor.fetchall()}
        await cursor.close()
        # source is 1:1 with event_type, so no redundant source index.
        assert indexes == {"ix_events_event_time", "ix_events_type_time"}
    finally:
        await database.close()


async def test_lookups_are_seeded_from_the_enums(repository: TelemetryRepository) -> None:
    connection = repository._database.connection
    cursor = await connection.execute("SELECT event_type FROM event_types ORDER BY event_type")
    stored_types = {str(row[0]) for row in await cursor.fetchall()}
    await cursor.close()
    assert stored_types == {event_type.value for event_type in EventType}

    cursor = await connection.execute("SELECT source FROM event_sources ORDER BY source")
    stored_sources = {str(row[0]) for row in await cursor.fetchall()}
    await cursor.close()
    assert stored_sources == {source.value for source in EventSource}


async def test_unknown_enum_values_are_rejected_by_foreign_keys(db_path) -> None:
    database = Database(db_path)
    await database.connect()
    try:
        with pytest.raises(Exception, match="FOREIGN KEY constraint failed"):
            await database.connection.execute(
                """
                INSERT INTO telemetry_events (
                    event_id, schema_version, event_type, source, topic,
                    sequence, event_time, ingested_at, payload_json
                ) VALUES ('bad-1', 1, 'not.a.type', 'sensor', 't', 0, 1.0, 1.0, '{}')
                """
            )
        with pytest.raises(Exception, match="FOREIGN KEY constraint failed"):
            await database.connection.execute(
                """
                INSERT INTO telemetry_events (
                    event_id, schema_version, event_type, source, topic,
                    sequence, event_time, ingested_at, payload_json
                ) VALUES ('bad-2', 1, 'sensor.snapshot', 'not-a-source', 't', 0, 1.0, 1.0, '{}')
                """
            )
    finally:
        await database.close()


async def test_check_constraints_reject_out_of_contract_rows(db_path) -> None:
    database = Database(db_path)
    await database.connect()
    try:
        base = (
            "INSERT INTO telemetry_events (event_id, schema_version, event_type, source,"
            " topic, sequence, event_time, ingested_at, payload_json)"
            " VALUES (?, ?, 'sensor.snapshot', 'sensor', ?, ?, ?, ?, '{}')"
        )
        for row in (
            ("c1", 1, "", 0, 1.0, 1.0),
            ("c2", 1, "t", -1, 1.0, 1.0),
            ("c3", 1, "t", 0, 0.0, 1.0),
            ("c4", 1, "t", 0, 1.0, -1.0),
            ("c5", 0, "t", 0, 1.0, 1.0),
        ):
            with pytest.raises(Exception, match="CHECK constraint failed"):
                await database.connection.execute(base, row)
    finally:
        await database.close()


async def test_generated_columns_expose_payload_fields(repository: TelemetryRepository) -> None:
    await repository.bulk_insert([make_sensor_event(sequence=0, event_time=epoch(0))])
    connection = repository._database.connection
    cursor = await connection.execute(
        "SELECT motor_current_amp, infrared_temp_celsius, conveyor_speed_mms,"
        " optical_curtain_clear, estop_circuit_closed FROM telemetry_events"
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row is not None
    assert row[0] is not None and row[1] is not None and row[2] is not None
    assert int(row[3]) in (0, 1)
    assert int(row[4]) in (0, 1)

    # A payload without those fields yields NULL rather than an error.
    await repository.bulk_insert([make_ui_event(sequence=1, event_time=epoch(1))])
    cursor = await connection.execute(
        "SELECT motor_current_amp, action FROM telemetry_events ORDER BY id DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row is not None and row[0] is None and row[1] == "pause"


async def test_schema_initialization_is_idempotent(db_path) -> None:
    for _ in range(2):
        database = Database(db_path)
        await database.connect()
        try:
            cursor = await database.connection.execute("SELECT COUNT(*) FROM event_types")
            row = await cursor.fetchone()
            await cursor.close()
            assert row is not None and int(row[0]) == len(EventType)
        finally:
            await database.close()


def test_source_event_type_map_is_a_bijection() -> None:
    """Guards the invariant that removed the (source, event_time) index.

    Every source must map to an event type and no two sources may share one;
    a drift here is what previously made `source=system` audits unreadable.
    """
    assert set(_SOURCE_EVENT_TYPES) == set(EventSource)
    mapped_types = list(_SOURCE_EVENT_TYPES.values())
    assert len(mapped_types) == len(set(mapped_types))
    assert set(mapped_types) == set(EventType)


async def test_bulk_insert_roundtrip(repository: TelemetryRepository) -> None:
    events = [
        make_sensor_event(sequence=0, event_time=epoch(0)),
        make_intent_event(sequence=0, event_time=epoch(1)),
        make_ui_event(sequence=0, event_time=epoch(2)),
    ]
    inserted = await repository.bulk_insert(events)
    assert inserted == 3
    assert await repository.count() == 3

    fetched = await repository.fetch_recent(limit=10)
    assert fetched == events
    assert [event.event_type for event in fetched] == [
        EventType.SENSOR_SNAPSHOT,
        EventType.STT_INTENT,
        EventType.UI_COMMAND,
    ]


async def test_duplicate_event_id_is_ignored(repository: TelemetryRepository) -> None:
    event = make_sensor_event(event_time=epoch(0))
    assert await repository.bulk_insert([event]) == 1
    assert await repository.bulk_insert([event]) == 0
    assert await repository.count() == 1


async def test_empty_batch_insert(repository: TelemetryRepository) -> None:
    assert await repository.bulk_insert([]) == 0
    assert await repository.count() == 0


async def test_fetch_recent_filters_and_ordering(repository: TelemetryRepository) -> None:
    events = [
        make_sensor_event(sequence=0, event_time=epoch(0)),
        make_intent_event(sequence=0, event_time=epoch(1)),
        make_sensor_event(sequence=1, event_time=epoch(2)),
        make_ui_event(sequence=0, event_time=epoch(3)),
        make_audit_event(sequence=0, event_time=epoch(4)),
    ]
    await repository.bulk_insert(events)

    sensors = await repository.fetch_recent(limit=10, event_type=EventType.SENSOR_SNAPSHOT)
    assert [event.sequence for event in sensors] == [0, 1]

    intents = await repository.fetch_recent(limit=10, source=EventSource.STT)
    assert len(intents) == 1
    assert intents[0].event_type is EventType.STT_INTENT

    assert await repository.fetch_recent(limit=10, source=EventSource.CV) == []
    assert (
        await repository.fetch_recent(
            limit=10,
            source=EventSource.STT,
            event_type=EventType.UI_COMMAND,
        )
        == []
    )

    windowed = await repository.fetch_recent(limit=10, since=epoch(1), until=epoch(2))
    assert [event.event_time for event in windowed] == [epoch(1), epoch(2)]

    assert await repository.fetch_recent(limit=0) == []
    assert await repository.fetch_recent(limit=2) == events[-2:]


async def test_system_audit_is_readable_by_source(repository: TelemetryRepository) -> None:
    """Regression: audit rows were written as ui.command/system and were invisible."""
    await repository.bulk_insert(
        [
            make_ui_event(sequence=0, event_time=epoch(0)),
            make_audit_event(sequence=1, event_time=epoch(1)),
        ]
    )

    audit_rows = await repository.fetch_recent(limit=10, source=EventSource.SYSTEM)
    assert len(audit_rows) == 1
    assert audit_rows[0].event_type is EventType.SYSTEM_AUDIT

    ui_rows = await repository.fetch_recent(limit=10, source=EventSource.UI)
    assert [event.source for event in ui_rows] == [EventSource.UI]
    assert all(event.event_type is EventType.UI_COMMAND for event in ui_rows)


async def test_prune_by_max_rows(repository: TelemetryRepository) -> None:
    for index in range(5):
        await repository.bulk_insert([make_sensor_event(sequence=index, event_time=epoch(index))])

    deleted = await repository.prune(max_rows=2)
    assert deleted == 3
    assert await repository.count() == 2
    remaining = await repository.fetch_recent(limit=10)
    assert [event.sequence for event in remaining] == [3, 4]


async def test_prune_keeps_all_rows_when_under_limit(repository: TelemetryRepository) -> None:
    for index in range(3):
        await repository.bulk_insert([make_sensor_event(sequence=index, event_time=epoch(index))])

    assert await repository.prune(max_rows=10) == 0
    assert await repository.count() == 3


async def test_prune_by_age(db_path) -> None:
    database = Database(db_path)
    repository = TelemetryRepository(database)
    await repository.initialize()
    try:
        await repository.bulk_insert(
            [
                make_sensor_event(sequence=10, event_time=epoch(0)),
                make_sensor_event(sequence=11, event_time=epoch(20)),
            ]
        )
        deleted = await repository.prune(max_age_seconds=10, now=epoch(20))
        assert deleted == 1
        remaining = await repository.fetch_recent(limit=10)
        assert [event.sequence for event in remaining] == [11]
    finally:
        await repository.close()


async def test_prune_validation(repository: TelemetryRepository) -> None:
    assert await repository.prune() == 0
    with pytest.raises(ValueError):
        await repository.prune(max_rows=0)
    with pytest.raises(ValueError):
        await repository.prune(max_age_seconds=-1)


async def test_worker_flushes_by_batch_size() -> None:
    fake = FakeRepository()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, batch_size=5, flush_interval=10.0, retry_backoff=0.01
    )
    await worker.start()
    try:
        for index in range(5):
            assert await worker.submit(make_sensor_event(sequence=index)) is SubmitOutcome.QUEUED
        await wait_until(lambda: len(fake.events) == 5)
    finally:
        await worker.stop()

    stats = worker.stats()
    assert stats.batches_flushed >= 1
    assert stats.rows_inserted == 5
    assert stats.running is False


async def test_worker_flushes_partial_batch_on_interval() -> None:
    fake = FakeRepository()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, batch_size=100, flush_interval=0.05, retry_backoff=0.01
    )
    await worker.start()
    try:
        for index in range(3):
            await worker.submit(make_sensor_event(sequence=index))
        await wait_until(lambda: len(fake.events) == 3)
    finally:
        await worker.stop()

    assert worker.stats().batches_flushed >= 1


async def test_worker_retries_transient_failures() -> None:
    fake = FakeRepository(fail_times=2)
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, batch_size=1, flush_interval=10.0, max_retries=3, retry_backoff=0.01
    )
    await worker.start()
    try:
        await worker.submit(make_sensor_event())
        await wait_until(lambda: len(fake.events) == 1)
    finally:
        await worker.stop()

    stats = worker.stats()
    assert fake.calls == 3
    assert stats.failed_batches == 0


async def test_worker_drops_batch_after_retry_exhaustion() -> None:
    fake = FakeRepository(fail_times=99)
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, batch_size=1, flush_interval=10.0, max_retries=1, retry_backoff=0.01
    )
    await worker.start()
    try:
        await worker.submit(make_sensor_event())
        await wait_until(lambda: worker.stats().failed_batches == 1)
    finally:
        await worker.stop()

    assert fake.events == []
    assert worker.stats().rows_inserted == 0


async def test_worker_drops_batch_when_disk_is_low() -> None:
    fake = FakeRepository(free_bytes_value=10)
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake,
        batch_size=1,
        flush_interval=0.05,
        retry_backoff=0.01,
        min_free_bytes=100,
    )
    await worker.start()
    try:
        assert await worker.submit(make_sensor_event()) is SubmitOutcome.QUEUED
        await wait_until(lambda: worker.stats().dropped_low_disk == 1)
    finally:
        await worker.stop()

    assert fake.events == []
    assert worker.stats().rows_inserted == 0


async def test_worker_queue_drops_oldest_under_pressure() -> None:
    fake = FakeRepository()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, queue_capacity=2, batch_size=100, flush_interval=10.0
    )
    await worker.start()
    try:
        for index in range(5):
            await worker.submit(make_sensor_event(sequence=index))
        before = worker.stats()
        assert before.queued == 5
        assert before.dropped_queue_full == 3
        assert before.queue_size == 2
    finally:
        await worker.stop(drain_timeout=1.0)

    assert [event.sequence for event in fake.events] == [3, 4]


async def test_worker_filters_event_types() -> None:
    fake = FakeRepository()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake,
        batch_size=1,
        flush_interval=0.05,
        persist_event_types=frozenset({EventType.SENSOR_SNAPSHOT}),
    )
    await worker.start()
    try:
        assert await worker.submit(make_cv_event()) is SubmitOutcome.FILTERED
        assert await worker.submit(make_sensor_event()) is SubmitOutcome.QUEUED
        await wait_until(lambda: len(fake.events) == 1)
    finally:
        await worker.stop()

    assert [event.event_type for event in fake.events] == [EventType.SENSOR_SNAPSHOT]
    assert worker.stats().filtered == 1


async def test_worker_drains_on_shutdown() -> None:
    fake = FakeRepository()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake, queue_capacity=100, batch_size=10, flush_interval=10.0
    )
    await worker.start()
    for index in range(25):
        await worker.submit(make_sensor_event(sequence=index))
    await worker.stop(drain_timeout=2.0)

    assert len(fake.events) == 25
    assert worker.stats().unflushed_on_shutdown == 0


async def test_worker_rejects_submit_after_stop() -> None:
    worker = HalfSyncHalfAsyncPersistenceWorker(FakeRepository())
    await worker.start()
    await worker.stop(drain_timeout=1.0)
    assert await worker.submit(make_sensor_event()) is SubmitOutcome.STOPPED


async def test_worker_retention_prune_is_called() -> None:
    fake = FakeRepository(prune_result=2)
    worker = HalfSyncHalfAsyncPersistenceWorker(
        fake,
        batch_size=1,
        flush_interval=0.05,
        retention=RetentionPolicy(max_rows=5, prune_interval_seconds=0.01),
    )
    await worker.start()
    try:
        await worker.submit(make_sensor_event())
        await wait_until(lambda: bool(fake.prune_calls))
    finally:
        await worker.stop()

    assert fake.prune_calls[0]["max_rows"] == 5
    assert worker.stats().pruned_rows == 2


async def test_worker_argument_validation() -> None:
    with pytest.raises(ValueError):
        HalfSyncHalfAsyncPersistenceWorker(FakeRepository(), queue_capacity=0)
    with pytest.raises(ValueError):
        HalfSyncHalfAsyncPersistenceWorker(FakeRepository(), batch_size=0)
    with pytest.raises(ValueError):
        HalfSyncHalfAsyncPersistenceWorker(FakeRepository(), flush_interval=0)
    with pytest.raises(ValueError):
        HalfSyncHalfAsyncPersistenceWorker(FakeRepository(), max_retries=-1)
    with pytest.raises(ValueError):
        HalfSyncHalfAsyncPersistenceWorker(FakeRepository(), min_free_bytes=-1)


async def test_real_repository_with_worker(repository: TelemetryRepository) -> None:
    worker = HalfSyncHalfAsyncPersistenceWorker(
        repository, batch_size=5, flush_interval=0.05, retry_backoff=0.01
    )
    await worker.start()
    try:
        for index in range(5):
            await worker.submit(make_sensor_event(sequence=index, event_time=epoch(index)))
        await wait_until(
            lambda: _count_is(repository, 5),
            message="rows were not flushed",
        )
    finally:
        await worker.stop()
    assert await repository.count() == 5


async def _count_is(repository: TelemetryRepository, expected: int) -> bool:
    return await repository.count() == expected


# ---------------------------------------------------------------------------
# Write-cost guard
# ---------------------------------------------------------------------------
#
# The relational schema was chosen only because it is write-neutral: lookup
# tables, foreign keys and VIRTUAL generated columns must not add WAL or
# checkpoint traffic.  This compares the current schema against the previous
# single-table shape in the same process, so filesystem variance cancels out.

_PREVIOUS_SCHEMA_DDL = (
    """
    CREATE TABLE telemetry_events (
        id INTEGER PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        schema_version INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        source TEXT NOT NULL,
        topic TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        event_time REAL NOT NULL,
        ingested_at REAL NOT NULL,
        payload_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX ix_events_event_time ON telemetry_events(event_time)",
    "CREATE INDEX ix_events_type_time ON telemetry_events(event_type, event_time)",
)

_CURRENT_SCHEMA_DDL = database_module._SCHEMA_STATEMENTS + database_module._INDEX_STATEMENTS

_INSERT_COLUMNS = (
    "(event_id, schema_version, event_type, source, topic, sequence,"
    " event_time, ingested_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_SENSOR_PAYLOAD = (
    '{"kind":"sensor.snapshot","motor_current_amp":1.2,"infrared_temp_celsius":41.5,'
    '"conveyor_speed_mms":100.0,"optical_curtain_clear":true,'
    '"estop_circuit_closed":true,"sensor_timestamp_ms":1700000000000}'
)


def _wal_bytes_per_row(
    ddl: tuple[str, ...],
    *,
    rows: int = 20_000,
    batch: int = 50,
    seed_lookups: bool = False,
) -> float:
    """Return (WAL + checkpoint) bytes per row written for one schema."""
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "cost.db")
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        # Disable auto-checkpointing so the WAL is drained deterministically.
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("PRAGMA foreign_keys=ON")
        for statement in ddl:
            connection.execute(statement)
        if seed_lookups:
            connection.executemany(
                "INSERT INTO event_types(event_type) VALUES (?) ON CONFLICT DO NOTHING",
                [(event_type.value,) for event_type in EventType],
            )
            connection.executemany(
                "INSERT INTO event_sources(source) VALUES (?) ON CONFLICT DO NOTHING",
                [(source.value,) for source in EventSource],
            )

        insert_sql = (
            "INSERT INTO telemetry_events "
            + _INSERT_COLUMNS
            + (" ON CONFLICT(event_id) DO NOTHING" if seed_lookups else "")
        )
        cursor = 0
        wal_path = path + "-wal"
        logged = 0
        for _ in range(rows // batch):
            payload_rows = [
                (
                    f"e{cursor + index}",
                    1,
                    "sensor.snapshot",
                    "sensor",
                    "sensor.snapshot",
                    cursor + index,
                    1_700_000_000.0 + (cursor + index) * 0.2,
                    1_700_000_000.0 + (cursor + index) * 0.2,
                    _SENSOR_PAYLOAD,
                )
                for index in range(batch)
            ]
            cursor += batch
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(insert_sql, payload_rows)
            connection.execute("COMMIT")
            wal_bytes = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
            if wal_bytes >= 4 * 1024 * 1024:
                logged += wal_bytes
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if os.path.exists(wal_path):
            logged += os.path.getsize(wal_path)
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    # Checkpointing copies roughly as many pages back to the main database.
    return (2 * logged) / rows


def test_write_cost_is_neutral_versus_previous_schema() -> None:
    previous = _wal_bytes_per_row(_PREVIOUS_SCHEMA_DDL)
    current = _wal_bytes_per_row(_CURRENT_SCHEMA_DDL, seed_lookups=True)
    assert previous > 0
    assert current <= previous * 1.05, (
        f"relational schema writes {current:.0f} B/row versus "
        f"{previous:.0f} B/row for the previous single-table schema"
    )

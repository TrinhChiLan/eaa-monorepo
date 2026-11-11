from __future__ import annotations

import pytest
from eaa_orchestrator.persistence.database import SCHEMA_VERSION, Database
from eaa_orchestrator.persistence.repository import (
    HalfSyncHalfAsyncPersistenceWorker,
    TelemetryRepository,
)

from shared.contracts import SubmitOutcome
from shared.schemas.events import EventSource, EventType
from shared.schemas.telemetry import RetentionPolicy
from tests.support import (
    FakeRepository,
    epoch,
    make_cv_event,
    make_intent_event,
    make_sensor_event,
    make_ui_event,
    wait_until,
)


async def test_wal_mode_schema_version_and_health(db_path) -> None:
    database = Database(db_path)
    await database.connect()
    try:
        cursor = await database.connection.execute("PRAGMA journal_mode")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and str(row[0]).lower() == "wal"

        cursor = await database.connection.execute("PRAGMA user_version")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and int(row[0]) == SCHEMA_VERSION

        assert await database.health_check() is True
    finally:
        await database.close()
        await database.close()  # idempotent


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
    ]
    await repository.bulk_insert(events)

    sensors = await repository.fetch_recent(limit=10, event_type=EventType.SENSOR_SNAPSHOT)
    assert [event.sequence for event in sensors] == [0, 1]

    intents = await repository.fetch_recent(limit=10, source=EventSource.STT)
    assert len(intents) == 1
    assert intents[0].event_type is EventType.STT_INTENT

    windowed = await repository.fetch_recent(limit=10, since=epoch(1), until=epoch(2))
    assert [event.event_time for event in windowed] == [epoch(1), epoch(2)]

    assert await repository.fetch_recent(limit=0) == []
    assert await repository.fetch_recent(limit=2) == events[-2:]


async def test_prune_by_max_rows(repository: TelemetryRepository) -> None:
    for index in range(5):
        await repository.bulk_insert([make_sensor_event(sequence=index, event_time=epoch(index))])

    deleted = await repository.prune(max_rows=2)
    assert deleted == 3
    assert await repository.count() == 2
    remaining = await repository.fetch_recent(limit=10)
    assert [event.sequence for event in remaining] == [3, 4]


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


async def test_newer_schema_version_is_refused(db_path) -> None:
    database = Database(db_path)
    await database.connect()
    await database.connection.execute("PRAGMA user_version=2")
    await database.connection.commit()
    await database.close()

    newer = Database(db_path)
    with pytest.raises(RuntimeError, match="newer than supported"):
        await newer.connect()


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

"""SQLite connection lifecycle, pragmas and schema initialization.

The repository owns query/insert logic; this module owns the database file,
connection settings and the physical schema.  Keeping the two separate keeps
migrations-testable logic out of the repository and matches the low-level design
document.

There is deliberately no schema-version or migration machinery: the system is
still in development and the database file is disposable.  Opening an existing
file created by an older revision will *not* reshape it, so delete the file when
the schema below changes (the development default under ``tests/db_test`` is
gitignored).
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import aiosqlite
from mock_data.schemas.events import EventSource, EventType

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    # Enum lookups.  Tiny, PK-only tables that make the domain values real
    # reference targets instead of Python-only conventions.
    """
    CREATE TABLE IF NOT EXISTS event_types (
        event_type      TEXT PRIMARY KEY
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS event_sources (
        source          TEXT PRIMARY KEY
    ) WITHOUT ROWID
    """,
    # One row per event.  The payload stays JSON (shredding it into per-type
    # fact tables costs ~33% more flash writes for no current reader); the
    # generated columns below expose the fields worth querying at zero storage
    # and zero flash-write cost.  They are VIRTUAL, so nothing is stored, but
    # SQLite still evaluates the expression on every insert: expect ~30% lower
    # insert throughput than a table without them (immaterial at 5 Hz, and they
    # are only paid on read when selected).
    """
    CREATE TABLE IF NOT EXISTS telemetry_events (
        id                          INTEGER PRIMARY KEY,
        event_id                    TEXT NOT NULL UNIQUE,
        schema_version              INTEGER NOT NULL CHECK (schema_version >= 1),
        event_type                  TEXT NOT NULL
                                    REFERENCES event_types(event_type)
                                    ON UPDATE CASCADE ON DELETE RESTRICT,
        source                      TEXT NOT NULL
                                    REFERENCES event_sources(source)
                                    ON UPDATE CASCADE ON DELETE RESTRICT,
        topic                       TEXT NOT NULL CHECK (length(topic) BETWEEN 1 AND 128),
        sequence                    INTEGER NOT NULL CHECK (sequence >= 0),
        event_time                  REAL NOT NULL CHECK (event_time > 0),
        ingested_at                 REAL NOT NULL CHECK (ingested_at > 0),
        payload_json                TEXT NOT NULL,

        confidence                  REAL
            GENERATED ALWAYS AS (json_extract(payload_json, '$.confidence')) VIRTUAL,
        action                      TEXT
            GENERATED ALWAYS AS (json_extract(payload_json, '$.action')) VIRTUAL,
        motor_current_amp           REAL
            GENERATED ALWAYS AS (json_extract(payload_json, '$.motor_current_amp')) VIRTUAL,
        infrared_temp_celsius       REAL
            GENERATED ALWAYS AS (json_extract(payload_json, '$.infrared_temp_celsius')) VIRTUAL,
        conveyor_speed_mms          REAL
            GENERATED ALWAYS AS (json_extract(payload_json, '$.conveyor_speed_mms')) VIRTUAL,
        optical_curtain_clear       INTEGER
            GENERATED ALWAYS AS (json_extract(payload_json, '$.optical_curtain_clear')) VIRTUAL,
        estop_circuit_closed        INTEGER
            GENERATED ALWAYS AS (json_extract(payload_json, '$.estop_circuit_closed')) VIRTUAL
    )
    """,
)

_INDEX_STATEMENTS: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS ix_events_event_time ON telemetry_events(event_time)",
    "CREATE INDEX IF NOT EXISTS ix_events_type_time ON telemetry_events(event_type, event_time)",
)

_SEED_EVENT_TYPES_SQL = "INSERT INTO event_types(event_type) VALUES (?) ON CONFLICT DO NOTHING"
_SEED_EVENT_SOURCES_SQL = "INSERT INTO event_sources(source) VALUES (?) ON CONFLICT DO NOTHING"


class Database:
    """A WAL-mode SQLite database with explicit, idempotent initialization."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 5000,
        journal_size_limit_bytes: int = 4_194_304,
    ) -> None:
        if busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be positive")
        if journal_size_limit_bytes <= 0:
            raise ValueError("journal_size_limit_bytes must be positive")
        self._path = str(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._journal_size_limit_bytes = journal_size_limit_bytes
        self._connection: aiosqlite.Connection | None = None
        self._logger = logging.getLogger(__name__)

    @property
    def path(self) -> str:
        return self._path

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("Database is not connected; call connect() first")
        return self._connection

    def free_bytes(self) -> int | None:
        """Return free bytes on the database volume, or ``None`` when unknowable."""
        if self._path.startswith((":memory:", "file:")):
            return None
        try:
            parent = Path(self._path).expanduser().resolve().parent
            return shutil.disk_usage(parent).free
        except OSError:
            self._logger.debug("Could not determine database free space", exc_info=True)
            return None

    async def connect(self) -> None:
        if self._connection is not None:
            return

        if not self._path.startswith((":memory:", "file:")):
            parent = Path(self._path).expanduser().resolve().parent  # noqa: ASYNC240
            parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if not os.access(parent, os.W_OK):
                raise RuntimeError(f"Database directory is not writable: {parent}")

        connection = await aiosqlite.connect(self._path, isolation_level=None)
        try:
            await self._apply_pragmas(connection)
            await self._initialize_schema(connection)
        except BaseException:
            await connection.close()
            raise
        self._connection = connection
        if not self._path.startswith((":memory:", "file:")):
            try:
                os.chmod(self._path, 0o600)
            except OSError:  # pragma: no cover - unusual filesystem
                self._logger.debug("Could not restrict database file permissions", exc_info=True)

    async def _apply_pragmas(self, connection: aiosqlite.Connection) -> None:
        await connection.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_ms)}")
        await connection.execute("PRAGMA foreign_keys=ON")
        # Edge/wear pragmas: no deleted-content overwrite, no page-moving
        # auto-vacuum, and temp work in memory rather than a temp file.
        await connection.execute("PRAGMA temp_store=MEMORY")
        await connection.execute("PRAGMA secure_delete=OFF")
        await connection.execute("PRAGMA auto_vacuum=NONE")

        cursor = await connection.execute("PRAGMA journal_mode=WAL")
        row = await cursor.fetchone()
        await cursor.close()
        journal_mode = str(row[0]).lower() if row else "unknown"
        if journal_mode != "wal":
            # :memory: databases cannot use WAL; keep running but make it visible.
            self._logger.warning(
                "SQLite journal_mode is %s, not WAL (path=%s)", journal_mode, self._path
            )

        await connection.execute("PRAGMA synchronous=NORMAL")
        await connection.execute("PRAGMA wal_autocheckpoint=1000")
        await connection.execute(f"PRAGMA journal_size_limit={int(self._journal_size_limit_bytes)}")

    async def _initialize_schema(self, connection: aiosqlite.Connection) -> None:
        """Create the schema and seed the enum lookups; safe to call repeatedly.

        The lookups must exist before any event row is inserted, because
        ``telemetry_events.event_type`` and ``.source`` are foreign keys into
        them.
        """
        for statement in _SCHEMA_STATEMENTS + _INDEX_STATEMENTS:
            await connection.execute(statement)

        await connection.executemany(
            _SEED_EVENT_TYPES_SQL, [(event_type.value,) for event_type in EventType]
        )
        await connection.executemany(
            _SEED_EVENT_SOURCES_SQL, [(source.value,) for source in EventSource]
        )

    async def health_check(self) -> bool:
        try:
            cursor = await self.connection.execute("SELECT 1")
            row = await cursor.fetchone()
            await cursor.close()
            return bool(row and row[0] == 1)
        except Exception:
            self._logger.exception("SQLite health check failed")
            return False

    async def close(self) -> None:
        if self._connection is None:
            return
        connection = self._connection
        self._connection = None
        try:
            # Truncate the WAL on a clean stop so the next start begins with a
            # small file; this is a space control, not a write reduction.
            await connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            self._logger.debug("WAL checkpoint failed during shutdown", exc_info=True)
        try:
            await connection.execute("PRAGMA optimize")
        except Exception:
            self._logger.debug("PRAGMA optimize failed during shutdown", exc_info=True)
        await connection.close()


__all__ = ["Database"]

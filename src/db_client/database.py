"""SQLite connection lifecycle, pragmas and schema migrations.

The repository owns query/insert logic; this module owns the database file,
connection settings and schema version.  Keeping the two separate matches the
low-level design document and makes migrations independently testable.
"""

from __future__ import annotations

import logging
from pathlib import Path

import aiosqlite

SCHEMA_VERSION = 1

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS telemetry_events (
        id              INTEGER PRIMARY KEY,
        event_id        TEXT NOT NULL UNIQUE,
        schema_version  INTEGER NOT NULL,
        event_type      TEXT NOT NULL,
        source          TEXT NOT NULL,
        topic           TEXT NOT NULL,
        sequence        INTEGER NOT NULL,
        event_time      REAL NOT NULL,
        ingested_at     REAL NOT NULL,
        payload_json    TEXT NOT NULL
    )
    """,
)

_INDEX_STATEMENTS: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS ix_events_event_time ON telemetry_events(event_time)",
    "CREATE INDEX IF NOT EXISTS ix_events_type_time ON telemetry_events(event_type, event_time)",
    "CREATE INDEX IF NOT EXISTS ix_events_source_time ON telemetry_events(source, event_time)",
)


class Database:
    """A WAL-mode SQLite database with explicit, idempotent initialization."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        if busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be positive")
        self._path = str(path)
        self._busy_timeout_ms = busy_timeout_ms
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

    async def connect(self) -> None:
        if self._connection is not None:
            return

        if not self._path.startswith((":memory:", "file:")):
            Path(self._path).expanduser().resolve().parent.mkdir(  # noqa: ASYNC240
                parents=True, exist_ok=True
            )

        connection = await aiosqlite.connect(self._path, isolation_level=None)
        try:
            await self._apply_pragmas(connection)
            await self._migrate(connection)
        except BaseException:
            await connection.close()
            raise
        self._connection = connection

    async def _apply_pragmas(self, connection: aiosqlite.Connection) -> None:
        await connection.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_ms)}")
        await connection.execute("PRAGMA foreign_keys=ON")

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

    async def _migrate(self, connection: aiosqlite.Connection) -> None:
        cursor = await connection.execute("PRAGMA user_version")
        row = await cursor.fetchone()
        await cursor.close()
        version = int(row[0]) if row else 0

        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {version} is newer than supported version "
                f"{SCHEMA_VERSION}; refusing to open {self._path}"
            )

        for statement in _SCHEMA_STATEMENTS + _INDEX_STATEMENTS:
            await connection.execute(statement)

        if version < SCHEMA_VERSION:
            await connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._logger.info(
                "Migrated SQLite schema from version %s to %s", version, SCHEMA_VERSION
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
            await connection.execute("PRAGMA optimize")
        except Exception:
            self._logger.debug("PRAGMA optimize failed during shutdown", exc_info=True)
        await connection.close()


__all__ = ["Database", "SCHEMA_VERSION"]

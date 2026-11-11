from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from eaa_orchestrator.persistence.database import Database
from eaa_orchestrator.persistence.repository import TelemetryRepository


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
async def repository(db_path: Path) -> AsyncIterator[TelemetryRepository]:
    database = Database(db_path)
    repo = TelemetryRepository(database)
    await repo.initialize()
    try:
        yield repo
    finally:
        await repo.close()

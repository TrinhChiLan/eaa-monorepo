PYTHON ?= python3

.PHONY: install test lint format run mock clean-db clean-ipc

install:
	$(PYTHON) -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check .

format:
	.venv/bin/ruff format .

run:
	.venv/bin/eaa-orchestrator

mock:
	.venv/bin/eaa-mock-feed

clean-db:
	rm -f data/edge_system.db data/edge_system.db-wal data/edge_system.db-shm

clean-ipc:
	rm -f /tmp/eaa/*.ipc /tmp/eaa/*.pub /tmp/eaa/*.pull /tmp/eaa/.router.lock

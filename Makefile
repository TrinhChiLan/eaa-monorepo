PYTHON ?= python3

.PHONY: install test lint format format-check run mock clean-db clean-ipc

install:
	$(PYTHON) -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check shared container-orchestrator nvhn

format:
	.venv/bin/ruff format shared container-orchestrator nvhn

format-check:
	.venv/bin/ruff format --check shared container-orchestrator nvhn

run:
	.venv/bin/eaa-orchestrator

mock:
	.venv/bin/eaa-mock-feed

clean-db:
	rm -f data/edge_system.db data/edge_system.db-wal data/edge_system.db-shm

clean-ipc:
	rm -f /tmp/eaa/*.ipc /tmp/eaa/*.pub /tmp/eaa/*.pull /tmp/eaa/.router.lock

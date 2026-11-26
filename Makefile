PYTHON ?= python3
VENV_BIN := .venv/bin

# The event pipeline owns its own tree; scope every target to it so changes
# elsewhere cannot break this workflow (and vice versa).
EVENT_DIR := src/event_pipeline

.PHONY: install test lint format format-check check run mock smoke clean-db clean-ipc

install:
	$(PYTHON) -m venv .venv
	$(VENV_BIN)/python -m pip install --upgrade pip
	$(VENV_BIN)/pip install -e ".[dev]"

test:
	$(VENV_BIN)/pytest $(EVENT_DIR)/tests -q

lint:
	$(VENV_BIN)/ruff check $(EVENT_DIR)

format:
	$(VENV_BIN)/ruff format $(EVENT_DIR)

format-check:
	$(VENV_BIN)/ruff format --check $(EVENT_DIR)

check: lint format-check test

run:
	$(VENV_BIN)/eaa-orchestrator

mock:
	$(VENV_BIN)/eaa-mock-feed

# End-to-end guard: the console scripts must exist, the orchestrator must run a
# real cycle, and the database it produces must still be WAL-mode with the
# relational schema and no redundant source index (the flash-wear change).
# There is no schema version any more: the schema is created idempotently, so
# this asserts the shape directly.
smoke:
	@set -e; s=$$(mktemp -d); trap 'rm -rf "$$s"' EXIT; \
	$(VENV_BIN)/eaa-orchestrator --help > /dev/null; \
	$(VENV_BIN)/eaa-mock-feed --help > /dev/null; \
	$(VENV_BIN)/eaa-orchestrator --duration 1 \
		--db-path "$$s/edge.db" --ipc-dir "$$s" > /dev/null; \
	$(VENV_BIN)/python -c 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); q=lambda s:c.execute(s).fetchall(); j=q("PRAGMA journal_mode")[0][0]; ix={r[0] for r in q("SELECT name FROM sqlite_master WHERE type=\"index\" AND name LIKE \"ix_%\"")}; tabs={r[0] for r in q("SELECT name FROM sqlite_master WHERE type=\"table\"")}; fk=q("PRAGMA foreign_key_list(telemetry_events)"); ok=(j=="wal" and ix=={"ix_events_event_time","ix_events_type_time"} and {"event_types","event_sources"}<=tabs and len(fk)==2); assert ok,(j,ix,tabs,fk); print("smoke ok: WAL, relational schema, no source index")' "$$s/edge.db"

clean-db:
	rm -f $(EVENT_DIR)/tests/db_test/edge_system.db*

clean-ipc:
	rm -f /tmp/eaa/*.ipc /tmp/eaa/*.pub /tmp/eaa/*.pull /tmp/eaa/.router.lock

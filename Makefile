PYTHON ?= python3
VENV_BIN := .venv/bin

ENTRYPOINT := src/main_nvhn.py
MOCK_FEED := src/tests/mock/feed.py

SOURCE_FILES := \
	src/main_nvhn.py \
	src/db_client/__init__.py \
	src/db_client/database.py \
	src/db_client/repository.py \
	src/event_router/__init__.py \
	src/event_router/event_router.py \
	src/telemetry_ring/__init__.py \
	src/telemetry_ring/ring_buffer.py \
	src/mock_data/__init__.py \
	src/mock_data/config.py \
	src/mock_data/contracts.py \
	src/mock_data/constants/__init__.py \
	src/mock_data/constants/zmq_topics.py \
	src/mock_data/schemas/__init__.py \
	src/mock_data/schemas/events.py \
	src/mock_data/schemas/telemetry.py

TEST_HELPER_FILES := \
	src/tests/__init__.py \
	src/tests/conftest.py \
	src/tests/test_mock_data.py \
	src/tests/mock/__init__.py \
	src/tests/mock/feed.py \
	src/tests/unit_tests/__init__.py

TEST_FILES := \
	src/tests/unit_tests/test_config.py \
	src/tests/unit_tests/test_events.py \
	src/tests/unit_tests/test_integration.py \
	src/tests/unit_tests/test_mocks.py \
	src/tests/unit_tests/test_persistence.py \
	src/tests/unit_tests/test_ring_buffer.py \
	src/tests/unit_tests/test_router.py

PY_FILES := $(SOURCE_FILES) $(TEST_HELPER_FILES) $(TEST_FILES)

.PHONY: install test lint format format-check run mock clean-db clean-ipc

install:
	$(PYTHON) -m venv .venv
	$(VENV_BIN)/python -m pip install --upgrade pip
	$(VENV_BIN)/pip install -e ".[dev]"

test:
	$(VENV_BIN)/pytest $(TEST_FILES)

lint:
	$(VENV_BIN)/ruff check $(PY_FILES)

format:
	$(VENV_BIN)/ruff format $(PY_FILES)

format-check:
	$(VENV_BIN)/ruff format --check $(PY_FILES)

run:
	$(VENV_BIN)/python $(ENTRYPOINT)

mock:
	$(VENV_BIN)/python $(MOCK_FEED)

clean-db:
	rm -f src/tests/db_test/edge_system.db src/tests/db_test/edge_system.db-wal src/tests/db_test/edge_system.db-shm
	rm -f data/edge_system.db data/edge_system.db-wal data/edge_system.db-shm

clean-ipc:
	rm -f /tmp/eaa/*.ipc /tmp/eaa/*.pub /tmp/eaa/*.pull /tmp/eaa/.router.lock

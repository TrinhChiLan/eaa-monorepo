PYTHON ?= python3
VENV_BIN := .venv/bin

ENTRYPOINT := root/main_nvhn.py
MOCK_FEED := root/tests/mock/feed.py

SOURCE_FILES := \
	root/main_nvhn.py \
	root/db_client/__init__.py \
	root/db_client/database.py \
	root/db_client/repository.py \
	root/event_router/__init__.py \
	root/event_router/event_router.py \
	root/telemetry_ring/__init__.py \
	root/telemetry_ring/ring_buffer.py \
	root/mock_data/__init__.py \
	root/mock_data/config.py \
	root/mock_data/contracts.py \
	root/mock_data/constants/__init__.py \
	root/mock_data/constants/zmq_topics.py \
	root/mock_data/schemas/__init__.py \
	root/mock_data/schemas/events.py \
	root/mock_data/schemas/telemetry.py

TEST_HELPER_FILES := \
	root/tests/__init__.py \
	root/tests/conftest.py \
	root/tests/test_mock_data.py \
	root/tests/mock/__init__.py \
	root/tests/mock/feed.py \
	root/tests/unit_tests/__init__.py

TEST_FILES := \
	root/tests/unit_tests/test_config.py \
	root/tests/unit_tests/test_events.py \
	root/tests/unit_tests/test_integration.py \
	root/tests/unit_tests/test_mocks.py \
	root/tests/unit_tests/test_persistence.py \
	root/tests/unit_tests/test_ring_buffer.py \
	root/tests/unit_tests/test_router.py

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
	rm -f root/tests/db_test/edge_system.db root/tests/db_test/edge_system.db-wal root/tests/db_test/edge_system.db-shm
	rm -f data/edge_system.db data/edge_system.db-wal data/edge_system.db-shm

clean-ipc:
	rm -f /tmp/eaa/*.ipc /tmp/eaa/*.pub /tmp/eaa/*.pull /tmp/eaa/.router.lock

# `nvhn/` — development-only assets

This directory is intentionally not production code.

It contains:

- `tests/` — unit and integration tests
- `mocks/` — mock CV, sensor, STT, and UI producers
- `docs/` — architecture and testing guides
- `reference/` — design PDF, architecture XML, and original pseudocode

Production code lives in:

- [`../container-orchestrator/`](../container-orchestrator/) — the orchestrator runtime
- [`../shared/`](../shared/) — the cross-container event contract

Runtime SQLite data is not stored here. The local default is `../data/edge_system.db`
(gitignored); production sets `EAA_DB_PATH` to a mounted volume.

Run project commands from the repository root, not from this directory.

# Relational Database Design Review — `db_client/`

Status: **implemented** (schema, audit fix and write-cost guard). See §6.
Scope: `src/event_pipeline/db_client/` (plus the callers that constrain it).

This document answers one question: *does moving from a single table to a fuller
relational schema help?* Short answer: **yes, but not in the way it is usually
framed.** The valuable relational work is fixing a correctness bug that the
single table currently hides, and normalizing the enum columns. Two "more
relational" moves are free (≈0 % write cost): `event_type`/`source` lookup tables
with foreign keys. The two most obvious *structural* moves are not: adding an
`(source, event_time)` index costs **+14–19 %** and splitting payloads into
per-type fact tables costs **+33–34 %** in flash writes — the exact metric this
codebase was optimized to minimize. Those are measurements on this host; see §3
for the table. A `test_write_cost_is_neutral_versus_previous_schema` guard in
`tests/unit_tests/test_persistence.py` now enforces the write-neutral result in
CI.

---

## 1. What the design is

`database.py` owns the file, pragmas and schema initialization;
`repository.py` owns all SQL (`TelemetryRepository`) plus the
half-sync/half-async batching worker. That split is already clean and is the
right seam to keep. There is **no schema-version or migration machinery**: the
database is created idempotently with `CREATE … IF NOT EXISTS`, and a schema
change means deleting the (disposable, gitignored) development database.

The relational schema is a conformed event table plus two enum lookups:

| column | role |
|---|---|
| `id INTEGER PRIMARY KEY` | insertion order; the retention cursor |
| `event_id TEXT UNIQUE` | producer idempotency (`INSERT OR IGNORE`) |
| `schema_version`, `event_type`, `source`, `topic`, `sequence` | envelope metadata |
| `event_time`, `ingested_at REAL` | time (unix seconds, float) |
| `payload_json TEXT` | the whole discriminated Pydantic payload from `events.py` |

Indexes: `ix_events_event_time(event_time)` and
`ix_events_type_time(event_type, event_time)`. `UNIQUE(event_id)` is implemented
as `sqlite_autoindex_telemetry_events_1`.

`event_type` and `source` are foreign keys into `event_types` and
`event_sources`, both seeded from the `EventType`/`EventSource` enums on every
connect. `CHECK` constraints mirror the envelope contract (positive timestamps,
non-negative sequence, bounded topic). Seven `VIRTUAL` generated columns expose
the payload fields worth querying at zero storage and zero flash-write cost.
They are not free on CPU: see §4.5.

**Key observation for any redesign:** the DB is a *write-mostly audit log*. The
only read paths are `fetch_recent()` (used by tests; the HMI reads the
in-memory ring buffer) and `count()`. There is no join and no aggregation. A
relational schema therefore buys nothing for existing readers — it has to be
justified by integrity and *future* queries (§4).

---

## 2. Bug found while reviewing: the 1:1 invariant was broken

Schema v2 removed `(source, event_time)` on the stated grounds that "`source` and
`event_type` are 1:1 in the current contract"
(`EDGE_OPTIMIZATION_REPORT.md` §4). That was **not true at runtime**:

`src/continuous_pipeline.py` wrote FSM state transitions as

```python
event_type=EventType.UI_COMMAND,
source=EventSource.SYSTEM,   # <-- ui.command with source=system
```

Reproduced against the real repository:

```
count total           = 2
count ui.command      = 2
fetch source=SYSTEM   = []        # the state audit rows are invisible
fetch source=UI       = ['system', 'ui']   # and leak into the UI query
```

Consequences:

1. **Audit records could not be retrieved.** `fetch_recent(source=SYSTEM)`
   returned `[]` (it maps `SYSTEM` to "no event type"), so the FSM/safety audit
   trail written by the continuous pipeline could never be read back by source.
   For a safety-adjacent audit log this is a real defect, not a nitpick.
2. **`fetch_recent(source=UI)` returned system events**, mixing audit into user
   commands.
3. **The query plan was wrong.** With no source index, the planner could not push
   `source` down, so `source=` filters scanned by `event_time` and post-filtered
   (`SEARCH … USING INDEX ix_events_event_time (event_time>?)`). At rest that is
   cheap (0.4 ms at 250k rows), but it degrades when the value matters: a narrow
   time window containing few matching rows reads far more than necessary and
   cannot use `LIMIT` early.

**Fixed** by giving audit records their own event type (§4.2, option A) rather
than by adding an index — no extra index, no extra write cost.
`EventType.SYSTEM_AUDIT` (`system.audit`) with a dedicated `SystemAuditPayload`
now pairs `source=system` with its own event type. Verified end to end through
`ContinuousPipeline.on_state_changed`:

```
total rows        : 1
system.audit rows : 1
readback (source=system): [('system.audit', 'transition:idle->running')]
readback (source=ui)    : []
```

`tests/unit_tests/test_persistence.py::test_source_event_type_map_is_a_bijection`
now fails if the mapping drifts from a 1:1 pair, which is what the schema's
"no source index" decision depends on.

---

## 3. Measured cost of each candidate change

Metric: logical WAL bytes + checkpoint pages per row, 50 000 sensor rows at the
edge profile's 50-row batch, `synchronous=NORMAL`, 4 MiB WAL cap, medians of 3
runs — the same methodology as `EDGE_OPTIMIZATION_REPORT.md` §2. Deltas moved by
a few points between runs on this host; ranges are given rather than a single
figure.

| Schema variant | WAL + checkpoint (B/row) | Δ vs v2 | DB file (B/row) |
|---|---:|---:|---:|
| **v2 single table (baseline)** | **2 915–2 929** | — | 362–363 |
| + `event_types` / `event_sources` lookup tables (current schema) | 2 916–2 930 | **≈ 0 %** | 362–364 |
| v2, `(event_time)` index only (drop `(type,time)`) | 2 275 | −22 % | 327 |
| **v2 + `(source, event_time)` index** | **3 342–3 475** | **+14–19 %** | 388–391 |
| **+ `sensor_snapshots` fact table (typed columns)** | **3 866–3 918** | **+33–34 %** | 413–422 |

Read the last two rows carefully — they are the mainstream "make it relational"
answers, and both lose on the metric this module was tuned for. A third,
non-monotonic index tree is dirtied by every insert, so the "just add a source
index" fix costs +14–19 %, and a child fact-table row adds a second B-tree to
touch per event.

For context, at 5 Hz (432 k rows/day) the +33 % regression is
1.26 → 1.68 GB/day, i.e. **~0.42 GB/day more** logical writes — the wrong
direction after a 45 % reduction was just won. (Absolute values here are
projections from this host's per-row measurement, not target-device figures.)

---

## 4. Design as implemented

### 4.1 The core: one row per event, JSON body

`telemetry_events` remains the conformed event table. It is the correct
*physical* shape for an append-only, retention-trimmed edge log, and it keeps
`event_id` idempotency and the monotonic `id` retention cursor. The
discriminated payload is **not** shredded into per-type fact tables.

### 4.2 The invariant break: option A (chosen)

**Option A was implemented: `SYSTEM` is a first-class source with its own event
type.** `EventType.SYSTEM_AUDIT = "system.audit"` plus a dedicated
`SystemAuditPayload`, written with `source=SYSTEM`. This makes the 1:1 contract
true rather than assumed, so the existing `(event_type, event_time)` index serves
`source=SYSTEM` — **zero extra index, zero extra write cost**.

Rejected: **option B**, relaxing the invariant and adding
`ix_events_source_time(source, event_time)` (+14–19 % writes). It is only worth
revisiting if many sources genuinely share event types.

The hard-coded `_SOURCE_EVENT_TYPES` dict is still the mapping, but it is now
guarded by `test_source_event_type_map_is_a_bijection`, which fails if every
`EventSource` is not paired with exactly one `EventType`. A
`test_system_audit_cannot_be_written_as_ui_command` test in `test_events.py` also
rejects the exact construction that caused the bug.

### 4.3 Enum lookups (free)

`event_type` and `source` are real tables and the event table references them:

```sql
CREATE TABLE event_types  (event_type TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE event_sources(source TEXT PRIMARY KEY)     WITHOUT ROWID;
```

Measured at **≈0 % write cost**. They buy real integrity: unknown enum values are
now rejected by a foreign key rather than by convention, and the tables are a
natural place to hang per-type retention policy later. They are seeded from the
`EventType`/`EventSource` enums on connect — the literals are never retyped.

A plain `CHECK` could not do this: a `CHECK` constraint cannot subquery another
table, so a foreign key is the correct mechanism.

`bulk_insert` uses `INSERT … ON CONFLICT(event_id) DO NOTHING` rather than
`INSERT OR IGNORE`. This matters: `OR IGNORE` also suppresses the foreign-key
violation, so a bad enum value would be discarded as if it were a duplicate,
silently losing an audit row. `ON CONFLICT(event_id)` keeps dedupe at 0 changes
and lets integrity errors propagate.

### 4.4 Retention is unchanged (deferred)

`prune(max_rows=...)` still uses the `OFFSET` cut-off (~90 ms at 250 k rows,
inside the worker's write lock). Replacing it with an in-memory cursor and a
contiguous head delete measured **~0.03 ms per row**, but it adds state that must
be re-seeded on connect and is a latency change, not a schema change. Deferred.

### 4.5 Typed columns without a second table (implemented)

Seven `VIRTUAL` generated columns expose the payload fields worth querying —
`confidence`, `action`, `motor_current_amp`, `infrared_temp_celsius`,
`conveyor_speed_mms`, `optical_curtain_clear`, `estop_circuit_closed`:

```sql
motor_current_amp REAL GENERATED ALWAYS AS
  (json_extract(payload_json, '$.motor_current_amp')) VIRTUAL
```

`VIRTUAL` stores nothing: measured WAL+checkpoint and DB bytes per row are
identical to a table without the columns, whereas `STORED` costs +34 B/row of
WAL and +22.6 B/row of DB file. A field absent from a given payload simply reads
as `NULL`.

They are **not** free on CPU, which an earlier draft of this document got wrong.
SQLite emits `PureFunc` for the generated expression on every `INSERT`, so the
`json_extract` runs per row even though the result is not persisted. Measured
insert throughput: 190k rows/s without the columns, 163k with one, 133k with all
seven (~30 %). Foreign keys account for only ~5 %, and `CHECK` constraints ~3 %.
Reads through the repository's explicit column list are unaffected (0.25 ms at
250k rows either way); a `SELECT *` is about 2× slower because it forces all
seven expressions to be evaluated.

None of this matters at the edge workload: 5 Hz needs 5 rows/s against a
~130k rows/s ceiling. It would matter if the sensor rate climbed into the tens of
kHz, at which point drop the generated columns and query the JSON directly. Add
more only when a query demands it.

A `payload` JSONB column is **not** available: the packaged SQLite is
**3.45.1**, below the 3.46 release that added JSONB.

`json_valid(payload_json)` is deliberately **not** a `CHECK`: it raises
`OperationalError` rather than `IntegrityError`, which would poison the worker's
retry path for a whole batch. Payloads are already validated by Pydantic.

### 4.6 `topic` and `source` are kept (slimming deferred)

`EDGE_OPTIMIZATION_REPORT.md` §6 defers "row slimming" (dropping `topic`/`source`,
which are derivable from `event_type`, and the repeated `"kind"` in
`payload_json`). That is now purely a DB-file-size question — it saves no write
bytes — and is not worth the churn while the payload contract is still moving.

---

## 5. What not to do

- **Don't add a system-state/audit table for the continuous pipeline.** FSM
  transitions are already emitted as envelopes (`on_state_changed`); making them
  a typed event type (4.2) gives them a table's worth of structure with no extra
  write. A separate table would duplicate the retention, batching and low-disk
  logic.
- **Don't adopt a second engine.** The constraints that shaped this design —
  single file, no server, WAL, 0700/0600 permissions, `ProtectSystem=strict`,
  a 64 MiB free-space floor — are all SQLite-specific. Postgres/TimescaleDB on
  this box would add a daemon, a network surface and its own WAL on the same
  flash.
- **Don't "just add the source index".** Measured: +14–19 %. Fix the invariant
  (4.2) instead unless you have chosen option B deliberately.

---

## 6. Implementation and verification

There is **no migration path**, by decision: the system is in development, the
database is disposable, and no production data exists. `Database._migrate` /
`SCHEMA_VERSION` / `PRAGMA user_version` were removed in favour of
`_initialize_schema()`, which issues idempotent `CREATE … IF NOT EXISTS`
statements and seeds the enum lookups. A schema change therefore means
**deleting the development database**; opening a file created by an older
revision will not reshape it.

What was implemented:

1. `database.py`: relational DDL (§4), lookup seeding from the enums, removal of
   the version/migration machinery.
2. `repository.py`: `INSERT … ON CONFLICT(event_id) DO NOTHING` instead of
   `INSERT OR IGNORE`, so a foreign-key violation surfaces rather than being
   swallowed as a duplicate; `_SOURCE_EVENT_TYPES` completed with `system`.
3. `mock_data/schemas/events.py`: `EventType.SYSTEM_AUDIT` and
   `SystemAuditPayload`; `continuous_pipeline.py` writes audit records with them.
4. Tests: lookup seeding, FK/CHECK enforcement, generated-column readback,
   idempotent re-open, audit readback by source, and the bijection guard.
5. `test_write_cost_is_neutral_versus_previous_schema` measures the previous
   single-table schema and the current schema in the same process and fails if
   the current one writes more than 5 % extra. Measured ratio: **1.002**; it
   fails at 1.194 when a `(source, event_time)` index is added, so the guard is
   not vacuous.

Verification: `pytest src/event_pipeline/tests -q` → 73 passed; `ruff check` and
`ruff format --check` clean on `src/event_pipeline`. Re-measure on the target
device before relying on the absolute numbers; the figures here are from the
development host (see the report's caveats about FTL and filesystem caching).

---

## 7. Resolved decisions

1. **Option A vs B** in §4.2 — **option A** (`system.audit`). It makes the 1:1
   mapping true rather than assumed and avoids the +14–19 % source index.
2. **Audit-log or queryable store?** Audit-log first; the seven generated
   columns (§4.5) provide a query surface without committing to fact tables.
   Fact tables remain rejected on measurement.
3. **JSONB?** Not available — the packaged SQLite is 3.45.1, below 3.46. Payloads
   stay `TEXT`.

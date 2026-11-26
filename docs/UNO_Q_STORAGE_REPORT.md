# Storage Report: eMMC Behaviour on the Arduino UNO Q

Date: 2026-11-26
Device: Arduino UNO Q `rora` (`10.1.1.61`, `board=unoq`, serial `1781473796`)
Scope: is the current persistence implementation the best we can do, and does the
board's eMMC offer anything the dev machine's storage does not?

- [UNO_Q_SETUP.md](UNO_Q_SETUP.md) — getting the repo onto the board
- [BENCHMARKING.md](BENCHMARKING.md) — what the benchmark measures
- [RELATIONAL_DB_DESIGN_REVIEW.md](RELATIONAL_DB_DESIGN_REVIEW.md) — schema rationale
- [EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md) — the flash-wear work

---

## 1. Conclusion first

1. **The implementation is already well matched to this board.** At the design
   rate (5 Hz) the writer runs at a ~0.13 % duty cycle. Storage metrics are
   byte-identical to the dev machine, so none of the flash-wear tuning needs to
   change.
2. **The eMMC is a managed Samsung device, not raw NAND**, and it does have
   properties the dev NVMe does not (see §5). But the two most consequential
   ones — reliable write and enhanced storage area — are **partition-time
   features** that cannot be enabled on a running system without repartitioning
   the eMMC that also holds the OS. They are not available to us.
3. **The mount-level levers (`discard`, `noatime`) and `--checkpoint-truncate`
   make no measurable difference** at this workload (§3). They are not worth
   enabling on this evidence.
4. **Wear is a non-issue.** The device reports `life_time 0x01 0x01` (0–10 %
   consumed) and `pre_eol_info 0x01` (normal). Estimates put the payload writes
   at well under 1 GB/day logical, against a multi-decade endurance budget.
   Optimising further for write volume buys almost nothing.
5. **The remaining wins are in software, not storage**: a separate read-only
   connection, and the deferred O(1) retention prune. Both attack the same real
   problem — read latency stalling behind a large write (§4), which I measured
   at **966 ms**.

---

## 2. The two machines

| | Dev host | `rora` (UNO Q) |
|---|---|---|
| CPU | x86-64, 12 threads | **aarch64, 4 cores** |
| RAM | 15 GiB class | **3.6 GiB** |
| Root storage | **NVMe SSD**, KIOXIA BG6 1 TB, `ext4` | **eMMC**, Samsung `BUTC42`, `ext4` |
| Free space | 257 GB | **2.5 GB (74 % used)** |
| Python / SQLite | 3.12.1 / 3.45.1 | 3.13.5 / 3.46.1 |
| Mount options | `relatime` | `rw,relatime` |

Same workload, identical flags (5 Hz, 60 s, 10 s flush), from
`bench-results/devhost/edge5hz/` and `bench-results/unoq/edge5hz/`:

| Metric | Dev host | rora | Ratio |
|---|---:|---:|---:|
| Rows inserted | 300 | 300 | 1.00× |
| **Bytes written (`wchar`)** | 489 344 B | **489 344 B** | **1.00×** |
| **Bytes/row** | 1 631.10 | **1 631.10** | **1.00×** |
| DB file at end | 159 744 B | 159 744 B | 1.00× |
| `flush` p50 | 2.45 ms | 13.11 ms | 5.35× |
| `read_fetch_recent` p50 | 4.91 ms | 14.74 ms | 3.00× |
| `read_count` p50 | 0.89 ms | 1.89 ms | 2.13× |
| `checkpoint` p50 | 0.75 ms | 1.31 ms | 1.76× |
| `enqueue` p50 | 0.02 ms | 0.07 ms | 3.7× (70 µs absolute) |
| `end_to_end` p50 / p99 | 4 903 / 9 803 ms | 4 911 / 9 809 ms | **1.00×** |
| RSS peak | 38 044 kB | 38 472 kB | 1.01× |

**The important row is bytes written: identical to the byte.** Write volume is a
property of the schema and batching policy, not hardware, so the flash-wear
conclusions transfer unchanged. The latency gap is real but is *not* storage:
isolating the same SQL on the board gave **0.36 ms** in memory versus **0.40 ms**
on the eMMC — storage was only 11 % of the per-flush cost. The gap is CPU and
event-loop contention.

---

## 3. Experiment: are the storage levers worth pulling?

Four configurations on `rora`, 5 Hz, 60 s each, `batch_size=50`,
`flush_interval=10`, manual checkpoints. A/B on the stock mount, C/D after
`mount -o remount,noatime,discard`:

| Run | Mount | Checkpoint | bytes/row | `flush` p50 | `ckpt` p50 | WAL at end |
|---|---|---|---:|---:|---:|---:|
| A | `rw,relatime` | `PASSIVE` | 1 631.1 | 13.35 ms | 2.61 ms | 416 152 B |
| B | `rw,relatime` | `TRUNCATE` | 1 631.1 | 13.42 ms | 3.16 ms | **0 B** |
| C | `rw,noatime,discard` | `PASSIVE` | 1 631.1 | 13.68 ms | 2.56 ms | 416 152 B |
| D | `rw,noatime,discard` | `TRUNCATE` | 1 631.1 | 13.50 ms | 3.05 ms | **0 B** |

Findings:

- **`discard`/`noatime` changed nothing measurable.** Bytes/row, flush latency and
  end-to-end latency are identical within noise. Continuous TRIM addresses
  long-term FTL housekeeping, not per-operation cost; a 60 s run cannot see it.
- **`TRUNCATE` costs ~0.5 ms more per checkpoint** (2.6 → 3.1 ms) and returns the
  WAL to zero instead of leaving it at its 416 KB high-water mark. On a partition
  with 2.5 GB free, 416 KB is irrelevant, so there is no reason to pay the extra
  half-millisecond.
- **End-to-end latency is unaffected by all four** (9 814 ms p99 in every run) —
  it is set by `flush_interval`, as designed.
- The `diskstats` deltas I captured (1.6–2.7 MB per run) are **not usable** for
  comparison: `/proc/diskstats` is system-wide and includes every other service
  on the board. Physical write attribution would need either a quiesced board or
  the eMMC's own counters.

The mount was restored to exactly `rw,relatime`, and `fstrim.timer` is still
active.

---

## 4. The real bottleneck: reads behind writes

Not a storage property, but the one number that matters operationally. Reads and
writes share a single connection, so a read can queue behind an in-flight write
(measured on `rora`, 20 000-row table):

```
fetch_recent(100) idle               :  10.6 ms
fetch_recent(100) during 20k-row write: 965.6 ms   (91× worse)
```

At the edge rate this is harmless (50 rows ≈ 13 ms). It becomes a problem if
retention pruning, a larger batch, or a manual maintenance operation runs while
an HMI query is in flight. WAL allows one writer and many readers concurrently —
the serialisation is an artefact of sharing one `aiosqlite` connection, not a
SQLite or eMMC limit.

---

## 5. What the eMMC actually is

Read from `/sys/block/mmcblk0/device/` on the board:

| Attribute | Value | Meaning |
|---|---|---|
| `name` | `BUTC42` | Managed eMMC part |
| `manfid` | `0x000015` | **Samsung** |
| `type` | `MMC` | eMMC, not SD |
| `erase_size` / `preferred_erase_size` | 512 KiB / **4 MiB** | Erase-block sizes the FTL prefers |
| `rel_sectors` | `0x1` | **Reliable write supported** |
| `enhanced_area_size` | ≈ 4 GiB | **Enhanced (SLC-mode) area supported** |
| `ffu_capable` | `1` | Field firmware update |
| `pre_eol_info` | `0x01` | Normal (not near end of life) |
| `life_time` | `0x01 0x01` | 0–10 % of rated endurance used |

ext4 on the root partition reports `metadata_csum`, `has_journal`, `orphan_file`,
`64bit`, `extent`, 4 KiB blocks — a normal modern ext4.

### Which of these can we use?

| Feature | Available to software? | Why |
|---|---|---|
| **Managed FTL / wear levelling** | Already active | Transparent; nothing to do |
| `discard` / periodic `fstrim` | **Yes** | Tested, no measurable effect at this workload (§3) |
| `preferred_erase_size` alignment | Partially | SQLite page size is 4 KiB; aligning allocations to 4 MiB is not controllable from userspace |
| **Reliable write (`rel_sectors`)** | **No** | Must be configured on the partition at creation time (`mmc` reliable-write config); cannot be enabled on a live filesystem |
| **Enhanced storage area** | **No** | Requires repartitioning the device to carve an SLC-mode region |
| **FFU (firmware update)** | Not advisable | Vendor-prescribed; unrelated to application performance |
| ext4 `data=journal` / `chattr +j` | Yes, but no | Would *increase* writes; wrong direction |

So the honest answer to "is it just basic NAND": **no, it is a Samsung managed
eMMC with reliable-write and enhanced-area support** — but both of the
interesting capabilities are provisioning features, and using them would mean
repartitioning the eMMC that also carries the OS. The risk/benefit is bad for a
telemetry log.

---

## 6. Is this the most the software can do?

For the current requirements, effectively yes — with two known, bounded
improvements:

| # | Change | Benefit | Cost |
|---|---|---|---|
| 1 | **Separate read-only connection** (`PRAGMA query_only=1`) for HMI reads | Removes the 966 ms read stall behind writes; WAL already supports concurrent readers | Small change in `Database`/`TelemetryRepository`; one extra connection |
| 2 | **O(1) retention prune** (head-delete cursor, deferred in the schema review) | Shrinks the longest single write, which is what produces read stalls; ~90 ms at 250 k rows today | Repository state to re-seed on connect |

Explicitly **not** worth doing: enabling `discard`, switching to `TRUNCATE`
checkpoints, further schema slimming (wear is negligible), or repartitioning for
eMMC features.

Beyond that, the design is at its natural limit for this workload: batching
already amortises commits, WAL already decouples readers from the writer at the
SQLite level, and the queue already isolates the router (enqueue p99 = 0.03 ms
under saturation). Further gains would require changing the durability contract
(e.g. accepting longer flush intervals), not the storage.

---

## 7. Limitations of this report

- **One run per configuration.** `checkpoint` n = 1 per run, so its p50 is a
  single sample and the 0.5 ms TRUNCATE difference is within noise. Treat §3 as
  "no detectable effect", not as a precise measurement.
- **`diskstats` is system-wide** on this board and could not attribute writes to
  the database; the physical-write column in §3 is therefore omitted rather than
  guessed.
- **No long-duration trim/GC study.** Any `discard` benefit is a
  multi-hour/multi-day effect that a 60 s run cannot observe.
- **`life_time` is coarse** (0–10 % buckets) and reflects the board's whole
  history, not this workload.
- rora also runs App Lab and other services; absolute latencies could shift with
  background load, though the ratios were stable across runs.
- The read-stall figure (§4) comes from a synthetic 20 000-row write, not from a
  production retention prune.

---

## 8. Reproduction

```bash
# on rora, from ~/Arduino/ArduinoApps/cos40005-database-testing
PYTHONPATH=src .venv/bin/python -m event_pipeline.bench_persistence \
  --label A_passive --rate 5 --duration 60

PYTHONPATH=src .venv/bin/python -m event_pipeline.bench_persistence \
  --label B_trunc --rate 5 --duration 60 --checkpoint-truncate

sudo mount -o remount,noatime,discard /
# ... rerun both as C and D, then restore:
sudo mount -o remount,rw,relatime,diratime,nodiscard /
```

Storage characteristics:

```bash
for f in name manfid erase_size preferred_erase_size rel_sectors \
         enhanced_area_size ffu_capable pre_eol_info life_time; do
  printf '%-22s ' "$f"; cat /sys/block/mmcblk0/device/$f; done
```

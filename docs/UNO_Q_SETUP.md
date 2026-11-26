# Running the Backend on an Arduino UNO Q (`rora`)

How to put this repo on an Arduino UNO Q and run the persistence benchmark on
real hardware. All facts below were verified on the board itself, not assumed
from datasheets.

- [BENCHMARKING.md](BENCHMARKING.md) — what the benchmark measures
- [UNO_Q_STORAGE_REPORT.md](UNO_Q_STORAGE_REPORT.md) — eMMC capabilities and measured storage behaviour
- [EDGE_DEPLOYMENT.md](EDGE_DEPLOYMENT.md) — the systemd service and production profile
- [EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md) — the numbers this re-verifies

---

## 1. The board, as verified

| Property | Value |
|---|---|
| Hostname / mDNS | `rora` / `rora.local` |
| Address on the LAN | `10.1.1.61` (DHCP; re-resolve before each session) |
| Arduino board id | `unoq`, `vid=0x2341 pid=0x0078`, serial `1781473796` |
| OS | **Debian GNU/Linux 13 (trixie)**, aarch64 |
| Kernel | `7.0.0-g122c2c22d838` |
| CPU / RAM | 4 cores, **3.6 GiB** (≈0.9 GiB free at idle) |
| Root filesystem | `/dev/mmcblk0p68`, 9.8 G with **~2.5 G free (74 % used)** |
| Python | **3.13.5** (`/usr/bin/python3`) |
| Present | `git`, `curl`, `tar`, `ssh` |
| **Missing** | `pip`, `venv` module, `rsync`, `make`, `gcc`, `sqlite3` |
| Package access | **PyPI unreachable**; `deb.debian.org` reachable |
| `sudo` | requires the `arduino` account password |

Two consequences drive everything below: **you cannot `pip install` from the
network**, and **the PyPI dependencies cannot be built locally** (no `gcc`).

Also note the apt versions are too old for this project — `python3-aiosqlite`
0.17 (needs ≥0.20) and `python3-zmq` 24 (needs ≥26) — so apt is not a substitute
for the wheelhouse.

---

## 2. Prerequisites: one apt package

```bash
# on the board
sudo apt-get update
sudo apt-get install -y python3-venv      # provides venv + ensurepip (pip)
```

Without `python3-venv`, `python3 -m venv` fails and `python3 -m pip` reports
`No module named pip`. Nothing else needs installing.

---

## 3. Build an offline wheelhouse (on the dev host)

The board has no PyPI and no compiler, so fetch aarch64 wheels matching its
Python (**3.13**) on a machine that does have PyPI:

```bash
pip download --dest wheelhouse --only-binary=:all: \
  --platform manylinux2014_aarch64 --python-version 3.13 \
  --implementation cp --abi cp313 \
  pydantic pyzmq aiosqlite
```

That produces ~3 MB, 7 wheels (the runtime dependencies plus
`pydantic-core`, `annotated-types`, `typing-extensions`, `typing-inspection`).
`pydantic-core` and `pyzmq` ship `cp313` `manylinux_2_17_aarch64` wheels, so no
compiler is ever needed.

> Match `--python-version` / `--abi` to the board's interpreter. For Python
> 3.12 use `--python-version 3.12 --abi cp312`. Check with
> `python3 --version` on the board first.

---

## 4. Copy the repo and the wheelhouse

`rsync` is not installed on the board, and PyPI is blocked, so stream a tarball
over `ssh`:

```bash
# on the dev host, from the repository root
git archive --format=tar.gz -o eaa-src.tgz HEAD
TARGET='~/Arduino/ArduinoApps/cos40005-database-testing'

ssh arduino@10.1.1.61 "mkdir -p $TARGET"
cat eaa-src.tgz | ssh arduino@10.1.1.61 "tar xz -C $TARGET"
tar cz -C wheelhouse . | ssh arduino@10.1.1.61 "mkdir -p $TARGET/wheelhouse && tar xz -C $TARGET/wheelhouse"
```

Do **not** copy `.venv` (host x86-64 binaries) or `bench-results/`.

---

## 5. Create the venv and install offline

```bash
# on the board
cd ~/Arduino/ArduinoApps/cos40005-database-testing
python3 -m venv .venv
.venv/bin/python -m pip install --no-index --find-links wheelhouse \
  pydantic pyzmq aiosqlite
```

`--no-index` is deliberate: it makes the offline constraint explicit instead of
letting pip hang on an unreachable PyPI. Expect
`Successfully installed aiosqlite-0.22.1 ... pydantic-2.14.4 ... pyzmq-26.4.0`.

Deliberately **not** done: `pip install -e .`. An editable install needs
`setuptools`/`wheel`, which a 3.13 venv does not ship, and there is no `gcc`.
Running from the source tree works just as well (next section).

---

## 6. Run it

Because the package is not installed, put `src/` on the path. This is the only
difference from the dev-host instructions:

```bash
cd ~/Arduino/ArduinoApps/cos40005-database-testing

# smoke: ~3 s, must write bench-results/smoke/summary.json
PYTHONPATH=src .venv/bin/python -m event_pipeline.bench_persistence \
  --label unoq-smoke --rate 200 --duration 3 \
  --flush-interval 0.5 --sample-interval 0.2 --read-interval 0.2 \
  --out bench-results/smoke

# edge profile: 5 Hz, 60 s
PYTHONPATH=src .venv/bin/python -m event_pipeline.bench_persistence \
  --label unoq-edge-5hz --rate 5 --duration 60 --out bench-results/edge5hz
```

`make bench` needs the same prefix (`PYTHONPATH=src make bench`), and `make`
itself is not installed on the board.

Copy results back:

```bash
ssh arduino@10.1.1.61 \
  "cd ~/Arduino/ArduinoApps/cos40005-database-testing && tar cz bench-results" \
  | tar xz -C bench-results/uno-q
```

---

## 7. Measured results on `rora`

Identical flags on both machines; dev host is x86-64.

| Metric (5 Hz, 60 s, 10 s flush) | Dev host (x86-64) | `rora` (UNO Q) |
|---|---:|---:|
| Rows inserted | 300 | 300 |
| `wchar` total | 489 344 B | **489 344 B** (identical) |
| **Bytes/row** | 1 631.10 | **1 631.10** (identical) |
| `flush` p50 / p99 | 2.45 / 3.09 ms | **13.11 / 14.29 ms** |
| `end_to_end` p50 / p99 | 4 902.86 / 9 802.58 ms | **4 910.90 / 9 808.84 ms** |
| `read_fetch_recent` p50 | 4.91 ms | **14.74 ms** |
| `read_count` p50 | 0.89 ms | 1.89 ms |
| `enqueue` p50 | 0.02 ms | 0.07 ms |
| `checkpoint` | 0.75 ms | 1.31 ms |
| RSS peak | 38 044 kB | 38 472 kB |

Interpretation:

- **Write volume is byte-for-byte identical.** That is the headline result:
  bytes written are governed by batching and page layout, not CPU or storage
  speed, so the flash-wear conclusions of `EDGE_OPTIMIZATION_REPORT.md` carry
  over to this device without re-tuning.
- **`flush` is ~5× slower** (13.1 ms vs 2.45 ms for the same 50-row batch) and
  reads ~3× slower — slower eMMC and a slower CPU. Still ~13 ms against a 10 s
  flush interval, so the writer is nowhere near being the bottleneck at 5 Hz.
- **`end_to_end` p99 ≈ 9.8 s on both** — the documented 10 s audit-loss window is
  confirmed on real hardware, and it is flush-interval-bound rather than
  device-bound.
- **Queue depth stays ~0** — expected; see `BENCHMARKING.md` §4.3.
- The 200 Hz smoke run agrees: 1 823.80 B/row on `rora` vs 1 803.20 on the dev
  host (≈1 % apart).

---

## 8. Gotchas specific to this board

1. **Only ~2.5 GB free.** Keep `EAA_RETENTION_MAX_ROWS` modest and delete the
   wheelhouse after installing (`rm -rf wheelhouse`) to reclaim ~3 MB.
2. **PyPI is blocked but apt works.** Any new Python dependency needs the
   wheelhouse rebuilt and re-copied; `pip install <pkg>` will simply fail.
3. **No `gcc`** — anything without an aarch64 wheel cannot be installed at all.
4. **No `rsync`, no `make`** — use `tar` over `ssh` and `python -m`.
5. **`sqlite3` CLI absent** — inspect the database with
   `.venv/bin/python -c "import sqlite3; ..."`.
6. **eMMC, not SD** — `/dev/mmcblk0p68`. For physical wear, read
   `/proc/diskstats` for that device before/after a long run; the benchmark's
   `io.diskstats_bytes` may be `null`.
7. **Password auth** works but is not scriptable without `sshpass` (absent). For
   regular use, install an SSH key (`ssh-copy-id arduino@10.1.1.61`) so
   transfers and reruns need no password.
8. **The board is not a UNO R3** — the Linux side is a full arm64 Debian. The
   STM32 side (Arduino Core on Zephyr) is not used by this project.

---

## 9. Checklist

- [ ] `python3 --version` matches the wheelhouse's `--python-version`
- [ ] `python3 -m venv .venv` succeeds
- [ ] `pip install --no-index --find-links wheelhouse ...` succeeds with no build
- [ ] `PYTHONPATH=src .venv/bin/python -m event_pipeline.bench_persistence --help`
- [ ] `--rate 200 --duration 3` smoke writes `summary.json` with
      `rows_inserted > 0` and non-null `latency_ms.flush.p50`
- [ ] 60 s edge run completes and results are copied back

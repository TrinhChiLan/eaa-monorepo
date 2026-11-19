> Plain-language summary of
> [EDGE_OPTIMIZATION_REPORT.md](EDGE_OPTIMIZATION_REPORT.md). The report is the
> authoritative source for methodology, measurements, and trade-offs.

### 1. The Core Problem: Why Flash Media Wears Out

When an edge device runs a backend service (like the EAA backend) continuously, it constantly writes data to local storage (like an eMMC or SD card).

* **The issue:** Every time data is saved, the device performs physical writes. Doing this inefficiently wears out the flash storage prematurely ("flash wear").
* **The bottleneck:** Before the optimization, the system used a default 2-second flush interval at a 5 Hz sensor event rate. This meant the worker only collected about **10 rows** per transaction. Writing small 10-row batches over and over dirtied many database index and table pages, generating a massive and unnecessary amount of write traffic (~7.3 GB/day).



---

### 2. The Main Solution: Smarter Batching

To fix this, the optimization aligns the database **flush interval** with the actual event rate:

* **The new profile:** It moves from a 2-second flush to a **10-second flush** with a **50-row batch size**.


* **How it works:** Instead of saving tiny 10-row chunks every 2 seconds, the system waits a little longer to collect about 50 rows at a time.


* **The result:** Because fewer transactions happen overall, the per-row overhead drops dramatically. Logical writes drop from **~7.3 GB/day to ~4.0 GB/day** at 5 Hz (**~16.6 KB/row to ~9.1 KB/row, about a 45% reduction**).



---

### 3. Supporting SQLite & Database Tweaks

SQLite was already running in WAL (Write-Ahead Logging) mode, but several settings were added to prevent hidden write overhead:

* **`PRAGMA temp_store=MEMORY`**: Forces temporary clean-up or query work to stay in RAM rather than writing temporary files to disk.


* **`PRAGMA secure_delete=OFF`**: Prevents the system from unnecessarily overwriting deleted payload bytes with zeros.


* **`PRAGMA auto_vacuum=NONE`**: Stops the database from performing expensive page-moving file rewrites.


* **Index Removal**: The old schema contained a redundant `(source, event_time)` index. Because `source` and `event_type` have a strict 1:1 relationship, this duplicate index was removed in schema v2 to save B-tree write volume.



---

### 4. Storage Safety & Error Prevention

To ensure the constrained device doesn't crash or ruin its storage if things go wrong, safety guardrails were added:

* **The ID Cutoff Retention Query**: Old retention cleanup was replaced with an ID cutoff method (`DELETE ... WHERE id <= ...`), which avoids heavy `NOT IN` statements and smoothly deletes old records.


* **Low-Disk Floor (`EAA_MIN_FREE_BYTES`)**: If the storage volume drops below a specified safety margin, the system stops trying to force writes, drops the batch safely, increments a `dropped_low_disk` counter, and logs it (preventing infinite retry loops on a full disk).


* **Clean Shutdown**: When the service stops, it triggers a final `PRAGMA wal_checkpoint(TRUNCATE)` so that the next startup begins with a clean, small WAL file.



---

### 5. System Hardening & Security

Finally, the deployment is tightly locked down for production Linux environments:

* **Non-Root User**: The service runs under a dedicated, restricted `eaa` system user via `systemd`.


* **Strict Permissions**: Runtime directories are created with `0700` permissions and files/locks are set to `0600`.


* **Path Protection**: Systemd restricts writable paths strictly to the required states (`StateDirectory=eaa` and `RuntimeDirectory=eaa`), and IPC socket paths are checked to ensure they don't exceed Linux Unix-socket length limits.
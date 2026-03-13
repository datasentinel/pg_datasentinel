# pg_datasentinel

`pg_datasentinel` is a PostgreSQL extension that enriches built-in monitoring with data not available from standard catalog views: per-backend memory and temporary file usage (read from `/proc`), in-memory ring buffers of autovacuum/autoanalyze/checkpoint/temporary-file log messages with parsed statistics, XID wraparound risk estimation, and container resource limit detection via cgroups.

It must be loaded via `shared_preload_libraries` and works on Linux.

---

## Features

- **Enhanced activity view** — extends `pg_stat_activity` with real-time memory usage, live temporary file bytes, and (PostgreSQL 18+) the current plan ID for each backend.
- **Vacuum history** — captures autovacuum LOG messages and manual VACUUM INFO messages in a shared-memory ring buffer; parses page/tuple counters, CPU timings, whether the run was aggressive, and whether it was triggered manually.
- **Analyze history** — captures autoanalyze LOG messages and manual ANALYZE INFO messages in a shared-memory ring buffer; parses sample block and extended-statistics counters, and whether the run was triggered automatically.
- **Temporary file history** — captures every `log_temp_files` LOG message with file size and role information.
- **Checkpoint history** — captures every checkpoint and restartpoint completion with detailed I/O and sync timings.
- **Wraparound risk estimation** — samples XID and MXID at each checkpoint (at most once per hour) and provides a single-row view with live distances to the aggressive-vacuum and wraparound limits for both XID and MXID, plus rate-based ETAs that reflect whichever counter is closer to danger.
- **Container resource limits** — reports cgroup v1/v2 CPU quota, memory hard limit, current memory usage, and CPU pressure (PSI avg60, cgroup v2 only) for the PostgreSQL process.

---

## Requirements

- PostgreSQL 14 or later (Linux builds only)
- `pg_config` accessible on `PATH`
- `/proc` filesystem (standard on any Linux host)

---

## Build and Install

```bash
# Build the shared library and SQL files
make USE_PGXS=1

# Install into the PostgreSQL installation
make install USE_PGXS=1
```

If `pg_config` is not on your `PATH`, pass its location explicitly:

```bash
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config
make install USE_PGXS=1 PG_CONFIG=/path/to/pg_config
```

---

## Configuration

### `postgresql.conf`

Add the extension to `shared_preload_libraries` — this is **required**:

```
shared_preload_libraries = 'pg_datasentinel'
```

To capture autovacuum and autoanalyze events, PostgreSQL must be configured to log them:

```
log_autovacuum_min_duration = 0   # log every autovacuum/autoanalyze run
```

To capture temporary file events:

```
log_temp_files = 0                # log every temporary file creation
```

### GUC parameters

| Parameter | Type | Default | Scope | Description |
|---|---|---|---|---|
| `pg_datasentinel.enabled` | `bool` | `on` | `superuser` | Enable or disable log message capture at runtime. When `off`, the hook returns immediately without writing to any ring buffer. |
| `pg_datasentinel.max` | `int` | `5000` | `postmaster` | Capacity of each ring buffer (autovacuum, autoanalyze, temp files, checkpoints, XID snapshots). When full, the oldest entry is overwritten. Requires a server restart. |

---

## Create the Extension

After loading the library and restarting PostgreSQL, create the extension in any database:

```sql
CREATE EXTENSION pg_datasentinel;
```

---

## Views

### `ds_stat_activity`

Extends `pg_stat_activity` with three additional columns populated from `/proc/<pid>/statm` and `/proc/<pid>/fd/`.

| Column | Type | Description |
|---|---|---|
| *(all pg_stat_activity columns)* | | |
| `memory_bytes` | `int8` | Resident set size of the backend process in bytes. `NULL` if `/proc` is not accessible. |
| `temp_bytes` | `int8` | Total bytes used by PostgreSQL temporary files currently open by the backend. `NULL` if `/proc` is not accessible. |
| `plan_id` | `int8` | Current plan identifier (PostgreSQL 18+ only; `NULL` on earlier versions). |

```sql
SELECT pid, usename, state, memory_bytes, temp_bytes
FROM ds_stat_activity
WHERE state = 'active';
```

---

### `ds_vacuum_activity`

A ring buffer of the last `pg_datasentinel.max` vacuum messages (both autovacuum LOG messages and manual VACUUM INFO messages). The message text is parsed to extract structured counters.

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the message was intercepted. |
| `datname` | `text` | Database name. |
| `schemaname` | `text` | Schema of the vacuumed table (`NULL` if not parsed). |
| `relname` | `text` | Name of the vacuumed table (`NULL` if not parsed). |
| `relid` | `oid` | OID of the relation (`NULL` if not resolved). |
| `heap_pages` | `int8` | Total heap pages from `pg_stat_progress_vacuum` at the time of the log message. |
| `pages_removed` | `int8` | Pages removed. |
| `pages_remain` | `int8` | Pages remaining after vacuum. |
| `pages_scanned` | `int8` | Pages scanned. |
| `tuples_removed` | `int8` | Dead tuples removed. |
| `tuples_remain` | `int8` | Live tuples remaining. |
| `user_cpu` | `float8` | User CPU time in seconds. |
| `sys_cpu` | `float8` | System CPU time in seconds. |
| `elapsed` | `float8` | Elapsed wall-clock time in seconds. |
| `aggressive` | `bool` | `true` if this was an aggressive vacuum to prevent wraparound (`automatic aggressive vacuum`). |
| `is_automatic` | `bool` | `true` if triggered by autovacuum; `false` if triggered by a manual `VACUUM` command. |
| `message` | `text` | Full raw log message text. |

```sql
-- Recent vacuum runs, slowest first
SELECT logged_at, schemaname, relname, elapsed, tuples_removed, aggressive, is_automatic
FROM ds_vacuum_activity
ORDER BY elapsed DESC;

SELECT ds_vacuum_activity_reset();
```

---

### `ds_analyze_activity`

A ring buffer of the last `pg_datasentinel.max` analyze messages (both autoanalyze LOG messages and manual ANALYZE INFO messages).

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the message was intercepted. |
| `datname` | `text` | Database name. |
| `schemaname` | `text` | Schema of the analyzed table. |
| `relname` | `text` | Name of the analyzed table. |
| `relid` | `oid` | OID of the relation. |
| `sample_blks_total` | `int8` | Total sample blocks from `pg_stat_progress_analyze`. |
| `ext_stats_total` | `int8` | Total extended statistics computed. |
| `child_tables_total` | `int8` | Total child tables processed. |
| `user_cpu` | `float8` | User CPU time in seconds. |
| `sys_cpu` | `float8` | System CPU time in seconds. |
| `elapsed` | `float8` | Elapsed wall-clock time in seconds. |
| `is_automatic` | `bool` | `true` if triggered by autoanalyze; `false` if triggered by a manual `ANALYZE` command. |
| `message` | `text` | Full raw log message text. |

```sql
SELECT logged_at, schemaname, relname, elapsed, is_automatic
FROM ds_analyze_activity
ORDER BY logged_at DESC
LIMIT 20;

SELECT ds_analyze_activity_reset();
```

---

### `ds_tempfile_activity`

A ring buffer of the last `pg_datasentinel.max` temporary-file LOG messages (emitted by PostgreSQL when `log_temp_files` is set).

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the message was intercepted. |
| `datname` | `text` | Database where the file was created. |
| `username` | `text` | Role that created the file. |
| `pid` | `int4` | Backend PID. |
| `bytes` | `int8` | Size of the temporary file in bytes. |
| `message` | `text` | Full raw LOG message text. |

```sql
SELECT logged_at, username, datname, pg_size_pretty(bytes) AS size
FROM ds_tempfile_activity
ORDER BY bytes DESC;

SELECT ds_tempfile_activity_reset();
```

---

### `ds_checkpoint_activity`

A ring buffer of the last `pg_datasentinel.max` checkpoint and restartpoint completions. Metrics are read directly from `CheckpointStats` — no log text parsing required.

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the checkpoint completed. |
| `is_restartpoint` | `bool` | `true` for a restartpoint, `false` for a regular checkpoint. |
| `start_t` | `timestamptz` | When the checkpoint started. |
| `end_t` | `timestamptz` | When the checkpoint ended. |
| `bufs_written` | `int4` | Dirty buffers written. |
| `segs_added` | `int4` | WAL segments added. |
| `segs_removed` | `int4` | WAL segments removed. |
| `segs_recycled` | `int4` | WAL segments recycled. |
| `write_time` | `float8` | Time spent writing buffers (seconds). |
| `sync_time` | `float8` | Time spent syncing buffers (seconds). |
| `total_time` | `float8` | Total checkpoint duration (seconds). |
| `sync_rels` | `int4` | Number of relations synced. |
| `longest_sync` | `float8` | Slowest single-relation sync (seconds). |
| `average_sync` | `float8` | Average per-relation sync time (seconds). |
| `message` | `text` | Full raw LOG message text. |

```sql
-- Slow checkpoints
SELECT logged_at, total_time, bufs_written, write_time, sync_time
FROM ds_checkpoint_activity
ORDER BY total_time DESC
LIMIT 10;

SELECT ds_checkpoint_activity_reset();
```

---

### `ds_wraparound_risk`

Always returns exactly **one row**. Combines live data from PostgreSQL shared memory with rate information derived from hourly XID/MXID snapshots to estimate wraparound risk for both regular transactions and multixacts.

Snapshots are taken automatically at each checkpoint, but no more than once per hour. The `eta_*` columns take the minimum across XID and MXID so a single value signals the nearest danger regardless of which counter is the bottleneck.

| Column | Type | Description |
|---|---|---|
| `snapshot_count` | `int4` | Number of XID/MXID snapshots stored. |
| `snapshot_span` | `interval` | Time elapsed between the oldest and newest snapshot (`NULL` if fewer than 2 snapshots). |
| `txid_rate_per_sec` | `float8` | Estimated transaction rate in XIDs/second. `NULL` if fewer than 2 snapshots. |
| `current_xid` | `int8` | Current next full transaction ID (epoch-aware). Live. |
| `xids_to_aggressive_vacuum` | `int8` | XIDs remaining before autovacuum starts aggressive freezing. Live. |
| `xids_to_wraparound` | `int8` | XIDs remaining before XID wraparound. Live. |
| `mxid_rate_per_sec` | `float8` | Estimated multixact consumption rate in MXIDs/second. `NULL` if fewer than 2 snapshots. |
| `current_mxid` | `int8` | Current next multixact ID. Live. |
| `mxids_to_aggressive_vacuum` | `int8` | MXIDs remaining before autovacuum starts aggressive MXID freezing. Live. |
| `mxids_to_wraparound` | `int8` | MXIDs remaining before MXID wraparound. Live. |
| `oldest_xid_database` | `text` | Database with the oldest frozen XID. Live. |
| `oldest_mxid_database` | `text` | Database with the oldest frozen MXID (catalog scan for minimum `datminmxid`). |
| `eta_aggressive_vacuum` | `interval` | Estimated time until aggressive vacuuming begins — minimum of XID and MXID ETAs. `NULL` if rate is unknown. |
| `eta_wraparound` | `interval` | Estimated time until wraparound risk — minimum of XID and MXID ETAs. `NULL` if rate is unknown. |

```sql
-- Current wraparound risk at a glance
SELECT
    snapshot_count,
    snapshot_span,
    current_xid, xids_to_aggressive_vacuum, xids_to_wraparound,
    current_mxid, mxids_to_aggressive_vacuum, mxids_to_wraparound,
    eta_aggressive_vacuum,
    eta_wraparound
FROM ds_wraparound_risk;
```

The raw hourly snapshots are available in `ds_xid_snapshots` for trending or external alerting:

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the snapshot was taken. |
| `next_xid` | `int8` | Next full transaction ID (epoch-aware) at the time of the snapshot. |
| `next_mxid` | `int8` | Next multixact ID at the time of the snapshot. |
| `oldest_xid_db` | `oid` | OID of the database holding the oldest frozen XID. |

```sql
SELECT seq, logged_at, next_xid, next_mxid, oldest_xid_db
FROM ds_xid_snapshots
ORDER BY seq;
```

---

### `ds_container_resources`

A single-row view that reports cgroup resource limits and current usage for the PostgreSQL process. Individual columns are `NULL` when the corresponding limit is not set, the cgroup file is absent, or the system does not use cgroups. Supports both cgroup v1 and cgroup v2.

| Column | Type | Description |
|---|---|---|
| `cgroup_version` | `int4` | Cgroup version in use (`1` or `2`). `NULL` if not under cgroups. |
| `cpu_limit` | `float8` | Hard CPU quota in fractional CPUs (e.g., `2.0` = 2 vCPUs). `NULL` if unlimited. |
| `cpu_pressure_pct_60s` | `float8` | CPU pressure (PSI `some avg60`, 0–100%). `NULL` if `cpu.pressure` is not available (cgroup v1 or kernel without PSI). |
| `mem_limit_bytes` | `int8` | Hard memory limit in bytes. `NULL` if unlimited. |
| `mem_used_bytes` | `int8` | Current memory usage in bytes (`memory.current` on cgroup v2, `memory.usage_in_bytes` on cgroup v1). |

```sql
SELECT
    cgroup_version,
    cpu_limit,
    cpu_pressure_pct_60s,
    pg_size_pretty(mem_limit_bytes) AS memory_limit,
    pg_size_pretty(mem_used_bytes)  AS memory_used
FROM ds_container_resources;
```

---

### `ds_activity_summary`

Always returns exactly **one row** with a three-column summary (count, oldest, latest) for each of the four ring buffers. Useful for a quick health-check dashboard or alerting.

| Column | Type | Description |
|---|---|---|
| `vacuum_count` | `int4` | Number of vacuum entries currently in the buffer. |
| `vacuum_oldest` | `timestamptz` | Timestamp of the oldest captured vacuum. `NULL` if the buffer is empty. |
| `vacuum_latest` | `timestamptz` | Timestamp of the most recent captured vacuum. |
| `analyze_count` | `int4` | Number of analyze entries in the buffer. |
| `analyze_oldest` | `timestamptz` | Timestamp of the oldest captured analyze. |
| `analyze_latest` | `timestamptz` | Timestamp of the most recent captured analyze. |
| `checkpoint_count` | `int4` | Number of checkpoint entries in the buffer. |
| `checkpoint_oldest` | `timestamptz` | Timestamp of the oldest captured checkpoint. |
| `checkpoint_latest` | `timestamptz` | Timestamp of the most recent captured checkpoint. |
| `tempfile_count` | `int4` | Number of temporary-file entries in the buffer. |
| `tempfile_oldest` | `timestamptz` | Timestamp of the oldest captured temporary-file event. |
| `tempfile_latest` | `timestamptz` | Timestamp of the most recent captured temporary-file event. |

```sql
SELECT * FROM ds_activity_summary;
```

---

## Utility Functions

| Function | Description |
|---|---|
| `ds_vacuum_activity_reset()` | Clear the vacuum ring buffer. |
| `ds_analyze_activity_reset()` | Clear the analyze ring buffer. |
| `ds_tempfile_activity_reset()` | Clear the temporary-file ring buffer. |
| `ds_checkpoint_activity_reset()` | Clear the checkpoint ring buffer. |
| `ds_activity_reset_all()` | Clear all ring buffers at once (autovacuum, autoanalyze, temp files, checkpoints, XID snapshots). |

```sql
-- Pause all capture, flush all buffers, then resume
SET pg_datasentinel.enabled = off;
SELECT ds_activity_reset_all();
SET pg_datasentinel.enabled = on;
```

---

## Access Control

The extension creates a `ds_reader` role at install time (if it does not already exist) and grants `SELECT` on all views to it:

```sql
GRANT ds_reader TO monitoring_user;
```

This lets non-superuser accounts query all observability views without requiring additional `GRANT` statements.

---

## Testing

```bash
make installcheck USE_PGXS=1
```

The `regress.conf` file used for the test cluster sets:

```
shared_preload_libraries = 'pg_datasentinel'
log_autovacuum_min_duration = 0
autovacuum_naptime = 1s
```
### Unit tests for internal parsing functions

A companion test module exercises the C-level message-parsing functions (`pgds_parse_table_from_message`, `pgds_parse_vacuum_stats`, `pgds_parse_cpu_stats`) without requiring a live PostgreSQL cluster:

```bash
cd test
make USE_PGXS=1
make installcheck USE_PGXS=1
```

---

## Architecture

```
shared_preload_libraries
        |
        v
   _PG_init()
        |
        +-- pg_datasentinel.enabled  (GUC, runtime toggle)
        +-- pg_datasentinel.max      (GUC, ring buffer capacity)
        +-- shmem_request_hook  -> allocate 5 ring buffers in shared memory
        +-- shmem_startup_hook  -> initialise ring buffer headers + LWLocks
        +-- emit_log_hook       -> intercept LOG messages
                                      |
             +------------------------+------------------------+----------+
             |                        |                        |          |
        autovacuum               autoanalyze             temp files  checkpoints
        ring buffer              ring buffer             ring buffer ring buffer
                                                                          |
                                                                    (at most 1/hour)
                                                                          |
                                                                   XID snapshots
                                                                   ring buffer
```

Each ring buffer is a fixed-size circular array stored in PostgreSQL shared memory, protected by a dedicated LWLock. When the buffer is full, the oldest entry is silently overwritten so that the most recent `pg_datasentinel.max` events are always retained. No background worker is required.

---

## License

PostgreSQL License (BSD-like). See [LICENSE](LICENSE).

# pg_datasentinel

`pg_datasentinel` is a PostgreSQL extension that enriches built-in monitoring with data not available from standard catalog views: per-backend memory and temporary file usage (read from `/proc`), in-memory ring buffers of autovacuum/autoanalyze log messages with parsed statistics, and container resource limit detection via cgroups.

It must be loaded via `shared_preload_libraries` and works on Linux.

---

## Features

- **Enhanced activity view** — extends `pg_stat_activity` with real-time memory usage, live temporary file bytes, and (PostgreSQL 18+) the current plan ID for each backend.
- **Autovacuum history** — captures every autovacuum LOG message in a shared-memory ring buffer and parses page/tuple counters and CPU timings out of the message text.
- **Autoanalyze history** — same as above for autoanalyze messages, with sample block and extended-statistics counters.
- **Temporary file history** — captures every `log_temp_files` LOG message with file size and role information.
- **Container resource limits** — reports cgroup v1/v2 CPU quota and memory hard limit for the PostgreSQL process.

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

### GUC parameter

| Parameter | Default | Min | Scope | Description |
|---|---|---|---|---|
| `pg_datasentinel.max` | `5000` | `100` | `postmaster` | Capacity of each ring buffer (autovacuum, autoanalyze, temp files). When full, the oldest entry is overwritten. |

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

### `ds_autovacuum_activity`

A ring buffer of the last `pg_datasentinel.max` autovacuum LOG messages. The message text is parsed to extract structured counters.

| Column | Type | Description |
|---|---|---|
| `seq` | `int4` | Ordinal position in the buffer (1 = oldest). |
| `logged_at` | `timestamptz` | Wall-clock time the message was intercepted. |
| `datname` | `text` | Database name. |
| `schemaname` | `text` | Schema of the vacuumed table (`NULL` if not parsed). |
| `relname` | `text` | Name of the vacuumed table (`NULL` if not parsed). |
| `relid` | `oid` | OID of the relation (`NULL` if not resolved). |
| `heap_pages` | `int8` | Total heap pages from `pg_stat_progress_vacuum` at the time of the log message. |
| `pages_removed` | `int8` | Pages removed (from log message). |
| `pages_remain` | `int8` | Pages remaining after vacuum. |
| `pages_scanned` | `int8` | Pages scanned. |
| `tuples_removed` | `int8` | Dead tuples removed. |
| `tuples_remain` | `int8` | Live tuples remaining. |
| `user_cpu` | `float8` | User CPU time in seconds. |
| `sys_cpu` | `float8` | System CPU time in seconds. |
| `elapsed` | `float8` | Elapsed wall-clock time in seconds. |
| `message` | `text` | Full raw LOG message text. |

```sql
-- Recent autovacuum runs, slowest first
SELECT logged_at, schemaname, relname, elapsed, tuples_removed
FROM ds_autovacuum_activity
ORDER BY elapsed DESC;

-- Reset the buffer
SELECT ds_autovacuum_activity_reset();
```

---

### `ds_autoanalyze_activity`

A ring buffer of the last `pg_datasentinel.max` autoanalyze LOG messages.

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
| `message` | `text` | Full raw LOG message text. |

```sql
-- Tables that triggered autoanalyze recently
SELECT logged_at, schemaname, relname, elapsed
FROM ds_autoanalyze_activity
ORDER BY logged_at DESC
LIMIT 20;

-- Reset the buffer
SELECT ds_autoanalyze_activity_reset();
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
-- Largest temporary files
SELECT logged_at, username, datname, pg_size_pretty(bytes) AS size
FROM ds_tempfile_activity
ORDER BY bytes DESC;

-- Reset the buffer
SELECT ds_tempfile_activity_reset();
```

---

### `ds_container_resources`

A single-row view that reports the cgroup resource limits applied to the PostgreSQL process. All columns are `NULL` when no cgroup limit is configured or when the system does not use cgroups.

Supports both cgroup v1 and cgroup v2.

| Column | Type | Description |
|---|---|---|
| `cgroup_version` | `int4` | Cgroup version in use (`1` or `2`). `NULL` if not running under cgroups. |
| `cpu_limit` | `float8` | Hard CPU quota in fractional CPUs (e.g., `2.0` = 2 vCPUs). `NULL` if unlimited. |
| `mem_limit_bytes` | `int8` | Hard memory limit in bytes. `NULL` if unlimited. |

```sql
SELECT cgroup_version,
       cpu_limit,
       pg_size_pretty(mem_limit_bytes) AS memory_limit
FROM ds_container_resources;
```

---

## Testing

The regression suite creates the extension inside a temporary PostgreSQL instance and runs four test scripts: `init`, `autovacuum`, `autoanalyze`, and `tempfiles`.

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

A companion test module exercises the C-level message-parsing functions (`pgds_parse_table_from_message`, `pgds_parse_vacuum_stats`, `pgds_parse_cpu_stats`) independently of a running PostgreSQL server:

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
        +-- DefineCustomIntVariable("pg_datasentinel.max")
        +-- shmem_request_hook  -> allocate 3 ring buffers in shared memory
        +-- shmem_startup_hook  -> initialise ring buffer headers + LWLocks
        +-- emit_log_hook       -> intercept LOG messages
                                      |
                    +-----------------+-----------------+
                    |                 |                 |
             autovacuum           autoanalyze       temp files
             ring buffer          ring buffer       ring buffer
```

Each ring buffer is a fixed-size circular array stored in PostgreSQL shared memory, protected by a dedicated LWLock. When the buffer is full, the oldest entry is silently overwritten so that the most recent `pg_datasentinel.max` events are always retained. No background worker is required.

---

## License

PostgreSQL License (BSD-like). See [LICENSE](LICENSE).

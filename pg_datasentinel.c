#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include <limits.h>
#include "funcapi.h"
#include "pgstat.h"
#include "utils/backend_status.h"
#include <sys/stat.h>
#include "common/file_utils.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "nodes/makefuncs.h"
#include "utils/timestamp.h"
#include "commands/dbcommands.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "tcop/utility.h"
#include "access/xlog.h"
#include "access/transam.h"
#include "access/multixact.h"
#include "access/heapam.h"
#include "catalog/pg_database.h"
#include "postmaster/autovacuum.h"
#include "pgds_linux.h"
#include "pgds_utils.h"

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

#define PROC_VIRTUAL_FS    "/proc"
#define DS_STAT_IDS_COLS		4
#define DS_AUTOVACUUM_COLS		17	/* seq, logged_at, datname, schemaname, relname, relid,
								 * heap_pages, pages_removed, pages_remain, pages_scanned,
								 * tuples_removed, tuples_remain, user_cpu, sys_cpu, elapsed,
								 * aggressive, message */
#define DS_ANALYZE_COLS			13	/* seq, logged_at, datname, schemaname, relname, relid,
								 * sample_blks_total, ext_stats_total, child_tables_total,
								 * user_cpu, sys_cpu, elapsed, message */
#define PGDS_AUTOVACUUM_MSG_LEN	3072	/* max length of a stored autovacuum log message */
#define PGDS_AUTOANALYZE_MSG_LEN	1024	/* max length of a stored autoanalyze log message */
#define DS_TEMPFILE_COLS		7		/* seq, logged_at, datname, username, pid, bytes, message */
#define PGDS_TEMPFILE_MSG_LEN	512		/* max length of a stored temp file log message */
#define DS_CHECKPOINT_COLS		16		/* seq, logged_at, is_restartpoint,
										 * start_t, end_t, bufs_written,
										 * segs_added, segs_removed, segs_recycled,
										 * write_time, sync_time, total_time,
										 * sync_rels, longest_sync, average_sync,
										 * message */
#define PGDS_CHECKPOINT_MSG_LEN	512		/* max length of a stored checkpoint log message */
#define DS_XID_SNAPSHOT_COLS	5		/* seq, logged_at, next_xid, next_mxid, oldest_xid_db */
#define DS_WRAPAROUND_RISK_COLS	17		/* snapshot_count,
										 * oldest_snapshot_at, newest_snapshot_at,
										 * current_xid, xids_to_aggressive_vacuum,
										 * xids_to_wraparound, txid_rate_per_sec,
										 * oldest_xid_database,
										 * eta_aggressive_vacuum, eta_wraparound,
										 * current_mxid, mxids_to_aggressive_vacuum,
										 * mxids_to_wraparound, mxid_rate_per_sec,
										 * oldest_mxid_database,
										 * eta_aggressive_vacuum_mxid,
										 * eta_wraparound_mxid */

/*
 * One slot in the ring buffer.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsAutovacuumEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];	/* database name */
	char		schemaname[NAMEDATALEN];	/* schema name */
	char		relname[NAMEDATALEN];	/* table name */
	Oid			reloid;					/* OID of the relation */
	/* vacuum progress counters (from pg_stat_progress_vacuum param2) */
	int64		heap_pages;
	/* vacuum stats parsed from the log message */
	int64		pages_removed;
	int64		pages_remain;
	int64		pages_scanned;
	int64		tuples_removed;
	int64		tuples_remain;
	/* CPU timings parsed from the log message */
	double		user_cpu;
	double		sys_cpu;
	double		elapsed;
	bool		aggressive;				/* true if "automatic aggressive vacuum" */
	char		message[PGDS_AUTOVACUUM_MSG_LEN];
} PgdsAutovacuumEntry;

/*
 * Shared-memory header for the FIFO ring buffer.
 *
 * head  – index of the oldest (next-to-be-read) slot.
 * tail  – index of the next write slot.
 * count – number of valid entries currently held (0 … max).
 * max   – capacity, fixed at postmaster start from max_actions.
 *
 * When the buffer is full a new write overwrites the oldest entry and
 * advances head, so the newest max_actions messages are always retained.
 */
typedef struct PgdsAutovacuumSharedState
{
	LWLock	   *lock;			/* protects all fields below */
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsAutovacuumEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsAutovacuumSharedState;

/*
 * One slot in the analyze ring buffer.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsAutoanalyzeEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];	/* database name */
	char		schemaname[NAMEDATALEN];	/* schema name */
	char		relname[NAMEDATALEN];	/* table name */
	Oid			reloid;					/* OID of the relation */
	/* analyze progress counters (from pg_stat_progress_analyze param2..param8) */
	int64		sample_blks_total;
	int64		ext_stats_total;
	int64		child_tables_total;
	/* CPU timings parsed from the log message */
	double		user_cpu;
	double		sys_cpu;
	double		elapsed;
	char		message[PGDS_AUTOANALYZE_MSG_LEN];
} PgdsAutoanalyzeEntry;

typedef struct PgdsAutoanalyzeSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsAutoanalyzeEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsAutoanalyzeSharedState;

/*
 * One slot in the temp-file ring buffer.
 * Captures each LOG message emitted when a temporary file is deleted
 * (controlled by log_temp_files).
 */
typedef struct PgdsTempfileEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];	/* database name */
	char		username[NAMEDATALEN];	/* role name */
	int			pid;					/* backend PID */
	int64		bytes;					/* temp file size in bytes */
	char		message[PGDS_TEMPFILE_MSG_LEN];
} PgdsTempfileEntry;

typedef struct PgdsTempfileSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsTempfileEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsTempfileSharedState;

/*
 * One slot in the checkpoint ring buffer.
 * Metrics are read directly from the CheckpointStats global (no text parsing).
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsCheckpointEntry
{
	TimestampTz	logged_at;			/* wall-clock time the message was intercepted */
	bool		is_restartpoint;	/* true = restartpoint, false = regular checkpoint */
	TimestampTz	start_t;			/* CheckpointStats.ckpt_start_t */
	TimestampTz	end_t;				/* CheckpointStats.ckpt_end_t */
	int32		bufs_written;		/* CheckpointStats.ckpt_bufs_written */
	int32		segs_added;			/* CheckpointStats.ckpt_segs_added */
	int32		segs_removed;		/* CheckpointStats.ckpt_segs_removed */
	int32		segs_recycled;		/* CheckpointStats.ckpt_segs_recycled */
	double		write_time;			/* seconds: ckpt_write_t to ckpt_sync_t */
	double		sync_time;			/* seconds: ckpt_sync_t to ckpt_sync_end_t */
	double		total_time;			/* seconds: ckpt_start_t to ckpt_end_t */
	int32		sync_rels;			/* CheckpointStats.ckpt_sync_rels */
	double		longest_sync;		/* seconds (converted from microseconds) */
	double		average_sync;		/* seconds (converted from microseconds) */
	char		message[PGDS_CHECKPOINT_MSG_LEN];
} PgdsCheckpointEntry;

typedef struct PgdsCheckpointSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsCheckpointEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsCheckpointSharedState;

/*
 * One slot in the XID snapshot ring buffer.
 * Captured at most once per hour, triggered by every checkpoint.
 * Stores the epoch-aware XID and MXID so that the view can compute
 * a transaction-rate and estimate time-to-wraparound.
 */
typedef struct PgdsXidSnapshotEntry
{
	TimestampTz	logged_at;		/* wall-clock time of the snapshot */
	int64		next_xid;		/* U64FromFullTransactionId(ReadNextFullTransactionId()) */
	int64		next_mxid;		/* (int64) ReadNextMultiXactId() */
	Oid			oldest_xid_db;	/* TransamVariables->oldestXidDB */
} PgdsXidSnapshotEntry;

typedef struct PgdsXidSnapshotSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsXidSnapshotEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsXidSnapshotSharedState;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/* Pointers to the shared-memory ring buffers */
static PgdsAutovacuumSharedState *pgds_autovacuum = NULL;
static PgdsAutoanalyzeSharedState *pgds_autoanalyze = NULL;
static PgdsTempfileSharedState *pgds_tempfile = NULL;
static PgdsCheckpointSharedState *pgds_checkpoint = NULL;
static PgdsXidSnapshotSharedState *pgds_xid_snapshot = NULL;

static Size pgds_memsize(void);
static Size pgds_analyze_memsize(void);
static Size pgds_tempfile_memsize(void);
static Size pgds_checkpoint_memsize(void);
static Size pgds_xid_snapshot_memsize(void);
static void pgds_shmem_request(void);
static void pgds_shmem_startup(void);
static void pgds_emit_log(ErrorData *edata);
static void pgds_log_autovacuum(ErrorData *edata);
static void pgds_log_autoanalyze(ErrorData *edata);
static void pgds_log_tempfile(ErrorData *edata);
static void pgds_log_checkpoint(ErrorData *edata, bool is_restartpoint);
static void pgds_log_xid_snapshot(void);


static int	max_actions;		/* max # actions to track */
static bool	pgds_enabled;		/* enable/disable log capture at runtime */

PG_FUNCTION_INFO_V1(ds_stat_pids);
PG_FUNCTION_INFO_V1(ds_autovacuum_msgs);
PG_FUNCTION_INFO_V1(ds_autovacuum_activity_reset);
PG_FUNCTION_INFO_V1(ds_autoanalyze_msgs);
PG_FUNCTION_INFO_V1(ds_autoanalyze_activity_reset);
PG_FUNCTION_INFO_V1(ds_tempfile_msgs);
PG_FUNCTION_INFO_V1(ds_tempfile_activity_reset);
PG_FUNCTION_INFO_V1(ds_checkpoint_msgs);
PG_FUNCTION_INFO_V1(ds_checkpoint_activity_reset);
PG_FUNCTION_INFO_V1(ds_activity_reset_all);
PG_FUNCTION_INFO_V1(ds_xid_snapshot_msgs);
PG_FUNCTION_INFO_V1(ds_wraparound_risk_info);
PG_FUNCTION_INFO_V1(ds_container_resource_info);

#define DS_CGROUP_COLS	3	/* cgroup_version, cpu_limit, mem_limit_bytes */


/*
 * ds_autovacuum_msgs: SRF backing the ds_autovacuum_activity view.
 *
 * Iterates the ring buffer under LW_SHARED and emits one row per captured
 * vacuum/analyze LOG message:
 *   seq     int4   – ordinal position (1 = oldest, count = newest)
 *   message text   – the raw log message text
 */
Datum
ds_autovacuum_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_autovacuum == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_autovacuum->lock, LW_SHARED);

	count = pgds_autovacuum->count;
	head = pgds_autovacuum->head;
	max = pgds_autovacuum->max;

	for (a = 0; a < count; a++)
	{
		int			idx = (head + a) % max;
		int         i = 0;
		Datum		values[DS_AUTOVACUUM_COLS];
		bool		nulls[DS_AUTOVACUUM_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(pgds_autovacuum->entries[idx].logged_at);
		values[i++] = CStringGetTextDatum(pgds_autovacuum->entries[idx].datname);
		if (pgds_autovacuum->entries[idx].schemaname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_autovacuum->entries[idx].schemaname);
		else
			nulls[i++] = true;
		if (pgds_autovacuum->entries[idx].relname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_autovacuum->entries[idx].relname);
		else
			nulls[i++] = true;
		if (pgds_autovacuum->entries[idx].reloid != InvalidOid)
			values[i++] = ObjectIdGetDatum(pgds_autovacuum->entries[idx].reloid);
		else
			nulls[i++] = true;
		/* vacuum progress counters */
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].heap_pages);
		/* vacuum stats from log message */
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].pages_removed);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].pages_remain);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].pages_scanned);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].tuples_removed);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].tuples_remain);
		/* CPU timings */
		values[i++] = Float8GetDatum(pgds_autovacuum->entries[idx].user_cpu);
		values[i++] = Float8GetDatum(pgds_autovacuum->entries[idx].sys_cpu);
		values[i++] = Float8GetDatum(pgds_autovacuum->entries[idx].elapsed);
		values[i++] = BoolGetDatum(pgds_autovacuum->entries[idx].aggressive);
		values[i++] = CStringGetTextDatum(pgds_autovacuum->entries[idx].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_autovacuum->lock);

	return (Datum) 0;
}

/*
 * ds_autovacuum_activity_reset: discard all entries from the ring buffer.
 */
Datum
ds_autovacuum_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_autovacuum == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_autovacuum->lock, LW_EXCLUSIVE);
	pgds_autovacuum->head = 0;
	pgds_autovacuum->tail = 0;
	pgds_autovacuum->count = 0;
	LWLockRelease(pgds_autovacuum->lock);

	PG_RETURN_VOID();
}


/*
 * ds_autoanalyze_msgs: SRF backing the ds_autoanalyze_activity view.
 *
 * Iterates the analyze ring buffer under LW_SHARED and emits one row per
 * captured autoanalyze LOG message:
 *   seq        int4        – ordinal position (1 = oldest, count = newest)
 *   logged_at  timestamptz
 *   datname    text
 *   schemaname text
 *   relname    text
 *   relid      oid
 */
Datum
ds_autoanalyze_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_autoanalyze == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_autoanalyze->lock, LW_SHARED);

	count = pgds_autoanalyze->count;
	head  = pgds_autoanalyze->head;
	max   = pgds_autoanalyze->max;

	for (a = 0; a < count; a++)
	{
		int			idx = (head + a) % max;
		int			i = 0;
		Datum		values[DS_ANALYZE_COLS];
		bool		nulls[DS_ANALYZE_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(pgds_autoanalyze->entries[idx].logged_at);
		values[i++] = CStringGetTextDatum(pgds_autoanalyze->entries[idx].datname);
		if (pgds_autoanalyze->entries[idx].schemaname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_autoanalyze->entries[idx].schemaname);
		else
			nulls[i++] = true;
		if (pgds_autoanalyze->entries[idx].relname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_autoanalyze->entries[idx].relname);
		else
			nulls[i++] = true;
		if (pgds_autoanalyze->entries[idx].reloid != InvalidOid)
			values[i++] = ObjectIdGetDatum(pgds_autoanalyze->entries[idx].reloid);
		else
			nulls[i++] = true;
		/* analyze progress counters */
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].sample_blks_total);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].ext_stats_total);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].child_tables_total);
		/* CPU timings */
		values[i++] = Float8GetDatum(pgds_autoanalyze->entries[idx].user_cpu);
		values[i++] = Float8GetDatum(pgds_autoanalyze->entries[idx].sys_cpu);
		values[i++] = Float8GetDatum(pgds_autoanalyze->entries[idx].elapsed);
		values[i++] = CStringGetTextDatum(pgds_autoanalyze->entries[idx].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_autoanalyze->lock);

	return (Datum) 0;
}

/*
 * ds_autoanalyze_activity_reset: discard all entries from the analyze ring buffer.
 */
Datum
ds_autoanalyze_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_autoanalyze == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_autoanalyze->lock, LW_EXCLUSIVE);
	pgds_autoanalyze->head  = 0;
	pgds_autoanalyze->tail  = 0;
	pgds_autoanalyze->count = 0;
	LWLockRelease(pgds_autoanalyze->lock);

	PG_RETURN_VOID();
}


/*
 * ds_tempfile_msgs: SRF backing the ds_tempfile_activity view.
 *
 * Iterates the temp-file ring buffer under LW_SHARED and emits one row per
 * captured temporary-file LOG message:
 *   seq       int4        – ordinal position (1 = oldest, count = newest)
 *   logged_at timestamptz
 *   datname   text
 *   username  text
 *   pid       int4
 *   bytes     int8
 *   message   text
 */
Datum
ds_tempfile_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_tempfile == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_tempfile->lock, LW_SHARED);

	count = pgds_tempfile->count;
	head  = pgds_tempfile->head;
	max   = pgds_tempfile->max;

	for (a = 0; a < count; a++)
	{
		int			idx = (head + a) % max;
		int			i = 0;
		Datum		values[DS_TEMPFILE_COLS];
		bool		nulls[DS_TEMPFILE_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(pgds_tempfile->entries[idx].logged_at);
		values[i++] = CStringGetTextDatum(pgds_tempfile->entries[idx].datname);
		values[i++] = CStringGetTextDatum(pgds_tempfile->entries[idx].username);
		values[i++] = Int32GetDatum(pgds_tempfile->entries[idx].pid);
		values[i++] = Int64GetDatum(pgds_tempfile->entries[idx].bytes);
		values[i++] = CStringGetTextDatum(pgds_tempfile->entries[idx].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_tempfile->lock);

	return (Datum) 0;
}

/*
 * ds_tempfile_activity_reset: discard all entries from the temp-file ring buffer.
 */
Datum
ds_tempfile_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_tempfile == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_tempfile->lock, LW_EXCLUSIVE);
	pgds_tempfile->head  = 0;
	pgds_tempfile->tail  = 0;
	pgds_tempfile->count = 0;
	LWLockRelease(pgds_tempfile->lock);

	PG_RETURN_VOID();
}


/*
 * ds_checkpoint_msgs: return all checkpoint/restartpoint entries from the ring buffer.
 */
Datum
ds_checkpoint_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_checkpoint == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_checkpoint->lock, LW_SHARED);

	count = pgds_checkpoint->count;
	head  = pgds_checkpoint->head;
	max   = pgds_checkpoint->max;

	for (a = 0; a < count; a++)
	{
		int					idx = (head + a) % max;
		int					i = 0;
		Datum				values[DS_CHECKPOINT_COLS];
		bool				nulls[DS_CHECKPOINT_COLS];
		PgdsCheckpointEntry *e = &pgds_checkpoint->entries[idx];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(e->logged_at);
		values[i++] = BoolGetDatum(e->is_restartpoint);
		values[i++] = TimestampTzGetDatum(e->start_t);
		values[i++] = TimestampTzGetDatum(e->end_t);
		values[i++] = Int32GetDatum(e->bufs_written);
		values[i++] = Int32GetDatum(e->segs_added);
		values[i++] = Int32GetDatum(e->segs_removed);
		values[i++] = Int32GetDatum(e->segs_recycled);
		values[i++] = Float8GetDatum(e->write_time);
		values[i++] = Float8GetDatum(e->sync_time);
		values[i++] = Float8GetDatum(e->total_time);
		values[i++] = Int32GetDatum(e->sync_rels);
		values[i++] = Float8GetDatum(e->longest_sync);
		values[i++] = Float8GetDatum(e->average_sync);
		values[i++] = CStringGetTextDatum(e->message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_checkpoint->lock);

	return (Datum) 0;
}

/*
 * ds_checkpoint_activity_reset: discard all entries from the checkpoint ring buffer.
 */
Datum
ds_checkpoint_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_checkpoint == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_checkpoint->lock, LW_EXCLUSIVE);
	pgds_checkpoint->head  = 0;
	pgds_checkpoint->tail  = 0;
	pgds_checkpoint->count = 0;
	LWLockRelease(pgds_checkpoint->lock);

	PG_RETURN_VOID();
}

/*
 * ds_activity_reset_all: discard all entries from every ring buffer at once.
 */
Datum
ds_activity_reset_all(PG_FUNCTION_ARGS)
{
	if (pgds_autovacuum != NULL)
	{
		LWLockAcquire(pgds_autovacuum->lock, LW_EXCLUSIVE);
		pgds_autovacuum->head  = 0;
		pgds_autovacuum->tail  = 0;
		pgds_autovacuum->count = 0;
		LWLockRelease(pgds_autovacuum->lock);
	}

	if (pgds_autoanalyze != NULL)
	{
		LWLockAcquire(pgds_autoanalyze->lock, LW_EXCLUSIVE);
		pgds_autoanalyze->head  = 0;
		pgds_autoanalyze->tail  = 0;
		pgds_autoanalyze->count = 0;
		LWLockRelease(pgds_autoanalyze->lock);
	}

	if (pgds_tempfile != NULL)
	{
		LWLockAcquire(pgds_tempfile->lock, LW_EXCLUSIVE);
		pgds_tempfile->head  = 0;
		pgds_tempfile->tail  = 0;
		pgds_tempfile->count = 0;
		LWLockRelease(pgds_tempfile->lock);
	}

	if (pgds_checkpoint != NULL)
	{
		LWLockAcquire(pgds_checkpoint->lock, LW_EXCLUSIVE);
		pgds_checkpoint->head  = 0;
		pgds_checkpoint->tail  = 0;
		pgds_checkpoint->count = 0;
		LWLockRelease(pgds_checkpoint->lock);
	}

	if (pgds_xid_snapshot != NULL)
	{
		LWLockAcquire(pgds_xid_snapshot->lock, LW_EXCLUSIVE);
		pgds_xid_snapshot->head  = 0;
		pgds_xid_snapshot->tail  = 0;
		pgds_xid_snapshot->count = 0;
		LWLockRelease(pgds_xid_snapshot->lock);
	}

	PG_RETURN_VOID();
}


/*
 * ds_xid_snapshot_msgs: SRF returning the raw XID snapshot ring buffer.
 * Intended for diagnostics; the ds_wraparound_risk view is the primary interface.
 */
Datum
ds_xid_snapshot_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_xid_snapshot == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_xid_snapshot->lock, LW_SHARED);

	count = pgds_xid_snapshot->count;
	head  = pgds_xid_snapshot->head;
	max   = pgds_xid_snapshot->max;

	for (a = 0; a < count; a++)
	{
		int					idx = (head + a) % max;
		int					i = 0;
		Datum				values[DS_XID_SNAPSHOT_COLS];
		bool				nulls[DS_XID_SNAPSHOT_COLS];
		PgdsXidSnapshotEntry *e = &pgds_xid_snapshot->entries[idx];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(e->logged_at);
		values[i++] = Int64GetDatum(e->next_xid);
		values[i++] = Int64GetDatum(e->next_mxid);
		if (OidIsValid(e->oldest_xid_db))
			values[i++] = ObjectIdGetDatum(e->oldest_xid_db);
		else
			nulls[i++] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_xid_snapshot->lock);

	return (Datum) 0;
}

/*
 * Build a palloc'd Interval from a duration expressed in seconds.
 * Negative values are clamped to zero (should not happen in practice).
 */
static Interval *
secs_to_interval(double secs)
{
	Interval   *iv = (Interval *) palloc(sizeof(Interval));

	if (secs < 0)
		secs = 0;
	iv->month = 0;
	iv->day   = (int32) (secs / 86400.0);
	iv->time  = (int64) ((secs - iv->day * 86400.0) * USECS_PER_SEC);
	return iv;
}

/*
 * get_oldest_mxid_database: scan pg_database to find the OID of the
 * database with the minimum datminmxid.  Requires catalog access, so must
 * only be called from a regular backend.
 */
static Oid
get_oldest_mxid_database(void)
{
	Relation		rel;
	TableScanDesc	scan;
	HeapTuple		tup;
	MultiXactId		oldest_mxid = MaxMultiXactId;
	Oid				result = InvalidOid;
	bool			first = true;

	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tup);

		if (first || MultiXactIdPrecedes(dbform->datminmxid, oldest_mxid))
		{
			oldest_mxid = dbform->datminmxid;
			result = dbform->oid;
			first = false;
		}
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);
	return result;
}

/*
 * ds_wraparound_risk_info: always returns exactly one composite row.
 *
 * Column layout (must match OUT parameters in pg_datasentinel--0.1.0.sql):
 *   [0]  snapshot_count
 *   [1]  oldest_snapshot_at
 *   [2]  newest_snapshot_at
 *   [3]  current_xid
 *   [4]  xids_to_aggressive_vacuum
 *   [5]  xids_to_wraparound
 *   [6]  txid_rate_per_sec
 *   [7]  oldest_xid_database
 *   [8]  eta_aggressive_vacuum
 *   [9]  eta_wraparound
 *   [10] current_mxid
 *   [11] mxids_to_aggressive_vacuum
 *   [12] mxids_to_wraparound
 *   [13] mxid_rate_per_sec
 *   [14] oldest_mxid_database
 *   [15] eta_aggressive_vacuum_mxid
 *   [16] eta_wraparound_mxid
 */
Datum
ds_wraparound_risk_info(PG_FUNCTION_ARGS)
{
	TupleDesc				tupdesc;
	Datum					values[DS_WRAPAROUND_RISK_COLS];
	bool					nulls[DS_WRAPAROUND_RISK_COLS];
	HeapTuple				tuple;

	/* XID state */
	FullTransactionId		cur_full_xid;
	TransactionId			cur_xid;
	int64					xids_to_vac;
	int64					xids_to_wrap;

	/* MXID state */
	MultiXactId				cur_mxid;
	MultiXactId				oldest_mxact;
	MultiXactId				mxid_vac_limit;
	MultiXactId				mxid_wrap_limit;
	int64					mxids_to_vac;
	int64					mxids_to_wrap;

	/* Ring-buffer snapshot data */
	int						snap_count = 0;
	PgdsXidSnapshotEntry	oldest_e,
							newest_e;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, true, sizeof(nulls));
	memset(values, 0, sizeof(values));

	/* Read ring-buffer state once under a single lock acquisition */
	if (pgds_xid_snapshot != NULL)
	{
		LWLockAcquire(pgds_xid_snapshot->lock, LW_SHARED);
		snap_count = pgds_xid_snapshot->count;
		if (snap_count >= 2)
		{
			oldest_e = pgds_xid_snapshot->entries[pgds_xid_snapshot->head];
			newest_e = pgds_xid_snapshot->entries[
				(pgds_xid_snapshot->tail + pgds_xid_snapshot->max - 1)
				% pgds_xid_snapshot->max];
		}
		LWLockRelease(pgds_xid_snapshot->lock);
	}

	/*
	 * XID distances — from TransamVariables (updated by autovacuum).
	 * Arithmetic is modular uint32: cast to int32 gives signed distance.
	 */
	cur_full_xid = ReadNextFullTransactionId();
	cur_xid      = XidFromFullTransactionId(cur_full_xid);
	xids_to_vac  = (int64) (int32) (TransamVariables->xidVacLimit  - cur_xid);
	xids_to_wrap = (int64) (int32) (TransamVariables->xidWrapLimit - cur_xid);

	/*
	 * MXID distances — recompute limits with the same formulas used by
	 * SetMultiXactIdLimit(), using exported GUC and ReadMultiXactIdRange().
	 */
	ReadMultiXactIdRange(&oldest_mxact, &cur_mxid);
	mxid_vac_limit  = oldest_mxact + (MultiXactId) autovacuum_multixact_freeze_max_age;
	if (mxid_vac_limit < FirstMultiXactId)
		mxid_vac_limit += FirstMultiXactId;
	mxid_wrap_limit = oldest_mxact + (MaxMultiXactId >> 1);
	if (mxid_wrap_limit < FirstMultiXactId)
		mxid_wrap_limit += FirstMultiXactId;
	mxids_to_vac  = (int64) (int32) (mxid_vac_limit  - cur_mxid);
	mxids_to_wrap = (int64) (int32) (mxid_wrap_limit - cur_mxid);

	/* [0] snapshot_count */
	values[0] = Int32GetDatum(snap_count);
	nulls[0]  = false;

	/* [1] oldest_snapshot_at, [2] newest_snapshot_at */
	if (snap_count >= 2)
	{
		values[1] = TimestampTzGetDatum(oldest_e.logged_at);
		nulls[1]  = false;
		values[2] = TimestampTzGetDatum(newest_e.logged_at);
		nulls[2]  = false;
	}

	/* [3] current_xid: epoch-aware full transaction ID */
	values[3] = Int64GetDatum((int64) U64FromFullTransactionId(cur_full_xid));
	nulls[3]  = false;

	/* [4] xids_to_aggressive_vacuum */
	if (xids_to_vac > 0)
	{
		values[4] = Int64GetDatum(xids_to_vac);
		nulls[4]  = false;
	}

	/* [5] xids_to_wraparound */
	if (xids_to_wrap > 0)
	{
		values[5] = Int64GetDatum(xids_to_wrap);
		nulls[5]  = false;
	}

	/* [6..9] XID rate and ETA — require at least 2 snapshots */
	if (snap_count >= 2)
	{
		double elapsed_sec = (double) (newest_e.logged_at - oldest_e.logged_at)
							 / USECS_PER_SEC;

		if (elapsed_sec > 0)
		{
			double txid_rate = (double) (newest_e.next_xid - oldest_e.next_xid)
							   / elapsed_sec;

			/* [6] txid_rate_per_sec */
			values[6] = Float8GetDatum(txid_rate);
			nulls[6]  = false;

			/* [8] eta_aggressive_vacuum */
			if (xids_to_vac > 0 && txid_rate > 0)
			{
				values[8] = IntervalPGetDatum(
					secs_to_interval((double) xids_to_vac / txid_rate));
				nulls[8] = false;
			}

			/* [9] eta_wraparound */
			if (xids_to_wrap > 0 && txid_rate > 0)
			{
				values[9] = IntervalPGetDatum(
					secs_to_interval((double) xids_to_wrap / txid_rate));
				nulls[9] = false;
			}
		}
	}

	/* [7] oldest_xid_database */
	if (OidIsValid(TransamVariables->oldestXidDB))
	{
		char *dbname = get_database_name(TransamVariables->oldestXidDB);
		if (dbname)
		{
			values[7] = CStringGetTextDatum(dbname);
			nulls[7]  = false;
		}
	}

	/* [10] current_mxid */
	values[10] = Int64GetDatum((int64) cur_mxid);
	nulls[10]  = false;

	/* [11] mxids_to_aggressive_vacuum */
	if (mxids_to_vac > 0)
	{
		values[11] = Int64GetDatum(mxids_to_vac);
		nulls[11]  = false;
	}

	/* [12] mxids_to_wraparound */
	if (mxids_to_wrap > 0)
	{
		values[12] = Int64GetDatum(mxids_to_wrap);
		nulls[12]  = false;
	}

	/* [13..16] MXID rate and ETA — require at least 2 snapshots */
	if (snap_count >= 2)
	{
		double elapsed_sec = (double) (newest_e.logged_at - oldest_e.logged_at)
							 / USECS_PER_SEC;

		if (elapsed_sec > 0)
		{
			double mxid_rate = (double) (newest_e.next_mxid - oldest_e.next_mxid)
							   / elapsed_sec;

			/* [13] mxid_rate_per_sec */
			values[13] = Float8GetDatum(mxid_rate);
			nulls[13]  = false;

			/* [15] eta_aggressive_vacuum_mxid */
			if (mxids_to_vac > 0 && mxid_rate > 0)
			{
				values[15] = IntervalPGetDatum(
					secs_to_interval((double) mxids_to_vac / mxid_rate));
				nulls[15] = false;
			}

			/* [16] eta_wraparound_mxid */
			if (mxids_to_wrap > 0 && mxid_rate > 0)
			{
				values[16] = IntervalPGetDatum(
					secs_to_interval((double) mxids_to_wrap / mxid_rate));
				nulls[16] = false;
			}
		}
	}

	/* [14] oldest_mxid_database — catalog scan for min datminmxid */
	{
		Oid mxid_db = get_oldest_mxid_database();
		if (OidIsValid(mxid_db))
		{
			char *dbname = get_database_name(mxid_db);
			if (dbname)
			{
				values[14] = CStringGetTextDatum(dbname);
				nulls[14]  = false;
			}
		}
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * ds_container_resource_info: return a single row describing the cgroup resource limits
 * that apply to the calling PostgreSQL backend process.
 *
 * All fields are NULL when:
 *   - the system does not use cgroups (bare-metal, VM without cgroup mounts)
 *   - the particular limit is not configured (unlimited)
 */
Datum
ds_container_resource_info(PG_FUNCTION_ARGS)
{
	TupleDesc		tupdesc;
	Datum			values[DS_CGROUP_COLS];
	bool			nulls[DS_CGROUP_COLS];
	HeapTuple		tuple;
	PgdsCgroupInfo	cg;
	int				i = 0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, true, sizeof(nulls));
	memset(values, 0, sizeof(values));

	if (pgds_read_cgroup_info(&cg))
	{
		/* cgroup_version */
		values[i] = Int32GetDatum(cg.version);
		nulls[i]  = false;
		i++;

		/* cpu_limit */
		if (cg.cpu_limit_set)
		{
			values[i] = Float8GetDatum(cg.cpu_limit);
			nulls[i]  = false;
		}
		i++;

		/* mem_limit_bytes */
		if (cg.mem_limit_set)
		{
			values[i] = Int64GetDatum(cg.mem_limit_bytes);
			nulls[i]  = false;
		}
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}


static Size
pgds_memsize(void)
{
	return add_size(offsetof(PgdsAutovacuumSharedState, entries),
					mul_size(max_actions, sizeof(PgdsAutovacuumEntry)));
}

static Size
pgds_analyze_memsize(void)
{
	return add_size(offsetof(PgdsAutoanalyzeSharedState, entries),
					mul_size(max_actions, sizeof(PgdsAutoanalyzeEntry)));
}

static Size
pgds_tempfile_memsize(void)
{
	return add_size(offsetof(PgdsTempfileSharedState, entries),
					mul_size(max_actions, sizeof(PgdsTempfileEntry)));
}

static Size
pgds_checkpoint_memsize(void)
{
	return add_size(offsetof(PgdsCheckpointSharedState, entries),
					mul_size(max_actions, sizeof(PgdsCheckpointEntry)));
}

static Size
pgds_xid_snapshot_memsize(void)
{
	return add_size(offsetof(PgdsXidSnapshotSharedState, entries),
					mul_size(max_actions, sizeof(PgdsXidSnapshotEntry)));
}

/*
 * pgds_shmem_request: request shared memory to the core.
 * Called as a hook in PG15 or later, otherwise called from _PG_init().
 */
static void
pgds_shmem_request(void)
{
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(add_size(add_size(add_size(add_size(pgds_memsize(),
												pgds_analyze_memsize()),
											pgds_tempfile_memsize()),
								   pgds_checkpoint_memsize()),
								   pgds_xid_snapshot_memsize()));
	RequestNamedLWLockTranche("pgds", 5);
}

/*
 * shmem_startup hook: allocate or attach to the shared ring buffer.
 * On first call (postmaster) the buffer is zeroed and the header is filled in.
 * On subsequent calls (forked backends) ShmemInitStruct returns found=true and
 * we just store the pointer — the data is already there.
 */
static void
pgds_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgds_autovacuum = ShmemInitStruct("pgds_vacuum", pgds_memsize(), &found);
	if (!found)
	{
		pgds_autovacuum->lock  = &(GetNamedLWLockTranche("pgds"))[0].lock;
		pgds_autovacuum->head  = 0;
		pgds_autovacuum->tail  = 0;
		pgds_autovacuum->count = 0;
		pgds_autovacuum->max   = max_actions;
		memset(pgds_autovacuum->entries, 0, mul_size(max_actions, sizeof(PgdsAutovacuumEntry)));
	}

	pgds_autoanalyze = ShmemInitStruct("pgds_analyze", pgds_analyze_memsize(), &found);
	if (!found)
	{
		pgds_autoanalyze->lock  = &(GetNamedLWLockTranche("pgds"))[1].lock;
		pgds_autoanalyze->head  = 0;
		pgds_autoanalyze->tail  = 0;
		pgds_autoanalyze->count = 0;
		pgds_autoanalyze->max   = max_actions;
		memset(pgds_autoanalyze->entries, 0, mul_size(max_actions, sizeof(PgdsAutoanalyzeEntry)));
	}

	pgds_tempfile = ShmemInitStruct("pgds_tempfile", pgds_tempfile_memsize(), &found);
	if (!found)
	{
		pgds_tempfile->lock  = &(GetNamedLWLockTranche("pgds"))[2].lock;
		pgds_tempfile->head  = 0;
		pgds_tempfile->tail  = 0;
		pgds_tempfile->count = 0;
		pgds_tempfile->max   = max_actions;
		memset(pgds_tempfile->entries, 0, mul_size(max_actions, sizeof(PgdsTempfileEntry)));
	}

	pgds_checkpoint = ShmemInitStruct("pgds_checkpoint", pgds_checkpoint_memsize(), &found);
	if (!found)
	{
		pgds_checkpoint->lock  = &(GetNamedLWLockTranche("pgds"))[3].lock;
		pgds_checkpoint->head  = 0;
		pgds_checkpoint->tail  = 0;
		pgds_checkpoint->count = 0;
		pgds_checkpoint->max   = max_actions;
		memset(pgds_checkpoint->entries, 0, mul_size(max_actions, sizeof(PgdsCheckpointEntry)));
	}

	pgds_xid_snapshot = ShmemInitStruct("pgds_xid_snapshot", pgds_xid_snapshot_memsize(), &found);
	if (!found)
	{
		pgds_xid_snapshot->lock  = &(GetNamedLWLockTranche("pgds"))[4].lock;
		pgds_xid_snapshot->head  = 0;
		pgds_xid_snapshot->tail  = 0;
		pgds_xid_snapshot->count = 0;
		pgds_xid_snapshot->max   = max_actions;
		memset(pgds_xid_snapshot->entries, 0, mul_size(max_actions, sizeof(PgdsXidSnapshotEntry)));
	}

	LWLockRelease(AddinShmemInitLock);
}

Datum
ds_stat_pids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	long		page_size = sysconf(_SC_PAGESIZE);
	bool		proc_accessible = pgds_is_dir_accessible(PROC_VIRTUAL_FS);


	InitMaterializedSRF(fcinfo, 0);

	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		Datum		values[DS_STAT_IDS_COLS] = {0};
		bool		nulls[DS_STAT_IDS_COLS] = {0};
		int			i = 0;

		local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
		beentry = &local_beentry->backendStatus;

		values[i++] = Int64GetDatum(beentry->st_procpid);


		/*
		 * memory usage and temp usage are only available if we can access the
		 * /proc filesystem
		 */
		if (proc_accessible)
		{
			long		rss_pages = pgds_get_rss_memory_pages(beentry->st_procpid);
			long		temp_bytes = pgds_get_temp_file_bytes(beentry->st_procpid);

			if (rss_pages < 0)
				nulls[i++] = true;
			else
				values[i++] = Int64GetDatum(rss_pages * page_size);

			if (temp_bytes < 0)
				nulls[i++] = true;
			else
				values[i++] = Int64GetDatum(temp_bytes);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;

		}

#if PG_VERSION_NUM >= 180000
		if (beentry->st_plan_id == 0)
			nulls[i++] = true;
		else
			values[i++] = UInt64GetDatum(beentry->st_plan_id);
#else
		nulls[i++] = true;
#endif
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}



/*
 * Write one autovacuum log message into the vacuum ring buffer.
 * Only called for vacuum messages.
 */
static void
pgds_log_autovacuum(ErrorData *edata)
{
	/*
	 * Write the message into the next slot of the ring buffer.
	 * When the buffer is full the oldest entry is silently overwritten.
	 */
	LWLockAcquire(pgds_autovacuum->lock, LW_EXCLUSIVE);

	pgds_autovacuum->entries[pgds_autovacuum->tail].logged_at = GetCurrentTimestamp();


	/* database: resolve the name from the OID (valid in any connected backend) */
	{
		const char *dbname = get_database_name(MyDatabaseId);

		strlcpy(pgds_autovacuum->entries[pgds_autovacuum->tail].datname,
				dbname ? dbname : "",
				NAMEDATALEN);
	}

	/* schema and table are parsed from the message text */
	pgds_parse_table_from_message(edata->message,
							 pgds_autovacuum->entries[pgds_autovacuum->tail].schemaname,
							 pgds_autovacuum->entries[pgds_autovacuum->tail].relname);

	{
		Oid			nsoid = get_namespace_oid(
										pgds_autovacuum->entries[pgds_autovacuum->tail].schemaname,
										true /* missing_ok */ );

		pgds_autovacuum->entries[pgds_autovacuum->tail].reloid =
			(nsoid != InvalidOid)
			? get_relname_relid(pgds_autovacuum->entries[pgds_autovacuum->tail].relname, nsoid)
			: InvalidOid;
	}

	if (MyBEEntry != NULL)
		pgds_autovacuum->entries[pgds_autovacuum->tail].heap_pages = MyBEEntry->st_progress_param[1];
	else
		pgds_autovacuum->entries[pgds_autovacuum->tail].heap_pages = 0;

	{
		PgdsAutovacuumEntry *e = &pgds_autovacuum->entries[pgds_autovacuum->tail];

		pgds_parse_vacuum_stats(edata->message,
								&e->pages_removed,
								&e->pages_remain,
								&e->pages_scanned,
								&e->tuples_removed,
								&e->tuples_remain);
		pgds_parse_cpu_stats(edata->message,
							 &e->user_cpu,
							 &e->sys_cpu,
							 &e->elapsed);
		e->aggressive = (strstr(edata->message, "automatic aggressive vacuum") != NULL);
	}

	strlcpy(pgds_autovacuum->entries[pgds_autovacuum->tail].message,
			edata->message, PGDS_AUTOVACUUM_MSG_LEN);
	pgds_autovacuum->tail = (pgds_autovacuum->tail + 1) % pgds_autovacuum->max;
	if (pgds_autovacuum->count < pgds_autovacuum->max)
		pgds_autovacuum->count++;
	else
		pgds_autovacuum->head = (pgds_autovacuum->head + 1) % pgds_autovacuum->max;

	LWLockRelease(pgds_autovacuum->lock);
}


/*
 * Write one autoanalyze log message into the analyze ring buffer.
 */
static void
pgds_log_autoanalyze(ErrorData *edata)
{
	LWLockAcquire(pgds_autoanalyze->lock, LW_EXCLUSIVE);

	pgds_autoanalyze->entries[pgds_autoanalyze->tail].logged_at = GetCurrentTimestamp();

	{
		const char *dbname = get_database_name(MyDatabaseId);

		strlcpy(pgds_autoanalyze->entries[pgds_autoanalyze->tail].datname,
				dbname ? dbname : "",
				NAMEDATALEN);
	}

	pgds_parse_table_from_message(edata->message,
							 pgds_autoanalyze->entries[pgds_autoanalyze->tail].schemaname,
							 pgds_autoanalyze->entries[pgds_autoanalyze->tail].relname);

	{
		Oid			nsoid = get_namespace_oid(
										pgds_autoanalyze->entries[pgds_autoanalyze->tail].schemaname,
										true /* missing_ok */ );

		pgds_autoanalyze->entries[pgds_autoanalyze->tail].reloid =
			(nsoid != InvalidOid)
			? get_relname_relid(pgds_autoanalyze->entries[pgds_autoanalyze->tail].relname, nsoid)
			: InvalidOid;
	}

	/*
	 * Capture analyze progress counters from the backend's own status entry.
	 * pgstat_progress_end_command() leaves st_progress_param intact.
	 */
	{
		PgdsAutoanalyzeEntry *e = &pgds_autoanalyze->entries[pgds_autoanalyze->tail];

		if (MyBEEntry != NULL)
		{
			e->sample_blks_total          = MyBEEntry->st_progress_param[1];
			e->ext_stats_total            = MyBEEntry->st_progress_param[3];
			e->child_tables_total         = MyBEEntry->st_progress_param[5];
		}
		else
		{
			e->sample_blks_total          = 0;
			e->ext_stats_total            = 0;
			e->child_tables_total         = 0;
		}
		pgds_parse_cpu_stats(edata->message,
							 &e->user_cpu,
							 &e->sys_cpu,
							 &e->elapsed);
	}

	strlcpy(pgds_autoanalyze->entries[pgds_autoanalyze->tail].message,
			edata->message, PGDS_AUTOANALYZE_MSG_LEN);
	pgds_autoanalyze->tail = (pgds_autoanalyze->tail + 1) % pgds_autoanalyze->max;
	if (pgds_autoanalyze->count < pgds_autoanalyze->max)
		pgds_autoanalyze->count++;
	else
		pgds_autoanalyze->head = (pgds_autoanalyze->head + 1) % pgds_autoanalyze->max;

	LWLockRelease(pgds_autoanalyze->lock);
}


/*
 * Write one temporary-file log message into the temp-file ring buffer.
 *
 * Only called when edata->message_id matches the known temp-file format string,
 * so we can safely extract the size as the last whitespace-delimited token of
 * edata->message regardless of the server locale.
 */
static void
pgds_log_tempfile(ErrorData *edata)
{
	PgdsTempfileEntry *e;
	const char *p;

	LWLockAcquire(pgds_tempfile->lock, LW_EXCLUSIVE);

	e = &pgds_tempfile->entries[pgds_tempfile->tail];

	e->logged_at = GetCurrentTimestamp();

	{
		const char *dbname = get_database_name(MyDatabaseId);

		strlcpy(e->datname, dbname ? dbname : "", NAMEDATALEN);
	}

	{
		const char *rolname = GetUserNameFromId(GetUserId(), true);

		strlcpy(e->username, rolname ? rolname : "", NAMEDATALEN);
	}

	e->pid = MyProcPid;

	/* size is the last token in the (translated) message */
	p = strrchr(edata->message_id, ' ');
	e->bytes = (p != NULL) ? (int64) strtoll(p + 1, NULL, 10) : 0;

	strlcpy(e->message, edata->message_id, PGDS_TEMPFILE_MSG_LEN);

	pgds_tempfile->tail = (pgds_tempfile->tail + 1) % pgds_tempfile->max;
	if (pgds_tempfile->count < pgds_tempfile->max)
		pgds_tempfile->count++;
	else
		pgds_tempfile->head = (pgds_tempfile->head + 1) % pgds_tempfile->max;

	LWLockRelease(pgds_tempfile->lock);
}

/*
 * pgds_log_checkpoint: capture a checkpoint or restartpoint complete message.
 * All metrics are read directly from CheckpointStats (PGDLLIMPORT) — no text
 * parsing required.  This function is called from within the emit_log_hook,
 * synchronously on the same call stack as LogCheckpointEnd(), so
 * CheckpointStats is fully populated at this point.
 */
static void
pgds_log_checkpoint(ErrorData *edata, bool is_restartpoint)
{
	PgdsCheckpointEntry *e;
	uint64		avg_sync_time;

	LWLockAcquire(pgds_checkpoint->lock, LW_EXCLUSIVE);

	e = &pgds_checkpoint->entries[pgds_checkpoint->tail];

	e->logged_at       = GetCurrentTimestamp();
	e->is_restartpoint = is_restartpoint;
	e->start_t         = CheckpointStats.ckpt_start_t;
	e->end_t           = CheckpointStats.ckpt_end_t;
	e->bufs_written    = CheckpointStats.ckpt_bufs_written;
	e->segs_added      = CheckpointStats.ckpt_segs_added;
	e->segs_removed    = CheckpointStats.ckpt_segs_removed;
	e->segs_recycled   = CheckpointStats.ckpt_segs_recycled;
	e->write_time      = (double) TimestampDifferenceMilliseconds(
							CheckpointStats.ckpt_write_t,
							CheckpointStats.ckpt_sync_t) / 1000.0;
	e->sync_time       = (double) TimestampDifferenceMilliseconds(
							CheckpointStats.ckpt_sync_t,
							CheckpointStats.ckpt_sync_end_t) / 1000.0;
	e->total_time      = (double) TimestampDifferenceMilliseconds(
							CheckpointStats.ckpt_start_t,
							CheckpointStats.ckpt_end_t) / 1000.0;
	e->sync_rels       = CheckpointStats.ckpt_sync_rels;
	/* ckpt_longest_sync and ckpt_agg_sync_time are in microseconds */
	e->longest_sync    = (double) ((CheckpointStats.ckpt_longest_sync + 999) / 1000) / 1000.0;
	avg_sync_time      = (CheckpointStats.ckpt_sync_rels > 0)
		? CheckpointStats.ckpt_agg_sync_time / CheckpointStats.ckpt_sync_rels
		: 0;
	e->average_sync    = (double) ((avg_sync_time + 999) / 1000) / 1000.0;
	strlcpy(e->message, edata->message, PGDS_CHECKPOINT_MSG_LEN);

	pgds_checkpoint->tail = (pgds_checkpoint->tail + 1) % pgds_checkpoint->max;
	if (pgds_checkpoint->count < pgds_checkpoint->max)
		pgds_checkpoint->count++;
	else
		pgds_checkpoint->head = (pgds_checkpoint->head + 1) % pgds_checkpoint->max;

	LWLockRelease(pgds_checkpoint->lock);

	/* Capture an XID snapshot at most once per hour */
	pgds_log_xid_snapshot();
}

/*
 * pgds_log_xid_snapshot: record the current next-XID and next-MXID in the
 * XID snapshot ring buffer, but only if the last entry is older than 1 hour.
 * Called from pgds_log_checkpoint() after every checkpoint/restartpoint.
 */
static void
pgds_log_xid_snapshot(void)
{
	PgdsXidSnapshotEntry *e;
	TimestampTz	now;

	if (pgds_xid_snapshot == NULL)
		return;

	now = GetCurrentTimestamp();

	LWLockAcquire(pgds_xid_snapshot->lock, LW_EXCLUSIVE);

	/* Skip if the last snapshot was taken less than 1 hour ago */
	if (pgds_xid_snapshot->count > 0)
	{
		int			last_idx = (pgds_xid_snapshot->tail + pgds_xid_snapshot->max - 1)
							   % pgds_xid_snapshot->max;
		TimestampTz	last_at  = pgds_xid_snapshot->entries[last_idx].logged_at;

		if (now - last_at < (int64) 3600 * USECS_PER_SEC)
		{
			LWLockRelease(pgds_xid_snapshot->lock);
			return;
		}
	}

	e = &pgds_xid_snapshot->entries[pgds_xid_snapshot->tail];

	e->logged_at     = now;
	e->next_xid      = (int64) U64FromFullTransactionId(ReadNextFullTransactionId());
	e->next_mxid     = (int64) ReadNextMultiXactId();
	e->oldest_xid_db = TransamVariables->oldestXidDB;

	pgds_xid_snapshot->tail = (pgds_xid_snapshot->tail + 1) % pgds_xid_snapshot->max;
	if (pgds_xid_snapshot->count < pgds_xid_snapshot->max)
		pgds_xid_snapshot->count++;
	else
		pgds_xid_snapshot->head = (pgds_xid_snapshot->head + 1) % pgds_xid_snapshot->max;

	LWLockRelease(pgds_xid_snapshot->lock);
}

/*
 * emit_log_hook: intercepts every log message emitted by the backend.
 * Routes vacuum messages to pgds_autovacuum, analyze messages to
 * pgds_autoanalyze, and temporary-file messages to pgds_tempfile.
 */
static void
pgds_emit_log(ErrorData *edata)
{
	/* Always chain to any previously installed hook */
	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);

	/* Skip all capture when disabled */
	if (!pgds_enabled)
		return;

	/* Only interested in LOG-level messages going to the server log */
	if (edata->elevel != LOG || !edata->output_to_server)
		return;

	if (edata->message == NULL)
		return;

	/* Route checkpoint / restartpoint complete messages via message_id */
	if (edata->message_id != NULL)
	{
		if (strncmp(edata->message_id, "checkpoint complete:", 19) == 0)
		{
			if (pgds_checkpoint != NULL)
				pgds_log_checkpoint(edata, false);
			return;
		}
		if (strncmp(edata->message_id, "restartpoint complete:", 21) == 0)
		{
			if (pgds_checkpoint != NULL)
				pgds_log_checkpoint(edata, true);
			return;
		}
		/* Route temporary-file messages via message_id */
		if (strcmp(edata->message_id, "temporary file: path \"%s\", size %lu") == 0)
		{
			if (pgds_tempfile != NULL)
				pgds_log_tempfile(edata);
			return;
		}
	}

	if (strstr(edata->message, "automatic") == NULL)
		return;

	if (strstr(edata->message, "vacuum") != NULL)
	{
		if (pgds_autovacuum != NULL)
			pgds_log_autovacuum(edata);
	}
	else if (strstr(edata->message, "analyze") != NULL)
	{
		if (pgds_autoanalyze != NULL)
			pgds_log_autoanalyze(edata);
	}
}


void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_datasentinel functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_datasentinel.enabled",
							 "Enables or disables log message capture by pg_datasentinel.",
							 NULL,
							 &pgds_enabled,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_datasentinel.max",
							"Sets the maximum number of actions tracked by pg_datasentinel.",
							NULL,
							&max_actions,
							5000,
							100,
							INT_MAX / 2,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
#if PG_VERSION_NUM < 150000
	pgds_shmem_request();
#endif

	/*
	 * Install hooks.
	 */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgds_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgds_shmem_startup;
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = pgds_emit_log;

}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
	shmem_startup_hook = prev_shmem_startup_hook;
	emit_log_hook = prev_emit_log_hook;
}

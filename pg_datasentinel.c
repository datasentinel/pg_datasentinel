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
#include "nodes/value.h"
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
#ifdef __linux__
#include "linux/pgds_proc.h"
#include "linux/pgds_cgroup.h"
#endif
#include "pgds_utils.h"

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

#ifdef __linux__
#define PROC_VIRTUAL_FS    "/proc"
#endif

#define DS_STAT_IDS_COLS		4
#define DS_VACUUM_COLS			18
#define DS_ANALYZE_COLS			14
#define DS_TEMPFILE_COLS		7
#define DS_CHECKPOINT_COLS		16
#define DS_XID_SNAPSHOT_COLS	5
#define DS_WRAPAROUND_RISK_COLS	17
#define DS_CGROUP_COLS	5


/* Message max length */
#define PGDS_VACUUM_MSG_LEN		3072
#define PGDS_ANALYZE_MSG_LEN		1024
#define PGDS_TEMPFILE_MSG_LEN	512
#define PGDS_CHECKPOINT_MSG_LEN	512

/*
 * One slot in the vacuum ring buffer.
 * Populated from LOG messages (autovacuum) or INFO messages (manual VACUUM VERBOSE).
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsVacuumEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	Oid			reloid;
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
	bool		is_automatic;			/* true if triggered by autovacuum */
	char		message[PGDS_VACUUM_MSG_LEN];
} PgdsVacuumEntry;

/* Shared-memory FIFO ring buffer; lock protects all fields. */
typedef struct PgdsVacuumSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsVacuumEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsVacuumSharedState;

/*
 * One slot in the analyze ring buffer.
 * Populated from LOG messages (autoanalyze) or INFO messages (manual ANALYZE VERBOSE).
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsAnalyzeEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	Oid			reloid;
	/* analyze progress counters (from pg_stat_progress_analyze param2..param8) */
	int64		sample_blks_total;
	int64		ext_stats_total;
	int64		child_tables_total;
	/* CPU timings parsed from the log message */
	double		user_cpu;
	double		sys_cpu;
	double		elapsed;
	bool		is_automatic;			/* true if triggered by autoanalyze */
	char		message[PGDS_ANALYZE_MSG_LEN];
} PgdsAnalyzeEntry;

/* Shared-memory FIFO ring buffer; lock protects all fields. */
typedef struct PgdsAnalyzeSharedState
{
	LWLock	   *lock;
	int			head;
	int			tail;
	int			count;
	int			max;
	PgdsAnalyzeEntry entries[FLEXIBLE_ARRAY_MEMBER];
} PgdsAnalyzeSharedState;

/*
 * One slot in the temp-file ring buffer.
 * Populated from LOG messages controlled by log_temp_files.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsTempfileEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		datname[NAMEDATALEN];
	char		username[NAMEDATALEN];
	int			pid;
	int64		bytes;					/* temp file size in bytes */
	char		message[PGDS_TEMPFILE_MSG_LEN];
} PgdsTempfileEntry;

/* Shared-memory FIFO ring buffer; lock protects all fields. */
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
 * Populated from LOG messages controlled by log_checkpoints.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsCheckpointEntry
{
	TimestampTz	logged_at;			/* wall-clock time the message was intercepted */
	bool		is_restartpoint;
	TimestampTz	start_t;
	TimestampTz	end_t;
	int32		bufs_written;
	int32		segs_added;
	int32		segs_removed;
	int32		segs_recycled;
	double		write_time;			/* seconds */
	double		sync_time;			/* seconds */
	double		total_time;			/* seconds */
	int32		sync_rels;
	double		longest_sync;		/* seconds (from microseconds) */
	double		average_sync;		/* seconds (from microseconds) */
	char		message[PGDS_CHECKPOINT_MSG_LEN];
} PgdsCheckpointEntry;

/* Shared-memory FIFO ring buffer; lock protects all fields. */
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
 * Captured on every checkpoint, throttled to at most once per hour.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct PgdsXidSnapshotEntry
{
	TimestampTz	logged_at;		/* wall-clock time of the snapshot */
	int64		next_xid;		/* U64FromFullTransactionId(ReadNextFullTransactionId()) */
	int64		next_mxid;		/* (int64) ReadNextMultiXactId() */
	Oid			oldest_xid_db;	/* TransamVariables->oldestXidDB */
} PgdsXidSnapshotEntry;

/* Shared-memory FIFO ring buffer; lock protects all fields. */
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
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/* Pointers to the shared-memory ring buffers */
static PgdsVacuumSharedState *pgds_vacuum = NULL;
static PgdsAnalyzeSharedState *pgds_analyze = NULL;
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
static void pgds_emit_log_process(ErrorData *edata);
static void pgds_emit_log(ErrorData *edata);
static void pgds_log_vacuum(ErrorData *edata, bool is_automatic);
static void pgds_log_analyze(ErrorData *edata, bool is_automatic);
static void pgds_log_tempfile(ErrorData *edata);
static void pgds_log_checkpoint(ErrorData *edata, bool is_restartpoint);
static void pgds_log_xid_snapshot(void);
static void pgds_process_utility(PlannedStmt *pstmt, const char *queryString,
								 bool readOnlyTree, ProcessUtilityContext context,
								 ParamListInfo params, QueryEnvironment *queryEnv,
								 DestReceiver *dest, QueryCompletion *qc);


static int	pgds_vacuum_nest_level = 0; /* Nesting level for manual VACUUM statements */
static bool	pgds_analyze_pending = false;	/* true after "analyzing" INFO, waiting for stats line */
static char	pgds_analyze_schemaname[NAMEDATALEN];	/* schema parsed from "analyzing" line */
static char	pgds_analyze_relname[NAMEDATALEN];		/* relname parsed from "analyzing" line */
static int	pgds_max_actions;		/* max # actions to track */
static bool	pgds_enabled;		/* enable/disable log capture at runtime */
static bool	pgds_maintenance_force_verbose; /* force VERBOSE on manual VACUUM/ANALYZE */
static bool	pgds_ignore_system_schemas; /* skip pg_catalog and information_schema entries */

PG_FUNCTION_INFO_V1(ds_stat_pids);
PG_FUNCTION_INFO_V1(ds_vacuum_msgs);
PG_FUNCTION_INFO_V1(ds_vacuum_activity_reset);
PG_FUNCTION_INFO_V1(ds_analyze_msgs);
PG_FUNCTION_INFO_V1(ds_analyze_activity_reset);
PG_FUNCTION_INFO_V1(ds_tempfile_msgs);
PG_FUNCTION_INFO_V1(ds_tempfile_activity_reset);
PG_FUNCTION_INFO_V1(ds_checkpoint_msgs);
PG_FUNCTION_INFO_V1(ds_checkpoint_activity_reset);
PG_FUNCTION_INFO_V1(ds_activity_reset_all);
PG_FUNCTION_INFO_V1(ds_xid_snapshot_msgs);
PG_FUNCTION_INFO_V1(ds_wraparound_risk_info);
PG_FUNCTION_INFO_V1(ds_container_resource_info);



/*
 * ds_vacuum_msgs: SRF backing the ds_vacuum_activity view.
 *
 * Iterates the ring buffer under LW_SHARED and emits one row per captured
 * vacuum/analyze LOG message:
 *   seq     int4   – ordinal position (1 = oldest, count = newest)
 *   message text   – the raw log message text
 */
Datum
ds_vacuum_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_vacuum == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_vacuum->lock, LW_SHARED);

	count = pgds_vacuum->count;
	head = pgds_vacuum->head;
	max = pgds_vacuum->max;

	for (a = 0; a < count; a++)
	{
		int			idx = (head + a) % max;
		int			i = 0;
		Datum		values[DS_VACUUM_COLS];
		bool		nulls[DS_VACUUM_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(pgds_vacuum->entries[idx].logged_at);
		values[i++] = CStringGetTextDatum(pgds_vacuum->entries[idx].datname);
		if (pgds_vacuum->entries[idx].schemaname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_vacuum->entries[idx].schemaname);
		else
			nulls[i++] = true;
		if (pgds_vacuum->entries[idx].relname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_vacuum->entries[idx].relname);
		else
			nulls[i++] = true;
		if (pgds_vacuum->entries[idx].reloid != InvalidOid)
			values[i++] = ObjectIdGetDatum(pgds_vacuum->entries[idx].reloid);
		else
			nulls[i++] = true;
		/* vacuum progress counters */
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].heap_pages);
		/* vacuum stats from log message */
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].pages_removed);
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].pages_remain);
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].pages_scanned);
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].tuples_removed);
		values[i++] = Int64GetDatum(pgds_vacuum->entries[idx].tuples_remain);
		/* CPU timings */
		values[i++] = Float8GetDatum(pgds_vacuum->entries[idx].user_cpu);
		values[i++] = Float8GetDatum(pgds_vacuum->entries[idx].sys_cpu);
		values[i++] = Float8GetDatum(pgds_vacuum->entries[idx].elapsed);
		values[i++] = BoolGetDatum(pgds_vacuum->entries[idx].aggressive);
		values[i++] = BoolGetDatum(pgds_vacuum->entries[idx].is_automatic);
		values[i++] = CStringGetTextDatum(pgds_vacuum->entries[idx].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_vacuum->lock);

	return (Datum) 0;
}

/*
 * ds_vacuum_activity_reset: discard all entries from the ring buffer.
 */
Datum
ds_vacuum_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_vacuum == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_vacuum->lock, LW_EXCLUSIVE);
	pgds_vacuum->head = 0;
	pgds_vacuum->tail = 0;
	pgds_vacuum->count = 0;
	LWLockRelease(pgds_vacuum->lock);

	PG_RETURN_VOID();
}


/*
 * ds_analyze_msgs: SRF backing the ds_autoanalyze_activity view.
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
ds_analyze_msgs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			count;
	int			head;
	int			max;
	int			a;

	InitMaterializedSRF(fcinfo, 0);

	if (pgds_analyze == NULL)
		return (Datum) 0;

	LWLockAcquire(pgds_analyze->lock, LW_SHARED);

	count = pgds_analyze->count;
	head  = pgds_analyze->head;
	max   = pgds_analyze->max;

	for (a = 0; a < count; a++)
	{
		int			idx = (head + a) % max;
		int			i = 0;
		Datum		values[DS_ANALYZE_COLS];
		bool		nulls[DS_ANALYZE_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[i++] = Int32GetDatum(a + 1);
		values[i++] = TimestampTzGetDatum(pgds_analyze->entries[idx].logged_at);
		values[i++] = CStringGetTextDatum(pgds_analyze->entries[idx].datname);
		if (pgds_analyze->entries[idx].schemaname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_analyze->entries[idx].schemaname);
		else
			nulls[i++] = true;
		if (pgds_analyze->entries[idx].relname[0] != '\0')
			values[i++] = CStringGetTextDatum(pgds_analyze->entries[idx].relname);
		else
			nulls[i++] = true;
		if (pgds_analyze->entries[idx].reloid != InvalidOid)
			values[i++] = ObjectIdGetDatum(pgds_analyze->entries[idx].reloid);
		else
			nulls[i++] = true;
		/* analyze progress counters */
		values[i++] = Int64GetDatum(pgds_analyze->entries[idx].sample_blks_total);
		values[i++] = Int64GetDatum(pgds_analyze->entries[idx].ext_stats_total);
		values[i++] = Int64GetDatum(pgds_analyze->entries[idx].child_tables_total);
		/* CPU timings */
		values[i++] = Float8GetDatum(pgds_analyze->entries[idx].user_cpu);
		values[i++] = Float8GetDatum(pgds_analyze->entries[idx].sys_cpu);
		values[i++] = Float8GetDatum(pgds_analyze->entries[idx].elapsed);
		values[i++] = BoolGetDatum(pgds_analyze->entries[idx].is_automatic);
		values[i++] = CStringGetTextDatum(pgds_analyze->entries[idx].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(pgds_analyze->lock);

	return (Datum) 0;
}

/*
 * ds_analyze_activity_reset: discard all entries from the analyze ring buffer.
 */
Datum
ds_analyze_activity_reset(PG_FUNCTION_ARGS)
{
	if (pgds_analyze == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(pgds_analyze->lock, LW_EXCLUSIVE);
	pgds_analyze->head  = 0;
	pgds_analyze->tail  = 0;
	pgds_analyze->count = 0;
	LWLockRelease(pgds_analyze->lock);

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
	if (pgds_vacuum != NULL)
	{
		LWLockAcquire(pgds_vacuum->lock, LW_EXCLUSIVE);
		pgds_vacuum->head  = 0;
		pgds_vacuum->tail  = 0;
		pgds_vacuum->count = 0;
		LWLockRelease(pgds_vacuum->lock);
	}

	if (pgds_analyze != NULL)
	{
		LWLockAcquire(pgds_analyze->lock, LW_EXCLUSIVE);
		pgds_analyze->head  = 0;
		pgds_analyze->tail  = 0;
		pgds_analyze->count = 0;
		LWLockRelease(pgds_analyze->lock);
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
#if PG_VERSION_NUM >= 170000
	xids_to_vac  = (int64) (int32) (TransamVariables->xidVacLimit  - cur_xid);
	xids_to_wrap = (int64) (int32) (TransamVariables->xidWrapLimit - cur_xid);
#else
	xids_to_vac  = (int64) (int32) (ShmemVariableCache->xidVacLimit  - cur_xid);
	xids_to_wrap = (int64) (int32) (ShmemVariableCache->xidWrapLimit - cur_xid);
#endif

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
					pgds_secs_to_interval((double) xids_to_vac / txid_rate));
				nulls[8] = false;
			}

			/* [9] eta_wraparound */
			if (xids_to_wrap > 0 && txid_rate > 0)
			{
				values[9] = IntervalPGetDatum(
					pgds_secs_to_interval((double) xids_to_wrap / txid_rate));
				nulls[9] = false;
			}
		}
	}

	/* [7] oldest_xid_database */
#if PG_VERSION_NUM >= 170000
	if (OidIsValid(TransamVariables->oldestXidDB))
	{
		char *dbname = get_database_name(TransamVariables->oldestXidDB);
#else
	if (OidIsValid(ShmemVariableCache->oldestXidDB))
	{
		char *dbname = get_database_name(ShmemVariableCache->oldestXidDB);
#endif
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
			/*
			 * MultiXactId is a wrapping uint32 counter stored as int64.
			 * Cast both values back to uint32 before subtracting, then
			 * interpret the result as int32 — identical to the modular
			 * arithmetic used for XID distance — so a wrap between
			 * snapshots produces the correct positive delta.
			 */
			double mxid_rate = (double) (int32) ((uint32) newest_e.next_mxid
												 - (uint32) oldest_e.next_mxid)
							   / elapsed_sec;

			/* [13] mxid_rate_per_sec */
			values[13] = Float8GetDatum(mxid_rate);
			nulls[13]  = false;

			/* [15] eta_aggressive_vacuum_mxid */
			if (mxids_to_vac > 0 && mxid_rate > 0)
			{
				values[15] = IntervalPGetDatum(
					pgds_secs_to_interval((double) mxids_to_vac / mxid_rate));
				nulls[15] = false;
			}

			/* [16] eta_wraparound_mxid */
			if (mxids_to_wrap > 0 && mxid_rate > 0)
			{
				values[16] = IntervalPGetDatum(
					pgds_secs_to_interval((double) mxids_to_wrap / mxid_rate));
				nulls[16] = false;
			}
		}
	}

	/* [14] oldest_mxid_database — catalog scan for min datminmxid */
	{
		Oid mxid_db = pgds_get_oldest_mxid_database();
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
	int				i = 0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, true, sizeof(nulls));
	memset(values, 0, sizeof(values));

#ifdef __linux__
	{
		PgdsCgroupInfo	cg;

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
			i++;

			/* cpu_pressure_pct_60s */
			if (cg.cpu_pressure_set)
			{
				values[i] = Float8GetDatum(cg.cpu_pressure_avg60);
				nulls[i]  = false;
			}
			i++;

			/* mem_used */
			if (cg.mem_used_set)
			{
				values[i] = Int64GetDatum(cg.mem_used_bytes);
				nulls[i]  = false;
			}
		}
	}
#endif

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}


static Size
pgds_memsize(void)
{
	return add_size(offsetof(PgdsVacuumSharedState, entries),
					mul_size(pgds_max_actions, sizeof(PgdsVacuumEntry)));
}

static Size
pgds_analyze_memsize(void)
{
	return add_size(offsetof(PgdsAnalyzeSharedState, entries),
					mul_size(pgds_max_actions, sizeof(PgdsAnalyzeEntry)));
}

static Size
pgds_tempfile_memsize(void)
{
	return add_size(offsetof(PgdsTempfileSharedState, entries),
					mul_size(pgds_max_actions, sizeof(PgdsTempfileEntry)));
}

static Size
pgds_checkpoint_memsize(void)
{
	return add_size(offsetof(PgdsCheckpointSharedState, entries),
					mul_size(pgds_max_actions, sizeof(PgdsCheckpointEntry)));
}

static Size
pgds_xid_snapshot_memsize(void)
{
	return add_size(offsetof(PgdsXidSnapshotSharedState, entries),
					mul_size(pgds_max_actions, sizeof(PgdsXidSnapshotEntry)));
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

	pgds_vacuum = ShmemInitStruct("pgds_vacuum", pgds_memsize(), &found);
	if (!found)
	{
		pgds_vacuum->lock  = &(GetNamedLWLockTranche("pgds"))[0].lock;
		pgds_vacuum->head  = 0;
		pgds_vacuum->tail  = 0;
		pgds_vacuum->count = 0;
		pgds_vacuum->max   = pgds_max_actions;
		memset(pgds_vacuum->entries, 0, mul_size(pgds_max_actions, sizeof(PgdsVacuumEntry)));
	}

	pgds_analyze = ShmemInitStruct("pgds_analyze", pgds_analyze_memsize(), &found);
	if (!found)
	{
		pgds_analyze->lock  = &(GetNamedLWLockTranche("pgds"))[1].lock;
		pgds_analyze->head  = 0;
		pgds_analyze->tail  = 0;
		pgds_analyze->count = 0;
		pgds_analyze->max   = pgds_max_actions;
		memset(pgds_analyze->entries, 0, mul_size(pgds_max_actions, sizeof(PgdsAnalyzeEntry)));
	}

	pgds_tempfile = ShmemInitStruct("pgds_tempfile", pgds_tempfile_memsize(), &found);
	if (!found)
	{
		pgds_tempfile->lock  = &(GetNamedLWLockTranche("pgds"))[2].lock;
		pgds_tempfile->head  = 0;
		pgds_tempfile->tail  = 0;
		pgds_tempfile->count = 0;
		pgds_tempfile->max   = pgds_max_actions;
		memset(pgds_tempfile->entries, 0, mul_size(pgds_max_actions, sizeof(PgdsTempfileEntry)));
	}

	pgds_checkpoint = ShmemInitStruct("pgds_checkpoint", pgds_checkpoint_memsize(), &found);
	if (!found)
	{
		pgds_checkpoint->lock  = &(GetNamedLWLockTranche("pgds"))[3].lock;
		pgds_checkpoint->head  = 0;
		pgds_checkpoint->tail  = 0;
		pgds_checkpoint->count = 0;
		pgds_checkpoint->max   = pgds_max_actions;
		memset(pgds_checkpoint->entries, 0, mul_size(pgds_max_actions, sizeof(PgdsCheckpointEntry)));
	}

	pgds_xid_snapshot = ShmemInitStruct("pgds_xid_snapshot", pgds_xid_snapshot_memsize(), &found);
	if (!found)
	{
		pgds_xid_snapshot->lock  = &(GetNamedLWLockTranche("pgds"))[4].lock;
		pgds_xid_snapshot->head  = 0;
		pgds_xid_snapshot->tail  = 0;
		pgds_xid_snapshot->count = 0;
		pgds_xid_snapshot->max   = pgds_max_actions;
		memset(pgds_xid_snapshot->entries, 0, mul_size(pgds_max_actions, sizeof(PgdsXidSnapshotEntry)));
	}

	LWLockRelease(AddinShmemInitLock);
}

Datum
ds_stat_pids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
#ifdef __linux__
	long		page_size = sysconf(_SC_PAGESIZE);
	bool		proc_accessible = pgds_is_dir_accessible(PROC_VIRTUAL_FS);
#endif


	InitMaterializedSRF(fcinfo, 0);

	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		Datum		values[DS_STAT_IDS_COLS] = {0};
		bool		nulls[DS_STAT_IDS_COLS] = {0};
		int			i = 0;

#if PG_VERSION_NUM >= 160000
		local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
#else
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
#endif
		beentry = &local_beentry->backendStatus;

		values[i++] = Int64GetDatum(beentry->st_procpid);


		/*
		 * memory usage and temp usage are only available if we can access the
		 * /proc filesystem (Linux only)
		 */
#ifdef __linux__
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
#else
		nulls[i++] = true;
		nulls[i++] = true;
#endif

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
 * Write one vacuum log message into the vacuum ring buffer.
 * Called for both autovacuum LOG messages and manual VACUUM INFO messages.
 */
static void
pgds_log_vacuum(ErrorData *edata, bool is_automatic)
{
	const char *dbname = get_database_name(MyDatabaseId);
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	Oid			nsoid;
	Oid			reloid;
	PgdsVacuumEntry *e;

	pgds_parse_table_from_message(edata->message, schemaname, relname);
	if (schemaname[0] == '\0')
		pgds_parse_table_from_vacuuming(edata->message, schemaname, relname);

	if (pgds_ignore_system_schemas &&
		(strcmp(schemaname, "pg_catalog") == 0 ||
		 strcmp(schemaname, "information_schema") == 0))
		return;

	nsoid  = get_namespace_oid(schemaname, true /* missing_ok */);
	reloid = (nsoid != InvalidOid) ? get_relname_relid(relname, nsoid) : InvalidOid;

	/*
	 * Write the message into the next slot of the ring buffer.
	 * When the buffer is full the oldest entry is silently overwritten.
	 */
	LWLockAcquire(pgds_vacuum->lock, LW_EXCLUSIVE);

	e = &pgds_vacuum->entries[pgds_vacuum->tail];

	e->logged_at = GetCurrentTimestamp();

	strlcpy(e->datname, dbname ? dbname : "", NAMEDATALEN);
	strlcpy(e->schemaname, schemaname, NAMEDATALEN);
	strlcpy(e->relname, relname, NAMEDATALEN);

	e->reloid = reloid;

	e->heap_pages = (MyBEEntry != NULL) ? MyBEEntry->st_progress_param[1] : 0;

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
	e->is_automatic = is_automatic;

	strlcpy(e->message, edata->message, PGDS_VACUUM_MSG_LEN);
	pgds_vacuum->tail = (pgds_vacuum->tail + 1) % pgds_vacuum->max;
	if (pgds_vacuum->count < pgds_vacuum->max)
		pgds_vacuum->count++;
	else
		pgds_vacuum->head = (pgds_vacuum->head + 1) % pgds_vacuum->max;

	LWLockRelease(pgds_vacuum->lock);
}


/*
 * Write one analyze log message into the analyze ring buffer.
 * Called for both autoanalyze LOG messages and manual ANALYZE INFO messages.
 */
static void
pgds_log_analyze(ErrorData *edata, bool is_automatic)
{
	const char *dbname = get_database_name(MyDatabaseId);
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	Oid			nsoid;
	Oid			reloid;
	PgdsAnalyzeEntry *e;

	pgds_parse_table_from_message(edata->message, schemaname, relname);
	if (schemaname[0] == '\0' && pgds_analyze_schemaname[0] != '\0')
	{
		strlcpy(schemaname, pgds_analyze_schemaname, NAMEDATALEN);
	}
	if (relname[0] == '\0' && pgds_analyze_relname[0] != '\0')
	{
		strlcpy(relname, pgds_analyze_relname, NAMEDATALEN);
	}

	if (pgds_ignore_system_schemas &&
		(strcmp(schemaname, "pg_catalog") == 0 ||
		 strcmp(schemaname, "information_schema") == 0))
		return;

	nsoid  = get_namespace_oid(schemaname, true /* missing_ok */);
	reloid = (nsoid != InvalidOid) ? get_relname_relid(relname, nsoid) : InvalidOid;

	LWLockAcquire(pgds_analyze->lock, LW_EXCLUSIVE);

	e = &pgds_analyze->entries[pgds_analyze->tail];

	e->logged_at = GetCurrentTimestamp();

	strlcpy(e->datname, dbname ? dbname : "", NAMEDATALEN);
	strlcpy(e->schemaname, schemaname, NAMEDATALEN);
	strlcpy(e->relname, relname, NAMEDATALEN);

	e->reloid = reloid;

	/*
	 * Capture analyze progress counters from the backend's own status entry.
	 * pgstat_progress_end_command() leaves st_progress_param intact.
	 */
	if (MyBEEntry != NULL)
	{
		e->sample_blks_total  = MyBEEntry->st_progress_param[1];
		e->ext_stats_total    = MyBEEntry->st_progress_param[3];
		e->child_tables_total = MyBEEntry->st_progress_param[5];
	}
	else
	{
		e->sample_blks_total  = 0;
		e->ext_stats_total    = 0;
		e->child_tables_total = 0;
	}
	pgds_parse_cpu_stats(edata->message,
						 &e->user_cpu,
						 &e->sys_cpu,
						 &e->elapsed);

	e->is_automatic = is_automatic;

	strlcpy(e->message, edata->message, PGDS_ANALYZE_MSG_LEN);
	pgds_analyze->tail = (pgds_analyze->tail + 1) % pgds_analyze->max;
	if (pgds_analyze->count < pgds_analyze->max)
		pgds_analyze->count++;
	else
		pgds_analyze->head = (pgds_analyze->head + 1) % pgds_analyze->max;

	LWLockRelease(pgds_analyze->lock);
}


/*
 * Write one temporary-file log message into the temp-file ring buffer.
 *
 * Only called when edata->message_id matches the known temp-file format string,
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

	/* size is the last token in the untranslated message */
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
#if PG_VERSION_NUM >= 170000
	e->oldest_xid_db = TransamVariables->oldestXidDB;
#else
	e->oldest_xid_db = ShmemVariableCache->oldestXidDB;
#endif

	pgds_xid_snapshot->tail = (pgds_xid_snapshot->tail + 1) % pgds_xid_snapshot->max;
	if (pgds_xid_snapshot->count < pgds_xid_snapshot->max)
		pgds_xid_snapshot->count++;
	else
		pgds_xid_snapshot->head = (pgds_xid_snapshot->head + 1) % pgds_xid_snapshot->max;

	LWLockRelease(pgds_xid_snapshot->lock);
}

static void
pgds_emit_log_process(ErrorData *edata)
{
	/* Manual VACUUM/ANALYZE: capture INFO messages */
	if (pgds_vacuum_nest_level > 0 && edata->elevel == INFO)
	{
		if (strstr(edata->message, "finished vacuuming") != NULL)
		{
			if (pgds_vacuum != NULL)
				pgds_log_vacuum(edata, false);
		}
		else if (strstr(edata->message, "analyzing ") != NULL)
		{
			/* Reset previous state before attempting to parse */
			pgds_analyze_schemaname[0] = '\0';
			pgds_analyze_relname[0] = '\0';

			pgds_parse_table_from_analyzing(edata->message,
											pgds_analyze_schemaname,
											pgds_analyze_relname);

			/*
			 * Only arm the pending flag if parsing produced non-empty
			 * schema and relation names. Otherwise, treat this as an
			 * unrecognized message format and ignore it.
			 */
			if (pgds_analyze_schemaname[0] != '\0' &&
				pgds_analyze_relname[0] != '\0')
				pgds_analyze_pending = true;
		}
		else if (pgds_analyze_pending)
		{
			/* Second INFO line: the per-table stats summary */
			if (pgds_analyze != NULL)
				pgds_log_analyze(edata, false);
			pgds_analyze_pending = false;
			pgds_analyze_schemaname[0] = '\0';
			pgds_analyze_relname[0] = '\0';
		}
		return;
	}

	/* Only interested in LOG-level messages going to the server log */
	if (edata->elevel != LOG || !edata->output_to_server)
		return;

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
		if (pgds_vacuum != NULL)
			pgds_log_vacuum(edata, true);
	}
	else if (strstr(edata->message, "analyze") != NULL)
	{
		if (pgds_analyze != NULL)
			pgds_log_analyze(edata, true);
	}
}

/*
 * emit_log_hook: intercepts every log message emitted by the backend.
 * Routes vacuum messages to pgds_vacuum, analyze messages to
 * pgds_analyze, and temporary-file messages to pgds_tempfile.
 */
static void
pgds_emit_log(ErrorData *edata)
{
	ErrorData  *errdata;

	/* Always chain to any previously installed hook */
	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);

	if (edata->elevel != LOG && edata->elevel != INFO)
		return;
	
	/* Skip all capture when disabled */
	if (!pgds_enabled)
		return;

	if (edata->message == NULL)
		return;

	PG_TRY();
	{
		pgds_emit_log_process(edata);
	}
	PG_CATCH();
	{
		errdata = CopyErrorData();
		elog(ERROR, "pg_datasentinel: error in emit_log: %s", errdata->message);
		FreeErrorData(errdata);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static void
pgds_process_utility(PlannedStmt *pstmt, const char *queryString,
					 bool readOnlyTree, ProcessUtilityContext context,
					 ParamListInfo params, QueryEnvironment *queryEnv,
					 DestReceiver *dest, QueryCompletion *qc)
{
	bool		is_vacuum;
	int			saved_log_min_messages;

	is_vacuum = IsA(pstmt->utilityStmt, VacuumStmt);

	if (is_vacuum && pgds_enabled && pgds_maintenance_force_verbose)
	{
		VacuumStmt *stmt = (VacuumStmt *) pstmt->utilityStmt;

		if (!pgds_vacuum_is_verbose(stmt))
		{
			PlannedStmt *pstmt_copy = copyObject(pstmt);
			VacuumStmt *stmt_copy = (VacuumStmt *) pstmt_copy->utilityStmt;

			stmt_copy->options = lappend(stmt_copy->options,
										 makeDefElem("verbose",
													 (Node *) makeBoolean(true),
													 -1));
			pstmt = pstmt_copy;
		}
	}

	if (is_vacuum)
	{
		pgds_vacuum_nest_level++;

		/*
		 * INFO messages from VACUUM/ANALYZE VERBOSE only have output_to_server
		 * set when log_min_messages <= INFO.  Lower it temporarily so that
		 * emit_log_hook fires for those messages.
		 */
		saved_log_min_messages = log_min_messages;
		if (pgds_enabled)
			log_min_messages = Min(log_min_messages, INFO);
	}

	PG_TRY();
	{
		if (prev_process_utility_hook)
			prev_process_utility_hook(pstmt, queryString, readOnlyTree,
									  context, params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv, dest, qc);
	}
	PG_CATCH();
	{
		if (is_vacuum)
		{
			pgds_vacuum_nest_level--;
			log_min_messages = saved_log_min_messages;
			pgds_analyze_pending = false;
			pgds_analyze_schemaname[0] = '\0';
			pgds_analyze_relname[0] = '\0';
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (is_vacuum)
	{
		pgds_vacuum_nest_level--;
		log_min_messages = saved_log_min_messages;
		pgds_analyze_pending = false;
		pgds_analyze_schemaname[0] = '\0';
		pgds_analyze_relname[0] = '\0';
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

	DefineCustomBoolVariable("pg_datasentinel.maintenance_force_verbose",
							 "Forces VERBOSE output on all manual VACUUM and ANALYZE commands.",
							 NULL,
							 &pgds_maintenance_force_verbose,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_datasentinel.ignore_system_schemas",
							 "Skips vacuum and analyze log entries for pg_catalog and information_schema.",
							 NULL,
							 &pgds_ignore_system_schemas,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_datasentinel.max",
							"Sets the maximum number of actions tracked by pg_datasentinel.",
							NULL,
							&pgds_max_actions,
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
	prev_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgds_process_utility;

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
	ProcessUtility_hook = prev_process_utility_hook;
}

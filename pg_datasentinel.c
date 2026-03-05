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
#include "pgds_linux.h"
#include "pgds_utils.h"

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

#define PROC_VIRTUAL_FS    "/proc"
#define DS_STAT_IDS_COLS		4
#define DS_AUTOVACUUM_COLS		16	/* seq, logged_at, datname, schemaname, relname, relid,
								 * heap_pages, pages_removed, pages_remain, pages_scanned,
								 * tuples_removed, tuples_remain, user_cpu, sys_cpu, elapsed, message */
#define DS_ANALYZE_COLS			13	/* seq, logged_at, datname, schemaname, relname, relid,
								 * sample_blks_total, ext_stats_total, child_tables_total,
								 * user_cpu, sys_cpu, elapsed, message */
#define PGDS_AUTOVACUUM_MSG_LEN	3072	/* max length of a stored autovacuum log message */
#define PGDS_AUTOANALYZE_MSG_LEN	1024	/* max length of a stored autoanalyze log message */
#define DS_TEMPFILE_COLS		7		/* seq, logged_at, datname, username, pid, bytes, message */
#define PGDS_TEMPFILE_MSG_LEN	512		/* max length of a stored temp file log message */

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

static Size pgds_memsize(void);
static Size pgds_analyze_memsize(void);
static Size pgds_tempfile_memsize(void);
static void pgds_shmem_request(void);
static void pgds_shmem_startup(void);
static void pgds_emit_log(ErrorData *edata);
static void pgds_log_autovacuum(ErrorData *edata);
static void pgds_log_autoanalyze(ErrorData *edata);
static void pgds_log_tempfile(ErrorData *edata);


static int	max_actions;		/* max # actions to track */

PG_FUNCTION_INFO_V1(ds_stat_pids);
PG_FUNCTION_INFO_V1(ds_autovacuum_msgs);
PG_FUNCTION_INFO_V1(ds_autovacuum_activity_reset);
PG_FUNCTION_INFO_V1(ds_autoanalyze_msgs);
PG_FUNCTION_INFO_V1(ds_autoanalyze_activity_reset);
PG_FUNCTION_INFO_V1(ds_tempfile_msgs);
PG_FUNCTION_INFO_V1(ds_tempfile_activity_reset);
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
	RequestAddinShmemSpace(add_size(add_size(pgds_memsize(), pgds_analyze_memsize()),
								   pgds_tempfile_memsize()));
	RequestNamedLWLockTranche("pgds", 3);
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

	/* Only interested in LOG-level messages going to the server log */
	if (edata->elevel != LOG || !edata->output_to_server)
		return;

	if (edata->message == NULL)
		return;

	/* Route temporary-file messages via message_id */
	if (edata->message_id != NULL &&
		strcmp(edata->message_id, "temporary file: path \"%s\", size %lu") == 0)
	{
		if (pgds_tempfile != NULL)
			pgds_log_tempfile(edata);
		return;
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

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
								 * heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count,
								 * max_dead_tuple_bytes, dead_tuple_bytes, num_dead_item_ids,
								 * indexes_total, indexes_processed, message */
#define DS_ANALYZE_COLS			14	/* seq, logged_at, datname, schemaname, relname, relid,
								 * sample_blks_total, sample_blks_scanned, ext_stats_total, ext_stats_computed,
								 * child_tables_total, child_tables_done, current_child_table_relid, message */
#define PGDS_MSG_LEN			4096	/* max length of a stored log message */

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
	/* vacuum progress counters (from pg_stat_progress_vacuum param2..param10) */
	int64		heap_blks_total;
	int64		heap_blks_scanned;
	int64		heap_blks_vacuumed;
	int64		index_vacuum_count;
	int64		max_dead_tuple_bytes;
	int64		dead_tuple_bytes;
	int64		num_dead_item_ids;
	int64		indexes_total;
	int64		indexes_processed;
	char		message[PGDS_MSG_LEN];
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
	int64		sample_blks_scanned;
	int64		ext_stats_total;
	int64		ext_stats_computed;
	int64		child_tables_total;
	int64		child_tables_done;
	Oid			current_child_table_relid;
	char		message[PGDS_MSG_LEN];
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

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/* Pointers to the shared-memory ring buffers */
static PgdsAutovacuumSharedState *pgds_autovacuum = NULL;
static PgdsAutoanalyzeSharedState *pgds_autoanalyze = NULL;

static Size pgds_memsize(void);
static Size pgds_analyze_memsize(void);
static void pgds_shmem_request(void);
static void pgds_shmem_startup(void);
static void pgds_emit_log(ErrorData *edata);
static void pgds_log_autovacuum(ErrorData *edata);
static void pgds_log_autoanalyze(ErrorData *edata);


static int	max_actions;		/* max # actions to track */
static bool dump_on_shutdown;	/* whether to save actions across shutdown */

PG_FUNCTION_INFO_V1(ds_stat_pids);
PG_FUNCTION_INFO_V1(ds_autovacuum_msgs);
PG_FUNCTION_INFO_V1(ds_autovacuum_activity_reset);
PG_FUNCTION_INFO_V1(ds_autoanalyze_msgs);
PG_FUNCTION_INFO_V1(ds_autoanalyze_activity_reset);


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
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].heap_blks_total);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].heap_blks_scanned);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].heap_blks_vacuumed);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].index_vacuum_count);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].max_dead_tuple_bytes);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].dead_tuple_bytes);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].num_dead_item_ids);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].indexes_total);
		values[i++] = Int64GetDatum(pgds_autovacuum->entries[idx].indexes_processed);
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
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].sample_blks_scanned);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].ext_stats_total);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].ext_stats_computed);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].child_tables_total);
		values[i++] = Int64GetDatum(pgds_autoanalyze->entries[idx].child_tables_done);
		if (pgds_autoanalyze->entries[idx].current_child_table_relid != InvalidOid)
			values[i++] = ObjectIdGetDatum(pgds_autoanalyze->entries[idx].current_child_table_relid);
		else
			nulls[i++] = true;
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
	RequestAddinShmemSpace(add_size(pgds_memsize(), pgds_analyze_memsize()));
	RequestNamedLWLockTranche("pgds", 2);
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

	/*
	 * Capture vacuum progress counters from the backend's own status entry.
	 * pgstat_progress_end_command() clears st_progress_command but leaves
	 * st_progress_param intact, so the final counters are still readable here.
	 */
	{
		PgdsAutovacuumEntry *e = &pgds_autovacuum->entries[pgds_autovacuum->tail];

		if (MyBEEntry != NULL)
		{
			e->heap_blks_total      = MyBEEntry->st_progress_param[1];
			e->heap_blks_scanned    = MyBEEntry->st_progress_param[2];
			e->heap_blks_vacuumed   = MyBEEntry->st_progress_param[3];
			e->index_vacuum_count   = MyBEEntry->st_progress_param[4];
			e->max_dead_tuple_bytes = MyBEEntry->st_progress_param[5];
			e->dead_tuple_bytes     = MyBEEntry->st_progress_param[6];
			e->num_dead_item_ids    = MyBEEntry->st_progress_param[7];
			e->indexes_total        = MyBEEntry->st_progress_param[8];
			e->indexes_processed    = MyBEEntry->st_progress_param[9];
		}
		else
		{
			e->heap_blks_total      = 0;
			e->heap_blks_scanned    = 0;
			e->heap_blks_vacuumed   = 0;
			e->index_vacuum_count   = 0;
			e->max_dead_tuple_bytes = 0;
			e->dead_tuple_bytes     = 0;
			e->num_dead_item_ids    = 0;
			e->indexes_total        = 0;
			e->indexes_processed    = 0;
		}
	}

	strlcpy(pgds_autovacuum->entries[pgds_autovacuum->tail].message,
			edata->message, PGDS_MSG_LEN);
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
			e->sample_blks_scanned        = MyBEEntry->st_progress_param[2];
			e->ext_stats_total            = MyBEEntry->st_progress_param[3];
			e->ext_stats_computed         = MyBEEntry->st_progress_param[4];
			e->child_tables_total         = MyBEEntry->st_progress_param[5];
			e->child_tables_done          = MyBEEntry->st_progress_param[6];
			e->current_child_table_relid  = (Oid) MyBEEntry->st_progress_param[7];
		}
		else
		{
			e->sample_blks_total          = 0;
			e->sample_blks_scanned        = 0;
			e->ext_stats_total            = 0;
			e->ext_stats_computed         = 0;
			e->child_tables_total         = 0;
			e->child_tables_done          = 0;
			e->current_child_table_relid  = InvalidOid;
		}
	}

	strlcpy(pgds_autoanalyze->entries[pgds_autoanalyze->tail].message,
			edata->message, PGDS_MSG_LEN);
	pgds_autoanalyze->tail = (pgds_autoanalyze->tail + 1) % pgds_autoanalyze->max;
	if (pgds_autoanalyze->count < pgds_autoanalyze->max)
		pgds_autoanalyze->count++;
	else
		pgds_autoanalyze->head = (pgds_autoanalyze->head + 1) % pgds_autoanalyze->max;

	LWLockRelease(pgds_autoanalyze->lock);
}


/*
 * emit_log_hook: intercepts every log message emitted by the backend.
 * Routes vacuum messages to pgds_autovacuum and analyze messages to pgds_autoanalyze.
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
	DefineCustomBoolVariable("pg_datasentinel.save",
							 "Save pg_datasentinel actions across server shutdowns.",
							 NULL,
							 &dump_on_shutdown,
							 true,
							 PGC_SIGHUP,
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

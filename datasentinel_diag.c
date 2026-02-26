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
#include "dsdiag_linux.h"
#include "dsdiag_utils.h"

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

#define PROC_VIRTUAL_FS    "/proc"
#define DS_STAT_IDS_COLS		4
#define DS_AUTOVACUUM_COLS		8	/* seq, logged_at, operation, datname, schemaname, relname, relid, message */
#define DSDIAG_MSG_LEN			4096	/* max length of a stored log message */

/*
 * One slot in the ring buffer.
 * Fixed-size fields only — this struct lives in shared memory.
 */
typedef struct DsdiagAutovacuumEntry
{
	TimestampTz	logged_at;				/* wall-clock time the message was intercepted */
	char		operation[16];			/* "vacuum" or "analyze" */
	char		datname[NAMEDATALEN];	/* database name */
	char		schemaname[NAMEDATALEN];	/* schema name */
	char		relname[NAMEDATALEN];	/* table name */
	Oid			reloid;					/* OID of the relation */
	char		message[DSDIAG_MSG_LEN];
} DsdiagAutovacuumEntry;

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
typedef struct DsdiagAutovacuumSharedState
{
	LWLock	   *lock;			/* protects all fields below */
	int			head;
	int			tail;
	int			count;
	int			max;
	DsdiagAutovacuumEntry entries[FLEXIBLE_ARRAY_MEMBER];
} DsdiagAutovacuumSharedState;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/* Pointer to the shared-memory ring buffer;*/
static DsdiagAutovacuumSharedState *dsdiag_autovacuum = NULL;

static Size dsdiag_memsize(void);
static void dsdiag_shmem_request(void);
static void dsdiag_shmem_startup(void);
static void dsdiag_emit_log(ErrorData *edata);


static int	max_actions;		/* max # actions to track */
static bool dump_on_shutdown;	/* whether to save actions across shutdown */

PG_FUNCTION_INFO_V1(ds_stat_pids);
PG_FUNCTION_INFO_V1(ds_autovacuum_msgs);


/*
 * ds_autovacuum_msgs: SRF backing the ds_autovacuum_activity view.
 *
 * Snapshots the ring buffer under LW_SHARED, releases the lock, then emits
 * one row per captured vacuum LOG message:
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
	DsdiagAutovacuumEntry *snapshot;
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	if (dsdiag_autovacuum == NULL)
		return (Datum) 0;

	/*
	 * Take a snapshot of the ring buffer contents under a shared lock so we
	 * hold it for the shortest possible time.  palloc under an LWLock is safe
	 * because error cleanup calls LWLockReleaseAll().
	 */
	LWLockAcquire(dsdiag_autovacuum->lock, LW_SHARED);

	count = dsdiag_autovacuum->count;
	head = dsdiag_autovacuum->head;
	max = dsdiag_autovacuum->max;
	snapshot = (DsdiagAutovacuumEntry *) palloc(count * sizeof(DsdiagAutovacuumEntry));
	for (i = 0; i < count; i++)
	{
		int			idx = (head + i) % max;

		memcpy(&snapshot[i], &dsdiag_autovacuum->entries[idx], sizeof(DsdiagAutovacuumEntry));
	}

	LWLockRelease(dsdiag_autovacuum->lock);

	/* Build the result set from the local snapshot */
	for (i = 0; i < count; i++)
	{
		Datum		values[DS_AUTOVACUUM_COLS];
		bool		nulls[DS_AUTOVACUUM_COLS];

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(i + 1);
		values[1] = TimestampTzGetDatum(snapshot[i].logged_at);
		values[2] = CStringGetTextDatum(snapshot[i].operation);
		values[3] = CStringGetTextDatum(snapshot[i].datname);
		values[4] = CStringGetTextDatum(snapshot[i].schemaname);
		values[5] = CStringGetTextDatum(snapshot[i].relname);
		if (snapshot[i].reloid != InvalidOid)
			values[6] = ObjectIdGetDatum(snapshot[i].reloid);
		else
			nulls[6] = true;
		values[7] = CStringGetTextDatum(snapshot[i].message);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	pfree(snapshot);

	return (Datum) 0;
}

static Size
dsdiag_memsize(void)
{
	return add_size(offsetof(DsdiagAutovacuumSharedState, entries),
					mul_size(max_actions, sizeof(DsdiagAutovacuumEntry)));
}

/*
 * dsdiag_shmem_request: request shared memory to the core.
 * Called as a hook in PG15 or later, otherwise called from _PG_init().
 */
static void
dsdiag_shmem_request(void)
{
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(dsdiag_memsize());
	RequestNamedLWLockTranche("dsdiag", 1);
}

/*
 * shmem_startup hook: allocate or attach to the shared ring buffer.
 * On first call (postmaster) the buffer is zeroed and the header is filled in.
 * On subsequent calls (forked backends) ShmemInitStruct returns found=true and
 * we just store the pointer — the data is already there.
 */
static void
dsdiag_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	dsdiag_autovacuum = ShmemInitStruct("dsdiag", dsdiag_memsize(), &found);
	if (!found)
	{
		dsdiag_autovacuum->lock = &(GetNamedLWLockTranche("dsdiag"))[0].lock;
		dsdiag_autovacuum->head = 0;
		dsdiag_autovacuum->tail = 0;
		dsdiag_autovacuum->count = 0;
		dsdiag_autovacuum->max = max_actions;
		memset(dsdiag_autovacuum->entries, 0, mul_size(max_actions, sizeof(DsdiagAutovacuumEntry)));
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
	bool		proc_accessible = is_dir_accessible(PROC_VIRTUAL_FS);


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
			long		rss_pages = get_rss_memory_pages(beentry->st_procpid);
			long		temp_bytes = get_temp_file_bytes(beentry->st_procpid);

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
		if (queryDesc != NULL && queryDesc->plannedstmt != NULL)
			values[i++] = Int64GetDatum(queryDesc->plannedstmt->planId);
		else
			nulls[i++] = true;
#else
		nulls[i++] = true;
#endif
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}



/*
 * emit_log_hook: intercepts every log message emitted by the backend.
 */
static void
dsdiag_emit_log(ErrorData *edata)
{
	/* Always chain to any previously installed hook */
	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);


	/* Shared memory is only available when loaded via shared_preload_libraries */
	if (dsdiag_autovacuum == NULL)
		return;

	/* Only interested in LOG-level messages going to the server log */
	if (edata->elevel != LOG || !edata->output_to_server)
		return;

	/*
	 * Use message_id (the original untranslated English source string) for a
	 * locale-independent match.  We only capture the two autovacuum worker
	 * messages; generic VACUUM commands are handled by ProcessUtility_hook.
	 */
	if (edata->message == NULL)
		return;

	if (strstr(edata->message, "automatic") == NULL)
		return;
	
		/*
		automatic aggressive vacuum to prevent wraparound of table 
		automatic vacuum to prevent wraparound of table 			}
		automatic aggressive vacuum of table 
		automatic vacuum of table
		*/

	if (strstr(edata->message, "vacuum") == NULL &&
		strstr(edata->message, "analyze") == NULL)
		return;

	/*
	 * Write the message into the next slot of the ring buffer.
	 * When the buffer is full the oldest entry is silently overwritten.
	 */
	LWLockAcquire(dsdiag_autovacuum->lock, LW_EXCLUSIVE);

	dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].logged_at = GetCurrentTimestamp();

	if (strstr(edata->message, "vacuum") != NULL)
		strlcpy(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].operation, "vacuum",
				sizeof(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].operation));
	else
		strlcpy(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].operation, "analyze",
				sizeof(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].operation));

	/* database: resolve the name from the OID (valid in any connected backend) */
	{
		const char *dbname = get_database_name(MyDatabaseId);

		strlcpy(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].datname,
				dbname ? dbname : "",
				NAMEDATALEN);
	}

	/* schema and table are parsed from the message text */
	parse_table_from_message(edata->message,
							 dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].schemaname,
							 dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].relname);

	{
		Oid			nsoid = get_namespace_oid(
										dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].schemaname,
										true /* missing_ok */ );

		dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].reloid =
			(nsoid != InvalidOid)
			? get_relname_relid(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].relname, nsoid)
			: InvalidOid;
	}

	strlcpy(dsdiag_autovacuum->entries[dsdiag_autovacuum->tail].message,
			edata->message, DSDIAG_MSG_LEN);
	dsdiag_autovacuum->tail = (dsdiag_autovacuum->tail + 1) % dsdiag_autovacuum->max;
	if (dsdiag_autovacuum->count < dsdiag_autovacuum->max)
		dsdiag_autovacuum->count++;
	else
		dsdiag_autovacuum->head = (dsdiag_autovacuum->head + 1) % dsdiag_autovacuum->max;

	LWLockRelease(dsdiag_autovacuum->lock);
}


void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the datasentinel_diag functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define custom GUC variables.
	 */
	DefineCustomIntVariable("datasentinel_diag.max",
							"Sets the maximum number of actions tracked by datasentinel_diag.",
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
	DefineCustomBoolVariable("datasentinel_diag.save",
							 "Save datasentinel_diag actions across server shutdowns.",
							 NULL,
							 &dump_on_shutdown,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

#if PG_VERSION_NUM < 150000
	dsdiag_shmem_request();
#endif

	/*
	 * Install hooks.
	 */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = dsdiag_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = dsdiag_shmem_startup;
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = dsdiag_emit_log;

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

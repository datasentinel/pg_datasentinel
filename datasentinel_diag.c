#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "tcop/utility.h"
#include "miscadmin.h"
#include <limits.h>
#include "funcapi.h"
#include "pgstat.h"
#include "utils/backend_status.h"
#include <sys/stat.h>
#include "common/file_utils.h"
#include "storage/ipc.h"
#include "nodes/makefuncs.h"

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

#define PROC_FILE_SYSTEM    "/proc"
#define DS_STAT_IDS_COLS	4

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

static void dsdiag_shmem_request(void);
static void dsdiag_shmem_startup(void);
static void dsdiag_process_utility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest,
					QueryCompletion *qc);


static int	max_actions;		/* max # actions to track */
static bool dump_on_shutdown;	/* whether to save actions across shutdown */

PG_FUNCTION_INFO_V1(ds_stat_pids);

static bool test_fs_access(char *file_system_path);
static long get_rss_memory_pages(int pid);
static long get_temporary_usage(int pid);

/*
 * dsdiag_shmem_request: request shared memory to the core.
 * Called as a hook in PG15 or later, otherwise called from _PG_init().
 */
static void
dsdiag_shmem_request(void)
{
	return;

}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 */
static void
dsdiag_shmem_startup(void)
{
	return;
}

Datum
ds_stat_pids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	long		page_size = sysconf(_SC_PAGESIZE);
	bool		is_allowed_fs_access = test_fs_access(PROC_FILE_SYSTEM);


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
		if (is_allowed_fs_access)
		{
			long		rss_pages = get_rss_memory_pages(beentry->st_procpid);
			long		temp_bytes = get_temporary_usage(beentry->st_procpid);

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

long
get_temporary_usage(int pid)
{
	char		fd_path[256];
	struct dirent *entry;
	ssize_t		len;
	DIR		   *dir;
	long		temporary_size = 0;
	char		link_target[MAXPGPATH];

	snprintf(fd_path, sizeof(fd_path), "/proc/%d/fd", pid);

	dir = opendir(fd_path);
	if (dir == NULL)
	{
		elog(DEBUG1, "Error opening directory \"%s\"", fd_path);
		return -1;
	}

	/**
	 * Iterates through the file descriptors of a process to identify temporary PostgreSQL files.
	 */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		snprintf(fd_path, sizeof(fd_path), "/proc/%d/fd/%s", pid, entry->d_name);

		len = readlink(fd_path, link_target, sizeof(link_target) - 1);
		if (len != -1)
		{
			link_target[len] = '\0';
			if (strstr(link_target, PG_TEMP_FILE_PREFIX) != NULL)
			{
				struct stat stat_buf;

				if (stat(link_target, &stat_buf) == 0)
				{
					elog(DEBUG1, "Temp file found: \"%s\" (target: \"%s\"), Size: %ld bytes",
						 fd_path, link_target, (long) stat_buf.st_size);
					temporary_size += (long) stat_buf.st_size;
				}
			}
		}

	}

	closedir(dir);

	elog(DEBUG1, "Total temporary file usage for PID %d: %ld bytes", pid, temporary_size);
	return temporary_size;
}

bool
test_fs_access(char *file_system_path)
{
	DIR		   *dirp = NULL;

	dirp = opendir(file_system_path);
	if (!dirp)
	{
		elog(DEBUG1, "Error opening directory \"%s\"", file_system_path);
		return false;
	}
	closedir(dirp);
	return true;
}

static void
dsdiag_process_utility(PlannedStmt *pstmt, const char *queryString,
							bool readOnlyTree, ProcessUtilityContext context,
							ParamListInfo params, QueryEnvironment *queryEnv,
							DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vac = (VacuumStmt *) parsetree;

		elog(INFO, "VACUUM command detected: %s", queryString);
	}

	if (prev_process_utility_hook)
		prev_process_utility_hook(pstmt, queryString, readOnlyTree, context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

long
get_rss_memory_pages(int pid)
{
	long		rss = 0;
	FILE	   *fp;
	char		filename[256];

	snprintf(filename, sizeof(filename), "/proc/%d/statm", pid);

	fp = fopen(filename, "r");
	if (fp != NULL)
	{
		long		size,
					share,
		text	   ,
					lib,
					data,
					dt;

		if (fscanf(fp, "%ld %ld %ld %ld %ld %ld %ld", &size, &rss, &share, &text, &lib, &data, &dt) != 7)
		{
			rss = -1;			/* Error reading */
		}
		fclose(fp);
	}
	else
	{
		rss = -1;				/* Error opening file */
	}

	return rss;
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
	 * Define (or redefine) custom GUC variables.
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
	prev_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = dsdiag_process_utility;

}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
	shmem_startup_hook = prev_shmem_startup_hook;
	ProcessUtility_hook = prev_process_utility_hook;
}

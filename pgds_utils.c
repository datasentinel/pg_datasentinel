#include "postgres.h"
#include "catalog/pg_type.h"
#include "catalog/pg_database.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include <regex.h>
#include <stdlib.h>

#include "pgds_utils.h"

/*
 * Scan pg_database to find the OID of the database with the minimum
 * datminmxid.  Requires catalog access, so must only be called from a
 * regular backend.
 */
Oid
pgds_get_oldest_mxid_database(void)
{
	volatile Relation		rel = NULL;
	volatile TableScanDesc	scan = NULL;
	HeapTuple		tup;
	MultiXactId		oldest_mxid = MaxMultiXactId;
	Oid				result = InvalidOid;
	bool			first = true;

	PG_TRY();
	{
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
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();
		elog(LOG, "pg_datasentinel: error scanning pg_database: %s", edata->message);
		FreeErrorData(edata);
		if (scan != NULL)
			table_endscan((TableScanDesc) scan);
		if (rel != NULL)
			table_close((Relation) rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * Build a palloc'd Interval from a duration expressed in seconds.
 * Negative values are clamped to zero (should not happen in practice).
 */
Interval *
pgds_secs_to_interval(double secs)
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
 * Return true if the VacuumStmt has the VERBOSE option set.
 */
bool
pgds_vacuum_is_verbose(VacuumStmt *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "verbose") == 0)
			return defGetBoolean(opt);
	}
	return false;
}

/*
 * Parse schemaname and relname from alog message.
 *
 * The message format is:
 *   automatic [aggressive] vacuum [to prevent wraparound] of table "db.schema.table": ...
 *   automatic analyze of table "db.schema.table": ...
 *
 * Writes empty strings if the pattern is not found.
 */
void
pgds_parse_table_from_message(const char *message, char *schemaname, char *relname)
{
	const char *start;
	const char *end;
	const char *dot1;
	const char *dot2;
	int			len;

	schemaname[0] = '\0';
	relname[0] = '\0';

	start = strstr(message, "of table \"");
	if (start == NULL)
		return;
	start += strlen("of table \"");

	end = strchr(start, '"');
	if (end == NULL)
		return;

	/* skip dbname (first component) */
	dot1 = memchr(start, '.', end - start);
	if (dot1 == NULL)
		return;
	dot1++;

	/* find the dot between schemaname and relname */
	dot2 = memchr(dot1, '.', end - dot1);
	if (dot2 == NULL)
		return;

	len = dot2 - dot1;
	if (len >= NAMEDATALEN)
		len = NAMEDATALEN - 1;
	memcpy(schemaname, dot1, len);
	schemaname[len] = '\0';

	dot2++;
	len = end - dot2;
	if (len >= NAMEDATALEN)
		len = NAMEDATALEN - 1;
	memcpy(relname, dot2, len);
	relname[len] = '\0';
}

/*
 * Copy a regex sub-match into a temporary buffer and convert it to int64.
 */
static int64
match_to_int64(const char *str, regmatch_t *pm, int n)
{
	char	buf[32];
	int		len = pm[n].rm_eo - pm[n].rm_so;

	if (len > (int) sizeof(buf) - 1)
		len = sizeof(buf) - 1;
	memcpy(buf, str + pm[n].rm_so, len);
	buf[len] = '\0';
	return (int64) strtoll(buf, NULL, 10);
}

/*
 * Copy a regex sub-match into a temporary buffer and convert it to double.
 */
static double
match_to_double(const char *str, regmatch_t *pm, int n)
{
	char	buf[32];
	int		len = pm[n].rm_eo - pm[n].rm_so;

	if (len > (int) sizeof(buf) - 1)
		len = sizeof(buf) - 1;
	memcpy(buf, str + pm[n].rm_so, len);
	buf[len] = '\0';
	return strtod(buf, NULL);
}

/*
 * Parse numeric fields from an autovacuum LOG message.
 *
 * Extracts:
 *   pages  line: "pages: N removed, N remain, N scanned ..."
 *   tuples line: "tuples: N removed, N remain ..."
 *
 * Patterns are compiled once per process (static) for efficiency.
 * All output parameters are set to 0 when the pattern is not found.
 */
void
pgds_parse_vacuum_stats(const char *message,
						int64 *pages_removed,
						int64 *pages_remain,
						int64 *pages_scanned,
						int64 *tuples_removed,
						int64 *tuples_remain)
{
	static regex_t	re_pages;
	static regex_t	re_tuples;
	static bool		initialized = false;
	regmatch_t		pm[4];

	*pages_removed  = 0;
	*pages_remain   = 0;
	*pages_scanned  = 0;
	*tuples_removed = 0;
	*tuples_remain  = 0;

	if (!initialized)
	{
		regcomp(&re_pages,
				"^pages: ([0-9]+) removed, ([0-9]+) remain, ([0-9]+) scanned",
				REG_EXTENDED | REG_NEWLINE);
		regcomp(&re_tuples,
				"^tuples: ([0-9]+) removed, ([0-9]+) remain",
				REG_EXTENDED | REG_NEWLINE);
		initialized = true;
	}

	if (regexec(&re_pages, message, 4, pm, 0) == 0)
	{
		*pages_removed = match_to_int64(message, pm, 1);
		*pages_remain  = match_to_int64(message, pm, 2);
		*pages_scanned = match_to_int64(message, pm, 3);
	}

	if (regexec(&re_tuples, message, 3, pm, 0) == 0)
	{
		*tuples_removed = match_to_int64(message, pm, 1);
		*tuples_remain  = match_to_int64(message, pm, 2);
	}
}

/*
 * Parse CPU timing fields from an autovacuum or autoanalyze LOG message.
 *
 * Extracts the values from the line:
 *   system usage: CPU: user: N.NN s, system: N.NN s, elapsed: N.NN s
 *
 * Pattern is compiled once per process (static) for efficiency.
 * All output parameters are set to 0.0 when the pattern is not found.
 */
void
pgds_parse_cpu_stats(const char *message,
					 double *user_cpu,
					 double *sys_cpu,
					 double *elapsed)
{
	static regex_t	re_cpu;
	static bool		initialized = false;
	regmatch_t		pm[4];

	*user_cpu = 0.0;
	*sys_cpu  = 0.0;
	*elapsed  = 0.0;

	if (!initialized)
	{
		regcomp(&re_cpu,
				"system usage: CPU: user: ([0-9]+\\.[0-9]+) s, system: ([0-9]+\\.[0-9]+) s, elapsed: ([0-9]+\\.[0-9]+) s",
				REG_EXTENDED | REG_NEWLINE);
		initialized = true;
	}

	if (regexec(&re_cpu, message, 4, pm, 0) == 0)
	{
		*user_cpu = match_to_double(message, pm, 1);
		*sys_cpu  = match_to_double(message, pm, 2);
		*elapsed  = match_to_double(message, pm, 3);
	}
}

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"

#include "../pgds_utils.h"

#ifdef __linux__
#include <unistd.h>
#include "linux/pgds_proc.h"
#include "linux/pgds_cgroup.h"
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pgds_parse_table_from_message);
PG_FUNCTION_INFO_V1(test_pgds_parse_table_from_vacuuming);
PG_FUNCTION_INFO_V1(test_pgds_parse_table_from_analyzing);
PG_FUNCTION_INFO_V1(test_pgds_parse_vacuum_stats);
PG_FUNCTION_INFO_V1(test_pgds_parse_cpu_stats);
PG_FUNCTION_INFO_V1(test_pgds_vacuum_is_verbose);
PG_FUNCTION_INFO_V1(test_pgds_proc_functions);
PG_FUNCTION_INFO_V1(test_pgds_cgroup_info);

#define PASS_FAIL(cond)   ((cond) ? "PASS" : "FAIL")
#define TEST(desc, cond) \
	do { \
		appendStringInfo(&buf, "Test: %-52s %s\n", (desc), PASS_FAIL(cond)); \
		if (!(cond)) failures++; \
	} while (0)
#define FLOAT_EQ(a, b)   (((a) - (b)) < 1e-6 && ((b) - (a)) < 1e-6)

Datum
test_pgds_parse_table_from_message(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	int			failures = 0;

	initStringInfo(&buf);

	/* Test: normal vacuum */
	pgds_parse_table_from_message(
		"automatic vacuum of table \"mydb.public.mytable\": index scans: 0",
		schemaname, relname);
	TEST("vacuum: schemaname = public",  strcmp(schemaname, "public") == 0);
	TEST("vacuum: relname = mytable",    strcmp(relname, "mytable") == 0);

	/* Test: aggressive vacuum */
	pgds_parse_table_from_message(
		"automatic aggressive vacuum of table \"mydb.pg_catalog.pg_class\": index scans: 1",
		schemaname, relname);
	TEST("aggressive vacuum: schemaname = pg_catalog", strcmp(schemaname, "pg_catalog") == 0);
	TEST("aggressive vacuum: relname = pg_class",      strcmp(relname, "pg_class") == 0);

	/* Test: vacuum to prevent wraparound */
	pgds_parse_table_from_message(
		"automatic vacuum to prevent wraparound of table \"postgres.myschema.orders\": pages: 10",
		schemaname, relname);
	TEST("wraparound vacuum: schemaname = myschema", strcmp(schemaname, "myschema") == 0);
	TEST("wraparound vacuum: relname = orders",      strcmp(relname, "orders") == 0);

	/* Test: analyze */
	pgds_parse_table_from_message(
		"automatic analyze of table \"mydb.public.pgbench_accounts\": system usage: CPU ...",
		schemaname, relname);
	TEST("analyze: schemaname = public",           strcmp(schemaname, "public") == 0);
	TEST("analyze: relname = pgbench_accounts",    strcmp(relname, "pgbench_accounts") == 0);

	/* Test: no "of table" pattern → empty strings */
	pgds_parse_table_from_message("some unrelated log message", schemaname, relname);
	TEST("no match: schemaname is empty", schemaname[0] == '\0');
	TEST("no match: relname is empty",    relname[0] == '\0');

	/* Test: missing closing quote → empty strings */
	pgds_parse_table_from_message(
		"automatic vacuum of table \"mydb.public.broken",
		schemaname, relname);
	TEST("missing close quote: schemaname is empty", schemaname[0] == '\0');
	TEST("missing close quote: relname is empty",    relname[0] == '\0');

	/* Test: only one dot inside quotes → empty strings */
	pgds_parse_table_from_message(
		"automatic vacuum of table \"mydb.public\"",
		schemaname, relname);
	TEST("one dot: schemaname is empty", schemaname[0] == '\0');
	TEST("one dot: relname is empty",    relname[0] == '\0');

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

Datum
test_pgds_parse_table_from_vacuuming(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	int			failures = 0;

	initStringInfo(&buf);

	/* Test: normal manual vacuum */
	pgds_parse_table_from_vacuuming(
		"finished vacuuming \"public.mytable\": index scans: 0",
		schemaname, relname);
	TEST("vacuum: schemaname = public",  strcmp(schemaname, "public") == 0);
	TEST("vacuum: relname = mytable",    strcmp(relname, "mytable") == 0);

	/* Test: non-default schema */
	pgds_parse_table_from_vacuuming(
		"finished vacuuming \"myschema.orders\": index scans: 1",
		schemaname, relname);
	TEST("schema: schemaname = myschema", strcmp(schemaname, "myschema") == 0);
	TEST("schema: relname = orders",      strcmp(relname, "orders") == 0);

	/* Test: no "finished vacuuming" pattern → empty strings */
	pgds_parse_table_from_vacuuming("some unrelated log message", schemaname, relname);
	TEST("no match: schemaname is empty", schemaname[0] == '\0');
	TEST("no match: relname is empty",    relname[0] == '\0');

	/* Test: missing closing quote → empty strings */
	pgds_parse_table_from_vacuuming(
		"finished vacuuming \"public.broken",
		schemaname, relname);
	TEST("missing close quote: schemaname is empty", schemaname[0] == '\0');
	TEST("missing close quote: relname is empty",    relname[0] == '\0');

	/* Test: no dot inside quotes → empty strings */
	pgds_parse_table_from_vacuuming(
		"finished vacuuming \"nodothere\"",
		schemaname, relname);
	TEST("no dot: schemaname is empty", schemaname[0] == '\0');
	TEST("no dot: relname is empty",    relname[0] == '\0');

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

Datum
test_pgds_parse_table_from_analyzing(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	int			failures = 0;

	initStringInfo(&buf);

	/* Test: normal case */
	pgds_parse_table_from_analyzing(
		"analyzing \"public.mytable\"",
		schemaname, relname);
	TEST("analyze: schemaname = public", strcmp(schemaname, "public") == 0);
	TEST("analyze: relname = mytable",   strcmp(relname, "mytable") == 0);

	/* Test: non-default schema */
	pgds_parse_table_from_analyzing(
		"analyzing \"myschema.orders\"",
		schemaname, relname);
	TEST("schema: schemaname = myschema", strcmp(schemaname, "myschema") == 0);
	TEST("schema: relname = orders",      strcmp(relname, "orders") == 0);

	/* Test: no "analyzing" pattern → empty strings */
	pgds_parse_table_from_analyzing("some unrelated log message", schemaname, relname);
	TEST("no match: schemaname is empty", schemaname[0] == '\0');
	TEST("no match: relname is empty",    relname[0] == '\0');

	/* Test: missing closing quote → empty strings */
	pgds_parse_table_from_analyzing(
		"analyzing \"public.broken",
		schemaname, relname);
	TEST("missing close quote: schemaname is empty", schemaname[0] == '\0');
	TEST("missing close quote: relname is empty",    relname[0] == '\0');

	/* Test: no dot inside quotes → empty strings */
	pgds_parse_table_from_analyzing(
		"analyzing \"nodothere\"",
		schemaname, relname);
	TEST("no dot: schemaname is empty", schemaname[0] == '\0');
	TEST("no dot: relname is empty",    relname[0] == '\0');

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * test_pgds_parse_vacuum_stats
 *
 * Unit tests for pgds_parse_vacuum_stats() and, indirectly, match_to_int64()
 * from pgds_utils.c.
 */
Datum
test_pgds_parse_vacuum_stats(PG_FUNCTION_ARGS)
{
	StringInfoData	buf;
	int64			pages_removed, pages_remain, pages_scanned;
	int64			tuples_removed, tuples_remain;
	int				failures = 0;

	initStringInfo(&buf);

	/*
	 * Test 1: full vacuum message from a real pgbench run.
	 * Exercises all five fields and validates match_to_int64() with zero
	 * and non-zero values.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"pgbench.public.pgbench_tellers\": index scans: 1\n"
			"pages: 0 removed, 236 remain, 236 scanned (100.00% of total), 0 eagerly scanned\n"
			"tuples: 6057 removed, 1053 remain, 53 are dead but not yet removable\n"
			"removable cutoff: 509815355, which was 120 XIDs old when operation ended\n"
			"new relfrozenxid: 509808318, which is 301211 XIDs ahead of previous value\n"
			"frozen: 0 pages from table (0.00% of total) had 0 tuples frozen\n"
			"visibility map: 180 pages set all-visible, 146 pages set all-frozen\n"
			"system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.01 s";

		pgds_parse_vacuum_stats(msg,
								&pages_removed, &pages_remain, &pages_scanned,
								&tuples_removed, &tuples_remain);
		TEST("full msg: pages_removed = 0",    pages_removed == 0);
		TEST("full msg: pages_remain = 236",   pages_remain == 236);
		TEST("full msg: pages_scanned = 236",  pages_scanned == 236);
		TEST("full msg: tuples_removed = 6057", tuples_removed == 6057);
		TEST("full msg: tuples_remain = 1053", tuples_remain == 1053);
	}

	/*
	 * Test 2: message with no pages/tuples lines → all outputs are zero.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.t\": index scans: 0\n"
			"system usage: CPU: user: 0.00 s";

		pgds_parse_vacuum_stats(msg,
								&pages_removed, &pages_remain, &pages_scanned,
								&tuples_removed, &tuples_remain);
		TEST("no lines: pages_removed = 0",   pages_removed == 0);
		TEST("no lines: pages_remain = 0",    pages_remain == 0);
		TEST("no lines: pages_scanned = 0",   pages_scanned == 0);
		TEST("no lines: tuples_removed = 0",  tuples_removed == 0);
		TEST("no lines: tuples_remain = 0",   tuples_remain == 0);
	}

	/*
	 * Test 3: pages line present but no tuples line.
	 * Tuples fields must remain zero.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.t\": index scans: 0\n"
			"pages: 10 removed, 500 remain, 510 scanned (100.00% of total)\n"
			"system usage: CPU: user: 0.00 s";

		pgds_parse_vacuum_stats(msg,
								&pages_removed, &pages_remain, &pages_scanned,
								&tuples_removed, &tuples_remain);
		TEST("pages only: pages_removed = 10",  pages_removed == 10);
		TEST("pages only: pages_remain = 500",  pages_remain == 500);
		TEST("pages only: pages_scanned = 510", pages_scanned == 510);
		TEST("pages only: tuples_removed = 0",  tuples_removed == 0);
		TEST("pages only: tuples_remain = 0",   tuples_remain == 0);
	}

	/*
	 * Test 4: tuples line present but no pages line.
	 * Pages fields must remain zero.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.t\": index scans: 0\n"
			"tuples: 100 removed, 200 remain, 0 are dead\n"
			"system usage: CPU: user: 0.00 s";

		pgds_parse_vacuum_stats(msg,
								&pages_removed, &pages_remain, &pages_scanned,
								&tuples_removed, &tuples_remain);
		TEST("tuples only: pages_removed = 0",    pages_removed == 0);
		TEST("tuples only: pages_remain = 0",     pages_remain == 0);
		TEST("tuples only: pages_scanned = 0",    pages_scanned == 0);
		TEST("tuples only: tuples_removed = 100", tuples_removed == 100);
		TEST("tuples only: tuples_remain = 200",  tuples_remain == 200);
	}

	/*
	 * Test 5: large values — exercises match_to_int64() with multi-digit
	 * numbers well within int64 range.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.big\": index scans: 2\n"
			"pages: 1000000 removed, 9999999 remain, 10999999 scanned (100.00% of total)\n"
			"tuples: 5000000 removed, 3000000 remain, 0 are dead\n"
			"system usage: CPU: user: 1.00 s";

		pgds_parse_vacuum_stats(msg,
								&pages_removed, &pages_remain, &pages_scanned,
								&tuples_removed, &tuples_remain);
		TEST("large: pages_removed = 1000000",  pages_removed == 1000000);
		TEST("large: pages_remain = 9999999",   pages_remain == 9999999);
		TEST("large: pages_scanned = 10999999", pages_scanned == 10999999);
		TEST("large: tuples_removed = 5000000", tuples_removed == 5000000);
		TEST("large: tuples_remain = 3000000",  tuples_remain == 3000000);
	}

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * test_pgds_parse_cpu_stats
 *
 * Unit tests for pgds_parse_cpu_stats() and, indirectly, match_to_double()
 * from pgds_utils.c.
 */
Datum
test_pgds_parse_cpu_stats(PG_FUNCTION_ARGS)
{
	StringInfoData	buf;
	double			user_cpu, sys_cpu, elapsed;
	int				failures = 0;

	initStringInfo(&buf);

	/*
	 * Test 1: full vacuum message — real pgbench example.
	 * All three fields must be parsed correctly from the system usage line.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"pgbench.public.pgbench_tellers\": index scans: 1\n"
			"pages: 0 removed, 236 remain, 236 scanned (100.00% of total), 0 eagerly scanned\n"
			"tuples: 6057 removed, 1053 remain, 53 are dead but not yet removable\n"
			"removable cutoff: 509815355, which was 120 XIDs old when operation ended\n"
			"new relfrozenxid: 509808318, which is 301211 XIDs ahead of previous value\n"
			"frozen: 0 pages from table (0.00% of total) had 0 tuples frozen\n"
			"visibility map: 180 pages set all-visible, 146 pages set all-frozen\n"
			"system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.01 s";

		pgds_parse_cpu_stats(msg, &user_cpu, &sys_cpu, &elapsed);
		TEST("full vacuum: user_cpu = 0.00",    FLOAT_EQ(user_cpu, 0.00));
		TEST("full vacuum: sys_cpu = 0.00",     FLOAT_EQ(sys_cpu,  0.00));
		TEST("full vacuum: elapsed = 0.01",     FLOAT_EQ(elapsed,  0.01));
	}

	/*
	 * Test 2: full autoanalyze message — real pgbench example.
	 */
	{
		const char *msg =
			"automatic analyze of table \"pgbench.public.pgbench_tellers\"\n"
			"I/O timings: read: 1.415 ms, write: 0.115 ms\n"
			"avg read rate: 115.234 MB/s, avg write rate: 1.953 MB/s\n"
			"buffer usage: 168 hits, 118 reads, 2 dirtied\n"
			"WAL usage: 7 records, 2 full page images, 14059 bytes, 0 buffers full\n"
			"system usage: CPU: user: 10.00 s, system: 4.14 s, elapsed: 1123.00 s";

		pgds_parse_cpu_stats(msg, &user_cpu, &sys_cpu, &elapsed);
		TEST("full analyze: user_cpu = 10.00",   FLOAT_EQ(user_cpu, 10.00));
		TEST("full analyze: sys_cpu = 4.14",    FLOAT_EQ(sys_cpu,  4.14));
		TEST("full analyze: elapsed = 1123.00", FLOAT_EQ(elapsed,  1123.00));
	}

	/*
	 * Test 3: non-zero CPU values — exercises match_to_double() with
	 * values that differ across all three fields.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.big\": index scans: 3\n"
			"pages: 500 removed, 1000 remain, 1500 scanned (100.00% of total)\n"
			"tuples: 10000 removed, 20000 remain, 0 are dead\n"
			"system usage: CPU: user: 1.23 s, system: 0.45 s, elapsed: 2.10 s";

		pgds_parse_cpu_stats(msg, &user_cpu, &sys_cpu, &elapsed);
		TEST("non-zero: user_cpu = 1.23",       FLOAT_EQ(user_cpu, 1.23));
		TEST("non-zero: sys_cpu = 0.45",        FLOAT_EQ(sys_cpu,  0.45));
		TEST("non-zero: elapsed = 2.10",        FLOAT_EQ(elapsed,  2.10));
	}

	/*
	 * Test 4: message with no system usage line → all outputs must be 0.0.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.t\": index scans: 0\n"
			"pages: 0 removed, 5 remain, 5 scanned (100.00% of total)";

		pgds_parse_cpu_stats(msg, &user_cpu, &sys_cpu, &elapsed);
		TEST("no cpu line: user_cpu = 0.00",    FLOAT_EQ(user_cpu, 0.00));
		TEST("no cpu line: sys_cpu = 0.00",     FLOAT_EQ(sys_cpu,  0.00));
		TEST("no cpu line: elapsed = 0.00",     FLOAT_EQ(elapsed,  0.00));
	}

	/*
	 * Test 5: large CPU values — exercises multi-digit integer parts.
	 */
	{
		const char *msg =
			"automatic vacuum of table \"mydb.public.huge\": index scans: 10\n"
			"system usage: CPU: user: 10.00 s, system: 5.00 s, elapsed: 15.50 s";

		pgds_parse_cpu_stats(msg, &user_cpu, &sys_cpu, &elapsed);
		TEST("large: user_cpu = 10.00",         FLOAT_EQ(user_cpu, 10.00));
		TEST("large: sys_cpu = 5.00",           FLOAT_EQ(sys_cpu,   5.00));
		TEST("large: elapsed = 15.50",          FLOAT_EQ(elapsed,  15.50));
	}

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Build a VacuumStmt with a single boolean DefElem option.
 */
static VacuumStmt *
make_vacuum_stmt(const char *optname, bool optval)
{
	VacuumStmt *stmt = makeNode(VacuumStmt);
	DefElem    *opt = makeDefElem(pstrdup(optname),
								  (Node *) makeBoolean(optval), -1);

	stmt->options = list_make1(opt);
	stmt->rels = NIL;
	stmt->is_vacuumcmd = true;
	return stmt;
}

/*
 * test_pgds_vacuum_is_verbose
 *
 * Unit tests for pgds_vacuum_is_verbose() from pgds_utils.c.
 */
Datum
test_pgds_vacuum_is_verbose(PG_FUNCTION_ARGS)
{
	StringInfoData	buf;
	VacuumStmt	   *stmt;
	int				failures = 0;

	initStringInfo(&buf);

	/* verbose = true */
	stmt = make_vacuum_stmt("verbose", true);
	TEST("verbose true: returns true", pgds_vacuum_is_verbose(stmt) == true);

	/* verbose = false */
	stmt = make_vacuum_stmt("verbose", false);
	TEST("verbose false: returns false", pgds_vacuum_is_verbose(stmt) == false);

	/* no options at all */
	stmt = makeNode(VacuumStmt);
	stmt->options = NIL;
	stmt->rels = NIL;
	stmt->is_vacuumcmd = true;
	TEST("no options: returns false", pgds_vacuum_is_verbose(stmt) == false);

	/* unrelated option only */
	stmt = make_vacuum_stmt("analyze", true);
	TEST("analyze option only: returns false", pgds_vacuum_is_verbose(stmt) == false);

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * test_pgds_proc_functions
 *
 * Unit tests for pgds_is_dir_accessible(), pgds_get_pss_memory_bytes(), and
 * pgds_get_temp_file_bytes() from linux/pgds_proc.c.
 * On non-Linux platforms the function returns a SKIPPED notice.
 */
Datum
test_pgds_proc_functions(PG_FUNCTION_ARGS)
{
#ifdef __linux__
	StringInfoData	buf;
	int				failures = 0;
	int				mypid;
	int64			val;

	initStringInfo(&buf);

	mypid = (int) getpid();

	TEST("is_dir_accessible(/tmp) = true",
		 pgds_is_dir_accessible("/tmp"));
	TEST("is_dir_accessible(/nonexistent) = false",
		 !pgds_is_dir_accessible("/nonexistent_pgds_test_xyz_123"));
	TEST("is_dir_accessible(/etc/passwd) = false",
		 !pgds_is_dir_accessible("/etc/passwd"));

	val = pgds_get_pss_memory_bytes(mypid);
	TEST("get_pss_memory_bytes(self) > 0", val > 0);

	val = pgds_get_pss_memory_bytes(-1);
	TEST("get_pss_memory_bytes(-1) = -1", val == -1);

	val = pgds_get_temp_file_bytes(mypid);
	TEST("get_temp_file_bytes(self) >= 0", val >= 0);

	val = pgds_get_temp_file_bytes(-1);
	TEST("get_temp_file_bytes(-1) = -1", val == -1);

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
#else
	PG_RETURN_TEXT_P(cstring_to_text("SKIPPED (not Linux)\n"));
#endif
}

/*
 * test_pgds_cgroup_info
 *
 * Unit tests for pgds_read_cgroup_info() from linux/pgds_cgroup.c.
 * Validates structural invariants that hold regardless of whether cgroups are
 * active on the host.  On non-Linux platforms returns a SKIPPED notice.
 */
Datum
test_pgds_cgroup_info(PG_FUNCTION_ARGS)
{
#ifdef __linux__
	StringInfoData	buf;
	int				failures = 0;
	PgdsCgroupInfo	info;
	bool			has_cgroup;

	initStringInfo(&buf);

	has_cgroup = pgds_read_cgroup_info(&info);

	TEST("read_cgroup_info completes without error", true);
	TEST("version matches cgroup availability",
		 has_cgroup ? (info.version == 1 || info.version == 2)
					: info.version == 0);
	TEST("cpu_limit_set implies cpu_limit > 0",
		 !info.cpu_limit_set || info.cpu_limit > 0.0);
	TEST("mem_limit_set implies mem_limit_bytes > 0",
		 !info.mem_limit_set || info.mem_limit_bytes > 0);
	TEST("mem_used_set implies mem_used_bytes >= 0",
		 !info.mem_used_set || info.mem_used_bytes >= 0);
	TEST("cpu_pressure_set implies version == 2",
		 !info.cpu_pressure_set || info.version == 2);

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
#else
	PG_RETURN_TEXT_P(cstring_to_text("SKIPPED (not Linux)\n"));
#endif
}

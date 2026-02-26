#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "../dsdiag_utils.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_dsdiag_utils);

#define PASS_FAIL(cond)   ((cond) ? "PASS" : "FAIL")
#define TEST(desc, cond) \
	do { \
		appendStringInfo(&buf, "Test: %-52s %s\n", (desc), PASS_FAIL(cond)); \
		if (!(cond)) failures++; \
	} while (0)

Datum
test_dsdiag_utils(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	char		schemaname[NAMEDATALEN];
	char		relname[NAMEDATALEN];
	int			failures = 0;

	initStringInfo(&buf);

	/* Test: normal vacuum */
	parse_table_from_message(
		"automatic vacuum of table \"mydb.public.mytable\": index scans: 0",
		schemaname, relname);
	TEST("vacuum: schemaname = public",  strcmp(schemaname, "public") == 0);
	TEST("vacuum: relname = mytable",    strcmp(relname, "mytable") == 0);

	/* Test: aggressive vacuum */
	parse_table_from_message(
		"automatic aggressive vacuum of table \"mydb.pg_catalog.pg_class\": index scans: 1",
		schemaname, relname);
	TEST("aggressive vacuum: schemaname = pg_catalog", strcmp(schemaname, "pg_catalog") == 0);
	TEST("aggressive vacuum: relname = pg_class",      strcmp(relname, "pg_class") == 0);

	/* Test: vacuum to prevent wraparound */
	parse_table_from_message(
		"automatic vacuum to prevent wraparound of table \"postgres.myschema.orders\": pages: 10",
		schemaname, relname);
	TEST("wraparound vacuum: schemaname = myschema", strcmp(schemaname, "myschema") == 0);
	TEST("wraparound vacuum: relname = orders",      strcmp(relname, "orders") == 0);

	/* Test: analyze */
	parse_table_from_message(
		"automatic analyze of table \"mydb.public.pgbench_accounts\": system usage: CPU ...",
		schemaname, relname);
	TEST("analyze: schemaname = public",           strcmp(schemaname, "public") == 0);
	TEST("analyze: relname = pgbench_accounts",    strcmp(relname, "pgbench_accounts") == 0);

	/* Test: no "of table" pattern → empty strings */
	parse_table_from_message("some unrelated log message", schemaname, relname);
	TEST("no match: schemaname is empty", schemaname[0] == '\0');
	TEST("no match: relname is empty",    relname[0] == '\0');

	/* Test: missing closing quote → empty strings */
	parse_table_from_message(
		"automatic vacuum of table \"mydb.public.broken",
		schemaname, relname);
	TEST("missing close quote: schemaname is empty", schemaname[0] == '\0');
	TEST("missing close quote: relname is empty",    relname[0] == '\0');

	/* Test: only one dot inside quotes → empty strings */
	parse_table_from_message(
		"automatic vacuum of table \"mydb.public\"",
		schemaname, relname);
	TEST("one dot: schemaname is empty", schemaname[0] == '\0');
	TEST("one dot: relname is empty",    relname[0] == '\0');

	appendStringInfo(&buf, "\n%s\n",
					 failures == 0 ? "All tests PASSED" : "Some tests FAILED");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

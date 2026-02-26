#include "postgres.h"
#include "catalog/pg_type.h"

#include "dsdiag_utils.h"

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
parse_table_from_message(const char *message, char *schemaname, char *relname)
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

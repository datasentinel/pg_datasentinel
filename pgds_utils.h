#ifndef PGDS_UTILS_H
#define PGDS_UTILS_H

extern void pgds_parse_table_from_message(const char *message, char *schemaname, char *relname);
extern void pgds_parse_vacuum_stats(const char *message,
									int64 *pages_removed,
									int64 *pages_remain,
									int64 *pages_scanned,
									int64 *tuples_removed,
									int64 *tuples_remain);

#endif							/* PGDS_UTILS_H */

#ifndef PGDS_UTILS_H
#define PGDS_UTILS_H

#include "postgres.h"
#include "utils/timestamp.h"

extern Oid       pgds_get_oldest_mxid_database(void);
extern Interval *pgds_secs_to_interval(double secs);
extern void pgds_parse_table_from_message(const char *message, char *schemaname, char *relname);
extern void pgds_parse_vacuum_stats(const char *message,
									int64 *pages_removed,
									int64 *pages_remain,
									int64 *pages_scanned,
									int64 *tuples_removed,
									int64 *tuples_remain);
extern void pgds_parse_cpu_stats(const char *message,
								 double *user_cpu,
								 double *sys_cpu,
								 double *elapsed);

#endif							/* PGDS_UTILS_H */

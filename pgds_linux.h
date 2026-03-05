#ifndef PGDS_LINUX_H
#define PGDS_LINUX_H

#include "postgres.h"

extern bool pgds_is_dir_accessible(const char *path);
extern long pgds_get_temp_file_bytes(int pid);
extern long pgds_get_rss_memory_pages(int pid);

/*
 * Cgroup resource limits and requests for the current process.
 *
 * Fields marked *_set are false when the limit/request is either absent
 * or set to "unlimited" in the cgroup hierarchy.  All values are expressed
 * in natural units: fractional CPUs for cpu_*, bytes for mem_*.
 *
 * version == 0 means the system is not running under cgroups at all;
 * in that case none of the other fields are meaningful.
 */
typedef struct PgdsCgroupInfo
{
	int		version;			/* 1 or 2; 0 = not under cgroups */

	bool	cpu_limit_set;
	double	cpu_limit;			/* hard CPU quota in fractional CPUs */

	bool	cpu_request_set;
	double	cpu_request;		/* CPU shares/weight in fractional CPUs */

	bool	mem_limit_set;
	int64	mem_limit_bytes;	/* hard memory limit */

	bool	mem_request_set;
	int64	mem_request_bytes;	/* soft memory limit (request) */
} PgdsCgroupInfo;

extern bool pgds_read_cgroup_info(PgdsCgroupInfo *info);

#endif							/* PGDS_LINUX_H */

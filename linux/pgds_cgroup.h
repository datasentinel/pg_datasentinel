#ifndef PGDS_CGROUP_H
#define PGDS_CGROUP_H

#include "postgres.h"

/*
 * Cgroup resource hard limits for the current process.
 *
 * Fields marked *_set are false when the corresponding limit is either absent
 * or set to "unlimited" in the cgroup hierarchy.  All values are expressed
 * in natural units: fractional CPUs for cpu_limit, bytes for mem_limit_bytes.
 *
 * version == 0 means the system is not running under cgroups at all;
 * in that case none of the other fields are meaningful.
 */
typedef struct PgdsCgroupInfo
{
	int		version;			/* 1 or 2; 0 = not under cgroups */

	bool	cpu_limit_set;
	double	cpu_limit;			/* hard CPU quota in fractional CPUs */

	bool	mem_limit_set;
	int64	mem_limit_bytes;	/* hard memory limit */

} PgdsCgroupInfo;

extern bool pgds_read_cgroup_info(PgdsCgroupInfo *info);

#endif							/* PGDS_CGROUP_H */

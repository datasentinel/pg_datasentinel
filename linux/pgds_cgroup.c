#include "postgres.h"
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pgds_cgroup.h"

static int
detect_cgroup_version(void)
{
	struct stat st;

	if (stat("/sys/fs/cgroup/cgroup.controllers", &st) == 0)
		return 2;
	if (stat("/sys/fs/cgroup/cpu", &st) == 0)
		return 1;
	if (stat("/sys/fs/cgroup/memory", &st) == 0)
		return 1;
	return 0;
}

/*
 * Read one line from a cgroup file and parse it as a long long integer.
 * Returns true and sets *val on success, false otherwise.
 */
static bool
read_cgroup_ll(const char *path, long long *val)
{
	FILE   *fp = fopen(path, "r");
	int		n;

	if (!fp)
		return false;
	n = fscanf(fp, "%lld", val);
	fclose(fp);
	return (n == 1);
}

/*
 * Parse the "some avg60=X.XX" value from a PSI cpu.pressure file.
 * Returns true and sets *avg60 on success, false otherwise.
 */
static bool
read_cpu_pressure_avg60(const char *cpath, double *avg60)
{
	char	fpath[MAXPGPATH];
	FILE   *fp;
	char	line[256];

	snprintf(fpath, sizeof(fpath), "%s/cpu.pressure", cpath);
	fp = fopen(fpath, "r");
	if (!fp)
		return false;

	while (fgets(line, sizeof(line), fp))
	{
		if (strncmp(line, "some ", 5) == 0)
		{
			char *p = strstr(line, "avg60=");

			if (p && sscanf(p, "avg60=%lf", avg60) == 1)
			{
				fclose(fp);
				return true;
			}
		}
	}
	fclose(fp);
	return false;
}

/*
 * Find the cgroup path for the current process from /proc/self/cgroup.
 *
 * For cgroup v2 pass controller = NULL: we look for the line "0::…".
 * For cgroup v1 pass the controller name (e.g. "cpu", "memory").
 *
 * The full filesystem path is built as base_dir + cgroup_path and written
 * into buf[bufsz].  Returns true on success.
 */
static bool
find_cgroup_path(int version, const char *controller,
				 const char *base_dir, char *buf, size_t bufsz)
{
	FILE   *fp;
	char	line[1024];
	bool	found = false;

	fp = fopen("/proc/self/cgroup", "r");
	if (!fp)
		return false;

	while (fgets(line, sizeof(line), fp) && !found)
	{
		char   *hier_id;
		char   *controllers;
		char   *cgroup_path;
		char   *colon1;
		char   *colon2;

		/* strip trailing newline */
		line[strcspn(line, "\n")] = '\0';

		/*
		 * Format: "hier_id:controllers:cgroup_path"
		 * For cgroup v2 unified hierarchy: "0::/path" — controllers is empty.
		 * Use strchr to avoid strtok_r skipping empty fields.
		 */
		colon1 = strchr(line, ':');
		if (!colon1) continue;
		*colon1 = '\0';
		hier_id = line;

		colon2 = strchr(colon1 + 1, ':');
		if (!colon2) continue;
		*colon2 = '\0';
		controllers = colon1 + 1;
		cgroup_path = colon2 + 1;

		if (*cgroup_path == '\0') continue;

		if (version == 2)
		{
			/* cgroup v2: the unified hierarchy always has hier_id == 0 */
			if (strcmp(hier_id, "0") == 0)
			{
				snprintf(buf, bufsz, "%s%s", base_dir, cgroup_path);
				found = true;
			}
		}
		else
		{
			/* cgroup v1: controllers field is comma-separated */
			char	ctrl_copy[128];
			char   *tok;
			char   *tp = NULL;

			strlcpy(ctrl_copy, controllers, sizeof(ctrl_copy));
			tok = strtok_r(ctrl_copy, ",", &tp);
			while (tok && !found)
			{
				if (strcmp(tok, controller) == 0)
				{
					snprintf(buf, bufsz, "%s%s", base_dir, cgroup_path);
					found = true;
				}
				tok = strtok_r(NULL, ",", &tp);
			}
		}
	}

	fclose(fp);
	return found;
}

/*
 * Very large value written by the kernel when no memory limit is set in
 * cgroup v1 (rounded down to nearest page on most kernels).
 */
#define CGROUP_V1_MEM_UNLIMITED		((long long) 0x7FFFFFFFFFFFF000LL)

/*
 * Populate *info with cgroup resource limits for the calling process
 * (currently hard CPU quota and memory limit, where available).  Returns
 * true if we are running under cgroups (v1 or v2), false otherwise.  On
 * false return, *info is zeroed and all _set flags are false.
 */
bool
pgds_read_cgroup_info(PgdsCgroupInfo *info)
{
	int		version;
	char	cpath[MAXPGPATH];
	char	fpath[MAXPGPATH];

	memset(info, 0, sizeof(*info));

	version = detect_cgroup_version();
	if (version == 0)
		return false;

	info->version = version;

	if (version == 2)
	{
		if (!find_cgroup_path(2, NULL, "/sys/fs/cgroup", cpath, sizeof(cpath)))
			strlcpy(cpath, "/sys/fs/cgroup", sizeof(cpath));

		/* CPU limit: cpu.max → "quota period" or "max period" */
		snprintf(fpath, sizeof(fpath), "%s/cpu.max", cpath);
		{
			FILE   *fp = fopen(fpath, "r");

			if (fp)
			{
				char		quota_str[32];
				long long	period;

				if (fscanf(fp, "%31s %lld", quota_str, &period) == 2 &&
					strcmp(quota_str, "max") != 0 && period > 0)
				{
					info->cpu_limit_set = true;
					info->cpu_limit = (double) atoll(quota_str) / (double) period;
				}
				fclose(fp);
			}
		}

		/* Memory limit: memory.max → bytes or "max" */
		snprintf(fpath, sizeof(fpath), "%s/memory.max", cpath);
		{
			FILE   *fp = fopen(fpath, "r");

			if (fp)
			{
				char val_str[32];

				if (fscanf(fp, "%31s", val_str) == 1 &&
					strcmp(val_str, "max") != 0)
				{
					info->mem_limit_set = true;
					info->mem_limit_bytes = (int64) atoll(val_str);
				}
				fclose(fp);
			}
		}

		/* CPU pressure: cpu.pressure → some avg60 */
		{
			double avg60;

			if (read_cpu_pressure_avg60(cpath, &avg60))
			{
				info->cpu_pressure_set = true;
				info->cpu_pressure_avg60 = avg60;
			}
		}

		/* Memory used: memory.current */
		{
			long long used;

			snprintf(fpath, sizeof(fpath), "%s/memory.current", cpath);
			if (read_cgroup_ll(fpath, &used))
			{
				info->mem_used_set = true;
				info->mem_used_bytes = (int64) used;
			}
		}
	}
	else	/* version == 1 */
	{
		/* ---- CPU ---- */
		if (find_cgroup_path(1, "cpu", "/sys/fs/cgroup/cpu", cpath, sizeof(cpath)))
		{
			long long quota,
					  period;

			snprintf(fpath, sizeof(fpath), "%s/cpu.cfs_quota_us", cpath);
			if (read_cgroup_ll(fpath, &quota) && quota > 0)
			{
				snprintf(fpath, sizeof(fpath), "%s/cpu.cfs_period_us", cpath);
				if (read_cgroup_ll(fpath, &period) && period > 0)
				{
					info->cpu_limit_set = true;
					info->cpu_limit = (double) quota / (double) period;
				}
			}

		}

		/* ---- Memory ---- */
		if (find_cgroup_path(1, "memory", "/sys/fs/cgroup/memory", cpath, sizeof(cpath)))
		{
			long long val;

			snprintf(fpath, sizeof(fpath), "%s/memory.limit_in_bytes", cpath);
			if (read_cgroup_ll(fpath, &val) && val < CGROUP_V1_MEM_UNLIMITED)
			{
				info->mem_limit_set = true;
				info->mem_limit_bytes = (int64) val;
			}

			snprintf(fpath, sizeof(fpath), "%s/memory.usage_in_bytes", cpath);
			if (read_cgroup_ll(fpath, &val))
			{
				info->mem_used_set = true;
				info->mem_used_bytes = (int64) val;
			}
		}
	}

	return true;
}

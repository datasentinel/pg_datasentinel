#include "postgres.h"
#include <dirent.h>
#include <math.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/file_utils.h"

#include "pgds_linux.h"

bool
pgds_is_dir_accessible(const char *path)
{
	DIR		   *dirp = opendir(path);

	if (!dirp)
	{
		elog(DEBUG1, "Error opening directory \"%s\"", path);
		return false;
	}
	closedir(dirp);
	return true;
}

long
pgds_get_temp_file_bytes(int pid)
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

	while ((entry = readdir(dir)) != NULL)
	{
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

long
pgds_get_rss_memory_pages(int pid)
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
			rss = -1;
		}
		fclose(fp);
	}
	else
	{
		rss = -1;
	}

	return rss;
}


static int
detect_cgroup_version(void)
{
	struct stat st;

	if (stat("/sys/fs/cgroup/cgroup.controllers", &st) == 0)
		return 2;
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
		char   *sp = NULL;
		char	copy[1024];

		/* strip trailing newline */
		line[strcspn(line, "\n")] = '\0';

		strlcpy(copy, line, sizeof(copy));
		hier_id     = strtok_r(copy, ":", &sp);
		if (!hier_id) continue;
		controllers = strtok_r(NULL, ":", &sp);
		if (!controllers) continue;
		cgroup_path = strtok_r(NULL, "", &sp);
		if (!cgroup_path) continue;

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
 * Populate *info with cgroup resource limits/requests for the calling
 * process.  Returns true if we are running under cgroups (v1 or v2),
 * false otherwise.  On false return, *info is zeroed and all _set flags
 * are false.
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

		/* CPU request: cpu.weight (100 ≈ 1 CPU in Kubernetes mapping) */
		snprintf(fpath, sizeof(fpath), "%s/cpu.weight", cpath);
		{
			long long weight;

			if (read_cgroup_ll(fpath, &weight))
			{
				info->cpu_request_set = true;
				info->cpu_request = (double) weight / 100.0;
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

		/* Memory request: memory.high → bytes or "max" */
		snprintf(fpath, sizeof(fpath), "%s/memory.high", cpath);
		{
			FILE   *fp = fopen(fpath, "r");

			if (fp)
			{
				char val_str[32];

				if (fscanf(fp, "%31s", val_str) == 1 &&
					strcmp(val_str, "max") != 0)
				{
					info->mem_request_set = true;
					info->mem_request_bytes = (int64) atoll(val_str);
				}
				fclose(fp);
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

			/* CPU shares: 1024 shares ≈ 1 CPU */
			snprintf(fpath, sizeof(fpath), "%s/cpu.shares", cpath);
			{
				long long shares;

				if (read_cgroup_ll(fpath, &shares))
				{
					info->cpu_request_set = true;
					info->cpu_request = (double) shares / 1024.0;
				}
			}
		}

		/* ---- Memory ---- */
		if (find_cgroup_path(1, "memory", "/sys/fs/cgroup/memory", cpath, sizeof(cpath)))
		{
			long long limit;

			snprintf(fpath, sizeof(fpath), "%s/memory.limit_in_bytes", cpath);
			if (read_cgroup_ll(fpath, &limit) && limit < CGROUP_V1_MEM_UNLIMITED)
			{
				info->mem_limit_set = true;
				info->mem_limit_bytes = (int64) limit;
			}

			snprintf(fpath, sizeof(fpath), "%s/memory.soft_limit_in_bytes", cpath);
			if (read_cgroup_ll(fpath, &limit) && limit < CGROUP_V1_MEM_UNLIMITED)
			{
				info->mem_request_set = true;
				info->mem_request_bytes = (int64) limit;
			}
		}
	}

	return true;
}

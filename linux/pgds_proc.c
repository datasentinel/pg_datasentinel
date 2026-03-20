#include "postgres.h"
#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/file_utils.h"

#include "pgds_proc.h"
#if PG_VERSION_NUM < 170000
#include "storage/fd.h"
#endif

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

int64
pgds_get_temp_file_bytes(int pid)
{
	char		fd_path[256];
	struct dirent *entry;
	ssize_t		len;
	DIR		   *dir;
	int64		temporary_size = 0;
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
					elog(DEBUG1, "Temp file found: \"%s\" (target: \"%s\"), Size: " INT64_FORMAT " bytes",
						fd_path, link_target, (int64) stat_buf.st_size);
					temporary_size += (int64) stat_buf.st_size;
				}
			}
		}
	}

	closedir(dir);

	elog(DEBUG1, "Total temporary file usage for PID %d: " INT64_FORMAT " bytes",
		 pid, temporary_size);
	return temporary_size;
}

int64
pgds_get_pss_memory_bytes(int pid)
{
	FILE	   *fp;
	char		filename[256];
	char		line[256];
	int64		pss_kb = -1;

	snprintf(filename, sizeof(filename), "/proc/%d/smaps_rollup", pid);

	fp = fopen(filename, "r");
	if (fp == NULL)
		return -1;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		if (sscanf(line, "Pss: " INT64_FORMAT " kB", &pss_kb) == 1)
			break;
	}

	fclose(fp);

	if (pss_kb < 0)
		return -1;

	return pss_kb * 1024;
}

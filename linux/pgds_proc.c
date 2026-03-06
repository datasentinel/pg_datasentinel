#include "postgres.h"
#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/file_utils.h"

#include "pgds_proc.h"

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

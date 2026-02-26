#include "postgres.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/file_utils.h"

#include "dsdiag_linux.h"

/*
 * Returns true if the directory at path exists and is accessible.
 */
bool
is_dir_accessible(const char *path)
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

/*
 * Returns the total size in bytes of all open PostgreSQL temporary files
 * for the process with the given PID, by inspecting /proc/<pid>/fd.
 * Returns -1 if the /proc directory cannot be opened (e.g. permission denied).
 */
long
get_temp_file_bytes(int pid)
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

	/* Iterate through the file descriptors to identify temporary PostgreSQL files. */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
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

/*
 * Returns the RSS (resident set size) of the process with the given PID
 * in pages, by reading /proc/<pid>/statm.
 * Returns -1 on error.
 */
long
get_rss_memory_pages(int pid)
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
			rss = -1;			/* Error reading */
		}
		fclose(fp);
	}
	else
	{
		rss = -1;				/* Error opening file */
	}

	return rss;
}

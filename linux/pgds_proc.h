#ifndef PGDS_PROC_H
#define PGDS_PROC_H

#include "postgres.h"

extern bool pgds_is_dir_accessible(const char *path);
extern int64 pgds_get_temp_file_bytes(int pid);
extern int64 pgds_get_rss_memory_pages(int pid);

#endif							/* PGDS_PROC_H */

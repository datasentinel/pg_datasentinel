#ifndef PGDS_LINUX_H
#define PGDS_LINUX_H

extern bool pgds_is_dir_accessible(const char *path);
extern long pgds_get_temp_file_bytes(int pid);
extern long pgds_get_rss_memory_pages(int pid);

#endif							/* PGDS_LINUX_H */

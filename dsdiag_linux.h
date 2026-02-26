#ifndef DSDIAG_LINUX_H
#define DSDIAG_LINUX_H

extern bool is_dir_accessible(const char *path);
extern long get_temp_file_bytes(int pid);
extern long get_rss_memory_pages(int pid);

#endif							/* DSDIAG_LINUX_H */

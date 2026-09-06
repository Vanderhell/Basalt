#ifndef JOBDB_FILE_H
#define JOBDB_FILE_H
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include "jobdb.h"
jobdb_result_t jobdb_make_dir(const char *path);
jobdb_result_t jobdb_read_file(const char *path, uint8_t *buf, size_t size, size_t *out_read);
jobdb_result_t jobdb_write_file(const char *path, const uint8_t *buf, size_t size);
jobdb_result_t jobdb_file_sync(FILE *file);
jobdb_result_t jobdb_sync_directory(const char *path);
jobdb_result_t jobdb_remove_file(const char *path);
jobdb_result_t jobdb_truncate_file(const char *path, uint64_t size);
int jobdb_file_exists(const char *path);
typedef int (*jobdb_file_visit_fn)(const char *name, void *context);
jobdb_result_t jobdb_visit_files(const char *directory, jobdb_file_visit_fn visit, void *context);
#endif

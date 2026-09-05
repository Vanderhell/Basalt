#ifndef JOBDB_FILE_H
#define JOBDB_FILE_H
#include <stddef.h>
#include <stdint.h>
#include "jobdb.h"
jobdb_result_t jobdb_make_dir(const char *path);
jobdb_result_t jobdb_read_file(const char *path, uint8_t *buf, size_t size, size_t *out_read);
jobdb_result_t jobdb_write_file(const char *path, const uint8_t *buf, size_t size);
int jobdb_file_exists(const char *path);
#endif

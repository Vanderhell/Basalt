#ifndef JOBDB_LOCK_H
#define JOBDB_LOCK_H
#include "jobdb.h"
jobdb_result_t jobdb_lock_acquire_impl(jobdb_t *db, const char *path, uint32_t timeout_ms, jobdb_lock_t **out);
#endif

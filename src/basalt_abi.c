#include "basalt_abi.h"

uint32_t basalt_abi_version(void) { return BASALT_ABI_VERSION; }
jobdb_result_t basalt_db_create(const char *path, jobdb_t **out_db) { return jobdb_create(path, out_db); }
jobdb_result_t basalt_db_open(const char *path, jobdb_t **out_db) { return jobdb_open(path, out_db); }
void basalt_db_close(jobdb_t *db) { jobdb_close(db); }
jobdb_result_t basalt_db_verify(const char *path) { return jobdb_verify(path); }

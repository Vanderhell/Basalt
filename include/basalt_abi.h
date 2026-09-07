#ifndef BASALT_ABI_H
#define BASALT_ABI_H

#include <stdint.h>
#include "jobdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BASALT_ABI_VERSION UINT32_C(1)

uint32_t basalt_abi_version(void);
jobdb_result_t basalt_db_create(const char *path, jobdb_t **out_db);
jobdb_result_t basalt_db_open(const char *path, jobdb_t **out_db);
void basalt_db_close(jobdb_t *db);
jobdb_result_t basalt_db_verify(const char *path);

#ifdef __cplusplus
}
#endif
#endif

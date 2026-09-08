#include "basalt_storage.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    basalt_storage_t *storage = NULL;
    jobdb_t *db = NULL;
    basalt_storage_vtable_v1 api;
    int context = 1;
    memset(&api, 0, sizeof(api));
    assert(basalt_storage_create(NULL, &context, &storage) == JOBDB_ERR_INVALID_ARGUMENT);
    assert(basalt_storage_create(&api, &context, &storage) == JOBDB_ERR_INVALID_ARGUMENT);
    api.abi_version = BASALT_STORAGE_ABI_VERSION + 1u;
    api.struct_size = (uint32_t)sizeof(api);
    api.provider_name = "invalid";
    assert(basalt_storage_create(&api, &context, &storage) == JOBDB_ERR_INVALID_ARGUMENT);
    api.abi_version = BASALT_STORAGE_ABI_VERSION;
    api.struct_size = (uint32_t)(sizeof(api) - sizeof(api.tx_rollback));
    assert(basalt_storage_create(&api, &context, &storage) == JOBDB_ERR_INVALID_ARGUMENT);
    assert(basalt_storage_provider(NULL) == NULL);
    assert(basalt_storage_capabilities(NULL) == 0);
    assert(jobdb_create("storage-contract-test", &db) == JOBDB_OK);
    assert(basalt_storage_from_jobdb(db, &storage) == JOBDB_OK);
    assert(strcmp(basalt_storage_provider(storage), "BasaltDB") == 0);
    assert((basalt_storage_capabilities(storage) & BASALT_STORAGE_CAP_ATOMIC_DOMAIN_OPS) != 0);
    assert(basalt_storage_health(storage) == JOBDB_OK);
    basalt_storage_retain(storage);
    basalt_storage_release(storage);
    basalt_storage_release(storage);
    jobdb_close(db);
    puts("basalt storage contract validation passed");
    return 0;
}

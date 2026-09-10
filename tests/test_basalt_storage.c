#include "basalt_storage.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
static void clean_db(const char *directory) { struct _finddata_t data; char pattern[320]; intptr_t handle; (void)snprintf(pattern, sizeof pattern, "%s/*", directory); handle = _findfirst(pattern, &data); if (handle != -1) { do { char path[320]; if (strcmp(data.name, ".") && strcmp(data.name, "..")) { (void)snprintf(path, sizeof path, "%s/%s", directory, data.name); (void)remove(path); } } while (_findnext(handle, &data) == 0); _findclose(handle); } (void)RemoveDirectoryA(directory); }
static void test_db_path(char *path, size_t size) { (void)snprintf(path, size, "storage-contract-test-%lu", (unsigned long)GetCurrentProcessId()); }
#else
#include <dirent.h>
#include <unistd.h>
static void clean_db(const char *directory) { DIR *dir = opendir(directory); if (dir) { struct dirent *entry; while ((entry = readdir(dir)) != NULL) { char path[320]; if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue; (void)snprintf(path, sizeof path, "%s/%s", directory, entry->d_name); (void)remove(path); } closedir(dir); } (void)rmdir(directory); }
static void test_db_path(char *path, size_t size) { (void)snprintf(path, size, "storage-contract-test-%ld", (long)getpid()); }
#endif

int main(void) {
    basalt_storage_t *storage = NULL;
    jobdb_t *db = NULL;
    basalt_storage_vtable_v1 api;
    int context = 1;
    char path[128];
    test_db_path(path, sizeof path);
    clean_db(path);
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
    assert(jobdb_create(path, &db) == JOBDB_OK);
    assert(basalt_storage_from_jobdb(db, &storage) == JOBDB_OK);
    assert(strcmp(basalt_storage_provider(storage), "BasaltDB") == 0);
    assert((basalt_storage_capabilities(storage) & BASALT_STORAGE_CAP_ATOMIC_DOMAIN_OPS) != 0);
    assert(basalt_storage_health(storage) == JOBDB_OK);
    basalt_storage_retain(storage);
    basalt_storage_release(storage);
    basalt_storage_release(storage);
    jobdb_close(db);
    clean_db(path);
    puts("basalt storage contract validation passed");
    return 0;
}

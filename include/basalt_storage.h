#ifndef BASALT_STORAGE_H
#define BASALT_STORAGE_H

#include <stddef.h>
#include <stdint.h>
#include "jobdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BASALT_STORAGE_ABI_VERSION 1u
#define BASALT_STORAGE_CAP_ATOMIC_DOMAIN_OPS UINT64_C(0x1)
#define BASALT_STORAGE_CAP_PROVIDER_CLOCK UINT64_C(0x2)
#define BASALT_STORAGE_CAP_BOUNDED_LISTS UINT64_C(0x4)

typedef struct basalt_storage basalt_storage_t;
typedef struct basalt_storage_tx basalt_storage_tx_t;

/* Buffers returned in jobdb_record_t are owned by the provider and released
   with record_free. All callbacks are synchronous and must catch errors at
   their language boundary. No callback is invoked while a Core mutex is held. */
typedef struct basalt_storage_vtable_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t capabilities;
    const char *provider_name;
    void (*retain)(void *context);
    void (*release)(void *context);
    jobdb_result_t (*health)(void *context);
    jobdb_result_t (*utc_now)(void *context, int64_t *seconds);
    jobdb_result_t (*allocate_execution_id)(void *, uint64_t *);
    jobdb_result_t (*record_create)(void *, uint32_t, uint64_t, const void *, uint32_t);
    jobdb_result_t (*record_get)(void *, uint32_t, uint64_t, jobdb_record_t *);
    void (*record_free)(void *, jobdb_record_t *);
    jobdb_result_t (*list_record_ids)(void *, uint32_t, uint64_t *, size_t, size_t *);
    jobdb_result_t (*execution_enqueue)(void *, const jobdb_execution_t *, uint32_t, const void *, uint32_t);
    jobdb_result_t (*execution_enqueue_extra)(void *, const jobdb_execution_t *, uint32_t, const void *, uint32_t, uint32_t, const void *, uint32_t);
    jobdb_result_t (*execution_enqueue_receipt)(void *, const jobdb_execution_t *, uint32_t, const void *, uint32_t, uint64_t, const void *, uint32_t);
    jobdb_result_t (*idempotency_get)(void *, uint64_t, void *, uint32_t, uint32_t *);
    jobdb_result_t (*execution_get)(void *, uint64_t, jobdb_execution_t *);
    jobdb_result_t (*execution_transition)(void *, uint64_t, uint64_t, jobdb_execution_state_t, uint64_t *);
    jobdb_result_t (*execution_start)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t, uint64_t *);
    jobdb_result_t (*claim_next)(void *, const jobdb_worker_id_t *, int64_t, int64_t, jobdb_execution_t *);
    jobdb_result_t (*renew_lease)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t);
    jobdb_result_t (*execution_complete)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t);
    jobdb_result_t (*execution_finalize)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t, jobdb_execution_state_t, int32_t, int32_t);
    jobdb_result_t (*execution_park)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t, uint64_t *);
    jobdb_result_t (*execution_retry)(void *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t, uint64_t *);
    jobdb_result_t (*schedule_create_extra)(void *, const jobdb_schedule_t *, uint32_t, const void *, uint32_t, uint32_t, const void *, uint32_t);
    jobdb_result_t (*schedule_get)(void *, uint64_t, jobdb_schedule_t *);
    jobdb_result_t (*schedule_update)(void *, const jobdb_schedule_t *, uint64_t);
    jobdb_result_t (*schedule_pause)(void *, uint64_t, uint64_t);
    jobdb_result_t (*schedule_resume)(void *, uint64_t, uint64_t);
    jobdb_result_t (*schedule_remove)(void *, uint64_t, uint64_t);
    jobdb_result_t (*schedule_try_fire_state)(void *, uint64_t, uint64_t, int64_t, uint64_t, int64_t, int64_t, jobdb_execution_state_t);
    jobdb_result_t (*get_stats)(void *, jobdb_stats_t *);
    jobdb_result_t (*tx_begin)(void *, void **);
    jobdb_result_t (*tx_put_create)(void *, uint32_t, uint64_t, const void *, uint32_t);
    jobdb_result_t (*tx_put_execution_create)(void *, const jobdb_execution_t *);
    jobdb_result_t (*tx_put_stats)(void *, const jobdb_stats_t *, uint64_t, int);
    jobdb_result_t (*tx_commit)(void *);
    void (*tx_rollback)(void *);
    /* Optional append-only v1 extension for diagnostic management records. */
    jobdb_result_t (*record_update)(void *, uint32_t, uint64_t, uint64_t, const void *, uint32_t, uint64_t *);
} basalt_storage_vtable_v1;

jobdb_result_t basalt_storage_create(const basalt_storage_vtable_v1 *, void *, basalt_storage_t **);
jobdb_result_t basalt_storage_from_jobdb(jobdb_t *, basalt_storage_t **);
void basalt_storage_retain(basalt_storage_t *);
void basalt_storage_release(basalt_storage_t *);
const char *basalt_storage_provider(const basalt_storage_t *);
uint64_t basalt_storage_capabilities(const basalt_storage_t *);
jobdb_result_t basalt_storage_health(basalt_storage_t *);
jobdb_result_t basalt_storage_utc_now(basalt_storage_t *, int64_t *);
jobdb_result_t basalt_storage_allocate_execution_id(basalt_storage_t *, uint64_t *);
jobdb_result_t basalt_storage_record_create(basalt_storage_t *, uint32_t, uint64_t, const void *, uint32_t);
jobdb_result_t basalt_storage_record_get(basalt_storage_t *, uint32_t, uint64_t, jobdb_record_t *);
jobdb_result_t basalt_storage_record_update(basalt_storage_t *, uint32_t, uint64_t, uint64_t, const void *, uint32_t, uint64_t *);
void basalt_storage_record_free(basalt_storage_t *, jobdb_record_t *);
jobdb_result_t basalt_storage_list_record_ids(basalt_storage_t *, uint32_t, uint64_t *, size_t, size_t *);
jobdb_result_t basalt_storage_execution_enqueue(basalt_storage_t *, const jobdb_execution_t *, uint32_t, const void *, uint32_t);
jobdb_result_t basalt_storage_execution_enqueue_extra(basalt_storage_t *, const jobdb_execution_t *, uint32_t, const void *, uint32_t, uint32_t, const void *, uint32_t);
jobdb_result_t basalt_storage_execution_enqueue_receipt(basalt_storage_t *, const jobdb_execution_t *, uint32_t, const void *, uint32_t, uint64_t, const void *, uint32_t);
jobdb_result_t basalt_storage_idempotency_get(basalt_storage_t *, uint64_t, void *, uint32_t, uint32_t *);
jobdb_result_t basalt_storage_execution_get(basalt_storage_t *, uint64_t, jobdb_execution_t *);
jobdb_result_t basalt_storage_execution_transition(basalt_storage_t *, uint64_t, uint64_t, jobdb_execution_state_t, uint64_t *);
jobdb_result_t basalt_storage_execution_start(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t, uint64_t *);
jobdb_result_t basalt_storage_claim_next(basalt_storage_t *, const jobdb_worker_id_t *, int64_t, int64_t, jobdb_execution_t *);
jobdb_result_t basalt_storage_renew_lease(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t);
jobdb_result_t basalt_storage_execution_complete(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t);
jobdb_result_t basalt_storage_execution_finalize(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t, jobdb_execution_state_t, int32_t, int32_t);
jobdb_result_t basalt_storage_execution_park(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t, uint64_t *);
jobdb_result_t basalt_storage_execution_retry(basalt_storage_t *, uint64_t, const jobdb_worker_id_t *, uint64_t, int64_t, uint64_t *);
jobdb_result_t basalt_storage_schedule_create_extra(basalt_storage_t *, const jobdb_schedule_t *, uint32_t, const void *, uint32_t, uint32_t, const void *, uint32_t);
jobdb_result_t basalt_storage_schedule_get(basalt_storage_t *, uint64_t, jobdb_schedule_t *);
jobdb_result_t basalt_storage_schedule_update(basalt_storage_t *, const jobdb_schedule_t *, uint64_t);
jobdb_result_t basalt_storage_schedule_pause(basalt_storage_t *, uint64_t, uint64_t);
jobdb_result_t basalt_storage_schedule_resume(basalt_storage_t *, uint64_t, uint64_t);
jobdb_result_t basalt_storage_schedule_remove(basalt_storage_t *, uint64_t, uint64_t);
jobdb_result_t basalt_storage_schedule_try_fire_state(basalt_storage_t *, uint64_t, uint64_t, int64_t, uint64_t, int64_t, int64_t, jobdb_execution_state_t);
jobdb_result_t basalt_storage_get_stats(basalt_storage_t *, jobdb_stats_t *);
jobdb_result_t basalt_storage_tx_begin(basalt_storage_t *, basalt_storage_tx_t **);
jobdb_result_t basalt_storage_tx_put_create(basalt_storage_tx_t *, uint32_t, uint64_t, const void *, uint32_t);
jobdb_result_t basalt_storage_tx_put_execution_create(basalt_storage_tx_t *, const jobdb_execution_t *);
jobdb_result_t basalt_storage_tx_put_stats(basalt_storage_tx_t *, const jobdb_stats_t *, uint64_t, int);
jobdb_result_t basalt_storage_tx_commit(basalt_storage_tx_t *);
void basalt_storage_tx_rollback(basalt_storage_tx_t *);

#ifdef __cplusplus
}
#endif
#endif

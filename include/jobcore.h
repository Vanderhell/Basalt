#ifndef JOBCORE_H
#define JOBCORE_H

#include <stddef.h>
#include <stdint.h>
#include "basalt_storage.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct jobcore jobcore_t;
typedef struct jobcore_execution_context {
    uint64_t execution_id, job_type, fencing_token;
    uint32_t payload_version, attempt;
    jobdb_worker_id_t worker;
    volatile int *cancellation_requested;
} jobcore_execution_context_t;
typedef int (*jobcore_handler_fn)(const void *, size_t, uint32_t,
                                  const jobcore_execution_context_t *, void *);

typedef enum jobcore_schedule_type {
    JOBCORE_SCHEDULE_IMMEDIATE = 1,
    JOBCORE_SCHEDULE_DELAYED,
    JOBCORE_SCHEDULE_ABSOLUTE,
    JOBCORE_SCHEDULE_INTERVAL
    ,JOBCORE_SCHEDULE_CRON
} jobcore_schedule_type_t;
typedef enum jobcore_interval_mode {
    JOBCORE_FIXED_RATE = 1,
    JOBCORE_FIXED_DELAY = 2
} jobcore_interval_mode_t;
typedef enum jobcore_misfire_policy { JOBCORE_MISFIRE_SKIP = 1, JOBCORE_MISFIRE_RUN_ONCE, JOBCORE_MISFIRE_RUN_LAST, JOBCORE_MISFIRE_CATCH_UP_ALL } jobcore_misfire_policy_t;
typedef enum jobcore_overlap_policy { JOBCORE_OVERLAP_ALLOW = 1, JOBCORE_OVERLAP_SKIP, JOBCORE_OVERLAP_QUEUE_ONE, JOBCORE_OVERLAP_QUEUE_ALL } jobcore_overlap_policy_t;
typedef enum jobcore_retry_policy { JOBCORE_RETRY_NONE = 0, JOBCORE_RETRY_FIXED, JOBCORE_RETRY_LINEAR, JOBCORE_RETRY_EXPONENTIAL, JOBCORE_RETRY_EXPONENTIAL_WITH_JITTER } jobcore_retry_policy_t;
typedef struct jobcore_retry_spec { jobcore_retry_policy_t policy; uint32_t max_attempts; int64_t initial_delay; int64_t max_delay; double backoff_factor; uint32_t jitter; } jobcore_retry_spec_t;
typedef enum jobcore_dependency_policy { JOBCORE_DEP_BLOCK = 1, JOBCORE_DEP_CANCEL, JOBCORE_DEP_CONTINUE, JOBCORE_DEP_FAIL_WORKFLOW } jobcore_dependency_policy_t;
typedef struct jobcore_workflow_node { uint64_t node_id, job_type; const void *payload; uint32_t payload_size, payload_version; uint32_t dependency_count; uint64_t dependencies[8]; } jobcore_workflow_node_t;
typedef struct jobcore_workflow_status { uint64_t workflow_id; uint32_t node_count, ready_count, blocked_count, running_count, terminal_count, failed_count, cancelled_count, cancel_requested; } jobcore_workflow_status_t;
typedef struct jobcore_schedule_spec {
    uint64_t schedule_id;
    uint64_t job_type;
    jobcore_schedule_type_t type;
    int64_t first_fire_at;
    int64_t interval;
    jobcore_interval_mode_t interval_mode;
    uint64_t max_occurrences;
    const void *payload;
    uint32_t payload_size;
    uint32_t payload_version;
    const char *cron_expression;
    const char *timezone;
    jobcore_misfire_policy_t misfire_policy;
    jobcore_overlap_policy_t overlap_policy;
    uint32_t catch_up_max;
} jobcore_schedule_spec_t;

jobdb_result_t jobcore_create(jobdb_t *, uint32_t, int64_t, jobcore_t **);
jobdb_t *jobcore_database(jobcore_t *);
jobdb_result_t jobcore_create_ex(jobdb_t *, uint32_t, int64_t, uint32_t, jobcore_t **);
jobdb_result_t jobcore_create_storage(basalt_storage_t *, uint32_t, int64_t, jobcore_t **);
jobdb_result_t jobcore_create_storage_ex(basalt_storage_t *, uint32_t, int64_t, uint32_t, jobcore_t **);
basalt_storage_t *jobcore_storage(jobcore_t *);
void jobcore_destroy(jobcore_t *);
jobdb_result_t jobcore_start(jobcore_t *);
jobdb_result_t jobcore_stop(jobcore_t *);
jobdb_result_t jobcore_worker_id(jobcore_t *, uint32_t, uint8_t out_worker_id[16]);
jobdb_result_t jobcore_stop_with_grace(jobcore_t *, uint32_t);
jobdb_result_t jobcore_register_handler(jobcore_t *, uint64_t, jobcore_handler_fn, void *);
jobdb_result_t jobcore_register_handler_name(jobcore_t *, const char *, jobcore_handler_fn, void *, uint64_t *);
jobdb_result_t jobcore_enqueue(jobcore_t *, uint64_t, const void *, uint32_t, uint32_t, int64_t, uint32_t, uint64_t *);
jobdb_result_t jobcore_enqueue_name(jobcore_t *, const char *, const void *, uint32_t, uint32_t, int64_t, uint32_t, uint64_t *);
jobdb_result_t jobcore_enqueue_with_retry(jobcore_t *, uint64_t, const void *, uint32_t, uint32_t, int64_t, const jobcore_retry_spec_t *, uint64_t *);
jobdb_result_t jobcore_enqueue_idempotent(jobcore_t *, const char *, uint64_t, const void *, uint32_t, uint32_t, int64_t, uint32_t, uint64_t *);
jobdb_result_t jobcore_enqueue_idempotent_with_retry(jobcore_t *, const char *, uint64_t, const void *, uint32_t, uint32_t, int64_t, const jobcore_retry_spec_t *, uint64_t *);
jobdb_result_t jobcore_workflow_submit(jobcore_t *, uint64_t, const jobcore_workflow_node_t *, size_t, jobcore_dependency_policy_t, int64_t);
jobdb_result_t jobcore_workflow_get(jobcore_t *, uint64_t, jobcore_workflow_status_t *);
jobdb_result_t jobcore_workflow_cancel(jobcore_t *, uint64_t);
jobdb_result_t jobcore_schedule_create(jobcore_t *, const jobcore_schedule_spec_t *);
jobdb_result_t jobcore_schedule_create_ex(jobcore_t *, const jobcore_schedule_spec_t *, int64_t end_at);
jobdb_result_t jobcore_schedule_get(jobcore_t *, uint64_t, jobdb_schedule_t *);
jobdb_result_t jobcore_schedule_update(jobcore_t *, const jobdb_schedule_t *, uint64_t);
jobdb_result_t jobcore_schedule_pause(jobcore_t *, uint64_t, uint64_t);
jobdb_result_t jobcore_schedule_resume(jobcore_t *, uint64_t, uint64_t);
jobdb_result_t jobcore_schedule_remove(jobcore_t *, uint64_t, uint64_t);
jobdb_result_t jobcore_cron_next_fire(const char *, const char *, int64_t, int64_t *);

#ifdef __cplusplus
}
#endif
#endif

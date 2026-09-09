#ifndef BASALT_ABI_H
#define BASALT_ABI_H

#include <stdint.h>
#include "jobdb.h"
#include "jobcore.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BASALT_ABI_VERSION UINT32_C(1)

typedef struct basalt_execution_view {
    uint64_t execution_id, job_definition_id, schedule_id, workflow_id;
    uint32_t state, payload_version, attempt, max_attempts;
    int64_t created_at, eligible_at, started_at, finished_at, lease_expires_at;
    int32_t priority;
    uint64_t revision, fencing_token;
    uint8_t worker_instance_id[16];
} basalt_execution_view_t;
typedef struct basalt_stats_view {
    uint64_t submitted_total, started_total, completed_total, failed_total;
    uint64_t retried_total, cancelled_total, dead_total, recovered_total;
} basalt_stats_view_t;
typedef struct basalt_ledger_view {
    uint64_t execution_id, job_definition_id, schedule_id, workflow_id;
    int64_t started_at, finished_at, duration;
    uint32_t attempt, final_state;
    int32_t result_code, error_code;
} basalt_ledger_view_t;
typedef struct basalt_schedule_view {
    uint64_t schedule_id, job_definition_id;
    uint32_t schedule_type, enabled;
    uint64_t timezone_reference, interval;
    uint32_t interval_mode, catch_up_max;
    int64_t start_at, end_at, last_fire_at, next_fire_at;
    uint64_t occurrence_count, max_occurrences, revision;
    uint32_t misfire_policy, overlap_policy;
} basalt_schedule_view_t;
typedef struct basalt_retry_spec_v1 { uint32_t policy, max_attempts, jitter, reserved; int64_t initial_delay, max_delay; double backoff_factor; } basalt_retry_spec_v1_t;
typedef struct basalt_workflow_node_v1 { uint64_t node_id, job_type; const void *payload; uint32_t payload_size, payload_version, dependency_count; uint64_t dependencies[8]; } basalt_workflow_node_v1_t;
typedef struct basalt_workflow_status_v1 { uint64_t workflow_id; uint32_t node_count, ready_count, blocked_count, running_count, terminal_count, failed_count, cancelled_count, cancel_requested; } basalt_workflow_status_v1_t;

uint32_t basalt_abi_version(void);
jobdb_result_t basalt_db_create(const char *path, jobdb_t **out_db);
jobdb_result_t basalt_db_open(const char *path, jobdb_t **out_db);
void basalt_db_close(jobdb_t *db);
jobdb_result_t basalt_db_verify(const char *path);
jobdb_result_t basalt_db_backup(jobdb_t *db, const char *target_path);
jobdb_result_t basalt_db_restore(const char *source_path, const char *target_path);
jobdb_result_t basalt_execution_get(jobdb_t *db, uint64_t execution_id, basalt_execution_view_t *out_view);
jobdb_result_t basalt_execution_list(jobdb_t *db, uint64_t *ids, size_t capacity, size_t *out_count);
jobdb_result_t basalt_execution_cancel(jobdb_t *db, uint64_t execution_id, uint64_t expected_revision);
jobdb_result_t basalt_execution_requeue(jobdb_t *db, uint64_t execution_id, uint64_t expected_revision);
jobdb_result_t basalt_ledger_get(jobdb_t *db, uint64_t execution_id, basalt_ledger_view_t *out_entry);
jobdb_result_t basalt_ledger_list(jobdb_t *db, uint64_t *ids, size_t capacity, size_t *out_count);
jobdb_result_t basalt_stats_get(jobdb_t *db, basalt_stats_view_t *out_stats);
jobdb_result_t basalt_db_health(jobdb_t *db);
jobdb_result_t basalt_schedule_create(jobcore_t *core, uint64_t schedule_id, uint64_t job_type, uint32_t schedule_type, int64_t first_fire_at, int64_t interval, uint32_t interval_mode, uint64_t max_occurrences, const void *payload, uint32_t payload_size, uint32_t payload_version, const char *cron_expression, const char *timezone, uint32_t misfire_policy, uint32_t overlap_policy, uint32_t catch_up_max);
jobdb_result_t basalt_schedule_get(jobcore_t *core, uint64_t schedule_id, basalt_schedule_view_t *out_schedule);
jobdb_result_t basalt_schedule_list(jobcore_t *core, uint64_t *ids, size_t capacity, size_t *out_count);
jobdb_result_t basalt_schedule_update(jobcore_t *core, const basalt_schedule_view_t *schedule, uint64_t expected_revision);
jobdb_result_t basalt_schedule_pause(jobcore_t *core, uint64_t schedule_id, uint64_t expected_revision);
jobdb_result_t basalt_schedule_resume(jobcore_t *core, uint64_t schedule_id, uint64_t expected_revision);
jobdb_result_t basalt_schedule_remove(jobcore_t *core, uint64_t schedule_id, uint64_t expected_revision);
jobdb_result_t basalt_core_create(jobdb_t *db, uint32_t workers, int64_t lease_duration, jobcore_t **out_core);
jobdb_result_t basalt_core_create_ex(jobdb_t *db, uint32_t workers, int64_t lease_duration, uint32_t grace_ms, jobcore_t **out_core);
jobdb_result_t basalt_storage_create_v1(const basalt_storage_vtable_v1 *vtable, void *context, basalt_storage_t **out_storage);
void basalt_storage_destroy(basalt_storage_t *storage);
jobdb_result_t basalt_core_create_storage_v1(basalt_storage_t *storage, uint32_t workers, int64_t lease_duration, uint32_t grace_ms, jobcore_t **out_core);
void basalt_core_destroy(jobcore_t *core);
jobdb_result_t basalt_core_start(jobcore_t *core);
jobdb_result_t basalt_core_stop(jobcore_t *core);
jobdb_result_t basalt_core_register_handler(jobcore_t *core, uint64_t job_type, jobcore_handler_fn handler, void *user_data);
jobdb_result_t basalt_core_enqueue(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id);
jobdb_result_t basalt_core_enqueue_retry(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, const basalt_retry_spec_v1_t *retry, uint64_t *out_execution_id);
jobdb_result_t basalt_core_enqueue_idempotent(jobcore_t *core, const char *idempotency_key, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id);
jobdb_result_t basalt_core_enqueue_idempotent_retry(jobcore_t *core, const char *idempotency_key, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, const basalt_retry_spec_v1_t *retry, uint64_t *out_execution_id);
jobdb_result_t basalt_core_schedule_create(jobcore_t *core, const jobcore_schedule_spec_t *spec);
jobdb_result_t basalt_core_workflow_submit(jobcore_t *core, uint64_t workflow_id, const basalt_workflow_node_v1_t *nodes, uint32_t count, uint32_t policy, int64_t now);
jobdb_result_t basalt_workflow_get(jobcore_t *core, uint64_t workflow_id, basalt_workflow_status_v1_t *out_status);
jobdb_result_t basalt_workflow_cancel(jobcore_t *core, uint64_t workflow_id);

#ifdef __cplusplus
}
#endif
#endif

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
jobdb_result_t basalt_stats_get(jobdb_t *db, basalt_stats_view_t *out_stats);
jobdb_result_t basalt_db_health(jobdb_t *db);
jobdb_result_t basalt_core_create(jobdb_t *db, uint32_t workers, int64_t lease_duration, jobcore_t **out_core);
jobdb_result_t basalt_core_create_ex(jobdb_t *db, uint32_t workers, int64_t lease_duration, uint32_t grace_ms, jobcore_t **out_core);
void basalt_core_destroy(jobcore_t *core);
jobdb_result_t basalt_core_start(jobcore_t *core);
jobdb_result_t basalt_core_stop(jobcore_t *core);
jobdb_result_t basalt_core_register_handler(jobcore_t *core, uint64_t job_type, jobcore_handler_fn handler, void *user_data);
jobdb_result_t basalt_core_enqueue(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id);
jobdb_result_t basalt_core_enqueue_retry(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, const jobcore_retry_spec_t *retry, uint64_t *out_execution_id);
jobdb_result_t basalt_core_schedule_create(jobcore_t *core, const jobcore_schedule_spec_t *spec);
jobdb_result_t basalt_core_workflow_submit(jobcore_t *core, uint64_t workflow_id, const jobcore_workflow_node_t *nodes, size_t count, jobcore_dependency_policy_t policy, int64_t now);

#ifdef __cplusplus
}
#endif
#endif

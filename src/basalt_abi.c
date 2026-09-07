#include "basalt_abi.h"

uint32_t basalt_abi_version(void) { return BASALT_ABI_VERSION; }
jobdb_result_t basalt_db_create(const char *path, jobdb_t **out_db) { return jobdb_create(path, out_db); }
jobdb_result_t basalt_db_open(const char *path, jobdb_t **out_db) { return jobdb_open(path, out_db); }
void basalt_db_close(jobdb_t *db) { jobdb_close(db); }
jobdb_result_t basalt_db_verify(const char *path) { return jobdb_verify(path); }
jobdb_result_t basalt_core_create(jobdb_t *db, uint32_t workers, int64_t lease_duration, jobcore_t **out_core) { return jobcore_create(db, workers, lease_duration, out_core); }
jobdb_result_t basalt_core_create_ex(jobdb_t *db, uint32_t workers, int64_t lease_duration, uint32_t grace_ms, jobcore_t **out_core) { return jobcore_create_ex(db, workers, lease_duration, grace_ms, out_core); }
void basalt_core_destroy(jobcore_t *core) { jobcore_destroy(core); }
jobdb_result_t basalt_core_start(jobcore_t *core) { return jobcore_start(core); }
jobdb_result_t basalt_core_stop(jobcore_t *core) { return jobcore_stop(core); }
jobdb_result_t basalt_core_register_handler(jobcore_t *core, uint64_t job_type, jobcore_handler_fn handler, void *user_data) { return jobcore_register_handler(core, job_type, handler, user_data); }
jobdb_result_t basalt_core_enqueue(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id) { return jobcore_enqueue(core, job_type, payload, payload_size, payload_version, now, max_attempts, out_execution_id); }
jobdb_result_t basalt_core_enqueue_retry(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, const jobcore_retry_spec_t *retry, uint64_t *out_execution_id) { return jobcore_enqueue_with_retry(core, job_type, payload, payload_size, payload_version, now, retry, out_execution_id); }
jobdb_result_t basalt_core_schedule_create(jobcore_t *core, const jobcore_schedule_spec_t *spec) { return jobcore_schedule_create(core, spec); }
jobdb_result_t basalt_core_workflow_submit(jobcore_t *core, uint64_t workflow_id, const jobcore_workflow_node_t *nodes, size_t count, jobcore_dependency_policy_t policy, int64_t now) { return jobcore_workflow_submit(core, workflow_id, nodes, count, policy, now); }

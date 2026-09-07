#include "basalt_abi.h"
#include <string.h>

uint32_t basalt_abi_version(void) { return BASALT_ABI_VERSION; }
jobdb_result_t basalt_db_create(const char *path, jobdb_t **out_db) { return jobdb_create(path, out_db); }
jobdb_result_t basalt_db_open(const char *path, jobdb_t **out_db) { return jobdb_open(path, out_db); }
void basalt_db_close(jobdb_t *db) { jobdb_close(db); }
jobdb_result_t basalt_db_verify(const char *path) { return jobdb_verify(path); }
jobdb_result_t basalt_db_backup(jobdb_t *db, const char *target_path) { return jobdb_backup(db, target_path); }
jobdb_result_t basalt_db_restore(const char *source_path, const char *target_path) { return jobdb_restore(source_path, target_path); }
jobdb_result_t basalt_execution_get(jobdb_t *db, uint64_t id, basalt_execution_view_t *out) { jobdb_execution_t e; jobdb_result_t r; if (!out) return JOBDB_ERR_INVALID_ARGUMENT; r=jobdb_execution_get(db,id,&e); if(r!=JOBDB_OK)return r; out->execution_id=e.execution_id; out->job_definition_id=e.job_definition_id; out->schedule_id=e.schedule_id; out->workflow_id=e.workflow_id; out->state=(uint32_t)e.state; out->payload_version=0; out->created_at=e.created_at; out->eligible_at=e.eligible_at; out->started_at=e.started_at; out->finished_at=e.finished_at; out->priority=e.priority; out->attempt=e.attempt; out->max_attempts=e.max_attempts; out->revision=e.revision; memcpy(out->worker_instance_id,e.worker_instance_id,16); out->lease_expires_at=e.lease_expires_at; out->fencing_token=e.fencing_token; return JOBDB_OK; }
jobdb_result_t basalt_execution_list(jobdb_t *db, uint64_t *ids, size_t capacity, size_t *out_count) { return jobdb_list_record_ids(db, 3, ids, capacity, out_count); }
jobdb_result_t basalt_execution_cancel(jobdb_t *db, uint64_t id, uint64_t expected) { return jobdb_execution_finalize_unleased(db,id,expected,JOBDB_EXEC_CANCELLED,0,0); }
jobdb_result_t basalt_execution_requeue(jobdb_t *db, uint64_t id, uint64_t expected) { return jobdb_execution_transition(db,id,expected,JOBDB_EXEC_READY,NULL); }
jobdb_result_t basalt_stats_get(jobdb_t *db, basalt_stats_view_t *out) { jobdb_stats_t s; jobdb_result_t r;if(!out)return JOBDB_ERR_INVALID_ARGUMENT;r=jobdb_get_stats(db,&s);if(r!=JOBDB_OK)return r;out->submitted_total=s.submitted_total;out->started_total=s.started_total;out->completed_total=s.completed_total;out->failed_total=s.failed_total;out->retried_total=s.retried_total;out->cancelled_total=s.cancelled_total;out->dead_total=s.dead_total;out->recovered_total=s.recovered_total;return JOBDB_OK; }
jobdb_result_t basalt_db_health(jobdb_t *db) { return jobdb_health(db); }
jobdb_result_t basalt_core_create(jobdb_t *db, uint32_t workers, int64_t lease_duration, jobcore_t **out_core) { return jobcore_create(db, workers, lease_duration, out_core); }
jobdb_result_t basalt_core_create_ex(jobdb_t *db, uint32_t workers, int64_t lease_duration, uint32_t grace_ms, jobcore_t **out_core) { return jobcore_create_ex(db, workers, lease_duration, grace_ms, out_core); }
void basalt_core_destroy(jobcore_t *core) { jobcore_destroy(core); }
jobdb_result_t basalt_core_start(jobcore_t *core) { return jobcore_start(core); }
jobdb_result_t basalt_core_stop(jobcore_t *core) { return jobcore_stop(core); }
jobdb_result_t basalt_core_register_handler(jobcore_t *core, uint64_t job_type, jobcore_handler_fn handler, void *user_data) { return jobcore_register_handler(core, job_type, handler, user_data); }
jobdb_result_t basalt_core_enqueue(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id) { return jobcore_enqueue(core, job_type, payload, payload_size, payload_version, now, max_attempts, out_execution_id); }
jobdb_result_t basalt_core_enqueue_retry(jobcore_t *core, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, const jobcore_retry_spec_t *retry, uint64_t *out_execution_id) { return jobcore_enqueue_with_retry(core, job_type, payload, payload_size, payload_version, now, retry, out_execution_id); }
jobdb_result_t basalt_core_enqueue_idempotent(jobcore_t *core, const char *key, uint64_t job_type, const void *payload, uint32_t payload_size, uint32_t payload_version, int64_t now, uint32_t max_attempts, uint64_t *out_execution_id) { return jobcore_enqueue_idempotent(core,key,job_type,payload,payload_size,payload_version,now,max_attempts,out_execution_id); }
jobdb_result_t basalt_core_schedule_create(jobcore_t *core, const jobcore_schedule_spec_t *spec) { return jobcore_schedule_create(core, spec); }
jobdb_result_t basalt_core_workflow_submit(jobcore_t *core, uint64_t workflow_id, const jobcore_workflow_node_t *nodes, size_t count, jobcore_dependency_policy_t policy, int64_t now) { return jobcore_workflow_submit(core, workflow_id, nodes, count, policy, now); }

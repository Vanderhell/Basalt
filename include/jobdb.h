#ifndef JOBDB_H
#define JOBDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct jobdb jobdb_t;
typedef struct jobdb_tx jobdb_tx_t;
typedef struct jobdb_lock jobdb_lock_t;
typedef struct jobdb_record {
    uint32_t record_type;
    uint32_t format_version;
    uint64_t record_id;
    uint64_t generation;
    uint64_t revision;
    uint64_t transaction_id;
    uint32_t flags;
    uint8_t *payload;
    uint32_t payload_size;
} jobdb_record_t;
typedef enum jobdb_execution_state {
    JOBDB_EXEC_CREATED = 0, JOBDB_EXEC_SCHEDULED, JOBDB_EXEC_READY,
    JOBDB_EXEC_LEASED, JOBDB_EXEC_RUNNING, JOBDB_EXEC_RETRY,
    JOBDB_EXEC_BLOCKED, JOBDB_EXEC_DONE, JOBDB_EXEC_FAILED,
    JOBDB_EXEC_DEAD, JOBDB_EXEC_CANCELLED, JOBDB_EXEC_PAUSED
} jobdb_execution_state_t;
typedef struct jobdb_execution {
    uint64_t execution_id, job_definition_id, schedule_id, workflow_id;
    jobdb_execution_state_t state;
    int64_t created_at, eligible_at, started_at, finished_at;
    int32_t priority;
    uint32_t attempt, max_attempts;
    uint64_t revision;
    uint8_t worker_instance_id[16];
    int64_t lease_expires_at;
    uint64_t fencing_token;
} jobdb_execution_t;
typedef struct jobdb_worker_id { uint8_t bytes[16]; } jobdb_worker_id_t;
typedef struct jobdb_schedule {
    uint64_t schedule_id, job_definition_id;
    uint32_t schedule_type;
    uint32_t enabled;
    uint64_t timezone_reference;
    int64_t start_at, end_at, last_fire_at, next_fire_at;
    uint64_t occurrence_count, max_occurrences;
    uint32_t misfire_policy, overlap_policy;
    uint64_t revision;
} jobdb_schedule_t;
typedef struct jobdb_stats { uint64_t submitted_total, started_total, completed_total, failed_total, retried_total, cancelled_total, dead_total, recovered_total; } jobdb_stats_t;
typedef struct jobdb_ledger_entry { uint64_t execution_id, job_definition_id, schedule_id, workflow_id; int64_t started_at, finished_at, duration; uint32_t attempt; jobdb_execution_state_t final_state; int32_t result_code, error_code; } jobdb_ledger_entry_t;
typedef struct jobdb_retention { int64_t max_age; uint64_t max_records, max_bytes; } jobdb_retention_t;
typedef enum jobdb_failure_point {
    JOBDB_FAILURE_NONE = 0,
    JOBDB_FAILURE_AFTER_BEGIN,
    JOBDB_FAILURE_AFTER_OPERATION,
    JOBDB_FAILURE_BEFORE_COMMIT,
    JOBDB_FAILURE_AFTER_COMMIT,
    JOBDB_FAILURE_DISK_FULL,
    JOBDB_FAILURE_CHECKPOINT_BEFORE_MANIFEST,
    JOBDB_FAILURE_CHECKPOINT_AFTER_MANIFEST,
    JOBDB_FAILURE_BEFORE_WAL_BEGIN,
    JOBDB_FAILURE_AFTER_WAL_BEGIN,
    JOBDB_FAILURE_AFTER_WAL_OPERATIONS,
    JOBDB_FAILURE_AFTER_WAL_PRECOMMIT_SYNC,
    JOBDB_FAILURE_AFTER_COMMIT_WRITE,
    JOBDB_FAILURE_AFTER_COMMIT_SYNC,
    JOBDB_FAILURE_DURING_APPLY,
    JOBDB_FAILURE_AFTER_APPLY,
    JOBDB_FAILURE_AFTER_APPLY_SYNC,
    JOBDB_FAILURE_BEFORE_MANIFEST_WRITE,
    JOBDB_FAILURE_AFTER_MANIFEST_WRITE,
    JOBDB_FAILURE_AFTER_MANIFEST_SYNC,
    JOBDB_FAILURE_BEFORE_WAL_TRUNCATE,
    JOBDB_FAILURE_AFTER_WAL_TRUNCATE
} jobdb_failure_point_t;

typedef enum jobdb_result {
    JOBDB_OK = 0,
    JOBDB_ERR_INVALID_ARGUMENT,
    JOBDB_ERR_NOT_FOUND,
    JOBDB_ERR_ALREADY_EXISTS,
    JOBDB_ERR_IO,
    JOBDB_ERR_CORRUPT,
    JOBDB_ERR_VERSION,
    JOBDB_ERR_LIMIT,
    JOBDB_ERR_INTERNAL,
    JOBDB_ERR_INVALID_TRANSACTION,
    JOBDB_ERR_CONFLICT,
    JOBDB_ERR_INVALID_STATE,
    JOBDB_ERR_BUSY,
    JOBDB_ERR_TIMEOUT,
    JOBDB_ERR_STALE_LEASE
} jobdb_result_t;

jobdb_result_t jobdb_create(const char *path, jobdb_t **out_db);
jobdb_result_t jobdb_open(const char *path, jobdb_t **out_db);
void jobdb_close(jobdb_t *db);
jobdb_result_t jobdb_verify(const char *path);
jobdb_result_t jobdb_lock_acquire(jobdb_t *db, uint32_t timeout_ms, jobdb_lock_t **out_lock);
void jobdb_lock_release(jobdb_lock_t *lock);
jobdb_result_t jobdb_record_create(jobdb_t *db, uint32_t record_type, uint64_t record_id,
                                   const void *payload, uint32_t payload_size);
jobdb_result_t jobdb_record_update(jobdb_t *db, uint32_t record_type, uint64_t record_id,
                                   uint64_t expected_revision, const void *payload,
                                   uint32_t payload_size, uint64_t *out_revision);
jobdb_result_t jobdb_record_get(jobdb_t *db, uint32_t record_type, uint64_t record_id,
                                jobdb_record_t *out_record);
jobdb_result_t jobdb_record_delete(jobdb_t *db, uint32_t record_type, uint64_t record_id,
                                   uint64_t expected_revision);
void jobdb_record_free(jobdb_record_t *record);
int jobdb_execution_transition_allowed(jobdb_execution_state_t from,
                                       jobdb_execution_state_t to);
jobdb_result_t jobdb_execution_validate_transition(jobdb_execution_state_t from,
                                                    jobdb_execution_state_t to);
jobdb_result_t jobdb_execution_create(jobdb_t *db, const jobdb_execution_t *execution);
jobdb_result_t jobdb_execution_enqueue(jobdb_t *db, const jobdb_execution_t *execution,
                                        uint32_t payload_record_type, const void *payload,
                                        uint32_t payload_size);
jobdb_result_t jobdb_execution_get(jobdb_t *db, uint64_t execution_id,
                                   jobdb_execution_t *out_execution);
jobdb_result_t jobdb_execution_transition(jobdb_t *db, uint64_t execution_id,
                                          uint64_t expected_revision,
                                          jobdb_execution_state_t new_state,
                                          uint64_t *out_revision);
jobdb_result_t jobdb_execution_start(jobdb_t *db, uint64_t execution_id,
                                      const jobdb_worker_id_t *worker,
                                      uint64_t fencing_token, int64_t now,
                                      uint64_t *out_revision);
jobdb_result_t jobdb_claim_next(jobdb_t *db, const jobdb_worker_id_t *worker,
                                int64_t now, int64_t lease_duration,
                                jobdb_execution_t *out_execution);
jobdb_result_t jobdb_renew_lease(jobdb_t *db, uint64_t execution_id,
                                 const jobdb_worker_id_t *worker,
                                 uint64_t fencing_token, int64_t lease_expires_at);
jobdb_result_t jobdb_execution_complete(jobdb_t *db, uint64_t execution_id,
                                        const jobdb_worker_id_t *worker,
                                        uint64_t fencing_token);
jobdb_result_t jobdb_execution_finalize(jobdb_t *db, uint64_t execution_id,
                                        const jobdb_worker_id_t *worker,
                                        uint64_t fencing_token,
                                        jobdb_execution_state_t final_state,
                                        int32_t result_code, int32_t error_code);
jobdb_result_t jobdb_execution_retry(jobdb_t *db, uint64_t execution_id,
                                     const jobdb_worker_id_t *worker,
                                     uint64_t fencing_token, int64_t eligible_at,
                                     uint64_t *out_revision);
jobdb_result_t jobdb_reclaim_expired(jobdb_t *db, int64_t now, uint32_t *out_count);
jobdb_result_t jobdb_schedule_create(jobdb_t *db, const jobdb_schedule_t *schedule);
jobdb_result_t jobdb_schedule_get(jobdb_t *db, uint64_t schedule_id,
                                  jobdb_schedule_t *out_schedule);
jobdb_result_t jobdb_schedule_try_fire(jobdb_t *db, uint64_t schedule_id,
                                       uint64_t expected_revision,
                                       int64_t expected_next_fire_at,
                                       uint64_t execution_id, int64_t fire_at,
                                       int64_t next_fire_at);
jobdb_result_t jobdb_get_stats(jobdb_t *db, jobdb_stats_t *out_stats);
jobdb_result_t jobdb_ledger_append(jobdb_t *db, const jobdb_ledger_entry_t *entry);
jobdb_result_t jobdb_ledger_get(jobdb_t *db, uint64_t execution_id,
                                jobdb_ledger_entry_t *out_entry);
jobdb_result_t jobdb_apply_retention(jobdb_t *db, int64_t now,
                                     const jobdb_retention_t *retention,
                                     uint32_t *out_deleted);
jobdb_result_t jobdb_checkpoint(jobdb_t *db);
jobdb_result_t jobdb_allocate_execution_id(jobdb_t *db, uint64_t *out_execution_id);
jobdb_result_t jobdb_list_record_ids(jobdb_t *db, uint32_t record_type,
                                     uint64_t *ids, size_t capacity,
                                     size_t *out_count);
jobdb_result_t jobdb_tx_begin(jobdb_t *db, jobdb_tx_t **out_tx);
jobdb_result_t jobdb_tx_put(jobdb_tx_t *tx, uint32_t target_type, uint64_t target_id,
                            const void *payload, uint32_t payload_size);
jobdb_result_t jobdb_tx_delete(jobdb_tx_t *tx, uint32_t target_type, uint64_t target_id);
jobdb_result_t jobdb_tx_commit(jobdb_tx_t *tx);
void jobdb_tx_rollback(jobdb_tx_t *tx);
/* Test-only deterministic fault injection; zeroes itself after triggering. */
void jobdb_test_fail_next(jobdb_t *db, jobdb_failure_point_t point);
const char *jobdb_result_string(jobdb_result_t result);

#ifdef __cplusplus
}
#endif
#endif

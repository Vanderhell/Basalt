#include "jobcore.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
 #include <io.h>
static void pause_ms(unsigned n) { Sleep(n); }
static void clean_db(void) { struct _finddata_t data; intptr_t handle = _findfirst("jobcore-test/*", &data); if (handle != -1) { do { char path[256]; if (strcmp(data.name, ".") && strcmp(data.name, "..")) { (void)snprintf(path, sizeof path, "jobcore-test/%s", data.name); (void)remove(path); } } while (_findnext(handle, &data) == 0); _findclose(handle); } }
#else
#include <unistd.h>
static void pause_ms(unsigned n) { (void)usleep(n * 1000u); }
static void clean_db(void) { (void)remove("jobcore-test/manifest.0"); (void)remove("jobcore-test/manifest.1"); (void)remove("jobcore-test/wal.0"); }
#endif

static int handled;
static int handler(const void *payload, size_t size, uint32_t version,
                   const jobcore_execution_context_t *context, void *data) {
    (void)data;
    assert(size == 3 && memcmp(payload, "job", 3) == 0);
    assert(version == 7 && context->fencing_token != 0);
    ++handled;
    return 0;
}

static void wait_state(jobdb_t *db, uint64_t id, jobdb_execution_state_t state) {
    jobdb_execution_t e;
    unsigned i;
    for (i = 0; i < 1000; ++i) {
        if (jobdb_execution_get(db, id, &e) == JOBDB_OK && e.state == state) return;
        pause_ms(10);
    }
    fprintf(stderr, "execution %llu did not reach state %d (actual %d)\n", (unsigned long long)id, (int)state, (int)e.state);
    assert(0 && "JobCore execution did not reach expected state");
}

static void wait_occurrences(jobdb_t *db, uint64_t id, uint64_t wanted) {
    jobdb_schedule_t schedule; unsigned i;
    for (i = 0; i < 500; ++i) {
        if (jobdb_schedule_get(db, id, &schedule) == JOBDB_OK && schedule.occurrence_count >= wanted) return;
        pause_ms(10);
    }
    fprintf(stderr, "schedule %llu reached only %llu occurrences (next %lld, revision %llu)\n", (unsigned long long)id, (unsigned long long)schedule.occurrence_count, (long long)schedule.next_fire_at, (unsigned long long)schedule.revision);
    assert(0 && "schedule did not fire enough times");
}

int main(void) {
    jobdb_t *db = NULL;
    jobcore_t *core = NULL;
    jobdb_execution_t persisted;
    uint64_t id = 0, missing = 0;
    jobcore_schedule_spec_t delayed, fixed_rate, fixed_delay;
    int64_t now = (int64_t)time(NULL);
    int64_t next_fire;
    assert(jobcore_cron_next_fire("*/15 * * * *", "UTC", now, &next_fire) == JOBDB_OK);
    assert(next_fire > now && next_fire % 900 == 0);
    assert(jobcore_cron_next_fire("0 2 * * *", "Europe/Bratislava", now, &next_fire) == JOBDB_OK);
    assert(jobcore_cron_next_fire("invalid", "UTC", now, &next_fire) == JOBDB_ERR_INVALID_ARGUMENT);
    clean_db();
    remove("jobcore-test/wal.0"); remove("jobcore-test/coordination.lock");
    remove("jobcore-test/record.3.1"); remove("jobcore-test/record.3.2");
    remove("jobcore-test/record.100.1"); remove("jobcore-test/record.100.2");
    remove("jobcore-test/record.2.50"); remove("jobcore-test/record.2.51"); remove("jobcore-test/record.2.52");
    remove("jobcore-test/record.101.50"); remove("jobcore-test/record.101.51"); remove("jobcore-test/record.101.52");
    assert(jobdb_create("jobcore-test", &db) == JOBDB_OK);
    assert(jobcore_create(db, 2, 10, &core) == JOBDB_OK);
    assert(jobcore_register_handler(core, 42, handler, NULL) == JOBDB_OK);
    assert(jobcore_enqueue(core, 42, "job", 3, 7, now, 1, &id) == JOBDB_OK);
    assert(jobcore_enqueue(core, 99, NULL, 0, 1, now, 1, &missing) == JOBDB_OK);
    memset(&delayed, 0, sizeof delayed); delayed.schedule_id = 50; delayed.job_type = 42; delayed.type = JOBCORE_SCHEDULE_DELAYED; delayed.first_fire_at = now + 1; delayed.payload = "job"; delayed.payload_size = 3; delayed.payload_version = 7;
    assert(jobcore_schedule_create(core, &delayed) == JOBDB_OK);
    memset(&fixed_rate, 0, sizeof fixed_rate); fixed_rate.schedule_id = 51; fixed_rate.job_type = 42; fixed_rate.type = JOBCORE_SCHEDULE_INTERVAL; fixed_rate.first_fire_at = now + 1; fixed_rate.interval = 1; fixed_rate.interval_mode = JOBCORE_FIXED_RATE; fixed_rate.max_occurrences = 1; fixed_rate.payload = "job"; fixed_rate.payload_size = 3; fixed_rate.payload_version = 7;
    assert(jobcore_schedule_create(core, &fixed_rate) == JOBDB_OK);
    memset(&fixed_delay, 0, sizeof fixed_delay); fixed_delay.schedule_id = 52; fixed_delay.job_type = 42; fixed_delay.type = JOBCORE_SCHEDULE_INTERVAL; fixed_delay.first_fire_at = now + 1; fixed_delay.interval = 1; fixed_delay.interval_mode = JOBCORE_FIXED_DELAY; fixed_delay.max_occurrences = 1; fixed_delay.payload = "job"; fixed_delay.payload_size = 3; fixed_delay.payload_version = 7;
    assert(jobcore_schedule_create(core, &fixed_delay) == JOBDB_OK);
    assert(jobcore_start(core) == JOBDB_OK);
    wait_state(db, id, JOBDB_EXEC_DONE);
    wait_state(db, missing, JOBDB_EXEC_FAILED);
    assert(handled >= 1);
    assert(jobcore_stop(core) == JOBDB_OK);
    jobcore_destroy(core); core = NULL;
    jobdb_close(db); db = NULL;
    assert(jobdb_open("jobcore-test", &db) == JOBDB_OK);
    assert(jobdb_execution_get(db, id, &persisted) == JOBDB_OK);
    { jobcore_t *test_core = NULL; jobcore_retry_spec_t retry = { JOBCORE_RETRY_FIXED, 3, 1, 10, 1.0, 0 }; uint64_t retry_id = 0; jobdb_record_t policy = {0}; assert(jobcore_create(db, 1, 10, &test_core) == JOBDB_OK); jobdb_test_fail_next(db, JOBDB_FAILURE_AFTER_COMMIT); assert(jobcore_enqueue_with_retry(test_core, 42, "job", 3, 7, now, &retry, &retry_id) == JOBDB_ERR_IO); jobcore_destroy(test_core); jobdb_close(db); db = NULL; assert(jobdb_open("jobcore-test", &db) == JOBDB_OK); assert(jobdb_execution_get(db, retry_id, &persisted) == JOBDB_OK && persisted.state == JOBDB_EXEC_READY); assert(jobdb_record_get(db, 103, retry_id, &policy) == JOBDB_OK && policy.payload_size > 0); jobdb_record_free(&policy); }
    assert(jobdb_verify("jobcore-test") == JOBDB_OK);
    jobdb_close(db);
    puts("jobcore tests passed");
    return 0;
}

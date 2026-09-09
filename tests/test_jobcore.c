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

static uint64_t workflow_execution(jobdb_t *db, uint64_t workflow_id, uint64_t node_id) {
    uint64_t ids[128], id, stored_workflow, stored_node; size_t count = 0, i; jobdb_record_t record = {0};
    assert(jobdb_list_record_ids(db, 104, ids, 128, &count) == JOBDB_OK);
    for (i = 0; i < count; ++i) if (jobdb_record_get(db, 104, ids[i], &record) == JOBDB_OK && record.payload_size >= 40) {
        memcpy(&stored_workflow, record.payload + 8, sizeof stored_workflow); memcpy(&id, record.payload + 16, sizeof id); memcpy(&stored_node, record.payload + 24, sizeof stored_node); jobdb_record_free(&record);
        if (stored_workflow == workflow_id && stored_node == node_id) return id;
    }
    return 0;
}
static void wait_not_blocked(jobdb_t *db, uint64_t id) { jobdb_execution_t e; unsigned i; for (i=0;i<1000;++i) { if (jobdb_execution_get(db,id,&e)==JOBDB_OK && e.state!=JOBDB_EXEC_BLOCKED) return; pause_ms(10); } assert(0 && "workflow child remained blocked"); }

int main(void) {
    jobdb_t *db = NULL;
    jobcore_t *core = NULL;
    jobdb_execution_t persisted;
    uint64_t id = 0, missing = 0;
    jobcore_schedule_spec_t delayed, fixed_rate, fixed_delay;
    int64_t now = (int64_t)time(NULL);
    int64_t next_fire;
    jobcore_workflow_node_t workflow_nodes[2], cycle_nodes[2];
    jobdb_stats_t stats_before, stats_after;
    uint64_t workflow_root = 0, workflow_child = 0, success_child = 0, recovery_child_block = 0, recovery_child_continue = 0, recovery_child_fail = 0;
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
    { uint64_t idem1 = 0, idem2 = 0; assert(jobcore_enqueue_idempotent(core, "stable-idem-key", 42, "job", 3, 7, now, 2, &idem1) == JOBDB_OK); assert(jobcore_enqueue_idempotent(core, "stable-idem-key", 42, "job", 3, 7, now, 2, &idem2) == JOBDB_OK && idem1 == idem2); assert(jobcore_enqueue_idempotent(core, "stable-idem-key", 42, "other", 5, 7, now, 2, &idem2) == JOBDB_ERR_CONFLICT); }
    { jobcore_retry_spec_t retry={JOBCORE_RETRY_FIXED,3,1,1,1.0,0}; uint64_t idem1=0,idem2=0; jobdb_record_t metadata={0}; jobdb_result_t duplicate_result; assert(jobcore_enqueue_idempotent_with_retry(core,"retry-idem-key",42,"job",3,7,now,&retry,&idem1)==JOBDB_OK); duplicate_result=jobcore_enqueue_idempotent_with_retry(core,"retry-idem-key",42,"job",3,7,now,&retry,&idem2); if(duplicate_result!=JOBDB_OK||idem1!=idem2)fprintf(stderr,"retry idempotency duplicate result=%d ids=%llu/%llu\n",(int)duplicate_result,(unsigned long long)idem1,(unsigned long long)idem2); assert(duplicate_result==JOBDB_OK&&idem1==idem2); assert(jobdb_record_get(db,103,idem1,&metadata)==JOBDB_OK&&metadata.payload_size==48);jobdb_record_free(&metadata);assert(jobcore_enqueue_idempotent_with_retry(core,"retry-idem-key",42,"bad",3,7,now,&retry,&idem2)==JOBDB_ERR_CONFLICT); }
    memset(&delayed, 0, sizeof delayed); delayed.schedule_id = 50; delayed.job_type = 42; delayed.type = JOBCORE_SCHEDULE_DELAYED; delayed.first_fire_at = now + 1; delayed.payload = "job"; delayed.payload_size = 3; delayed.payload_version = 7;
    assert(jobcore_schedule_create(core, &delayed) == JOBDB_OK);
    memset(&fixed_rate, 0, sizeof fixed_rate); fixed_rate.schedule_id = 51; fixed_rate.job_type = 42; fixed_rate.type = JOBCORE_SCHEDULE_INTERVAL; fixed_rate.first_fire_at = now + 1; fixed_rate.interval = 1; fixed_rate.interval_mode = JOBCORE_FIXED_RATE; fixed_rate.max_occurrences = 1; fixed_rate.payload = "job"; fixed_rate.payload_size = 3; fixed_rate.payload_version = 7;
    assert(jobcore_schedule_create(core, &fixed_rate) == JOBDB_OK);
    memset(&fixed_delay, 0, sizeof fixed_delay); fixed_delay.schedule_id = 52; fixed_delay.job_type = 42; fixed_delay.type = JOBCORE_SCHEDULE_INTERVAL; fixed_delay.first_fire_at = now + 1; fixed_delay.interval = 1; fixed_delay.interval_mode = JOBCORE_FIXED_DELAY; fixed_delay.max_occurrences = 1; fixed_delay.payload = "job"; fixed_delay.payload_size = 3; fixed_delay.payload_version = 7;
    assert(jobcore_schedule_create(core, &fixed_delay) == JOBDB_OK);
    memset(workflow_nodes, 0, sizeof workflow_nodes);
    workflow_nodes[0].node_id = 7001; workflow_nodes[0].job_type = 42; workflow_nodes[0].payload = "job"; workflow_nodes[0].payload_size = 3; workflow_nodes[0].payload_version = 7;
    workflow_nodes[1].node_id = 7002; workflow_nodes[1].job_type = 42; workflow_nodes[1].payload = "job"; workflow_nodes[1].payload_size = 3; workflow_nodes[1].payload_version = 7; workflow_nodes[1].dependency_count = 1; workflow_nodes[1].dependencies[0] = 7001;
    assert(jobcore_workflow_submit(core, 7000, workflow_nodes, 2, JOBCORE_DEP_CANCEL, now) == JOBDB_OK);
    workflow_root = workflow_execution(db, 7000, 7001); workflow_child = workflow_execution(db, 7000, 7002);
    { uint64_t ids[128]; size_t n=0; jobdb_record_t wr={0}; assert(jobdb_list_record_ids(db,104,ids,128,&n)==JOBDB_OK && n>=2); assert(jobdb_record_get(db,104,ids[0],&wr)==JOBDB_OK && wr.payload_size==104); assert(wr.payload[0]=='W' && wr.payload[1]=='L' && wr.payload[2]=='F' && wr.payload[3]=='1'); jobdb_record_free(&wr); }
    { jobdb_execution_t workflow_first, workflow_second; assert(workflow_root && workflow_child); assert(jobdb_execution_get(db, workflow_root, &workflow_first) == JOBDB_OK && workflow_first.state == JOBDB_EXEC_READY); assert(jobdb_execution_get(db, workflow_child, &workflow_second) == JOBDB_OK && workflow_second.state == JOBDB_EXEC_BLOCKED); }
    assert(jobdb_get_stats(db, &stats_before) == JOBDB_OK);
    memcpy(cycle_nodes, workflow_nodes, sizeof cycle_nodes); cycle_nodes[0].node_id = 7101; cycle_nodes[0].dependency_count = 1; cycle_nodes[0].dependencies[0] = 7102; cycle_nodes[1].node_id = 7102; cycle_nodes[1].dependency_count = 1; cycle_nodes[1].dependencies[0] = 7101;
    assert(jobcore_workflow_submit(core, 7100, cycle_nodes, 2, JOBCORE_DEP_BLOCK, now) == JOBDB_ERR_INVALID_ARGUMENT);
    assert(jobdb_get_stats(db, &stats_after) == JOBDB_OK && stats_after.submitted_total == stats_before.submitted_total);
    assert(jobdb_execution_finalize_unleased(db, workflow_root, 1, JOBDB_EXEC_CANCELLED, 0, 0) == JOBDB_OK);
    { uint64_t root; jobdb_execution_t child; jobcore_dependency_policy_t policies[3]={JOBCORE_DEP_BLOCK,JOBCORE_DEP_CONTINUE,JOBCORE_DEP_FAIL_WORKFLOW}; uint64_t workflows[3]={7300,7400,7500}, roots[3]={8203,8307,8411}, child_nodes[3]={8204,8308,8412}; uint64_t *children[3]={&recovery_child_block,&recovery_child_continue,&recovery_child_fail}; size_t k; for(k=0;k<3;++k){ workflow_nodes[0].node_id=roots[k]; workflow_nodes[0].dependency_count=0; workflow_nodes[1].node_id=child_nodes[k]; workflow_nodes[1].dependencies[0]=roots[k]; assert(jobcore_workflow_submit(core,workflows[k],workflow_nodes,2,policies[k],now)==JOBDB_OK); root=workflow_execution(db,workflows[k],roots[k]); *children[k]=workflow_execution(db,workflows[k],child_nodes[k]); assert(root&&*children[k]); assert(jobdb_execution_finalize_unleased(db,root,1,JOBDB_EXEC_CANCELLED,0,0)==JOBDB_OK); assert(jobdb_execution_get(db,*children[k],&child)==JOBDB_OK&&child.state==JOBDB_EXEC_BLOCKED); } workflow_nodes[0].node_id=8101; workflow_nodes[0].dependency_count=0; workflow_nodes[1].node_id=8102; workflow_nodes[1].dependencies[0]=8101; assert(jobcore_workflow_submit(core,7200,workflow_nodes,2,JOBCORE_DEP_BLOCK,now)==JOBDB_OK); success_child=workflow_execution(db,7200,8102); assert(success_child); }
    assert(jobcore_start(core) == JOBDB_OK);
    wait_state(db, workflow_child, JOBDB_EXEC_CANCELLED);
    wait_not_blocked(db,success_child); { jobdb_execution_t e; assert(jobdb_execution_get(db,recovery_child_block,&e)==JOBDB_OK&&e.state==JOBDB_EXEC_BLOCKED); }
    wait_not_blocked(db,recovery_child_continue); wait_state(db,recovery_child_fail,JOBDB_EXEC_CANCELLED);
     wait_state(db, id, JOBDB_EXEC_DONE);
     wait_state(db, missing, JOBDB_EXEC_PAUSED);
     { jobdb_execution_t parked; jobdb_result_t resume=JOBDB_ERR_BUSY; unsigned retry; assert(jobdb_execution_get(db, missing, &parked) == JOBDB_OK); for(retry=0;retry<1000&&resume==JOBDB_ERR_BUSY;++retry){ resume=jobdb_execution_resume_paused(db,missing,parked.revision,NULL); if(resume==JOBDB_ERR_BUSY)pause_ms(2); } assert(resume==JOBDB_OK); assert(jobdb_execution_get(db, missing, &parked) == JOBDB_OK && parked.state == JOBDB_EXEC_READY); }
     { jobdb_ledger_entry_t ledger; jobdb_execution_t completed; assert(jobdb_ledger_get(db, id, &ledger) == JOBDB_OK && ledger.final_state == JOBDB_EXEC_DONE); assert(jobdb_execution_get(db, id, &completed) == JOBDB_OK); assert(jobdb_execution_requeue_admin(db, id, completed.revision, NULL) == JOBDB_OK); assert(jobdb_execution_get(db, id, &completed) == JOBDB_OK && completed.state == JOBDB_EXEC_READY); assert(jobdb_ledger_get(db, id, &ledger) == JOBDB_OK && ledger.final_state == JOBDB_EXEC_DONE); assert(jobdb_execution_requeue_admin(db, id, completed.revision, NULL) == JOBDB_ERR_INVALID_ARGUMENT); }
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

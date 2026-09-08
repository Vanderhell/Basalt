#include "jobcore.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <io.h>
static void pause_ms(unsigned n) { Sleep(n); }
static void clean_db(void) { struct _finddata_t d; intptr_t h = _findfirst("scheduler-policy-test/*", &d); if (h != -1) { do { char p[256]; if (strcmp(d.name, ".") && strcmp(d.name, "..")) { (void)snprintf(p, sizeof p, "scheduler-policy-test/%s", d.name); (void)remove(p); } } while (_findnext(h, &d) == 0); _findclose(h); } }
#else
#include <unistd.h>
static void pause_ms(unsigned n) { (void)usleep(n * 1000u); }
static void clean_db(void) { (void)remove("scheduler-policy-test/manifest.0"); (void)remove("scheduler-policy-test/manifest.1"); (void)remove("scheduler-policy-test/wal.0"); }
#endif

static volatile int handled;
static int handler(const void *payload, size_t size, uint32_t version, const jobcore_execution_context_t *context, void *data) {
    (void)payload; (void)size; (void)version; (void)context; (void)data; ++handled; pause_ms(100); return 0;
}
static void wait_occurrences(jobdb_t *db, uint64_t id, uint64_t count) {
    unsigned i; jobdb_schedule_t s;
    for (i = 0; i < 1000; ++i) { if (jobdb_schedule_get(db, id, &s) == JOBDB_OK && s.occurrence_count >= count) return; pause_ms(10); }
    (void)jobdb_schedule_get(db, id, &s); fprintf(stderr, "schedule %llu timeout: occurrences=%llu next=%lld last=%lld\n", (unsigned long long)id, (unsigned long long)s.occurrence_count, (long long)s.next_fire_at, (long long)s.last_fire_at); assert(0 && "schedule occurrence timeout");
}
static void wait_done(jobdb_t *db, uint64_t id) { unsigned i; jobdb_execution_t e; for (i=0;i<1000;++i) { if (jobdb_execution_get(db,id,&e)==JOBDB_OK && e.state==JOBDB_EXEC_DONE) return; pause_ms(10); } assert(0 && "execution completion timeout"); }
static void wait_handled(jobdb_t *db, int count) { unsigned i; for (i = 0; i < 1000 && handled < count; ++i) pause_ms(10); if (handled < count) { jobdb_execution_t a={0},b={0}; (void)jobdb_execution_get(db,9001,&a); (void)jobdb_execution_get(db,9002,&b); fprintf(stderr,"queue timeout: handled=%d active=%d/r%llu queued=%d/r%llu\n",(int)handled,(int)a.state,(unsigned long long)a.revision,(int)b.state,(unsigned long long)b.revision); } assert(handled >= count); }
static void create_interval(jobcore_t *core, uint64_t id, int64_t first, uint64_t max, uint32_t catch_up, jobcore_interval_mode_t mode, jobcore_overlap_policy_t overlap) {
    jobcore_schedule_spec_t s; memset(&s, 0, sizeof s); s.schedule_id=id; s.job_type=42; s.type=JOBCORE_SCHEDULE_INTERVAL; s.first_fire_at=first; s.interval=60; s.interval_mode=mode; s.max_occurrences=max; s.payload="x"; s.payload_size=1; s.payload_version=1; s.misfire_policy=JOBCORE_MISFIRE_CATCH_UP_ALL; s.overlap_policy=overlap; s.catch_up_max=catch_up; assert(jobcore_schedule_create(core, &s)==JOBDB_OK);
}

int main(void) {
    jobdb_t *db=NULL; jobcore_t *core=NULL; jobdb_schedule_t s; int64_t now=(int64_t)time(NULL), first=now-600;
    clean_db(); assert(jobdb_create("scheduler-policy-test", &db)==JOBDB_OK); assert(jobcore_create(db, 2, 10, &core)==JOBDB_OK); assert(jobcore_register_handler(core,42,handler,NULL)==JOBDB_OK);
    create_interval(core,1001,first,8,3,JOBCORE_FIXED_RATE,JOBCORE_OVERLAP_ALLOW);
    create_interval(core,1002,first,0,10,JOBCORE_FIXED_RATE,JOBCORE_OVERLAP_ALLOW); assert(jobcore_schedule_get(core,1002,&s)==JOBDB_OK); s.end_at=first+240; assert(jobcore_schedule_update(core,&s,s.revision)==JOBDB_OK);
    create_interval(core,1003,now,1,0,JOBCORE_FIXED_DELAY,JOBCORE_OVERLAP_ALLOW);
    assert(jobcore_start(core)==JOBDB_OK); wait_occurrences(db,1001,8); wait_occurrences(db,1002,5); wait_occurrences(db,1003,1); pause_ms(300);
    assert(jobcore_schedule_get(core,1001,&s)==JOBDB_OK && s.occurrence_count==8 && s.next_fire_at==INT64_MAX && (s.last_fire_at-first)%60==0);
    assert(jobcore_schedule_get(core,1002,&s)==JOBDB_OK && s.occurrence_count==5 && s.last_fire_at==first+240 && s.next_fire_at==INT64_MAX);
    assert(jobcore_stop(core)==JOBDB_OK); jobcore_destroy(core); core=NULL; jobdb_close(db); db=NULL;
    assert(jobdb_open("scheduler-policy-test",&db)==JOBDB_OK); assert(jobcore_create(db,1,10,&core)==JOBDB_OK); assert(jobcore_register_handler(core,42,handler,NULL)==JOBDB_OK); assert(jobcore_start(core)==JOBDB_OK); pause_ms(300); assert(jobcore_schedule_get(core,1003,&s)==JOBDB_OK && s.occurrence_count==1 && s.next_fire_at==INT64_MAX); assert(jobcore_stop(core)==JOBDB_OK); jobcore_destroy(core); core=NULL;

    /* Persist one active and exactly one blocked occurrence, then prove restart releases it. */
    handled = 0;
    assert(jobcore_create(db,1,10,&core)==JOBDB_OK); create_interval(core,1004,now+3600,2,0,JOBCORE_FIXED_RATE,JOBCORE_OVERLAP_QUEUE_ONE);
    assert(jobcore_schedule_get(core,1004,&s)==JOBDB_OK); assert(jobdb_schedule_try_fire(db,1004,s.revision,s.next_fire_at,9001,now,now+60)==JOBDB_OK); assert(jobcore_schedule_get(core,1004,&s)==JOBDB_OK); assert(jobdb_schedule_try_fire_state(db,1004,s.revision,s.next_fire_at,9002,now,INT64_MAX,JOBDB_EXEC_BLOCKED)==JOBDB_OK);
    jobcore_destroy(core); core=NULL; jobdb_close(db); db=NULL; assert(jobdb_open("scheduler-policy-test",&db)==JOBDB_OK); assert(jobcore_create(db,1,10,&core)==JOBDB_OK); assert(jobcore_register_handler(core,42,handler,NULL)==JOBDB_OK); assert(jobcore_start(core)==JOBDB_OK); wait_handled(db,2);
    wait_done(db,9001); wait_done(db,9002);
    assert(jobcore_schedule_get(core,1004,&s)==JOBDB_OK && s.occurrence_count==2 && s.next_fire_at==INT64_MAX); assert(jobcore_stop(core)==JOBDB_OK); jobcore_destroy(core); jobdb_close(db); puts("scheduler policy tests passed"); return 0;
}

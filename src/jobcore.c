#include "jobcore.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <process.h>
typedef CRITICAL_SECTION core_mutex_t;
typedef HANDLE core_thread_t;
static void mutex_init(core_mutex_t *m) { InitializeCriticalSection(m); }
static void mutex_destroy(core_mutex_t *m) { DeleteCriticalSection(m); }
static void mutex_lock(core_mutex_t *m) { EnterCriticalSection(m); }
static void mutex_unlock(core_mutex_t *m) { LeaveCriticalSection(m); }
static void sleep_ms(unsigned ms) { Sleep(ms); }
static unsigned process_id(void) { return (unsigned)GetCurrentProcessId(); }
#else
#include <pthread.h>
#include <unistd.h>
typedef pthread_mutex_t core_mutex_t;
typedef pthread_t core_thread_t;
static void mutex_init(core_mutex_t *m) { (void)pthread_mutex_init(m, NULL); }
static void mutex_destroy(core_mutex_t *m) { (void)pthread_mutex_destroy(m); }
static void mutex_lock(core_mutex_t *m) { (void)pthread_mutex_lock(m); }
static void mutex_unlock(core_mutex_t *m) { (void)pthread_mutex_unlock(m); }
static void sleep_ms(unsigned ms) { (void)usleep(ms * 1000u); }
static unsigned process_id(void) { return (unsigned)getpid(); }
#endif

#define PAYLOAD_RECORD_TYPE 100u
#define SCHEDULE_PAYLOAD_RECORD_TYPE 101u
#define CRON_RECORD_TYPE 102u
#define RETRY_RECORD_TYPE 103u
#define WORKFLOW_NODE_RECORD_TYPE 104u
#define PAYLOAD_HEADER_SIZE 16u
#define MAX_HANDLERS 128u

typedef struct { uint64_t type; jobcore_handler_fn fn; void *data; } core_handler_t;
typedef struct { jobcore_t *core; jobdb_worker_id_t worker; uint64_t execution_id; uint64_t token; volatile int stop; core_thread_t thread; int started; } heartbeat_t;
struct jobcore { jobdb_t *db; uint32_t worker_count; int64_t lease_duration; uint32_t grace_ms; volatile int stopping; int started; uint64_t identity; core_mutex_t mutex; uint32_t handler_count; core_handler_t handlers[MAX_HANDLERS]; core_thread_t *threads; core_thread_t scheduler; int scheduler_started; };
typedef struct { uint32_t policy, max_attempts, jitter, reserved; int64_t initial_delay, max_delay; double backoff_factor; } retry_record_t;
typedef struct { uint64_t workflow_id, execution_id, node_id; uint32_t policy, dependency_count; uint64_t dependencies[8]; } workflow_record_t;
static void workflow_progress(jobcore_t *, uint64_t);
static void workflow_reconcile(jobcore_t *);

static int64_t current_time(void) { return (int64_t)time(NULL); }
static uint64_t hash_name(const char *s) { uint64_t h = UINT64_C(1469598103934665603); while (*s) { h ^= (unsigned char)*s++; h *= UINT64_C(1099511628211); } return h ? h : 1; }
static core_handler_t *find_handler(jobcore_t *c, uint64_t type) { uint32_t i; for (i = 0; i < c->handler_count; ++i) if (c->handlers[i].type == type) return &c->handlers[i]; return NULL; }
static void make_worker(jobcore_t *c, jobdb_worker_id_t *id, uint32_t index) { memset(id, 0, sizeof *id); memcpy(id->bytes, &c->identity, sizeof c->identity); memcpy(id->bytes + 8, &index, sizeof index); }

static jobdb_result_t store_payload(jobcore_t *c, uint32_t record_type, uint64_t id, uint64_t type, uint32_t version, const void *payload, uint32_t size) {
    uint8_t *buffer; jobdb_result_t result;
    if (size > UINT32_MAX - PAYLOAD_HEADER_SIZE) return JOBDB_ERR_LIMIT;
    buffer = (uint8_t *)malloc(PAYLOAD_HEADER_SIZE + (size_t)size);
    if (!buffer) return JOBDB_ERR_INTERNAL;
    memcpy(buffer, &type, 8); memcpy(buffer + 8, &version, 4); memcpy(buffer + 12, &size, 4);
    if (size) memcpy(buffer + PAYLOAD_HEADER_SIZE, payload, size);
    result = jobdb_record_create(c->db, record_type, id, buffer, PAYLOAD_HEADER_SIZE + size);
    free(buffer); return result;
}
static jobdb_result_t store_cron(jobcore_t *c, uint64_t id, const char *expression, const char *timezone) {
    size_t a, b; uint8_t *p; jobdb_result_t r;
    if (!expression || !timezone || !*expression || !*timezone) return JOBDB_ERR_INVALID_ARGUMENT;
    a = strlen(timezone); b = strlen(expression); if (a > UINT32_MAX - b - 2u) return JOBDB_ERR_LIMIT;
    p = (uint8_t *)malloc(a + b + 2u); if (!p) return JOBDB_ERR_INTERNAL;
    memcpy(p, timezone, a + 1u); memcpy(p + a + 1u, expression, b + 1u);
    r = jobdb_record_create(c->db, CRON_RECORD_TYPE, id, p, (uint32_t)(a + b + 2u)); free(p); return r;
}
static int64_t retry_delay(const retry_record_t *p, uint32_t attempt, uint64_t id) { double delay = (double)p->initial_delay; uint32_t i; if (p->policy == JOBCORE_RETRY_LINEAR) delay *= attempt; else if (p->policy == JOBCORE_RETRY_EXPONENTIAL || p->policy == JOBCORE_RETRY_EXPONENTIAL_WITH_JITTER) for (i = 1; i < attempt; ++i) delay *= p->backoff_factor > 1.0 ? p->backoff_factor : 2.0; if (p->max_delay > 0 && delay > p->max_delay) delay = (double)p->max_delay; if (p->policy == JOBCORE_RETRY_EXPONENTIAL_WITH_JITTER && p->jitter) delay += (double)((id + attempt * 2654435761u) % (p->jitter + 1)); return delay < 0 ? 0 : (int64_t)delay; }

#ifdef _WIN32
static DWORD WINAPI heartbeat_main(void *argument)
#else
static void *heartbeat_main(void *argument)
#endif
{
    heartbeat_t *heartbeat = (heartbeat_t *)argument;
    unsigned interval = (unsigned)(heartbeat->core->lease_duration > 1 ? heartbeat->core->lease_duration * 333u : 100u);
    while (!heartbeat->stop) {
        unsigned waited = 0;
        while (!heartbeat->stop && waited < interval) {
            unsigned step = interval - waited > 10u ? 10u : interval - waited;
            sleep_ms(step);
            waited += step;
        }
        if (!heartbeat->stop) (void)jobdb_renew_lease(heartbeat->core->db, heartbeat->execution_id, &heartbeat->worker, heartbeat->token, current_time() + heartbeat->core->lease_duration);
    }
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

static int heartbeat_start(heartbeat_t *heartbeat) {
#ifdef _WIN32
    heartbeat->thread = CreateThread(NULL, 0, heartbeat_main, heartbeat, 0, NULL);
    heartbeat->started = heartbeat->thread != NULL;
#else
    heartbeat->started = pthread_create(&heartbeat->thread, NULL, heartbeat_main, heartbeat) == 0;
#endif
    return heartbeat->started;
}
static void heartbeat_stop(heartbeat_t *heartbeat) {
    if (!heartbeat->started) return;
    heartbeat->stop = 1;
#ifdef _WIN32
    (void)WaitForSingleObject(heartbeat->thread, INFINITE); CloseHandle(heartbeat->thread);
#else
    (void)pthread_join(heartbeat->thread, NULL);
#endif
    heartbeat->started = 0;
}

static jobdb_result_t complete_with_retry(jobdb_t *db,uint64_t id,const jobdb_worker_id_t*w,uint64_t token){jobdb_result_t r=JOBDB_ERR_BUSY;for(unsigned i=0;i<100&&(r==JOBDB_ERR_BUSY||r==JOBDB_ERR_CONFLICT);i++){r=jobdb_execution_complete(db,id,w,token);if(r==JOBDB_ERR_BUSY||r==JOBDB_ERR_CONFLICT)sleep_ms(2);}return r;}
static jobdb_result_t finalize_with_retry(jobdb_t *db,uint64_t id,const jobdb_worker_id_t*w,uint64_t token,jobdb_execution_state_t state,int32_t result_code,int32_t error_code){jobdb_result_t r=JOBDB_ERR_BUSY;for(unsigned i=0;i<100&&(r==JOBDB_ERR_BUSY||r==JOBDB_ERR_CONFLICT);i++){r=jobdb_execution_finalize(db,id,w,token,state,result_code,error_code);if(r==JOBDB_ERR_BUSY||r==JOBDB_ERR_CONFLICT)sleep_ms(2);}return r;}
#define jobdb_execution_complete(db,id,w,token) complete_with_retry((db),(id),(w),(token))
static void process_one(jobcore_t *c, const jobdb_worker_id_t *worker) {
    jobdb_execution_t execution; jobdb_record_t record = {0}; core_handler_t handler = {0};
    jobcore_execution_context_t context; heartbeat_t heartbeat; int64_t started_at = current_time();
    jobdb_result_t result = jobdb_claim_next(c->db, worker, started_at, c->lease_duration, &execution);
    if (result != JOBDB_OK) { sleep_ms(2); return; }
    result = jobdb_execution_start(c->db, execution.execution_id, worker, execution.fencing_token, (int64_t)time(NULL), &execution.revision);
    if (result != JOBDB_OK) return;
    mutex_lock(&c->mutex); { core_handler_t *registered = find_handler(c, execution.job_definition_id); if (registered) handler = *registered; } mutex_unlock(&c->mutex);
    memset(&context, 0, sizeof context); context.execution_id = execution.execution_id; context.job_type = execution.job_definition_id; context.attempt = execution.attempt; context.fencing_token = execution.fencing_token; context.worker = *worker; context.cancellation_requested = &c->stopping;
    memset(&heartbeat, 0, sizeof heartbeat); heartbeat.core = c; heartbeat.worker = *worker; heartbeat.execution_id = execution.execution_id; heartbeat.token = execution.fencing_token;
    (void)heartbeat_start(&heartbeat);
    result = jobdb_record_get(c->db, PAYLOAD_RECORD_TYPE, execution.execution_id, &record);
    if (result == JOBDB_ERR_NOT_FOUND && execution.schedule_id != 0)
        result = jobdb_record_get(c->db, SCHEDULE_PAYLOAD_RECORD_TYPE, execution.schedule_id, &record);
    if (result == JOBDB_OK && record.payload_size >= PAYLOAD_HEADER_SIZE) {
        uint64_t type; uint32_t version, size;
        memcpy(&type, record.payload, 8); memcpy(&version, record.payload + 8, 4); memcpy(&size, record.payload + 12, 4); context.payload_version = version;
        if (type != execution.job_definition_id || size != record.payload_size - PAYLOAD_HEADER_SIZE) result = JOBDB_ERR_CORRUPT;
        else if (!handler.fn) result = JOBDB_ERR_NOT_FOUND;
        else if (handler.fn(record.payload + PAYLOAD_HEADER_SIZE, size, version, &context, handler.data) != 0) result = JOBDB_ERR_INTERNAL;
    } else if (result == JOBDB_OK) result = JOBDB_ERR_CORRUPT;
    if (record.payload) jobdb_record_free(&record); heartbeat_stop(&heartbeat);
    if (result == JOBDB_OK) { if (jobdb_execution_complete(c->db, execution.execution_id, worker, execution.fencing_token) == JOBDB_OK) workflow_progress(c, execution.execution_id); }
    else { jobdb_record_t retry_record = {0}; retry_record_t policy = {0}; int has_retry = jobdb_record_get(c->db, RETRY_RECORD_TYPE, execution.execution_id, &retry_record) == JOBDB_OK && retry_record.payload_size == sizeof policy; if (has_retry) memcpy(&policy, retry_record.payload, sizeof policy); if (retry_record.payload) jobdb_record_free(&retry_record); if (has_retry && policy.policy != JOBCORE_RETRY_NONE && execution.attempt + 1u < policy.max_attempts) { int64_t eligible = current_time() + retry_delay(&policy, execution.attempt + 1u, execution.execution_id); jobdb_result_t retry_result = JOBDB_ERR_BUSY; unsigned retry_count; for (retry_count = 0; retry_count < 100 && retry_result == JOBDB_ERR_BUSY; ++retry_count) { retry_result = jobdb_execution_retry(c->db, execution.execution_id, worker, execution.fencing_token, eligible, NULL); if (retry_result == JOBDB_ERR_BUSY || retry_result == JOBDB_ERR_CONFLICT) sleep_ms(2); } } else { jobdb_execution_state_t final_state = has_retry && policy.policy != JOBCORE_RETRY_NONE ? JOBDB_EXEC_DEAD : JOBDB_EXEC_FAILED; (void)finalize_with_retry(c->db, execution.execution_id, worker, execution.fencing_token, final_state, 0, (int32_t)result); } }
}

static uint64_t allocate_execution_id(jobcore_t *c) {
    uint64_t id=0;
    return jobdb_allocate_execution_id(c->db,&id)==JOBDB_OK?id:0;
}
static int load_all_record_ids(jobcore_t *c, uint32_t type, uint64_t **out, size_t *count) {
    uint64_t *ids = NULL; size_t capacity = 0, observed = 0;
    unsigned attempt;
    if (jobdb_list_record_ids(c->db, type, NULL, 0, &capacity) != JOBDB_OK) return 0;
    for (attempt = 0; attempt < 8; ++attempt) {
        if (capacity > SIZE_MAX / sizeof *ids) return 0;
        if (!ids && capacity) { ids = (uint64_t *)malloc(capacity * sizeof *ids); if (!ids) return 0; }
        observed = 0;
        if (jobdb_list_record_ids(c->db, type, ids, capacity, &observed) != JOBDB_OK) { free(ids); return 0; }
        if (observed <= capacity) { *out = ids; *count = observed; return 1; }
        if (observed > SIZE_MAX / sizeof *ids) { free(ids); return 0; }
        { uint64_t *grown = (uint64_t *)realloc(ids, observed * sizeof *ids); if (!grown) { free(ids); return 0; } ids = grown; capacity = observed; }
    }
    free(ids); return 0;
}

static int64_t latest_finished_for_schedule(jobcore_t *c, uint64_t schedule_id) {
    uint64_t *ids = NULL; size_t count = 0, i; int64_t finished = 0; jobdb_execution_t execution;
    if (!load_all_record_ids(c, 3, &ids, &count)) return 0;
    for (i = 0; i < count; ++i) if (jobdb_execution_get(c->db, ids[i], &execution) == JOBDB_OK && execution.schedule_id == schedule_id && execution.finished_at > finished && (execution.state == JOBDB_EXEC_DONE || execution.state == JOBDB_EXEC_FAILED || execution.state == JOBDB_EXEC_DEAD || execution.state == JOBDB_EXEC_CANCELLED)) finished = execution.finished_at;
    free(ids); return finished;
}
static int schedule_active(jobcore_t *c, uint64_t schedule_id) {
    uint64_t *ids = NULL; size_t count = 0, i; jobdb_execution_t e;
    if (!load_all_record_ids(c, 3, &ids, &count)) return 0;
    for (i = 0; i < count; ++i) if (jobdb_execution_get(c->db, ids[i], &e) == JOBDB_OK && e.schedule_id == schedule_id && (e.state == JOBDB_EXEC_READY || e.state == JOBDB_EXEC_LEASED || e.state == JOBDB_EXEC_RUNNING)) { free(ids); return 1; }
    free(ids); return 0;
}
static void discard_scheduled_fire(jobcore_t *c, jobdb_schedule_t *schedule, int64_t fire_at, int64_t next_fire) {
    uint64_t id = allocate_execution_id(c), revision;
    if (id && jobdb_schedule_try_fire(c->db, schedule->schedule_id, schedule->revision, schedule->next_fire_at, id, fire_at, next_fire) == JOBDB_OK) {
        jobdb_execution_t e; if (jobdb_execution_get(c->db, id, &e) == JOBDB_OK) (void)jobdb_execution_transition(c->db, id, e.revision, JOBDB_EXEC_CANCELLED, &revision);
  }
}

static void scheduler_iteration(jobcore_t *c) {
    uint64_t *ids = NULL; size_t count = 0, i; int64_t now = current_time(); jobdb_schedule_t schedule;
    if (!load_all_record_ids(c, 2, &ids, &count)) return;
    for (i = 0; i < count && !c->stopping; ++i) {
        int64_t fire_at, next_fire; uint64_t execution_id;
        if (jobdb_schedule_get(c->db, ids[i], &schedule) != JOBDB_OK || !schedule.enabled) continue;
        fire_at = schedule.next_fire_at;
        if (schedule.schedule_type == JOBCORE_SCHEDULE_CRON) {
            jobdb_record_t cron_record = {0};
            if (jobdb_record_get(c->db, CRON_RECORD_TYPE, schedule.schedule_id, &cron_record) != JOBDB_OK || cron_record.payload_size < 3) { if (cron_record.payload) jobdb_record_free(&cron_record); continue; }
            { const char *timezone = (const char *)cron_record.payload; const char *expression = timezone + strlen(timezone) + 1; int64_t next;
              if (jobcore_cron_next_fire(expression, timezone, fire_at - 60, &next) == JOBDB_OK && next != fire_at) { uint64_t revision; (void)jobdb_schedule_try_fire(c->db, schedule.schedule_id, schedule.revision, fire_at, allocate_execution_id(c), fire_at, next); (void)revision; }
            }
            jobdb_record_free(&cron_record); continue;
        }
        if (schedule.schedule_type == JOBCORE_SCHEDULE_INTERVAL && schedule.overlap_policy == JOBCORE_FIXED_DELAY && fire_at == INT64_MAX) {
            int64_t finished = latest_finished_for_schedule(c, schedule.schedule_id);
            if (finished > 0 && schedule.timezone_reference <= (uint64_t)(INT64_MAX - finished)) fire_at = finished + (int64_t)schedule.timezone_reference;
        }
        if (fire_at > now || fire_at == INT64_MAX) continue;
        {
            int64_t cadence = schedule.schedule_type == JOBCORE_SCHEDULE_INTERVAL && schedule.timezone_reference ? (int64_t)schedule.timezone_reference : 60;
            int64_t missed = now > fire_at ? (now - fire_at) / cadence : 0;
            int active = schedule_active(c, schedule.schedule_id);
            if (active && (schedule.overlap_policy == JOBCORE_OVERLAP_SKIP || schedule.overlap_policy == JOBCORE_OVERLAP_QUEUE_ONE)) {
                discard_scheduled_fire(c, &schedule, fire_at, fire_at + cadence);
                continue;
            }
            if (missed > 0 && schedule.misfire_policy == JOBCORE_MISFIRE_SKIP) {
                discard_scheduled_fire(c, &schedule, fire_at, fire_at + (missed + 1) * cadence);
                continue;
            }
            if (missed > 0 && (schedule.misfire_policy == JOBCORE_MISFIRE_RUN_ONCE || schedule.misfire_policy == JOBCORE_MISFIRE_RUN_LAST)) fire_at = now;
            if (schedule.misfire_policy == JOBCORE_MISFIRE_CATCH_UP_ALL && schedule.occurrence_count >= (schedule.max_occurrences ? schedule.max_occurrences : 100u)) continue;
        }
        execution_id = allocate_execution_id(c); if (!execution_id) continue;
        next_fire = INT64_MAX;
        if (schedule.schedule_type == JOBCORE_SCHEDULE_INTERVAL) {
            next_fire = schedule.overlap_policy == JOBCORE_FIXED_DELAY ? INT64_MAX : fire_at + (int64_t)schedule.timezone_reference;
            if (schedule.max_occurrences && schedule.occurrence_count + 1 >= schedule.max_occurrences) next_fire = INT64_MAX;
            if (schedule.end_at && next_fire > schedule.end_at) next_fire = INT64_MAX;
        }
         {
             if (jobdb_schedule_try_fire(c->db, schedule.schedule_id, schedule.revision, schedule.next_fire_at, execution_id, fire_at, next_fire) == JOBDB_OK) {
             if (schedule.schedule_type != JOBCORE_SCHEDULE_INTERVAL) { /* next_fire == INT64_MAX makes one-shot schedules inert */ }
             }
         }
     }
    free(ids);
}

#ifdef _WIN32
static DWORD WINAPI scheduler_main(void *argument)
#else
static void *scheduler_main(void *argument)
#endif
{
    jobcore_t *c = (jobcore_t *)argument;
    while (!c->stopping) { scheduler_iteration(c); sleep_ms(20); }
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

#ifdef _WIN32
static DWORD WINAPI worker_main(void *argument)
#else
static void *worker_main(void *argument)
#endif
{ struct { jobcore_t *core; uint32_t index; } *args = argument; jobcore_t *c = args->core; jobdb_worker_id_t worker; make_worker(c, &worker, args->index); free(args); while (!c->stopping) process_one(c, &worker);
#ifdef _WIN32
return 0;
#else
return NULL;
#endif
}

jobdb_result_t jobcore_create_ex(jobdb_t *db, uint32_t workers, int64_t lease, uint32_t grace_ms, jobcore_t **out) {
    jobcore_t *c;
    if (!db || !out || !workers || lease <= 0) return JOBDB_ERR_INVALID_ARGUMENT;
    c = (jobcore_t *)calloc(1, sizeof *c); if (!c) return JOBDB_ERR_INTERNAL;
    c->db = db; c->worker_count = workers; c->lease_duration = lease; c->grace_ms = grace_ms; c->identity = ((uint64_t)process_id() << 32) ^ (uint64_t)time(NULL) ^ (uintptr_t)c; mutex_init(&c->mutex); *out = c; return JOBDB_OK;
}
jobdb_result_t jobcore_create(jobdb_t *db, uint32_t workers, int64_t lease, jobcore_t **out) { return jobcore_create_ex(db, workers, lease, 1000, out); }
void jobcore_destroy(jobcore_t *c) { if (c) { (void)jobcore_stop(c); mutex_destroy(&c->mutex); free(c); } }
jobdb_result_t jobcore_start(jobcore_t *c) { uint32_t i; if (!c) return JOBDB_ERR_INVALID_ARGUMENT; if (c->started) return JOBDB_ERR_INVALID_STATE; c->threads = (core_thread_t *)calloc(c->worker_count, sizeof *c->threads); if (!c->threads) return JOBDB_ERR_INTERNAL; c->stopping = 0; workflow_reconcile(c);
    for (i = 0; i < c->worker_count; ++i) { struct { jobcore_t *core; uint32_t index; } *args = malloc(sizeof *args); if (!args) return JOBDB_ERR_INTERNAL; args->core = c; args->index = i + 1;
#ifdef _WIN32
        c->threads[i] = CreateThread(NULL, 0, worker_main, args, 0, NULL); if (!c->threads[i]) { free(args); return JOBDB_ERR_INTERNAL; }
#else
        if (pthread_create(&c->threads[i], NULL, worker_main, args) != 0) { free(args); return JOBDB_ERR_INTERNAL; }
#endif
    }
#ifdef _WIN32
    c->scheduler = CreateThread(NULL, 0, scheduler_main, c, 0, NULL);
    if (!c->scheduler) { c->stopping = 1; return JOBDB_ERR_INTERNAL; }
#else
    if (pthread_create(&c->scheduler, NULL, scheduler_main, c) != 0) { c->stopping = 1; return JOBDB_ERR_INTERNAL; }
#endif
    c->scheduler_started = 1; c->started = 1; return JOBDB_OK;
}
jobdb_result_t jobcore_stop_with_grace(jobcore_t *c, uint32_t grace_ms) { uint32_t i; if (!c) return JOBDB_ERR_INVALID_ARGUMENT; if (!c->started) return JOBDB_OK; c->stopping = 1;
#ifdef _WIN32
    if (c->scheduler_started) { (void)WaitForSingleObject(c->scheduler, INFINITE); CloseHandle(c->scheduler); }
#else
    if (c->scheduler_started) (void)pthread_join(c->scheduler, NULL);
#endif
    c->scheduler_started = 0; if (grace_ms) sleep_ms(grace_ms); for (i = 0; i < c->worker_count; ++i) {
#ifdef _WIN32
        (void)WaitForSingleObject(c->threads[i], INFINITE); CloseHandle(c->threads[i]);
#else
        (void)pthread_join(c->threads[i], NULL);
#endif
    } free(c->threads); c->threads = NULL; c->started = 0; return JOBDB_OK; }
jobdb_result_t jobcore_stop(jobcore_t *c) { return c ? jobcore_stop_with_grace(c, c->grace_ms) : JOBDB_ERR_INVALID_ARGUMENT; }
jobdb_result_t jobcore_register_handler(jobcore_t *c, uint64_t type, jobcore_handler_fn fn, void *data) { core_handler_t *h; if (!c || !type || !fn) return JOBDB_ERR_INVALID_ARGUMENT; mutex_lock(&c->mutex); h = find_handler(c, type); if (!h && c->handler_count < MAX_HANDLERS) h = &c->handlers[c->handler_count++]; if (!h) { mutex_unlock(&c->mutex); return JOBDB_ERR_LIMIT; } h->type = type; h->fn = fn; h->data = data; mutex_unlock(&c->mutex); return JOBDB_OK; }
jobdb_result_t jobcore_register_handler_name(jobcore_t *c, const char *name, jobcore_handler_fn fn, void *data, uint64_t *out) { uint64_t type; if (!name || !*name) return JOBDB_ERR_INVALID_ARGUMENT; type = hash_name(name); if (out) *out = type; return jobcore_register_handler(c, type, fn, data); }
jobdb_result_t jobcore_enqueue(jobcore_t *c, uint64_t type, const void *payload, uint32_t size, uint32_t version, int64_t now, uint32_t max_attempts, uint64_t *out) { jobdb_execution_t e; uint64_t id; uint8_t *buffer; jobdb_result_t result; if (!c || !type || (size && !payload) || !out) return JOBDB_ERR_INVALID_ARGUMENT; if (size > UINT32_MAX - PAYLOAD_HEADER_SIZE) return JOBDB_ERR_LIMIT; id=allocate_execution_id(c);if(!id)return JOBDB_ERR_LIMIT; buffer=(uint8_t *)malloc(PAYLOAD_HEADER_SIZE + (size_t)size); if (!buffer) return JOBDB_ERR_INTERNAL; memcpy(buffer, &type, 8); memcpy(buffer + 8, &version, 4); memcpy(buffer + 12, &size, 4); if (size) memcpy(buffer + PAYLOAD_HEADER_SIZE, payload, size); memset(&e, 0, sizeof e); e.execution_id = id; e.job_definition_id = type; e.state = JOBDB_EXEC_READY; e.created_at = now; e.eligible_at = now; e.max_attempts = max_attempts ? max_attempts : 1; result = jobdb_execution_enqueue(c->db, &e, PAYLOAD_RECORD_TYPE, buffer, PAYLOAD_HEADER_SIZE + size); free(buffer); if (result == JOBDB_OK) *out = id; return result; }
jobdb_result_t jobcore_enqueue_name(jobcore_t *c, const char *name, const void *payload, uint32_t size, uint32_t version, int64_t now, uint32_t max_attempts, uint64_t *out) { if (!name || !*name) return JOBDB_ERR_INVALID_ARGUMENT; return jobcore_enqueue(c, hash_name(name), payload, size, version, now, max_attempts, out); }
jobdb_result_t jobcore_enqueue_with_retry(jobcore_t *c, uint64_t type, const void *payload, uint32_t size, uint32_t version, int64_t now, const jobcore_retry_spec_t *spec, uint64_t *out) { retry_record_t record; jobdb_execution_t e; uint8_t *buffer; uint64_t id; jobdb_result_t result; if (!c || !type || (size && !payload) || !spec || !out || spec->policy < JOBCORE_RETRY_NONE || spec->policy > JOBCORE_RETRY_EXPONENTIAL_WITH_JITTER || spec->max_attempts == 0 || spec->initial_delay < 0 || spec->max_delay < 0 || spec->jitter > 86400u) return JOBDB_ERR_INVALID_ARGUMENT; if (spec->policy == JOBCORE_RETRY_NONE) return jobcore_enqueue(c, type, payload, size, version, now, spec->max_attempts, out); if (size > UINT32_MAX - PAYLOAD_HEADER_SIZE) return JOBDB_ERR_LIMIT; id=allocate_execution_id(c); if (!id) return JOBDB_ERR_LIMIT; buffer=(uint8_t *)malloc(PAYLOAD_HEADER_SIZE + (size_t)size); if (!buffer) return JOBDB_ERR_INTERNAL; memcpy(buffer, &type, 8); memcpy(buffer + 8, &version, 4); memcpy(buffer + 12, &size, 4); if (size) memcpy(buffer + PAYLOAD_HEADER_SIZE, payload, size); memset(&e, 0, sizeof e); e.execution_id=id; e.job_definition_id=type; e.state=JOBDB_EXEC_READY; e.created_at=now; e.eligible_at=now; e.max_attempts=spec->max_attempts; memset(&record, 0, sizeof record); record.policy=(uint32_t)spec->policy; record.max_attempts=spec->max_attempts; record.initial_delay=spec->initial_delay; record.max_delay=spec->max_delay; record.backoff_factor=spec->backoff_factor; record.jitter=spec->jitter; result=jobdb_execution_enqueue_extra(c->db,&e,PAYLOAD_RECORD_TYPE,buffer,PAYLOAD_HEADER_SIZE+size,RETRY_RECORD_TYPE,&record,(uint32_t)sizeof record); free(buffer); *out=id; return result; }
jobdb_result_t jobcore_schedule_create(jobcore_t *c, const jobcore_schedule_spec_t *spec) {
    jobdb_schedule_t schedule; jobdb_result_t result; uint8_t *payload_record = NULL, *cron_record = NULL; size_t cron_size = 0;
    if (!c || !spec || !spec->schedule_id || !spec->job_type || (spec->payload_size && !spec->payload)) return JOBDB_ERR_INVALID_ARGUMENT;
    if (spec->type < JOBCORE_SCHEDULE_IMMEDIATE || spec->type > JOBCORE_SCHEDULE_CRON) return JOBDB_ERR_INVALID_ARGUMENT;
    if (spec->type == JOBCORE_SCHEDULE_CRON && (!spec->cron_expression || !spec->timezone)) return JOBDB_ERR_INVALID_ARGUMENT;
    if (spec->type == JOBCORE_SCHEDULE_INTERVAL && (spec->interval <= 0 || (spec->interval_mode != JOBCORE_FIXED_RATE && spec->interval_mode != JOBCORE_FIXED_DELAY))) return JOBDB_ERR_INVALID_ARGUMENT;
    if (spec->misfire_policy > JOBCORE_MISFIRE_CATCH_UP_ALL || spec->overlap_policy > JOBCORE_OVERLAP_QUEUE_ALL) return JOBDB_ERR_INVALID_ARGUMENT;
    memset(&schedule, 0, sizeof schedule); schedule.schedule_id = spec->schedule_id; schedule.job_definition_id = spec->job_type; schedule.schedule_type = (uint32_t)spec->type; schedule.enabled = 1; schedule.start_at = spec->first_fire_at; schedule.next_fire_at = spec->first_fire_at; schedule.max_occurrences = spec->max_occurrences; schedule.timezone_reference = (uint64_t)spec->interval; schedule.overlap_policy = spec->overlap_policy ? (uint32_t)spec->overlap_policy : (uint32_t)spec->interval_mode; schedule.misfire_policy = (uint32_t)spec->misfire_policy;
    if (spec->payload_size > UINT32_MAX - PAYLOAD_HEADER_SIZE) return JOBDB_ERR_LIMIT;
    payload_record = (uint8_t *)malloc(PAYLOAD_HEADER_SIZE + (size_t)spec->payload_size); if (!payload_record) return JOBDB_ERR_INTERNAL;
    memcpy(payload_record, &spec->job_type, 8); memcpy(payload_record + 8, &spec->payload_version, 4); memcpy(payload_record + 12, &spec->payload_size, 4); if (spec->payload_size) memcpy(payload_record + PAYLOAD_HEADER_SIZE, spec->payload, spec->payload_size);
    if (spec->type == JOBCORE_SCHEDULE_CRON) { int64_t next; size_t a = strlen(spec->timezone), b = strlen(spec->cron_expression); if (jobcore_cron_next_fire(spec->cron_expression, spec->timezone, spec->first_fire_at - 60, &next) != JOBDB_OK || a > SIZE_MAX - b - 2u) { free(payload_record); return JOBDB_ERR_INVALID_ARGUMENT; } cron_size = a + b + 2u; cron_record = (uint8_t *)malloc(cron_size); if (!cron_record) { free(payload_record); return JOBDB_ERR_INTERNAL; } memcpy(cron_record, spec->timezone, a + 1u); memcpy(cron_record + a + 1u, spec->cron_expression, b + 1u); schedule.next_fire_at = next; }
    result = jobdb_schedule_create_extra(c->db, &schedule, SCHEDULE_PAYLOAD_RECORD_TYPE, payload_record, PAYLOAD_HEADER_SIZE + spec->payload_size, spec->type == JOBCORE_SCHEDULE_CRON ? CRON_RECORD_TYPE : 0, cron_record, (uint32_t)cron_size);
    free(cron_record); free(payload_record); return result;
}

jobdb_result_t jobcore_schedule_get(jobcore_t *c, uint64_t id, jobdb_schedule_t *out) { if (!c) return JOBDB_ERR_INVALID_ARGUMENT; return jobdb_schedule_get(c->db, id, out); }
jobdb_result_t jobcore_schedule_update(jobcore_t *c, const jobdb_schedule_t *schedule, uint64_t expected_revision) { if (!c) return JOBDB_ERR_INVALID_ARGUMENT; return jobdb_schedule_update(c->db, schedule, expected_revision); }
jobdb_result_t jobcore_schedule_pause(jobcore_t *c, uint64_t id, uint64_t expected_revision) { if (!c) return JOBDB_ERR_INVALID_ARGUMENT; return jobdb_schedule_pause(c->db, id, expected_revision); }
jobdb_result_t jobcore_schedule_resume(jobcore_t *c, uint64_t id, uint64_t expected_revision) { if (!c) return JOBDB_ERR_INVALID_ARGUMENT; return jobdb_schedule_resume(c->db, id, expected_revision); }
jobdb_result_t jobcore_schedule_remove(jobcore_t *c, uint64_t id, uint64_t expected_revision) { if (!c) return JOBDB_ERR_INVALID_ARGUMENT; return jobdb_schedule_remove(c->db, id, expected_revision); }

static int workflow_state(jobcore_t *c, uint64_t id, jobdb_execution_state_t *state) { jobdb_execution_t e; if (jobdb_execution_get(c->db, id, &e) != JOBDB_OK) return 0; *state = e.state; return 1; }
static uint64_t workflow_node_record_id(uint64_t workflow_id, uint64_t node_id) { uint64_t h = workflow_id ^ UINT64_C(1469598103934665603); h ^= node_id; h *= UINT64_C(1099511628211); return h ? h : 1; }
static void workflow_progress(jobcore_t *c, uint64_t completed_id) {
    uint64_t *ids = NULL; size_t count = 0, i, j; workflow_record_t current = {0}, node; jobdb_record_t r = {0};
    if (!load_all_record_ids(c, WORKFLOW_NODE_RECORD_TYPE, &ids, &count)) return;
    for (i = 0; i < count; ++i) if (jobdb_record_get(c->db, WORKFLOW_NODE_RECORD_TYPE, ids[i], &r) == JOBDB_OK) { if (r.payload_size == sizeof current) { memcpy(&node, r.payload, sizeof node); if (node.execution_id == completed_id) current = node; } jobdb_record_free(&r); }
    if (!current.workflow_id) { free(ids); return; }
    for (i = 0; i < count; ++i) if (jobdb_record_get(c->db, WORKFLOW_NODE_RECORD_TYPE, ids[i], &r) == JOBDB_OK) {
        if (r.payload_size == sizeof node) { int ready = 1, failed = 0; memcpy(&node, r.payload, sizeof node); for (j = 0; j < node.dependency_count; ++j) { jobdb_execution_state_t state; if (!workflow_state(c, node.dependencies[j], &state) || (state != JOBDB_EXEC_DONE && state != JOBDB_EXEC_FAILED && state != JOBDB_EXEC_DEAD && state != JOBDB_EXEC_CANCELLED)) ready = 0; if (state == JOBDB_EXEC_FAILED || state == JOBDB_EXEC_DEAD || state == JOBDB_EXEC_CANCELLED) failed = 1; } if (node.workflow_id == current.workflow_id && node.dependency_count && ready && node.policy != JOBCORE_DEP_BLOCK) { uint64_t revision; if (failed && (node.policy == JOBCORE_DEP_CANCEL || node.policy == JOBCORE_DEP_FAIL_WORKFLOW)) (void)jobdb_execution_transition(c->db, node.execution_id, 1, JOBDB_EXEC_CANCELLED, &revision); else if (!failed || node.policy == JOBCORE_DEP_CONTINUE) (void)jobdb_execution_transition(c->db, node.execution_id, 1, JOBDB_EXEC_READY, &revision); } }
        jobdb_record_free(&r);
    }
    free(ids);
}

static void workflow_reconcile(jobcore_t *c) {
    uint64_t *ids = NULL; size_t count = 0, i;
    if (!load_all_record_ids(c, WORKFLOW_NODE_RECORD_TYPE, &ids, &count)) return;
    for (i = 0; i < count; ++i) {
        jobdb_record_t record = {0};
        workflow_record_t node;
        if (jobdb_record_get(c->db, WORKFLOW_NODE_RECORD_TYPE, ids[i], &record) == JOBDB_OK) {
            if (record.payload_size == sizeof node) { memcpy(&node, record.payload, sizeof node); workflow_progress(c, node.execution_id); }
            jobdb_record_free(&record);
        }
    }
    free(ids);
}

static int workflow_cycle_dfs(const jobcore_workflow_node_t *nodes, size_t count, size_t index, unsigned char *visiting, unsigned char *done) {
    size_t i, j;
    if (visiting[index]) return 1;
    if (done[index]) return 0;
    visiting[index] = 1;
    for (j = 0; j < nodes[index].dependency_count; ++j) {
        for (i = 0; i < count; ++i) if (nodes[i].node_id == nodes[index].dependencies[j]) {
            if (workflow_cycle_dfs(nodes, count, i, visiting, done)) return 1;
            break;
        }
    }
    visiting[index] = 0;
    done[index] = 1;
    return 0;
}

static int workflow_has_cycle(const jobcore_workflow_node_t *nodes, size_t count) {
    unsigned char visiting[128] = {0}, done[128] = {0};
    size_t i;
    for (i = 0; i < count; ++i) if (workflow_cycle_dfs(nodes, count, i, visiting, done)) return 1;
    return 0;
}

jobdb_result_t jobcore_workflow_submit(jobcore_t *c, uint64_t workflow_id, const jobcore_workflow_node_t *nodes, size_t count, jobcore_dependency_policy_t policy, int64_t now) {
    size_t i, j, k; uint64_t execution_ids[128], stats_revision=0; jobdb_stats_t stats; jobdb_record_t sr={0}; jobdb_tx_t *tx=NULL; int has_stats=0; jobdb_result_t result;
    if (!c || !workflow_id || !nodes || !count || count > 128 || policy < JOBCORE_DEP_BLOCK || policy > JOBCORE_DEP_FAIL_WORKFLOW) return JOBDB_ERR_INVALID_ARGUMENT;
    if (workflow_has_cycle(nodes, count)) return JOBDB_ERR_INVALID_ARGUMENT;
    for (i=0; i<count; ++i) { if (!nodes[i].node_id || !nodes[i].job_type || nodes[i].dependency_count>8 || (nodes[i].payload_size && !nodes[i].payload) || nodes[i].payload_size>UINT32_MAX-PAYLOAD_HEADER_SIZE) return JOBDB_ERR_INVALID_ARGUMENT; for (j=0; j<i; ++j) if (nodes[j].node_id==nodes[i].node_id) return JOBDB_ERR_ALREADY_EXISTS; for (j=0; j<nodes[i].dependency_count; ++j) { if (nodes[i].dependencies[j]==nodes[i].node_id) return JOBDB_ERR_INVALID_ARGUMENT; for (k=0;k<count;++k) if (nodes[k].node_id==nodes[i].dependencies[j]) break; if (k==count) return JOBDB_ERR_NOT_FOUND; } execution_ids[i]=allocate_execution_id(c); if (!execution_ids[i]) return JOBDB_ERR_LIMIT; }
    result=jobdb_get_stats(c->db,&stats); if (result!=JOBDB_OK) return result; if (count > UINT64_MAX - stats.submitted_total) return JOBDB_ERR_LIMIT; stats.submitted_total += count; result=jobdb_record_get(c->db,5,1,&sr); if (result==JOBDB_OK) { has_stats=1; stats_revision=sr.revision; jobdb_record_free(&sr); } else if (result!=JOBDB_ERR_NOT_FOUND) return result;
    result=jobdb_tx_begin(c->db,&tx); if (result!=JOBDB_OK) return result;
    for (i=0; i<count && result==JOBDB_OK; ++i) { jobdb_execution_t e={0}; uint8_t *p; e.execution_id=execution_ids[i]; e.job_definition_id=nodes[i].job_type; e.workflow_id=workflow_id; e.state=nodes[i].dependency_count?JOBDB_EXEC_BLOCKED:JOBDB_EXEC_READY; e.created_at=now; e.eligible_at=now; e.max_attempts=1; result=jobdb_tx_put_execution_create(tx,&e); if (result!=JOBDB_OK) break; p=(uint8_t*)malloc(PAYLOAD_HEADER_SIZE+(size_t)nodes[i].payload_size); if (!p) { result=JOBDB_ERR_INTERNAL; break; } memcpy(p,&nodes[i].job_type,8); memcpy(p+8,&nodes[i].payload_version,4); memcpy(p+12,&nodes[i].payload_size,4); if(nodes[i].payload_size) memcpy(p+PAYLOAD_HEADER_SIZE,nodes[i].payload,nodes[i].payload_size); result=jobdb_tx_put_create(tx,100,e.execution_id,p,PAYLOAD_HEADER_SIZE+nodes[i].payload_size); free(p); if(result!=JOBDB_OK) break; workflow_record_t wr={0}; wr.workflow_id=workflow_id; wr.execution_id=e.execution_id; wr.node_id=nodes[i].node_id; wr.policy=policy; wr.dependency_count=nodes[i].dependency_count; for(j=0;j<nodes[i].dependency_count;++j) for(k=0;k<count;++k) if(nodes[k].node_id==nodes[i].dependencies[j]) wr.dependencies[j]=execution_ids[k]; result=jobdb_tx_put_create(tx,104,workflow_node_record_id(workflow_id,nodes[i].node_id),&wr,sizeof wr); }
    if (result==JOBDB_OK) result=jobdb_tx_put_stats(tx,&stats,stats_revision,has_stats);
    if (result==JOBDB_OK) result=jobdb_tx_commit(tx); jobdb_tx_rollback(tx); return result;
 }

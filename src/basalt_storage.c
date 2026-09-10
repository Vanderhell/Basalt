#include "basalt_storage.h"
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#endif

struct basalt_storage {
    basalt_storage_vtable_v1 api;
    void *context;
    uint32_t references;
};
struct basalt_storage_tx { basalt_storage_t *storage; void *provider_tx; };

static int valid_vtable(const basalt_storage_vtable_v1 *v) {
    return v && v->abi_version == BASALT_STORAGE_ABI_VERSION &&
           v->struct_size >= offsetof(basalt_storage_vtable_v1, record_update) && v->provider_name && *v->provider_name &&
           v->retain && v->release && v->health && v->allocate_execution_id &&
           v->record_create && v->record_get && v->record_free && v->list_record_ids &&
           v->execution_enqueue && v->execution_enqueue_extra &&
           v->execution_enqueue_receipt && v->idempotency_get && v->execution_get &&
           v->execution_transition && v->execution_start && v->claim_next &&
           v->renew_lease && v->execution_complete && v->execution_finalize &&
           v->execution_park && v->execution_retry && v->schedule_create_extra &&
           v->schedule_get && v->schedule_update && v->schedule_pause &&
           v->schedule_resume && v->schedule_remove && v->schedule_try_fire_state &&
           v->get_stats && v->tx_begin && v->tx_put_create &&
           v->tx_put_execution_create && v->tx_put_stats && v->tx_commit && v->tx_rollback;
}

jobdb_result_t basalt_storage_create(const basalt_storage_vtable_v1 *v, void *context,
                                     basalt_storage_t **out) {
    basalt_storage_t *storage;
    if (!out || !context || !valid_vtable(v)) return JOBDB_ERR_INVALID_ARGUMENT;
    storage = (basalt_storage_t *)calloc(1, sizeof(*storage));
    if (!storage) return JOBDB_ERR_INTERNAL;
    memset(&storage->api,0,sizeof(storage->api)); memcpy(&storage->api, v, v->struct_size < sizeof(storage->api) ? v->struct_size : sizeof(storage->api)); storage->context = context; storage->references = 1;
    v->retain(context); *out = storage; return JOBDB_OK;
}

void basalt_storage_retain(basalt_storage_t *storage) {
    if (!storage) return;
#ifdef _WIN32
    (void)InterlockedIncrement((volatile LONG *)&storage->references);
#else
    (void)__atomic_add_fetch(&storage->references, 1u, __ATOMIC_SEQ_CST);
#endif
}

void basalt_storage_release(basalt_storage_t *storage) {
    uint32_t remaining;
    if (!storage) return;
#ifdef _WIN32
    remaining = (uint32_t)InterlockedDecrement((volatile LONG *)&storage->references);
#else
    remaining = __atomic_sub_fetch(&storage->references, 1u, __ATOMIC_SEQ_CST);
#endif
    if (!remaining) { storage->api.release(storage->context); free(storage); }
}

const char *basalt_storage_provider(const basalt_storage_t *storage) {
    return storage ? storage->api.provider_name : NULL;
}
uint64_t basalt_storage_capabilities(const basalt_storage_t *storage) {
    return storage ? storage->api.capabilities : 0;
}

static void embedded_retain(void *p) { (void)p; }
static void embedded_release(void *p) { (void)p; }
static jobdb_result_t embedded_health(void *p) { return jobdb_health((jobdb_t *)p); }
static jobdb_result_t embedded_utc_now(void *p, int64_t *out) { (void)p; (void)out; return JOBDB_ERR_INVALID_ARGUMENT; }
static jobdb_result_t embedded_allocate(void *p,uint64_t*out){return jobdb_allocate_execution_id((jobdb_t*)p,out);}
static jobdb_result_t embedded_record_create(void*p,uint32_t t,uint64_t i,const void*b,uint32_t n){return jobdb_record_create((jobdb_t*)p,t,i,b,n);}
static jobdb_result_t embedded_record_get(void*p,uint32_t t,uint64_t i,jobdb_record_t*r){return jobdb_record_get((jobdb_t*)p,t,i,r);}
static jobdb_result_t embedded_record_update(void*p,uint32_t t,uint64_t i,uint64_t r,const void*b,uint32_t n,uint64_t*o){return jobdb_record_update((jobdb_t*)p,t,i,r,b,n,o);}
static jobdb_result_t embedded_record_delete(void*p,uint32_t t,uint64_t i,uint64_t r){return jobdb_record_delete((jobdb_t*)p,t,i,r);}
static void embedded_record_free(void*p,jobdb_record_t*r){(void)p;jobdb_record_free(r);}
static jobdb_result_t embedded_list(void*p,uint32_t t,uint64_t*i,size_t n,size_t*c){return jobdb_list_record_ids((jobdb_t*)p,t,i,n,c);}
static jobdb_result_t embedded_enqueue(void*p,const jobdb_execution_t*e,uint32_t t,const void*b,uint32_t n){return jobdb_execution_enqueue((jobdb_t*)p,e,t,b,n);}
static jobdb_result_t embedded_enqueue_extra(void*p,const jobdb_execution_t*e,uint32_t t,const void*b,uint32_t n,uint32_t x,const void*y,uint32_t z){return jobdb_execution_enqueue_extra((jobdb_t*)p,e,t,b,n,x,y,z);}
static jobdb_result_t embedded_enqueue_receipt(void*p,const jobdb_execution_t*e,uint32_t t,const void*b,uint32_t n,uint64_t i,const void*r,uint32_t z){return jobdb_execution_enqueue_receipt((jobdb_t*)p,e,t,b,n,i,r,z);}
static jobdb_result_t embedded_receipt(void*p,uint64_t i,void*b,uint32_t n,uint32_t*s){return jobdb_idempotency_get((jobdb_t*)p,i,b,n,s);}
static jobdb_result_t embedded_execution_get(void*p,uint64_t i,jobdb_execution_t*e){return jobdb_execution_get((jobdb_t*)p,i,e);}
static jobdb_result_t embedded_transition(void*p,uint64_t i,uint64_t r,jobdb_execution_state_t s,uint64_t*o){return jobdb_execution_transition((jobdb_t*)p,i,r,s,o);}
static jobdb_result_t embedded_start(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t n,uint64_t*r){return jobdb_execution_start((jobdb_t*)p,i,w,f,n,r);}
static jobdb_result_t embedded_claim(void*p,const jobdb_worker_id_t*w,int64_t n,int64_t l,jobdb_execution_t*e){return jobdb_claim_next((jobdb_t*)p,w,n,l,e);}
static jobdb_result_t embedded_renew(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t x){return jobdb_renew_lease((jobdb_t*)p,i,w,f,x);}
static jobdb_result_t embedded_complete(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f){return jobdb_execution_complete((jobdb_t*)p,i,w,f);}
static jobdb_result_t embedded_finalize(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,jobdb_execution_state_t s,int32_t r,int32_t e){return jobdb_execution_finalize((jobdb_t*)p,i,w,f,s,r,e);}
static jobdb_result_t embedded_park(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,uint64_t*r){return jobdb_execution_park((jobdb_t*)p,i,w,f,r);}
static jobdb_result_t embedded_retry(void*p,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t e,uint64_t*r){return jobdb_execution_retry((jobdb_t*)p,i,w,f,e,r);}
static jobdb_result_t embedded_schedule_create(void*p,const jobdb_schedule_t*s,uint32_t a,const void*b,uint32_t c,uint32_t d,const void*e,uint32_t f){return jobdb_schedule_create_extra((jobdb_t*)p,s,a,b,c,d,e,f);}
static jobdb_result_t embedded_schedule_get(void*p,uint64_t i,jobdb_schedule_t*s){return jobdb_schedule_get((jobdb_t*)p,i,s);}
static jobdb_result_t embedded_schedule_update(void*p,const jobdb_schedule_t*s,uint64_t r){return jobdb_schedule_update((jobdb_t*)p,s,r);}
static jobdb_result_t embedded_schedule_pause(void*p,uint64_t i,uint64_t r){return jobdb_schedule_pause((jobdb_t*)p,i,r);}
static jobdb_result_t embedded_schedule_resume(void*p,uint64_t i,uint64_t r){return jobdb_schedule_resume((jobdb_t*)p,i,r);}
static jobdb_result_t embedded_schedule_remove(void*p,uint64_t i,uint64_t r){return jobdb_schedule_remove((jobdb_t*)p,i,r);}
static jobdb_result_t embedded_schedule_fire(void*p,uint64_t i,uint64_t r,int64_t x,uint64_t e,int64_t f,int64_t n,jobdb_execution_state_t s){return jobdb_schedule_try_fire_state((jobdb_t*)p,i,r,x,e,f,n,s);}
static jobdb_result_t embedded_stats(void*p,jobdb_stats_t*s){return jobdb_get_stats((jobdb_t*)p,s);}
static jobdb_result_t embedded_tx_begin(void*p,void**t){return jobdb_tx_begin((jobdb_t*)p,(jobdb_tx_t**)t);}
static jobdb_result_t embedded_tx_create(void*t,uint32_t y,uint64_t i,const void*p,uint32_t n){return jobdb_tx_put_create((jobdb_tx_t*)t,y,i,p,n);}
static jobdb_result_t embedded_tx_execution(void*t,const jobdb_execution_t*e){return jobdb_tx_put_execution_create((jobdb_tx_t*)t,e);}
static jobdb_result_t embedded_tx_stats(void*t,const jobdb_stats_t*s,uint64_t r,int e){return jobdb_tx_put_stats((jobdb_tx_t*)t,s,r,e);}
static jobdb_result_t embedded_tx_commit(void*t){return jobdb_tx_commit((jobdb_tx_t*)t);}
static void embedded_tx_rollback(void*t){jobdb_tx_rollback((jobdb_tx_t*)t);}

jobdb_result_t basalt_storage_from_jobdb(jobdb_t *db, basalt_storage_t **out) {
    basalt_storage_vtable_v1 v;
    if (!db || !out) return JOBDB_ERR_INVALID_ARGUMENT;
    memset(&v,0,sizeof(v)); v.abi_version=BASALT_STORAGE_ABI_VERSION; v.struct_size=(uint32_t)sizeof(v);
    v.capabilities=BASALT_STORAGE_CAP_ATOMIC_DOMAIN_OPS|BASALT_STORAGE_CAP_BOUNDED_LISTS; v.provider_name="BasaltDB";
    v.retain=embedded_retain;v.release=embedded_release;v.health=embedded_health;v.utc_now=embedded_utc_now;v.allocate_execution_id=embedded_allocate;
    v.record_create=embedded_record_create;v.record_get=embedded_record_get;v.record_update=embedded_record_update;v.record_delete=embedded_record_delete;v.record_free=embedded_record_free;v.list_record_ids=embedded_list;
    v.execution_enqueue=embedded_enqueue;v.execution_enqueue_extra=embedded_enqueue_extra;v.execution_enqueue_receipt=embedded_enqueue_receipt;v.idempotency_get=embedded_receipt;
    v.execution_get=embedded_execution_get;v.execution_transition=embedded_transition;v.execution_start=embedded_start;v.claim_next=embedded_claim;v.renew_lease=embedded_renew;
    v.execution_complete=embedded_complete;v.execution_finalize=embedded_finalize;v.execution_park=embedded_park;v.execution_retry=embedded_retry;
    v.schedule_create_extra=embedded_schedule_create;v.schedule_get=embedded_schedule_get;v.schedule_update=embedded_schedule_update;v.schedule_pause=embedded_schedule_pause;v.schedule_resume=embedded_schedule_resume;v.schedule_remove=embedded_schedule_remove;v.schedule_try_fire_state=embedded_schedule_fire;
    v.get_stats=embedded_stats;v.tx_begin=embedded_tx_begin;v.tx_put_create=embedded_tx_create;v.tx_put_execution_create=embedded_tx_execution;v.tx_put_stats=embedded_tx_stats;v.tx_commit=embedded_tx_commit;v.tx_rollback=embedded_tx_rollback;
    return basalt_storage_create(&v,db,out);
}

#define S_OR_INVALID(s) if(!(s))return JOBDB_ERR_INVALID_ARGUMENT
jobdb_result_t basalt_storage_health(basalt_storage_t*s){S_OR_INVALID(s);return s->api.health(s->context);}
jobdb_result_t basalt_storage_utc_now(basalt_storage_t*s,int64_t*o){S_OR_INVALID(s);return s->api.utc_now?s->api.utc_now(s->context,o):JOBDB_ERR_INVALID_ARGUMENT;}
jobdb_result_t basalt_storage_allocate_execution_id(basalt_storage_t*s,uint64_t*o){S_OR_INVALID(s);return s->api.allocate_execution_id(s->context,o);}
jobdb_result_t basalt_storage_record_create(basalt_storage_t*s,uint32_t t,uint64_t i,const void*p,uint32_t n){S_OR_INVALID(s);return s->api.record_create(s->context,t,i,p,n);}
jobdb_result_t basalt_storage_record_get(basalt_storage_t*s,uint32_t t,uint64_t i,jobdb_record_t*r){S_OR_INVALID(s);return s->api.record_get(s->context,t,i,r);}
jobdb_result_t basalt_storage_record_update(basalt_storage_t*s,uint32_t t,uint64_t i,uint64_t r,const void*p,uint32_t n,uint64_t*o){S_OR_INVALID(s);return s->api.record_update?s->api.record_update(s->context,t,i,r,p,n,o):JOBDB_ERR_UNSUPPORTED;}
jobdb_result_t basalt_storage_record_delete(basalt_storage_t*s,uint32_t t,uint64_t i,uint64_t r){S_OR_INVALID(s);return s->api.record_delete?s->api.record_delete(s->context,t,i,r):JOBDB_ERR_UNSUPPORTED;}
void basalt_storage_record_free(basalt_storage_t*s,jobdb_record_t*r){if(s&&r)s->api.record_free(s->context,r);}
jobdb_result_t basalt_storage_list_record_ids(basalt_storage_t*s,uint32_t t,uint64_t*i,size_t n,size_t*c){S_OR_INVALID(s);return s->api.list_record_ids(s->context,t,i,n,c);}
jobdb_result_t basalt_storage_execution_enqueue(basalt_storage_t*s,const jobdb_execution_t*e,uint32_t t,const void*p,uint32_t n){S_OR_INVALID(s);return s->api.execution_enqueue(s->context,e,t,p,n);}
jobdb_result_t basalt_storage_execution_enqueue_extra(basalt_storage_t*s,const jobdb_execution_t*e,uint32_t t,const void*p,uint32_t n,uint32_t x,const void*y,uint32_t z){S_OR_INVALID(s);return s->api.execution_enqueue_extra(s->context,e,t,p,n,x,y,z);}
jobdb_result_t basalt_storage_execution_enqueue_receipt(basalt_storage_t*s,const jobdb_execution_t*e,uint32_t t,const void*p,uint32_t n,uint64_t i,const void*r,uint32_t z){S_OR_INVALID(s);return s->api.execution_enqueue_receipt(s->context,e,t,p,n,i,r,z);}
jobdb_result_t basalt_storage_idempotency_get(basalt_storage_t*s,uint64_t i,void*p,uint32_t n,uint32_t*o){S_OR_INVALID(s);return s->api.idempotency_get(s->context,i,p,n,o);}
jobdb_result_t basalt_storage_execution_get(basalt_storage_t*s,uint64_t i,jobdb_execution_t*e){S_OR_INVALID(s);return s->api.execution_get(s->context,i,e);}
jobdb_result_t basalt_storage_execution_transition(basalt_storage_t*s,uint64_t i,uint64_t r,jobdb_execution_state_t t,uint64_t*o){S_OR_INVALID(s);return s->api.execution_transition(s->context,i,r,t,o);}
jobdb_result_t basalt_storage_execution_start(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t n,uint64_t*r){S_OR_INVALID(s);return s->api.execution_start(s->context,i,w,f,n,r);}
jobdb_result_t basalt_storage_claim_next(basalt_storage_t*s,const jobdb_worker_id_t*w,int64_t n,int64_t l,jobdb_execution_t*e){S_OR_INVALID(s);return s->api.claim_next(s->context,w,n,l,e);}
jobdb_result_t basalt_storage_renew_lease(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t e){S_OR_INVALID(s);return s->api.renew_lease(s->context,i,w,f,e);}
jobdb_result_t basalt_storage_execution_complete(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f){S_OR_INVALID(s);return s->api.execution_complete(s->context,i,w,f);}
jobdb_result_t basalt_storage_execution_finalize(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,jobdb_execution_state_t t,int32_t r,int32_t e){S_OR_INVALID(s);return s->api.execution_finalize(s->context,i,w,f,t,r,e);}
jobdb_result_t basalt_storage_execution_park(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,uint64_t*r){S_OR_INVALID(s);return s->api.execution_park(s->context,i,w,f,r);}
jobdb_result_t basalt_storage_execution_retry(basalt_storage_t*s,uint64_t i,const jobdb_worker_id_t*w,uint64_t f,int64_t e,uint64_t*r){S_OR_INVALID(s);return s->api.execution_retry(s->context,i,w,f,e,r);}
jobdb_result_t basalt_storage_schedule_create_extra(basalt_storage_t*s,const jobdb_schedule_t*a,uint32_t b,const void*c,uint32_t d,uint32_t e,const void*f,uint32_t g){S_OR_INVALID(s);return s->api.schedule_create_extra(s->context,a,b,c,d,e,f,g);}
jobdb_result_t basalt_storage_schedule_get(basalt_storage_t*s,uint64_t i,jobdb_schedule_t*o){S_OR_INVALID(s);return s->api.schedule_get(s->context,i,o);}
jobdb_result_t basalt_storage_schedule_update(basalt_storage_t*s,const jobdb_schedule_t*v,uint64_t r){S_OR_INVALID(s);return s->api.schedule_update(s->context,v,r);}
jobdb_result_t basalt_storage_schedule_pause(basalt_storage_t*s,uint64_t i,uint64_t r){S_OR_INVALID(s);return s->api.schedule_pause(s->context,i,r);}
jobdb_result_t basalt_storage_schedule_resume(basalt_storage_t*s,uint64_t i,uint64_t r){S_OR_INVALID(s);return s->api.schedule_resume(s->context,i,r);}
jobdb_result_t basalt_storage_schedule_remove(basalt_storage_t*s,uint64_t i,uint64_t r){S_OR_INVALID(s);return s->api.schedule_remove(s->context,i,r);}
jobdb_result_t basalt_storage_schedule_try_fire_state(basalt_storage_t*s,uint64_t i,uint64_t r,int64_t x,uint64_t e,int64_t f,int64_t n,jobdb_execution_state_t t){S_OR_INVALID(s);return s->api.schedule_try_fire_state(s->context,i,r,x,e,f,n,t);}
jobdb_result_t basalt_storage_get_stats(basalt_storage_t*s,jobdb_stats_t*o){S_OR_INVALID(s);return s->api.get_stats(s->context,o);}
jobdb_result_t basalt_storage_tx_begin(basalt_storage_t*s,basalt_storage_tx_t**o){void*p=NULL;basalt_storage_tx_t*t;jobdb_result_t r;S_OR_INVALID(s);if(!o)return JOBDB_ERR_INVALID_ARGUMENT;r=s->api.tx_begin(s->context,&p);if(r!=JOBDB_OK)return r;t=(basalt_storage_tx_t*)malloc(sizeof(*t));if(!t){s->api.tx_rollback(p);return JOBDB_ERR_INTERNAL;}t->storage=s;t->provider_tx=p;basalt_storage_retain(s);*o=t;return JOBDB_OK;}
jobdb_result_t basalt_storage_tx_put_create(basalt_storage_tx_t*t,uint32_t y,uint64_t i,const void*p,uint32_t n){return t?t->storage->api.tx_put_create(t->provider_tx,y,i,p,n):JOBDB_ERR_INVALID_TRANSACTION;}
jobdb_result_t basalt_storage_tx_put_execution_create(basalt_storage_tx_t*t,const jobdb_execution_t*e){return t?t->storage->api.tx_put_execution_create(t->provider_tx,e):JOBDB_ERR_INVALID_TRANSACTION;}
jobdb_result_t basalt_storage_tx_put_stats(basalt_storage_tx_t*t,const jobdb_stats_t*s,uint64_t r,int e){return t?t->storage->api.tx_put_stats(t->provider_tx,s,r,e):JOBDB_ERR_INVALID_TRANSACTION;}
jobdb_result_t basalt_storage_tx_commit(basalt_storage_tx_t*t){if(!t||!t->provider_tx)return JOBDB_ERR_INVALID_TRANSACTION;return t->storage->api.tx_commit(t->provider_tx);}
void basalt_storage_tx_rollback(basalt_storage_tx_t*t){basalt_storage_t*s;if(!t)return;s=t->storage;s->api.tx_rollback(t->provider_tx);free(t);basalt_storage_release(s);}

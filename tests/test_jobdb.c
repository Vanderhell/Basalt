#include "jobdb.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <direct.h>
#include <process.h>
#include <io.h>
#define unlink _unlink
#else
#include <unistd.h>
#endif
static void rmdb(void) {
#ifdef _WIN32
    { struct _finddata_t fd; intptr_t h = _findfirst("jobdb-test/*", &fd); if (h != -1) { do { char p[128]; if (strcmp(fd.name, ".") && strcmp(fd.name, "..")) { sprintf(p, "jobdb-test/%s", fd.name); remove(p); } } while (_findnext(h, &fd) == 0); _findclose(h); } }
#endif
    remove("jobdb-test/manifest.0");
    remove("jobdb-test/manifest.1");
    remove("jobdb-test/wal.0");
    remove("jobdb-test/record.20.99");
    remove("jobdb-test/record.20.99.tmp");
    remove("jobdb-test/record.3.700");
    remove("jobdb-test/record.3.700.tmp");
    remove("jobdb-test/record.3.701");
    remove("jobdb-test/record.3.701.tmp");
    remove("jobdb-test/record.3.702");
    remove("jobdb-test/record.3.702.tmp");
    remove("jobdb-test/record.4.901");
    remove("jobdb-test/record.4.901.tmp");
    remove("jobdb-test/record.4.910"); remove("jobdb-test/record.4.910.tmp");
    remove("jobdb-test/record.4.911"); remove("jobdb-test/record.4.911.tmp");
    remove("jobdb-test/record.4.912"); remove("jobdb-test/record.4.912.tmp");
    remove("jobdb-test/record.5.1");
    remove("jobdb-test/record.5.1.tmp");
    remove("jobdb-test/record.6.999"); remove("jobdb-test/record.6.999.tmp");
    remove("jobdb-test/record.7.77"); remove("jobdb-test/record.7.77.tmp");
    remove("jobdb-test/record.7.78"); remove("jobdb-test/record.7.78.tmp");
    remove("jobdb-test/record.9.909"); remove("jobdb-test/record.9.909.tmp");
    remove("jobdb-test/record.2.801");
    remove("jobdb-test/record.2.801.tmp");
    remove("jobdb-test/record.3.801");
    remove("jobdb-test/record.3.801.tmp");
    remove("jobdb-test/coordination.lock");
#ifdef _WIN32
    _rmdir("jobdb-test");
#else
    rmdir("jobdb-test");
#endif
}
static void raw_manifest(unsigned slot, const unsigned char *data, size_t size) {
    char path[64];
    FILE *f;
    sprintf(path, "jobdb-test/manifest.%u", slot);
    f = fopen(path, "wb");
    assert(f != NULL);
    assert(fwrite(data, 1, size, f) == size);
    assert(fclose(f) == 0);
}
static void raw_file(const char *path, const unsigned char *data, size_t size) {
    FILE *f = fopen(path, "wb");
    assert(f != NULL);
    assert(fwrite(data, 1, size, f) == size);
    assert(fclose(f) == 0);
}
static void fresh(void) {
    jobdb_t *db = NULL;
    rmdb();
    assert(jobdb_create("jobdb-test", &db) == JOBDB_OK);
    jobdb_close(db);
}
int main(int argc, char **argv) {
    jobdb_t *d = NULL;
    unsigned char manifest[64];
    FILE *f;

#ifdef _WIN32
    if (argc > 1 && strcmp(argv[1], "lock-child") == 0) {
        jobdb_lock_t *child_lock = NULL;
        assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
        assert(jobdb_lock_acquire(d, 50, &child_lock) == JOBDB_ERR_TIMEOUT);
        jobdb_close(d);
        return 0;
    }
    if (argc > 2 && strcmp(argv[1], "writer-child") == 0) {
        jobdb_tx_t *tx = NULL; unsigned long id = strtoul(argv[2], NULL, 10); unsigned char value = (unsigned char)id;
        assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
        assert(jobdb_tx_begin(d, &tx) == JOBDB_OK); assert(jobdb_tx_put(tx, 8, 1000 + id, &value, 1) == JOBDB_OK); { jobdb_result_t cr; do { cr=jobdb_tx_commit(tx); if(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY) { jobdb_tx_rollback(tx); tx=NULL; Sleep(20); assert(jobdb_tx_begin(d,&tx)==JOBDB_OK); assert(jobdb_tx_put(tx,8,1000+id,&value,1)==JOBDB_OK); } } while(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY); assert(cr==JOBDB_OK); } jobdb_tx_rollback(tx);
        jobdb_close(d); return 0;
    }
    if (argc > 1 && strcmp(argv[1], "reader-child") == 0) {
        assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
        for (unsigned i=0; i<8; ++i) assert(jobdb_verify("jobdb-test") == JOBDB_OK);
        jobdb_close(d); return 0;
    }
#endif

    rmdb();
    assert(jobdb_create(NULL, &d) == JOBDB_ERR_INVALID_ARGUMENT);
    assert(jobdb_open("", &d) == JOBDB_ERR_INVALID_ARGUMENT);
    assert(jobdb_create("jobdb-test", &d) == JOBDB_OK);
    assert(d != NULL);
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_test_fail_next(d,JOBDB_FAILURE_CHECKPOINT_BEFORE_MANIFEST); assert(jobdb_checkpoint(d)==JOBDB_ERR_IO); jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); jobdb_test_fail_next(d,JOBDB_FAILURE_CHECKPOINT_AFTER_MANIFEST); assert(jobdb_checkpoint(d)==JOBDB_ERR_IO); jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); { const unsigned char v=1; assert(jobdb_record_create(d,9,909,&v,1)==JOBDB_OK); } jobdb_close(d); raw_file("jobdb-test/record.9.909",(const unsigned char*)"bad",3); assert(jobdb_verify("jobdb-test")==JOBDB_ERR_CORRUPT);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
#ifdef _WIN32
     if (getenv("JOBDB_SKIP_MP") == NULL) { intptr_t children[13]; char id_text[10][16]; char *args[4]; args[0]=argv[0]; args[3]=NULL;
       args[1]="reader-child"; args[2]=NULL; for(unsigned i=0;i<3;i++){children[i]=_spawnv(_P_NOWAIT,argv[0],(const char * const*)args);assert(children[i]!=-1);}
       args[1]="writer-child"; for(unsigned i=0;i<10;i++){sprintf(id_text[i],"%u",i+1);args[2]=id_text[i];children[i+3]=_spawnv(_P_NOWAIT,argv[0],(const char * const*)args);assert(children[i+3]!=-1);}
       for(unsigned i=0;i<13;i++){int status=0;assert(_cwait(&status,children[i],0)!=-1);assert(status==0);}
       { size_t found=0;uint64_t ids[16];assert(jobdb_list_record_ids(d,8,ids,16,&found)==JOBDB_OK&&found==10); }
     }
#endif
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK);
     { jobdb_tx_t *tx=NULL; jobdb_record_t rr; const unsigned char z=43; assert(jobdb_tx_begin(d,&tx)==JOBDB_OK); assert(jobdb_tx_put(tx,7,78,&z,1)==JOBDB_OK); jobdb_test_fail_next(d,JOBDB_FAILURE_AFTER_COMMIT); assert(jobdb_tx_commit(tx)==JOBDB_ERR_IO); jobdb_tx_rollback(tx); jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); assert(jobdb_record_get(d,7,78,&rr)==JOBDB_OK && rr.payload[0]==43); jobdb_record_free(&rr); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_tx_t *tx=NULL; jobdb_record_t rr; const unsigned char z=42; assert(jobdb_tx_begin(d,&tx)==JOBDB_OK); assert(jobdb_tx_put(tx,7,77,&z,1)==JOBDB_OK); assert(jobdb_tx_commit(tx)==JOBDB_OK); jobdb_tx_rollback(tx); jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); assert(jobdb_record_get(d,7,77,&rr)==JOBDB_OK && rr.payload_size==1 && rr.payload[0]==42); jobdb_record_free(&rr); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_tx_t *tx=NULL; const unsigned char q=1; assert(jobdb_tx_begin(d,&tx)==JOBDB_OK); assert(jobdb_tx_put(tx,6,998,&q,1)==JOBDB_OK); jobdb_test_fail_next(d,JOBDB_FAILURE_DISK_FULL); assert(jobdb_tx_commit(tx)==JOBDB_ERR_IO); jobdb_tx_rollback(tx); jobdb_test_fail_next(d,JOBDB_FAILURE_DISK_FULL); assert(jobdb_record_create(d,6,999,&q,1)==JOBDB_ERR_IO); assert(jobdb_verify("jobdb-test")==JOBDB_OK); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); { jobdb_result_t cp=jobdb_checkpoint(d); if(cp!=JOBDB_OK) fprintf(stderr,"checkpoint: %s\n",jobdb_result_string(cp)); assert(cp==JOBDB_OK); } jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_ledger_entry_t a={0},b={0},c={0}; jobdb_retention_t rt={0,2,0}; uint32_t deleted=0;
       a.execution_id=910; a.final_state=JOBDB_EXEC_DONE; a.finished_at=10; b.execution_id=911; b.final_state=JOBDB_EXEC_DONE; b.finished_at=20; c.execution_id=912; c.final_state=JOBDB_EXEC_DONE; c.finished_at=30;
       assert(jobdb_ledger_append(d,&a)==JOBDB_OK && jobdb_ledger_append(d,&b)==JOBDB_OK && jobdb_ledger_append(d,&c)==JOBDB_OK);
       assert(jobdb_apply_retention(d,30,&rt,&deleted)==JOBDB_OK && deleted==1); assert(jobdb_ledger_get(d,910,&a)==JOBDB_ERR_NOT_FOUND); assert(jobdb_ledger_get(d,912,&c)==JOBDB_OK); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_stats_t st={0}, st2; jobdb_ledger_entry_t le={0}, got;
       assert(jobdb_get_stats(d,&st)==JOBDB_OK && st.completed_total==0);
       le.execution_id=901; le.job_definition_id=55; le.final_state=JOBDB_EXEC_DONE; le.attempt=1; le.duration=7;
       assert(jobdb_ledger_append(d,&le)==JOBDB_OK); assert(jobdb_ledger_get(d,901,&got)==JOBDB_OK && got.duration==7 && got.final_state==JOBDB_EXEC_DONE);
       st.completed_total=1; assert(jobdb_record_create(d,5,1,&st,sizeof st)==JOBDB_OK); assert(jobdb_get_stats(d,&st2)==JOBDB_OK && st2.completed_total==1);
       jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); assert(jobdb_get_stats(d,&st2)==JOBDB_OK && st2.completed_total==1); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_schedule_t s={0}, got; jobdb_execution_t e; s.schedule_id=801; s.job_definition_id=55; s.enabled=1; s.next_fire_at=100;
       assert(jobdb_schedule_create(d,&s)==JOBDB_OK); assert(jobdb_schedule_try_fire(d,801,9,100,801,100,200)==JOBDB_ERR_CONFLICT);
       assert(jobdb_schedule_try_fire(d,801,1,100,801,100,200)==JOBDB_OK); assert(jobdb_schedule_try_fire(d,801,1,100,802,100,200)==JOBDB_ERR_CONFLICT);
       assert(jobdb_schedule_get(d,801,&got)==JOBDB_OK && got.last_fire_at==100 && got.next_fire_at==200 && got.occurrence_count==1 && got.revision==2);
       assert(jobdb_execution_get(d,801,&e)==JOBDB_OK && e.schedule_id==801 && e.state==JOBDB_EXEC_READY); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_execution_t e={0}, claimed; jobdb_worker_id_t w={{1}}; jobdb_worker_id_t other={{2}};
       e.execution_id=701; e.job_definition_id=12; e.state=JOBDB_EXEC_READY; e.eligible_at=10;
       assert(jobdb_execution_create(d,&e)==JOBDB_OK); assert(jobdb_claim_next(d,&w,10,100,&claimed)==JOBDB_OK); assert(claimed.state==JOBDB_EXEC_LEASED && claimed.fencing_token==1);
       assert(jobdb_renew_lease(d,701,&other,1,200)==JOBDB_ERR_STALE_LEASE); assert(jobdb_renew_lease(d,701,&w,1,200)==JOBDB_OK);
       assert(jobdb_execution_transition(d,701,claimed.revision,JOBDB_EXEC_RUNNING,NULL)==JOBDB_ERR_CONFLICT); assert(jobdb_execution_transition(d,701,claimed.revision+1,JOBDB_EXEC_RUNNING,NULL)==JOBDB_OK);
       assert(jobdb_execution_finalize(d,701,&other,1,JOBDB_EXEC_DONE,0,0)==JOBDB_ERR_STALE_LEASE); assert(jobdb_execution_complete(d,701,&w,1)==JOBDB_OK); assert(jobdb_execution_retry(d,701,&w,1,20,NULL)==JOBDB_ERR_STALE_LEASE); { jobdb_ledger_entry_t le; jobdb_stats_t st; assert(jobdb_ledger_get(d,701,&le)==JOBDB_OK && le.final_state==JOBDB_EXEC_DONE); assert(jobdb_get_stats(d,&st)==JOBDB_OK && st.completed_total==1); } }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_execution_t e={0}, got; jobdb_worker_id_t w={{3}}; uint32_t count=0;
       e.execution_id=702; e.state=JOBDB_EXEC_READY; assert(jobdb_execution_create(d,&e)==JOBDB_OK); assert(jobdb_claim_next(d,&w,1,10,&got)==JOBDB_OK);
       assert(jobdb_reclaim_expired(d,10,&count)==JOBDB_OK && count==0); assert(jobdb_reclaim_expired(d,11,&count)==JOBDB_OK && count==1);
       assert(jobdb_execution_get(d,702,&got)==JOBDB_OK && got.state==JOBDB_EXEC_READY && got.fencing_token==1 && got.worker_instance_id[0]==0); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_execution_t e = {0}, got; uint64_t rev = 0;
       e.execution_id=700; e.job_definition_id=11; e.state=JOBDB_EXEC_CREATED; e.max_attempts=3;
       assert(jobdb_execution_create(d,&e)==JOBDB_OK); assert(jobdb_execution_get(d,700,&got)==JOBDB_OK); assert(got.revision==1 && got.job_definition_id==11);
       assert(jobdb_execution_transition(d,700,9,JOBDB_EXEC_READY,&rev)==JOBDB_ERR_CONFLICT);
       assert(jobdb_execution_transition(d,700,1,JOBDB_EXEC_READY,&rev)==JOBDB_OK && rev==2);
       assert(jobdb_execution_transition(d,700,2,JOBDB_EXEC_DONE,&rev)==JOBDB_ERR_INVALID_STATE);
       jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK); assert(jobdb_execution_get(d,700,&got)==JOBDB_OK && got.state==JOBDB_EXEC_READY && got.revision==2); }
     jobdb_close(d);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_CREATED, JOBDB_EXEC_READY) == JOBDB_OK);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_READY, JOBDB_EXEC_LEASED) == JOBDB_OK);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_LEASED, JOBDB_EXEC_RUNNING) == JOBDB_OK);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_RUNNING, JOBDB_EXEC_DONE) == JOBDB_OK);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_DONE, JOBDB_EXEC_RUNNING) == JOBDB_ERR_INVALID_STATE);
     assert(jobdb_execution_validate_transition(JOBDB_EXEC_DEAD, JOBDB_EXEC_READY) == JOBDB_ERR_INVALID_STATE);
     assert(jobdb_execution_validate_transition((jobdb_execution_state_t)99, JOBDB_EXEC_READY) == JOBDB_ERR_INVALID_STATE);
     assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_lock_t *a = NULL, *b = NULL; assert(jobdb_lock_acquire(d, 0, &a) == JOBDB_OK); assert(jobdb_lock_acquire(d, 0, &b) == JOBDB_ERR_BUSY); assert(jobdb_lock_acquire(d, 20, &b) == JOBDB_ERR_TIMEOUT); jobdb_lock_release(a); assert(jobdb_lock_acquire(d, 0, &b) == JOBDB_OK); jobdb_lock_release(b); }
     jobdb_close(d);
    assert(jobdb_create("jobdb-test", &d) == JOBDB_ERR_ALREADY_EXISTS);
     assert(jobdb_open("missing", &d) == JOBDB_ERR_CORRUPT);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     { jobdb_lock_t *a = NULL, *b = NULL; assert(jobdb_lock_acquire(d, 0, &a) == JOBDB_OK); assert(jobdb_lock_acquire(d, 0, &b) == JOBDB_ERR_BUSY); assert(jobdb_lock_acquire(d, 20, &b) == JOBDB_ERR_TIMEOUT); jobdb_lock_release(a); assert(jobdb_lock_acquire(d, 0, &b) == JOBDB_OK); jobdb_lock_release(b); }
     jobdb_close(d);
     fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
     jobdb_close(d);

     fresh();
    f = fopen("jobdb-test/manifest.0", "rb"); assert(f != NULL);
    assert(fread(manifest, 1, sizeof manifest, f) == sizeof manifest); fclose(f);
    manifest[56] ^= 1; raw_manifest(0, manifest, sizeof manifest);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);

    fresh(); raw_manifest(1, (const unsigned char *)"garbage", 7);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh(); raw_manifest(0, (const unsigned char *)"garbage", 7);
    raw_manifest(1, (const unsigned char *)"garbage", 7);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_ERR_CORRUPT);

    fresh(); raw_manifest(0, (const unsigned char *)"x", 1);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh();
    f = fopen("jobdb-test/manifest.0", "rb"); assert(f != NULL);
    assert(fread(manifest, 1, sizeof manifest, f) == sizeof manifest); fclose(f);
    manifest[8] = 2; raw_manifest(0, manifest, sizeof manifest);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh();
    f = fopen("jobdb-test/manifest.0", "rb"); assert(f != NULL);
    assert(fread(manifest, 1, sizeof manifest, f) == sizeof manifest); fclose(f);
    manifest[20] ^= 0x80; raw_manifest(0, manifest, sizeof manifest);
    raw_manifest(1, manifest, sizeof manifest);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_ERR_CORRUPT);
    fresh();
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    {
        jobdb_tx_t *tx = NULL;
        const unsigned char payload[] = {1, 2, 3, 4};
        assert(jobdb_tx_begin(d, &tx) == JOBDB_OK);
        assert(jobdb_tx_put(tx, 10, 42, payload, sizeof payload) == JOBDB_OK);
        assert(jobdb_tx_delete(tx, 10, 42) == JOBDB_OK);
        assert(jobdb_tx_put(tx, 10, 43, NULL, 1) == JOBDB_ERR_INVALID_TRANSACTION);
        assert(jobdb_tx_commit(tx) == JOBDB_OK);
        assert(jobdb_tx_commit(tx) == JOBDB_ERR_INVALID_TRANSACTION);
        jobdb_tx_rollback(tx);
    }
    jobdb_close(d);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    {
        jobdb_tx_t *tx = NULL;
        assert(jobdb_tx_begin(d, &tx) == JOBDB_OK);
        jobdb_tx_rollback(tx);
    }
    jobdb_close(d);
    fresh();
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    { jobdb_tx_t *tx = NULL; assert(jobdb_tx_begin(d, &tx) == JOBDB_OK); jobdb_tx_rollback(tx); }
    jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    { jobdb_tx_t *tx = NULL; const unsigned char x = 9; assert(jobdb_tx_begin(d, &tx) == JOBDB_OK); jobdb_test_fail_next(d, JOBDB_FAILURE_AFTER_OPERATION); assert(jobdb_tx_put(tx, 1, 1, &x, 1) == JOBDB_ERR_IO); jobdb_tx_rollback(tx); }
    jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    { jobdb_tx_t *tx = NULL; assert(jobdb_tx_begin(d, &tx) == JOBDB_OK); jobdb_test_fail_next(d, JOBDB_FAILURE_BEFORE_COMMIT); assert(jobdb_tx_commit(tx) == JOBDB_ERR_IO); jobdb_tx_rollback(tx); }
    jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    { jobdb_tx_t *tx = NULL; assert(jobdb_tx_begin(d, &tx) == JOBDB_OK); jobdb_test_fail_next(d, JOBDB_FAILURE_AFTER_COMMIT); assert(jobdb_tx_commit(tx) == JOBDB_ERR_IO); jobdb_tx_rollback(tx); }
    jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); jobdb_close(d);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    { const unsigned char p1[] = {4, 5, 6}; const unsigned char p2[] = {7, 8}; jobdb_record_t r; uint64_t rev = 0;
      assert(jobdb_record_create(d, 20, 99, p1, sizeof p1) == JOBDB_OK);
      assert(jobdb_record_create(d, 20, 99, p1, sizeof p1) == JOBDB_ERR_ALREADY_EXISTS);
      assert(jobdb_record_get(d, 20, 99, &r) == JOBDB_OK); assert(r.revision == 1 && r.payload_size == 3 && r.payload[1] == 5); jobdb_record_free(&r);
      assert(jobdb_record_update(d, 20, 99, 9, p2, sizeof p2, &rev) == JOBDB_ERR_CONFLICT);
      assert(jobdb_record_update(d, 20, 99, 1, p2, sizeof p2, &rev) == JOBDB_OK && rev == 2);
      jobdb_close(d); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK); assert(jobdb_record_get(d, 20, 99, &r) == JOBDB_OK); assert(r.revision == 2 && r.payload_size == 2); jobdb_record_free(&r);
      assert(jobdb_record_delete(d, 20, 99, 1) == JOBDB_ERR_CONFLICT); assert(jobdb_record_delete(d, 20, 99, 2) == JOBDB_OK); assert(jobdb_record_get(d, 20, 99, &r) == JOBDB_ERR_NOT_FOUND); }
    jobdb_close(d);
    raw_file("jobdb-test/wal.0", (const unsigned char *)"garbage", 7);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    jobdb_close(d);
    raw_manifest(0, (const unsigned char *)"garbage", 7);
    raw_manifest(1, (const unsigned char *)"garbage", 7);
    assert(jobdb_open("jobdb-test", &d) == JOBDB_ERR_CORRUPT);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    {
        jobdb_execution_t e={0}, got; jobdb_worker_id_t w={{9}}; jobdb_execution_t claimed; jobdb_stats_t stats; jobdb_ledger_entry_t ledger; jobdb_record_t payload={0};
        const unsigned char bytes[] = {7, 8, 9};
        e.execution_id=1000; e.job_definition_id=77; e.state=JOBDB_EXEC_READY; e.created_at=10; e.eligible_at=10; e.max_attempts=3;
        jobdb_test_fail_next(d, JOBDB_FAILURE_AFTER_COMMIT);
        assert(jobdb_execution_enqueue(d,&e,100,bytes,sizeof bytes)==JOBDB_ERR_IO);
        jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK);
        assert(jobdb_execution_get(d,1000,&got)==JOBDB_OK && got.state==JOBDB_EXEC_READY);
        assert(jobdb_record_get(d,100,1000,&payload)==JOBDB_OK && payload.payload_size==sizeof bytes); jobdb_record_free(&payload);
        assert(jobdb_get_stats(d,&stats)==JOBDB_OK && stats.submitted_total==1);
        assert(jobdb_claim_next(d,&w,10,100,&claimed)==JOBDB_OK);
        assert(jobdb_execution_start(d,1000,&w,claimed.fencing_token,10,NULL)==JOBDB_OK);
        jobdb_test_fail_next(d, JOBDB_FAILURE_AFTER_COMMIT);
        assert(jobdb_execution_finalize(d,1000,&w,claimed.fencing_token,JOBDB_EXEC_DONE,0,0)==JOBDB_ERR_IO);
        jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK);
        assert(jobdb_execution_get(d,1000,&got)==JOBDB_OK && got.state==JOBDB_EXEC_DONE && got.finished_at!=0);
        assert(jobdb_ledger_get(d,1000,&ledger)==JOBDB_OK && ledger.final_state==JOBDB_EXEC_DONE);
        assert(jobdb_get_stats(d,&stats)==JOBDB_OK && stats.submitted_total==1 && stats.completed_total==1);
    }
    jobdb_close(d);
    fresh(); assert(jobdb_open("jobdb-test", &d) == JOBDB_OK);
    {
        jobdb_execution_t e={0}, claimed, got; jobdb_worker_id_t w={{10}}; jobdb_stats_t stats; const unsigned char byte=1;
        e.execution_id=1001; e.job_definition_id=78; e.state=JOBDB_EXEC_READY; e.created_at=10; e.eligible_at=10; e.max_attempts=3;
        assert(jobdb_execution_enqueue(d,&e,100,&byte,1)==JOBDB_OK); assert(jobdb_claim_next(d,&w,10,100,&claimed)==JOBDB_OK);
        assert(jobdb_execution_start(d,1001,&w,claimed.fencing_token,10,NULL)==JOBDB_OK);
        jobdb_test_fail_next(d, JOBDB_FAILURE_AFTER_COMMIT);
        assert(jobdb_execution_retry(d,1001,&w,claimed.fencing_token,20,NULL)==JOBDB_ERR_IO);
        jobdb_close(d); assert(jobdb_open("jobdb-test",&d)==JOBDB_OK);
        assert(jobdb_execution_get(d,1001,&got)==JOBDB_OK && got.state==JOBDB_EXEC_READY && got.attempt==1 && got.eligible_at==20);
        assert(jobdb_get_stats(d,&stats)==JOBDB_OK && stats.retried_total==1);
    }
    jobdb_close(d);
    rmdb();
    puts("jobdb tests passed");
    return 0;
}

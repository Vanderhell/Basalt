#include "jobdb.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <process.h>
#include <windows.h>
#define gate_pid ((unsigned long)_getpid())
#else
#include <unistd.h>
#include <sys/wait.h>
#define gate_pid ((unsigned long)getpid())
#endif

static void path_for(char *out, size_t n, const char *tag) {
    static unsigned sequence;
    snprintf(out, n, "jobdb-gate-%s-%lu-%u", tag, gate_pid, ++sequence);
}
static void put_u32(unsigned char *p, unsigned v) { p[0]=(unsigned char)v; p[1]=(unsigned char)(v>>8); p[2]=(unsigned char)(v>>16); p[3]=(unsigned char)(v>>24); }
static void put_u64(unsigned char *p, unsigned long long v) { for (unsigned i=0;i<8;i++) p[i]=(unsigned char)(v>>(8*i)); }
static void make_db(const char *path, jobdb_t **db) { assert(jobdb_create(path, db)==JOBDB_OK); }

static int wal_test(void) {
    char path[256], wal[320]; jobdb_t *db=NULL; unsigned char x=7, bad[52]={0}; FILE *f; long before, after;
    path_for(path,sizeof path,"wal"); snprintf(wal,sizeof wal,"%s/wal.0",path); make_db(path,&db);
    assert(jobdb_record_create(db,8,1,&x,1)==JOBDB_OK); jobdb_close(db); db=NULL;
    f=fopen(wal,"rb"); assert(f); fseek(f,0,SEEK_END); before=ftell(f); fclose(f);
    f=fopen(wal,"ab"); assert(f); assert(fwrite("JOB",1,3,f)==3); fclose(f);
    assert(jobdb_open(path,&db)==JOBDB_OK); jobdb_close(db); db=NULL;
    f=fopen(wal,"rb"); assert(f); fseek(f,0,SEEK_END); after=ftell(f); fclose(f); assert(after==before);
    assert(jobdb_open(path,&db)==JOBDB_OK); x=8; assert(jobdb_record_create(db,8,2,&x,1)==JOBDB_OK); jobdb_close(db); db=NULL;
    { unsigned char h[48]={0}, payload[2]={9,9}; memcpy(h,"JOBDBWAL",8);put_u32(h+8,1);put_u32(h+12,2);put_u64(h+16,56);put_u64(h+24,99);put_u32(h+32,8);put_u64(h+36,3);put_u32(h+44,4);f=fopen(wal,"ab");assert(f);assert(fwrite(h,1,sizeof h,f)==sizeof h&&fwrite(payload,1,sizeof payload,f)==sizeof payload);fclose(f);assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_close(db);db=NULL;}
    { unsigned char h[48]={0}, payload[4]={1,2,3,4}, crc[2]={0,0}; memcpy(h,"JOBDBWAL",8);put_u32(h+8,1);put_u32(h+12,2);put_u64(h+16,56);put_u64(h+24,100);put_u32(h+32,8);put_u64(h+36,4);put_u32(h+44,4);f=fopen(wal,"ab");assert(f);assert(fwrite(h,1,sizeof h,f)==sizeof h&&fwrite(payload,1,sizeof payload,f)==sizeof payload&&fwrite(crc,1,sizeof crc,f)==sizeof crc);fclose(f);assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_close(db);db=NULL;}
    assert(jobdb_open(path,&db)==JOBDB_OK); x=10; assert(jobdb_record_create(db,8,3,&x,1)==JOBDB_OK); jobdb_close(db); db=NULL;
    memcpy(bad,"JOBDBWAL",8);put_u32(bad+8,1);put_u32(bad+12,2);put_u64(bad+16,52);put_u64(bad+24,101);put_u32(bad+32,8);put_u64(bad+36,5);put_u32(bad+44,0);
    f=fopen(wal,"ab"); assert(f); assert(fwrite(bad,1,sizeof bad,f)==sizeof bad); fclose(f);
    assert(jobdb_open(path,&db)==JOBDB_ERR_CORRUPT);
    return 0;
}

static int stress_test(void) {
    char path[256]; jobdb_t *db=NULL; unsigned char x=1; size_t n=0; uint64_t *ids;
    path_for(path,sizeof path,"stress"); make_db(path,&db);
    for (uint64_t i=1;i<=2500;i++) assert(jobdb_record_create(db,8,i,&x,1)==JOBDB_OK);
    assert(jobdb_checkpoint(db)==JOBDB_OK);
    for (uint64_t i=2501;i<=5000;i++) assert(jobdb_record_create(db,8,i,&x,1)==JOBDB_OK);
    assert(jobdb_list_record_ids(db,8,NULL,0,&n)==JOBDB_OK && n==5000); ids=(uint64_t*)malloc(n*sizeof *ids); assert(ids);
    assert(jobdb_list_record_ids(db,8,ids,n,&n)==JOBDB_OK && n==5000); free(ids); assert(jobdb_verify(path)==JOBDB_OK); jobdb_close(db); assert(jobdb_open(path,&db)==JOBDB_OK); assert(jobdb_verify(path)==JOBDB_OK); jobdb_close(db); return 0;
}
static int transaction_test(void) {
    char path[256]; jobdb_t *db=NULL; jobdb_tx_t *tx=NULL; unsigned char x=4; jobdb_record_t r;
    path_for(path,sizeof path,"transactions"); make_db(path,&db); assert(jobdb_tx_begin(db,&tx)==JOBDB_OK);
    for(uint64_t i=1;i<=128;i++) assert(jobdb_tx_put(tx,8,i,&x,1)==JOBDB_OK);
    assert(jobdb_tx_commit(tx)==JOBDB_OK); jobdb_tx_rollback(tx); assert(jobdb_record_get(db,8,128,&r)==JOBDB_OK); jobdb_record_free(&r); assert(jobdb_verify(path)==JOBDB_OK); jobdb_close(db); return 0;
}
static int corruption_test(void) {
    char path[256], record[320]; jobdb_t *db=NULL; unsigned char x=4; FILE *f;
    path_for(path,sizeof path,"corruption"); snprintf(record,sizeof record,"%s/record.8.1",path); make_db(path,&db); assert(jobdb_record_create(db,8,1,&x,1)==JOBDB_OK); jobdb_close(db);
    f=fopen(record,"r+b"); assert(f); assert(fputc('X',f)!=EOF); fclose(f); assert(jobdb_verify(path)==JOBDB_ERR_CORRUPT); return 0;
}

static int lease_test(void) {
    char path[256]; jobdb_t *db=NULL; jobdb_execution_t e={0},got; jobdb_worker_id_t a={{1}},b={{2}}; jobdb_stats_t st; uint32_t count=0; uint64_t oldrev;
    path_for(path,sizeof path,"lease"); make_db(path,&db); e.execution_id=1;e.job_definition_id=1;e.state=JOBDB_EXEC_READY;e.eligible_at=100;assert(jobdb_execution_create(db,&e)==JOBDB_OK);
    assert(jobdb_claim_next(db,&a,100,10,&got)==JOBDB_OK); oldrev=got.revision; assert(jobdb_execution_start(db,1,&a,got.fencing_token,105,NULL)==JOBDB_OK); assert(jobdb_renew_lease(db,1,&a,got.fencing_token,120)==JOBDB_OK); assert(jobdb_execution_get(db,1,&got)==JOBDB_OK&&got.state==JOBDB_EXEC_RUNNING&&got.revision==oldrev+2);
    assert(jobdb_reclaim_expired(db,121,&count)==JOBDB_OK&&count==1); assert(jobdb_execution_get(db,1,&got)==JOBDB_OK&&got.state==JOBDB_EXEC_READY&&got.worker_instance_id[0]==0&&got.lease_expires_at==0); assert(jobdb_get_stats(db,&st)==JOBDB_OK&&st.recovered_total==1);
    assert(jobdb_claim_next(db,&b,122,10,&got)==JOBDB_OK&&got.fencing_token>1); assert(jobdb_execution_complete(db,1,&a,1)==JOBDB_ERR_STALE_LEASE); jobdb_close(db); return 0;
}

static int failure_test(void) {
    const jobdb_failure_point_t points[] = { JOBDB_FAILURE_BEFORE_WAL_BEGIN, JOBDB_FAILURE_AFTER_WAL_BEGIN, JOBDB_FAILURE_AFTER_WAL_OPERATIONS, JOBDB_FAILURE_AFTER_COMMIT_SYNC, JOBDB_FAILURE_DURING_APPLY, JOBDB_FAILURE_CHECKPOINT_BEFORE_MANIFEST, JOBDB_FAILURE_BEFORE_WAL_TRUNCATE, JOBDB_FAILURE_AFTER_WAL_TRUNCATE };
    for (unsigned k=0;k<sizeof(points)/sizeof(points[0]);k++) { char path[256]; jobdb_t *db=NULL; unsigned char x=3; jobdb_record_t r; path_for(path,sizeof(path),"failure"); make_db(path,&db); if(k<5){jobdb_tx_t *tx=NULL;assert(jobdb_tx_begin(db,&tx)==JOBDB_OK);assert(jobdb_tx_put(tx,8,1,&x,1)==JOBDB_OK);jobdb_test_fail_next(db,points[k]);assert(jobdb_tx_commit(tx)==JOBDB_ERR_IO);jobdb_tx_rollback(tx);jobdb_close(db);db=NULL;assert(jobdb_open(path,&db)==JOBDB_OK);if(k>=3)assert(jobdb_record_get(db,8,1,&r)==JOBDB_OK);else assert(jobdb_record_get(db,8,1,&r)==JOBDB_ERR_NOT_FOUND);if(k>=3)jobdb_record_free(&r);}else{jobdb_test_fail_next(db,points[k]);assert(jobdb_checkpoint(db)==JOBDB_ERR_IO);jobdb_close(db);db=NULL;assert(jobdb_open(path,&db)==JOBDB_OK);}jobdb_close(db); }
    return 0;
}

#ifdef _WIN32
static int child_tx(const char *path, unsigned base) { jobdb_t *db=NULL; unsigned char x; jobdb_result_t opened=JOBDB_ERR_TIMEOUT; for(unsigned retry=0;retry<20&&opened!=JOBDB_OK;retry++){opened=jobdb_open(path,&db);if(opened!=JOBDB_OK)Sleep(50);} assert(opened==JOBDB_OK); for(unsigned i=0;i<100;i++){jobdb_tx_t *tx=NULL;uint64_t id=(uint64_t)base*100+i+1;x=(unsigned char)i;jobdb_result_t cr;do{assert(jobdb_tx_begin(db,&tx)==JOBDB_OK);assert(jobdb_tx_put(tx,8,id,&x,1)==JOBDB_OK);cr=jobdb_tx_commit(tx);jobdb_tx_rollback(tx);if(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY)Sleep(20);else if(cr!=JOBDB_OK){fprintf(stderr,"child %u commit %u: %s\n",base,i,jobdb_result_string(cr));assert(cr==JOBDB_OK);}}while(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY);}jobdb_close(db);return 0; }
static int child_conflict(const char *path, unsigned v) { jobdb_t *db=NULL;jobdb_record_t r;unsigned char x=(unsigned char)v;assert(jobdb_open(path,&db)==JOBDB_OK);assert(jobdb_record_get(db,8,9001,&r)==JOBDB_OK);uint64_t rev=r.revision;jobdb_record_free(&r);Sleep(100);jobdb_result_t z=jobdb_record_update(db,8,9001,rev,&x,1,NULL);jobdb_close(db);return z==JOBDB_OK?0:(z==JOBDB_ERR_CONFLICT?2:3); }
static int child_claim(const char *path, unsigned v) { jobdb_t *db=NULL;jobdb_execution_t e;jobdb_worker_id_t w={{0}};w.bytes[0]=(unsigned char)v;assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_result_t z=jobdb_claim_next(db,&w,(int64_t)time(NULL),60,&e);jobdb_close(db);return z==JOBDB_OK?0:1; }
#else
static int child_tx(const char *path, unsigned base) { jobdb_t *db=NULL; unsigned char x; jobdb_result_t opened=JOBDB_ERR_TIMEOUT; for(unsigned retry=0;retry<20&&opened!=JOBDB_OK;retry++){opened=jobdb_open(path,&db);if(opened!=JOBDB_OK)usleep(50000);} assert(opened==JOBDB_OK); for(unsigned i=0;i<100;i++){jobdb_tx_t *tx=NULL;uint64_t id=(uint64_t)base*100+i+1;x=(unsigned char)i;jobdb_result_t cr;do{assert(jobdb_tx_begin(db,&tx)==JOBDB_OK);assert(jobdb_tx_put(tx,8,id,&x,1)==JOBDB_OK);cr=jobdb_tx_commit(tx);jobdb_tx_rollback(tx);if(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY)usleep(20000);else assert(cr==JOBDB_OK);}while(cr==JOBDB_ERR_TIMEOUT||cr==JOBDB_ERR_BUSY);}jobdb_close(db);return 0; }
static int child_conflict(const char *path, unsigned v) { jobdb_t *db=NULL;jobdb_record_t r;unsigned char x=(unsigned char)v;assert(jobdb_open(path,&db)==JOBDB_OK);assert(jobdb_record_get(db,8,9001,&r)==JOBDB_OK);uint64_t rev=r.revision;jobdb_record_free(&r);usleep(100000);jobdb_result_t z=jobdb_record_update(db,8,9001,rev,&x,1,NULL);jobdb_close(db);return z==JOBDB_OK?0:(z==JOBDB_ERR_CONFLICT?2:3); }
static int child_claim(const char *path, unsigned v) { jobdb_t *db=NULL;jobdb_execution_t e;jobdb_worker_id_t w={{0}};w.bytes[0]=(unsigned char)v;assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_result_t z=jobdb_claim_next(db,&w,(int64_t)time(NULL),60,&e);jobdb_close(db);return z==JOBDB_OK?0:1; }
#endif

int main(int argc, char **argv) {
#ifdef _WIN32
    if(argc>1&&strcmp(argv[1],"tx-child")==0)return child_tx(argv[2],(unsigned)strtoul(argv[3],NULL,10));
    if(argc>1&&strcmp(argv[1],"conflict-child")==0)return child_conflict(argv[2],(unsigned)strtoul(argv[3],NULL,10));
    if(argc>1&&strcmp(argv[1],"claim-child")==0)return child_claim(argv[2],(unsigned)strtoul(argv[3],NULL,10));
#endif
    if(argc<2)return 2;
    if(strcmp(argv[1],"wal")==0)return wal_test();
    if(strcmp(argv[1],"corruption")==0)return corruption_test();
    if(strcmp(argv[1],"failure")==0)return failure_test();
    if(strcmp(argv[1],"stress")==0)return stress_test();
    if(strcmp(argv[1],"transactions")==0)return transaction_test();
    if(strcmp(argv[1],"lease")==0)return lease_test();
#ifdef _WIN32
    if(strcmp(argv[1],"multiprocess")==0){char path[256],num[32];jobdb_t*db=NULL;size_t n=0;uint64_t*ids;unsigned char x=1;path_for(path,sizeof path,"multiprocess");make_db(path,&db);intptr_t tx_children[10];for(unsigned p=0;p<10;p++){sprintf(num,"%u",p);const char*args[]={argv[0],"tx-child",path,num,NULL};tx_children[p]=_spawnv(_P_NOWAIT,argv[0],args);assert(tx_children[p]!=-1);}for(unsigned p=0;p<10;p++){int s=0;assert(_cwait(&s,tx_children[p],0)!=-1&&s==0);}assert(jobdb_list_record_ids(db,8,NULL,0,&n)==JOBDB_OK&&n==1000);ids=(uint64_t*)malloc(n*sizeof*ids);assert(ids);assert(jobdb_list_record_ids(db,8,ids,n,&n)==JOBDB_OK);free(ids);
        assert(jobdb_record_create(db,8,9001,&x,1)==JOBDB_OK);jobdb_close(db);db=NULL;{const char*args1[]={argv[0],"conflict-child",path,"1",NULL};const char*args2[]={argv[0],"conflict-child",path,"2",NULL};intptr_t h1=_spawnv(_P_NOWAIT,argv[0],args1),h2=_spawnv(_P_NOWAIT,argv[0],args2);int s1=0,s2=0;assert(h1!=-1&&h2!=-1);assert(_cwait(&s1,h1,0)!=-1&&_cwait(&s2,h2,0)!=-1);assert((s1==0&&s2==2)||(s1==2&&s2==0));}
        assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_execution_t e={0};e.execution_id=9002;e.job_definition_id=1;e.state=JOBDB_EXEC_READY;e.eligible_at=(int64_t)time(NULL);assert(jobdb_execution_create(db,&e)==JOBDB_OK);jobdb_close(db);db=NULL;{intptr_t claim_children[4];for(unsigned p=1;p<=4;p++){sprintf(num,"%u",p);const char*args[]={argv[0],"claim-child",path,num,NULL};claim_children[p-1]=_spawnv(_P_NOWAIT,argv[0],args);assert(claim_children[p-1]!=-1);}int winners=0;for(unsigned p=0;p<4;p++){int s=0;assert(_cwait(&s,claim_children[p],0)!=-1);assert(s==0||s==1);if(s==0)winners++;}assert(winners==1);}
        assert(jobdb_open(path,&db)==JOBDB_OK);assert(jobdb_execution_get(db,9002,&e)==JOBDB_OK&&e.state==JOBDB_EXEC_LEASED&&e.fencing_token>0);assert(jobdb_verify(path)==JOBDB_OK);assert(jobdb_checkpoint(db)==JOBDB_OK);jobdb_close(db);assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_close(db);return 0;}
#else
    if(strcmp(argv[1],"multiprocess")==0){char path[256];jobdb_t*db=NULL;size_t n=0;uint64_t*ids;unsigned char x=1;path_for(path,sizeof path,"multiprocess");make_db(path,&db);pid_t tx_children[10];for(unsigned p=0;p<10;p++){tx_children[p]=fork();assert(tx_children[p]>=0);if(tx_children[p]==0)return child_tx(path,p);}for(unsigned p=0;p<10;p++){int s=0;assert(waitpid(tx_children[p],&s,0)>0&&WIFEXITED(s)&&WEXITSTATUS(s)==0);}assert(jobdb_list_record_ids(db,8,NULL,0,&n)==JOBDB_OK&&n==1000);ids=(uint64_t*)malloc(n*sizeof*ids);assert(ids);assert(jobdb_list_record_ids(db,8,ids,n,&n)==JOBDB_OK);free(ids);assert(jobdb_record_create(db,8,9001,&x,1)==JOBDB_OK);jobdb_close(db);db=NULL;pid_t a=fork();assert(a>=0);if(a==0)return child_conflict(path,1);pid_t b=fork();assert(b>=0);if(b==0)return child_conflict(path,2);int sa=0,sb=0;assert(waitpid(a,&sa,0)>0&&waitpid(b,&sb,0)>0);assert((WEXITSTATUS(sa)==0&&WEXITSTATUS(sb)==2)||(WEXITSTATUS(sa)==2&&WEXITSTATUS(sb)==0));assert(jobdb_open(path,&db)==JOBDB_OK);jobdb_execution_t e={0};e.execution_id=9002;e.job_definition_id=1;e.state=JOBDB_EXEC_READY;e.eligible_at=(int64_t)time(NULL);assert(jobdb_execution_create(db,&e)==JOBDB_OK);jobdb_close(db);pid_t claims[4];for(unsigned p=0;p<4;p++){claims[p]=fork();assert(claims[p]>=0);if(claims[p]==0)return child_claim(path,p+1);}int winners=0;for(unsigned p=0;p<4;p++){int s=0;assert(waitpid(claims[p],&s,0)>0&&WIFEXITED(s));assert(WEXITSTATUS(s)==0||WEXITSTATUS(s)==1);if(WEXITSTATUS(s)==0)winners++;}assert(winners==1);assert(jobdb_open(path,&db)==JOBDB_OK);assert(jobdb_execution_get(db,9002,&e)==JOBDB_OK&&e.state==JOBDB_EXEC_LEASED);assert(jobdb_verify(path)==JOBDB_OK);jobdb_close(db);return 0;}
#endif
    return 2;
}

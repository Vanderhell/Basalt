#include "jobdb.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#include <io.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

static void remove_db(const char *path) {
    char file[256]; unsigned slot;
    for (slot=0;slot<2;slot++){snprintf(file,sizeof file,"%s/manifest.%u",path,slot);remove(file);}
    snprintf(file,sizeof file,"%s/wal.0",path);remove(file);
    snprintf(file,sizeof file,"%s/coordination.lock",path);remove(file);
#ifdef _WIN32
    { struct _finddata_t e; intptr_t h; char pattern[256];snprintf(pattern,sizeof pattern,"%s/*",path);h=_findfirst(pattern,&e);if(h!=-1){do{if(strcmp(e.name,".")&&strcmp(e.name,"..")){snprintf(file,sizeof file,"%s/%s",path,e.name);remove(file);}}while(_findnext(h,&e)==0);_findclose(h);} } _rmdir(path);
#else
    { DIR *d=opendir(path);struct dirent *e;if(d){while((e=readdir(d))!=NULL){if(strcmp(e->d_name,".")&&strcmp(e->d_name,"..")){snprintf(file,sizeof file,"%s/%s",path,e->d_name);remove(file);}}closedir(d);}rmdir(path); }
#endif
}

int main(void) {
    jobdb_t *a=NULL,*b=NULL,*open=NULL; jobdb_tx_t *tx=NULL; jobdb_record_t record; const unsigned char value=7; size_t count=0; uint64_t ids[2];
    remove_db("foundation-a");remove_db("foundation-b");
    assert(jobdb_create("foundation-a",&a)==JOBDB_OK);assert(jobdb_create("foundation-b",&b)==JOBDB_OK);
    assert(jobdb_record_create(a,90,900,&value,1)==JOBDB_OK);
    assert(jobdb_record_get(a,90,900,&record)==JOBDB_OK&&record.revision==1&&record.payload[0]==7);jobdb_record_free(&record);
    assert(jobdb_record_create(a,90,900,&value,1)==JOBDB_ERR_ALREADY_EXISTS);
    assert(jobdb_tx_begin(a,&tx)==JOBDB_OK);assert(jobdb_tx_put(tx,91,910,&value,1)==JOBDB_OK);assert(jobdb_record_get(a,91,910,&record)==JOBDB_ERR_NOT_FOUND);assert(jobdb_tx_commit(tx)==JOBDB_OK);jobdb_tx_rollback(tx);tx=NULL;
    assert(jobdb_record_get(a,91,910,&record)==JOBDB_OK);jobdb_record_free(&record);
    { jobdb_execution_t execution={0},claimed,started;jobdb_worker_id_t worker={{1}};jobdb_result_t claim;int64_t now=(int64_t)time(NULL);execution.execution_id=7000;execution.job_definition_id=1;execution.state=JOBDB_EXEC_READY;execution.eligible_at=now;assert(jobdb_execution_create(a,&execution)==JOBDB_OK);assert(jobdb_list_record_ids(a,3,ids,2,&count)==JOBDB_OK&&count==1);claim=jobdb_claim_next(a,&worker,now,10,&claimed);if(claim!=JOBDB_OK)fprintf(stderr,"claim: %s\n",jobdb_result_string(claim));assert(claim==JOBDB_OK);assert(jobdb_execution_start(a,7000,&worker,claimed.fencing_token,now,&count)==JOBDB_OK);assert(jobdb_execution_get(a,7000,&started)==JOBDB_OK&&started.state==JOBDB_EXEC_RUNNING); }
    assert(jobdb_list_record_ids(a,90,ids,2,&count)==JOBDB_OK&&count==1);
    assert(jobdb_checkpoint(a)==JOBDB_OK);jobdb_close(a);a=NULL;
    assert(jobdb_open("foundation-a",&open)==JOBDB_OK);assert(jobdb_verify("foundation-a")==JOBDB_OK);assert(jobdb_record_get(open,91,910,&record)==JOBDB_OK);jobdb_record_free(&record);jobdb_close(open);
    jobdb_close(b);remove_db("foundation-a");remove_db("foundation-b");puts("jobdb foundation tests passed");return 0;
}

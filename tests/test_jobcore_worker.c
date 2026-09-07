#include "jobcore.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
static void wait_ms(unsigned n){Sleep(n);}
#else
#include <unistd.h>
static void wait_ms(unsigned n){usleep(n*1000u);}
#endif
static volatile unsigned calls;
static int handler(const void *payload,size_t size,uint32_t version,const jobcore_execution_context_t *context,void *data){(void)data;assert(size==3&&memcmp(payload,"job",3)==0);assert(version==7&&context->fencing_token!=0);calls++;return 0;}
static void clean_db(void){remove("jobcore-worker/manifest.0");remove("jobcore-worker/manifest.1");remove("jobcore-worker/wal.0");remove("jobcore-worker/coordination.lock");remove("jobcore-worker/record.3.1");remove("jobcore-worker/record.4.1");remove("jobcore-worker/record.5.1");remove("jobcore-worker/record.100.1");}
int main(void){jobdb_t*db=NULL;jobcore_t*core=NULL;uint64_t id=0;jobdb_execution_t e;int64_t now=(int64_t)time(NULL);clean_db();assert(jobdb_create("jobcore-worker",&db)==JOBDB_OK);assert(jobcore_create(db,1,10,&core)==JOBDB_OK);assert(jobcore_register_handler(core,42,handler,NULL)==JOBDB_OK);assert(jobcore_enqueue(core,42,"job",3,7,now,1,&id)==JOBDB_OK);assert(jobcore_start(core)==JOBDB_OK);for(unsigned i=0;i<300;i++){assert(jobdb_execution_get(db,id,&e)==JOBDB_OK);if(e.state==JOBDB_EXEC_DONE)break;wait_ms(10);}assert(e.state==JOBDB_EXEC_DONE);assert(calls==1);assert(jobcore_stop(core)==JOBDB_OK);jobcore_destroy(core);jobdb_close(db);puts("jobcore worker test passed");return 0;}

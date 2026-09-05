#include "jobdb_lock.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
struct jobdb_lock { HANDLE handle; };
jobdb_result_t jobdb_lock_acquire_impl(jobdb_t *db,const char *path,uint32_t timeout_ms,jobdb_lock_t **out){(void)db;if(!path||!out)return JOBDB_ERR_INVALID_ARGUMENT;*out=NULL;char p[4096];(void)snprintf(p,sizeof p,"%s/coordination.lock",path);DWORD elapsed=0;for(;;){HANDLE h=CreateFileA(p,GENERIC_READ|GENERIC_WRITE,0,NULL,OPEN_ALWAYS,FILE_ATTRIBUTE_NORMAL,NULL);if(h!=INVALID_HANDLE_VALUE){jobdb_lock_t*l=(jobdb_lock_t*)calloc(1,sizeof*l);if(!l){CloseHandle(h);return JOBDB_ERR_INTERNAL;}l->handle=h;*out=l;return JOBDB_OK;}if(timeout_ms==0)return JOBDB_ERR_BUSY;if(elapsed>=timeout_ms)return JOBDB_ERR_TIMEOUT;Sleep(10);elapsed+=10;}}
void jobdb_lock_release(jobdb_lock_t*l){if(l){CloseHandle(l->handle);free(l);}}
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
struct jobdb_lock { int fd; };
jobdb_result_t jobdb_lock_acquire_impl(jobdb_t *db,const char *path,uint32_t timeout_ms,jobdb_lock_t **out){(void)db;if(!path||!out)return JOBDB_ERR_INVALID_ARGUMENT;*out=NULL;char p[4096];(void)snprintf(p,sizeof p,"%s/coordination.lock",path);uint32_t elapsed=0;for(;;){int fd=open(p,O_CREAT|O_RDWR,0600);if(fd>=0&&flock(fd,LOCK_EX|LOCK_NB)==0){jobdb_lock_t*l=(jobdb_lock_t*)calloc(1,sizeof*l);if(!l){close(fd);return JOBDB_ERR_INTERNAL;}l->fd=fd;*out=l;return JOBDB_OK;}if(fd>=0)close(fd);if(timeout_ms==0)return JOBDB_ERR_BUSY;if(elapsed>=timeout_ms)return JOBDB_ERR_TIMEOUT;usleep(10000);elapsed+=10;}}
void jobdb_lock_release(jobdb_lock_t*l){if(l){flock(l->fd,LOCK_UN);close(l->fd);free(l);}}
#endif

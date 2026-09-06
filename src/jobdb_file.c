#include "jobdb_file.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#include <io.h>
#define mkdir(p,m) _mkdir(p)
#else
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#endif
jobdb_result_t jobdb_make_dir(const char *path) {
    if (mkdir(path, 0700) == 0) return JOBDB_OK;
#ifdef _WIN32
    DWORD attrs = GetFileAttributesA(path);
    if (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY)) return JOBDB_ERR_ALREADY_EXISTS;
#else
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return JOBDB_ERR_ALREADY_EXISTS;
#endif
    return JOBDB_ERR_IO;
}
int jobdb_file_exists(const char *path) { FILE *f=fopen(path,"rb"); if(!f)return 0; fclose(f); return 1; }
jobdb_result_t jobdb_file_sync(FILE *file) {
    if (!file || fflush(file) != 0) return JOBDB_ERR_IO;
#ifdef _WIN32
    { int fd = _fileno(file); intptr_t handle = fd < 0 ? -1 : _get_osfhandle(fd);
      return handle == -1 || !FlushFileBuffers((HANDLE)handle) ? JOBDB_ERR_IO : JOBDB_OK; }
#else
    return fsync(fileno(file)) == 0 ? JOBDB_OK : JOBDB_ERR_IO;
#endif
}
jobdb_result_t jobdb_sync_directory(const char *path) {
#ifdef _WIN32
    (void)path; return JOBDB_OK;
#else
    int fd; if (!path) return JOBDB_ERR_INVALID_ARGUMENT; fd=open(path,O_RDONLY|O_DIRECTORY); if(fd<0)return JOBDB_ERR_IO;
    { int r=fsync(fd); close(fd); return r==0?JOBDB_OK:JOBDB_ERR_IO; }
#endif
}
jobdb_result_t jobdb_read_file(const char *path,uint8_t *buf,size_t size,size_t *out_read){ FILE*f=fopen(path,"rb"); if(!f)return JOBDB_ERR_NOT_FOUND; size_t n=fread(buf,1,size,f); int bad=ferror(f); fclose(f); if(out_read)*out_read=n; return bad?JOBDB_ERR_IO:JOBDB_OK; }
jobdb_result_t jobdb_write_file(const char *path,const uint8_t *buf,size_t size){char tmp[4096];FILE*f;size_t n;jobdb_result_t r;if(!path||size>0&& !buf)return JOBDB_ERR_INVALID_ARGUMENT;if(snprintf(tmp,sizeof tmp,"%s.write.tmp",path)<0)return JOBDB_ERR_IO;f=fopen(tmp,"wb");if(!f)return JOBDB_ERR_IO;n=fwrite(buf,1,size,f);r=n!=size?JOBDB_ERR_IO:jobdb_file_sync(f);if(fclose(f)!=0)r=JOBDB_ERR_IO;if(r!=JOBDB_OK){remove(tmp);return r;}
#ifdef _WIN32
    if(!MoveFileExA(tmp,path,MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH)){remove(tmp);return JOBDB_ERR_IO;}
#else
    if(rename(tmp,path)!=0){remove(tmp);return JOBDB_ERR_IO;}{const char *slash=strrchr(path,'/');if(slash){char dir[4096];size_t len=(size_t)(slash-path);if(len>=sizeof dir)return JOBDB_ERR_IO;memcpy(dir,path,len);dir[len]=0;r=jobdb_sync_directory(dir);if(r!=JOBDB_OK)return r;}}
#endif
    return JOBDB_OK; }
jobdb_result_t jobdb_remove_file(const char *path) {
    const char *s;
    if (!path) return JOBDB_ERR_INVALID_ARGUMENT;
    if (remove(path) != 0 && errno != ENOENT) return JOBDB_ERR_IO;
    s = strrchr(path, '/');
#ifdef _WIN32
    { const char *b = strrchr(path, '\\'); if (!s || (b && b > s)) s = b; }
#endif
    if (!s) return JOBDB_OK;
    { size_t n=(size_t)(s-path); char *d=(char*)malloc(n+1); jobdb_result_t r;
      if (!d) return JOBDB_ERR_INTERNAL; memcpy(d,path,n); d[n]=0; r=jobdb_sync_directory(d); free(d); return r; }
}
jobdb_result_t jobdb_truncate_file(const char *path, uint64_t size) {
    if (!path || size > (uint64_t)SIZE_MAX) return JOBDB_ERR_INVALID_ARGUMENT;
#ifdef _WIN32
    { FILE *f=fopen(path,"r+b"); int fd; if(!f)f=fopen(path,"w+b"); if(!f)return JOBDB_ERR_IO; if(_fseeki64(f,(__int64)size,SEEK_SET)!=0){fclose(f);return JOBDB_ERR_IO;} fd=_fileno(f); if(_chsize_s(fd,(__int64)size)!=0){fclose(f);return JOBDB_ERR_IO;} if(jobdb_file_sync(f)!=JOBDB_OK){fclose(f);return JOBDB_ERR_IO;} fclose(f); return JOBDB_OK; }
#else
    if (truncate(path,(off_t)size)!=0) { if(errno!=ENOENT)return JOBDB_ERR_IO; { FILE *nf=fopen(path,"wb"); if(!nf)return JOBDB_ERR_IO; if(jobdb_file_sync(nf)!=JOBDB_OK){fclose(nf);return JOBDB_ERR_IO;} fclose(nf); return JOBDB_OK; } }
    { FILE *f=fopen(path,"ab"); jobdb_result_t r=f?jobdb_file_sync(f):JOBDB_ERR_IO; if(f)fclose(f); return r; }
#endif
}
jobdb_result_t jobdb_visit_files(const char *directory, jobdb_file_visit_fn visit, void *context) {
    if (!directory || !visit) return JOBDB_ERR_INVALID_ARGUMENT;
#ifdef _WIN32
    { char pattern[4096]; struct _finddata_t e; intptr_t h;
      if (snprintf(pattern,sizeof pattern,"%s\\*",directory)<0)return JOBDB_ERR_IO;
      h=_findfirst(pattern,&e); if(h==-1)return errno==ENOENT?JOBDB_ERR_NOT_FOUND:JOBDB_ERR_IO;
      do { if(strcmp(e.name,".")&&strcmp(e.name,"..")&&!(e.attrib&_A_SUBDIR)&&!visit(e.name,context)){_findclose(h);return JOBDB_OK;} } while(_findnext(h,&e)==0);
      _findclose(h); return JOBDB_OK; }
#else
    { DIR *d=opendir(directory); struct dirent *e;
      if(!d)return errno==ENOENT?JOBDB_ERR_NOT_FOUND:JOBDB_ERR_IO;
      while((e=readdir(d))!=NULL)if(strcmp(e->d_name,".")&&strcmp(e->d_name,"..")&&!visit(e->d_name,context)){closedir(d);return JOBDB_OK;}
      closedir(d); return JOBDB_OK; }
#endif
}

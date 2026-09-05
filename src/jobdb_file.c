#include "jobdb_file.h"
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#define mkdir(p,m) _mkdir(p)
#else
#include <sys/stat.h>
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
jobdb_result_t jobdb_read_file(const char *path,uint8_t *buf,size_t size,size_t *out_read){ FILE*f=fopen(path,"rb"); if(!f)return JOBDB_ERR_NOT_FOUND; size_t n=fread(buf,1,size,f); int bad=ferror(f); fclose(f); if(out_read)*out_read=n; return bad?JOBDB_ERR_IO:JOBDB_OK; }
jobdb_result_t jobdb_write_file(const char *path,const uint8_t *buf,size_t size){ FILE*f=fopen(path,"wb"); if(!f)return JOBDB_ERR_IO; size_t n=fwrite(buf,1,size,f); int bad=(n!=size)||fflush(f)!=0||fclose(f)!=0; return bad?JOBDB_ERR_IO:JOBDB_OK; }

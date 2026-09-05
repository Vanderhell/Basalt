#include "jobdb_codec.h"
void jobdb_put_u32le(uint8_t *p, uint32_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24); }
void jobdb_put_u64le(uint8_t *p, uint64_t v) { for (unsigned i=0;i<8;++i) p[i]=(uint8_t)(v>>(8*i)); }
uint32_t jobdb_get_u32le(const uint8_t *p) { return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24); }
uint64_t jobdb_get_u64le(const uint8_t *p) { uint64_t v=0; for (unsigned i=0;i<8;++i) v|=((uint64_t)p[i])<<(8*i); return v; }

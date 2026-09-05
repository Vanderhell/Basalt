#ifndef JOBDB_CODEC_H
#define JOBDB_CODEC_H
#include <stddef.h>
#include <stdint.h>
uint32_t jobdb_crc32c(const uint8_t *data, size_t size);
void jobdb_put_u32le(uint8_t *p, uint32_t v);
void jobdb_put_u64le(uint8_t *p, uint64_t v);
uint32_t jobdb_get_u32le(const uint8_t *p);
uint64_t jobdb_get_u64le(const uint8_t *p);
#endif

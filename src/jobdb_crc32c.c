#include "jobdb_codec.h"
uint32_t jobdb_crc32c(const uint8_t *data, size_t size) {
    uint32_t crc = 0xffffffffu;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (unsigned b = 0; b < 8; ++b)
            crc = (crc >> 1) ^ (0x82f63b78u & (uint32_t)-(int)(crc & 1u));
    }
    return ~crc;
}

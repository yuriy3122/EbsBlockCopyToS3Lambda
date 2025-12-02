#include <stdint.h>
#include <string.h>

#if defined(__GNUC__)
    #include <x86intrin.h>
#else
    #include <intrin.h>
#endif

// Computes a 64-bit fingerprint using two CRC32C streams combined.
uint64_t sse42_crc32(const uint64_t *data, size_t len)
{
    const uint8_t* p = (const uint8_t*)data;

    uint32_t crcA = 0xFFFFFFFFu;        // conventional CRC32C seed
    uint32_t crcB = 0xDEADBEEFu;        // independent second seed

    // Process 8-byte chunks (use memcpy to avoid alignment UB)
    while (len >= 8) {
        uint64_t v;
        memcpy(&v, p, 8);
        crcA = (uint32_t)_mm_crc32_u64((uint64_t)crcA, v);
        crcB = (uint32_t)_mm_crc32_u64((uint64_t)crcB, ~v);  // diversify stream
        p   += 8;
        len -= 8;
    }

    // Tail bytes
    while (len--) {
        uint8_t b = *p++;
        crcA = _mm_crc32_u8(crcA, b);
        crcB = _mm_crc32_u8(crcB, (uint8_t)~b);
    }

    // Finalize each CRC32C
    crcA = ~crcA;
    crcB = ~crcB;

    // Merge two 32-bit CRCs into one 64-bit fingerprint
    return ((uint64_t)crcA << 32) | (uint64_t)crcB;
}

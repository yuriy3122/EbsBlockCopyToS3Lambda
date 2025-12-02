#pragma once

#include <vector>
#include <cstdint>

// Result of processing a block of data.
struct BlockResult
{
    std::vector<uint8_t> compressedData;  // compressed payload
    uint64_t crc64 = 0;                   // 64-bit fingerprint of original data
};

// Encapsulates block processing logic: checksum + compression.
class BlockProcessor
{
public:
    // Computes a 64-bit CRC-like fingerprint of input data
    // and compresses the input using zlib.
    BlockResult Process(const std::vector<uint8_t>& input) const;
};

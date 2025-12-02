#include "block_processor.h"
#include "compression.h"
#include "crc32.h"

#include <stdexcept>

BlockResult BlockProcessor::Process(const std::vector<uint8_t>& input) const
{
    if (input.empty())
    {
        return BlockResult{};
    }

    BlockResult result;

    // 1. Compute CRC
    result.crc64 = sse42_crc32(reinterpret_cast<const uint64_t*>(input.data()), input.size());

    // 2. Compress the block
    result.compressedData.resize(input.size());
    size_t outSize = input.size();

    compress_raw_data(
        const_cast<uint8_t*>(input.data()),
        input.size(),
        result.compressedData.data(),
        outSize);

    if (outSize == 0 || outSize > result.compressedData.size())
    {
        throw std::runtime_error("Compression failed or produced invalid size");
    }

    result.compressedData.resize(outSize);
    return result;
}

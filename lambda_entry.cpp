#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <iterator>

#include "block_processor.h"
#include "s3_client.h"

// Simple CLI-style entry point intended to be used as a Lambda
// container image command or for local testing:
//   1) reads a binary block from stdin,
//   2) computes a CRC-like 64-bit fingerprint and compresses the block,
//   3) uploads the compressed block to S3,
//   4) prints a JSON response to stdout.
int main(int argc, char** argv)
{
    std::string bucket;
    std::string key;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if ((arg == "--bucket" || arg == "-b") && i + 1 < argc)
        {
            bucket = argv[++i];
        }
        else if ((arg == "--key" || arg == "-k") && i + 1 < argc)
        {
            key = argv[++i];
        }
    }

    if (bucket.empty() || key.empty())
    {
        std::cerr << "Usage: " << argv[0]
                  << " --bucket <bucket-name> --key <object-key>\n";
        return 1;
    }

    // Read all input bytes from stdin into memory.
    std::vector<uint8_t> input(
        (std::istreambuf_iterator<char>(std::cin)),
        std::istreambuf_iterator<char>());

    if (input.empty())
    {
        std::cerr << "No input data received on stdin.\n";
        return 1;
    }

    try
    {
        BlockProcessor processor;
        BlockResult result = processor.Process(input);

        S3Client s3;
        std::string error;
        bool ok = s3.UploadObject(bucket, key, result.compressedData, error);

        if (!ok)
        {
            std::cerr << error << "\n";
            std::cout << "{"
                         "\"success\": false,"
                         "\"bucket\": \"" << bucket << "\","
                         "\"key\": \"" << key << "\","
                         "\"crc64\": " << result.crc64 << ","
                         "\"error\": \"" << error << "\""
                         "}\n";
            return 1;
        }

        std::cout << "{"
                     "\"success\": true,"
                     "\"bucket\": \"" << bucket << "\","
                     "\"key\": \"" << key << "\","
                     "\"crc64\": " << result.crc64 << ","
                     "\"compressed_size\": " << result.compressedData.size() <<
                     "}\n";
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Exception: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}

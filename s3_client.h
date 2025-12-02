#pragma once

#include <string>
#include <vector>
#include <cstdint>

// Minimal S3 client wrapper used by the Lambda entry point.
// Assumes AWS credentials and region are provided by the environment.
class S3Client
{
public:
    S3Client();
    ~S3Client();

    // Uploads a binary object to S3.
    // Returns true on success and fills errorMessage on failure.
    bool UploadObject(
        const std::string& bucket,
        const std::string& key,
        const std::vector<uint8_t>& data,
        std::string& errorMessage);
};

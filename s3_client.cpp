#include "s3_client.h"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <sstream>

S3Client::S3Client()
{
    static Aws::SDKOptions options;
    static bool initialized = false;
    
    if (!initialized)
    {
        Aws::InitAPI(options);
        initialized = true;
    }
}

S3Client::~S3Client()
{
}

bool S3Client::UploadObject(
    const std::string& bucket,
    const std::string& key,
    const std::vector<uint8_t>& data,
    std::string& errorMessage)
{
    Aws::S3::S3Client client;

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key.c_str());

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadObjectStream");
    stream->write(reinterpret_cast<const char*>(data.data()), data.size());
    stream->seekg(0);

    request.SetBody(stream);

    auto outcome = client.PutObject(request);
    if (!outcome.IsSuccess())
    {
        std::ostringstream ss;
        ss << "S3 upload failed: "
           << outcome.GetError().GetExceptionName() << " - "
           << outcome.GetError().GetMessage();
        errorMessage = ss.str();
        return false;
    }

    return true;
}

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/ebs/EBSClient.h>
#include <aws/ebs/model/GetSnapshotBlockRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "block_processor.h"

using namespace Aws::Utils::Json;

static const size_t BLOCK_SIZE = 512 * 1024;

int main()
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    std::string inputStr((std::istreambuf_iterator<char>(std::cin)),
                          std::istreambuf_iterator<char>());
    JsonValue inputJson(inputStr);
    if (!inputJson.WasParseSuccessful()) {
        std::cerr << "Invalid JSON" << std::endl;
        return 1;
    }
    auto v = inputJson.View();

    Aws::String snapshotId = v.GetString("SnapshotId");
    int blockIndex = v.GetInteger("BlockIndex");
    Aws::String bucket = v.GetString("Bucket");
    Aws::String key = v.GetString("Key");

    Aws::EBS::EBSClient ebs;
    Aws::EBS::Model::GetSnapshotBlockRequest req;
    req.SetSnapshotId(snapshotId);
    req.SetBlockIndex(blockIndex);

    auto outcome = ebs.GetSnapshotBlock(req);
    if (!outcome.IsSuccess()) {
        std::cerr << outcome.GetError().GetMessage() << std::endl;
        Aws::ShutdownAPI(options);
        return 1;
    }

    auto &result = outcome.GetResult();
    auto body = result.GetData();

    std::vector<uint8_t> block(BLOCK_SIZE);
    body->read(reinterpret_cast<char*>(block.data()), BLOCK_SIZE);
    size_t bytesRead = body->gcount();
    block.resize(bytesRead);

    BlockProcessor processor;
    BlockResult processed = processor.Process(block);

    Aws::S3::S3Client s3;
    Aws::S3::Model::PutObjectRequest putReq;
    putReq.SetBucket(bucket);
    putReq.SetKey(key);

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadData");
    stream->write(reinterpret_cast<const char*>(processed.compressedData.data()), processed.compressedData.size());
    stream->seekg(0);
    putReq.SetBody(stream);

    auto putOutcome = s3.PutObject(putReq);
    bool success = putOutcome.IsSuccess();

    JsonValue out;
    out.WithBool("Success", success);
    out.WithString("SnapshotId", snapshotId);
    out.WithInteger("BlockIndex", blockIndex);
    out.WithString("Bucket", bucket);
    out.WithString("Key", key);
    out.WithInt64("CRC64", processed.crc64);
    out.WithInteger("CompressedSize", (int)processed.compressedData.size());
    if (!success) out.WithString("Error", putOutcome.GetError().GetMessage());

    std::cout << out.View().WriteReadable();
    Aws::ShutdownAPI(options);
    return success ? 0 : 1;
}

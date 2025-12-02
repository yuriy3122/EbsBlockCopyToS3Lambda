#include "list_blocks.h"

static const size_t BLOCK_SIZE = 512 * 1024;

BlockPageResult ProcessSnapshotPart(
    const Aws::String& snapshotId,
    int partId,
    int pageSize,
    const Aws::String& bucket,
    const Aws::String& keyPrefix)
{
    Aws::EBS::EBSClient ebs;

    Aws::EBS::Model::ListSnapshotBlocksRequest listReq;
    int startingIndex = partId * pageSize;

    listReq.SetSnapshotId(snapshotId);
    listReq.SetStartingBlockIndex(startingIndex);
    listReq.SetMaxResults(pageSize);

    auto listOutcome = ebs.ListSnapshotBlocks(listReq);
    if (!listOutcome.IsSuccess()) {
        throw std::runtime_error("ListSnapshotBlocks failed: " +
            listOutcome.GetError().GetMessage());
    }

    const auto& blocks = listOutcome.GetResult().GetBlocks();

    BlockPageResult summary;
    summary.snapshotId = snapshotId;
    summary.partId = partId;
    summary.blocksProcessed = 0;

    Aws::S3::S3Client s3;

    for (const auto& blockInfo : blocks)
    {
        int blockIndex = blockInfo.GetBlockIndex();
        Aws::String blockToken = blockInfo.GetBlockToken();

        Aws::EBS::Model::GetSnapshotBlockRequest getReq;
        getReq.SetSnapshotId(snapshotId);
        getReq.SetBlockIndex(blockIndex);
        getReq.SetBlockToken(blockToken);

        auto getOutcome = ebs.GetSnapshotBlock(getReq);
        if (!getOutcome.IsSuccess()) {
            throw std::runtime_error("GetSnapshotBlock failed: " +
                getOutcome.GetError().GetMessage());
        }

        auto body = getOutcome.GetResult().GetData();

        std::vector<uint8_t> block(BLOCK_SIZE);
        body->read(reinterpret_cast<char*>(block.data()), BLOCK_SIZE);
        size_t bytesRead = body->gcount();
        block.resize(bytesRead);

        BlockProcessor processor;
        BlockResult processed = processor.Process(block);

        Aws::String key = keyPrefix + "-" + std::to_string(blockIndex);

        Aws::S3::Model::PutObjectRequest putReq;
        putReq.SetBucket(bucket);
        putReq.SetKey(key);

        auto stream = Aws::MakeShared<Aws::StringStream>("UploadStream");
        stream->write(reinterpret_cast<const char*>(processed.compressedData.data()),
                      processed.compressedData.size());
        stream->seekg(0);

        putReq.SetBody(stream);

        auto putOutcome = s3.PutObject(putReq);
        if (!putOutcome.IsSuccess()) {
            throw std::runtime_error("S3 upload failed: " +
                putOutcome.GetError().GetMessage());
        }

        summary.uploadedKeys.push_back(key);
        summary.blocksProcessed++;
    }

    return summary;
}

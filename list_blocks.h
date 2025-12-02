#pragma once
#include <aws/core/Aws.h>
#include <aws/ebs/EBSClient.h>
#include <aws/ebs/model/ListSnapshotBlocksRequest.h>
#include <aws/ebs/model/GetSnapshotBlockRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <vector>

#include "block_processor.h"

struct BlockPageResult {
    Aws::String snapshotId;
    int partId;
    int blocksProcessed;
    Aws::Vector<Aws::String> uploadedKeys;
};

BlockPageResult ProcessSnapshotPart(
    const Aws::String& snapshotId,
    int partId,
    int pageSize,
    const Aws::String& bucket,
    const Aws::String& keyPrefix);

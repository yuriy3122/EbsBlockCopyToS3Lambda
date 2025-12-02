#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include "list_blocks.h"

using namespace Aws::Utils::Json;

int main() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    std::string inputStr((std::istreambuf_iterator<char>(std::cin)),
                         std::istreambuf_iterator<char>());
    JsonValue in(inputStr);
    if (!in.WasParseSuccessful()) {
        std::cerr << "Invalid JSON\n";
        return 1;
    }
    auto v = in.View();

    Aws::String snapshotId = v.GetString("SnapshotId");
    int partId = v.GetInteger("PartId");
    int pageSize = v.GetInteger("PageSize");
    Aws::String bucket = v.GetString("Bucket");
    Aws::String keyPrefix = v.GetString("KeyPrefix");

    BlockPageResult result = ProcessSnapshotPart(snapshotId, partId, pageSize, bucket, keyPrefix);

    JsonValue out;
    out.WithBool("Success", true);
    out.WithString("SnapshotId", snapshotId);
    out.WithInteger("PartId", partId);
    out.WithInteger("BlocksProcessed", result.blocksProcessed);

    Aws::Array<JsonValue> arr(result.uploadedKeys.size());
    for (size_t i = 0; i < result.uploadedKeys.size(); i++)
        arr[i].AsString(result.uploadedKeys[i]);
    out.WithArray("UploadedKeys", arr);

    std::cout << out.View().WriteReadable();
    Aws::ShutdownAPI(options);
    return 0;
}

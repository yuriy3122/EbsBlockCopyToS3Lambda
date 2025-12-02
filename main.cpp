// main.cpp

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/ebs/EBSClient.h>
#include <aws/ebs/model/ListSnapshotBlocksRequest.h>
#include <aws/ebs/model/GetSnapshotBlockRequest.h>

#include <zlib.h>
#include "minizip/zip.h"

#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>
#include <future>
#include <thread>
#include <cstring>
#include <stdexcept>
#include <sstream>
#include <cstdint>

using namespace aws::lambda_runtime;
using Aws::String;

// ------------------------- Структуры данных -------------------------

struct BlockHash {
    int BlockIndex;
    int Offset;
    std::string Hash;
};

struct InputParams {
    String S3Bucket;
    String VolumeId;
    String SnapshotId;
    String BackupId;
    String SnapshotRegion;
    String BucketRegion;
    String UserData;
    String EncryptionKey;
    int PartId = 0;
};

// ------------------------ Base64 для SSE-C ключа --------------------

static const char b64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string Base64Encode(const std::string& in) {
    std::string out;
    int val = 0;
    int valb = -6;
    for (uint8_t c : in) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(b64_table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(b64_table[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

// ---------------------- ZIP в память (zlib + minizip) ----------------

struct ZipMemBuffer {
    std::vector<uint8_t> data;
};

static voidpf ZCALLBACK mem_open(voidpf opaque, const char* /*filename*/, int /*mode*/) {
    return opaque; // opaque = ZipMemBuffer*
}

static uLong ZCALLBACK mem_write(voidpf opaque, voidpf /*stream*/, const void* buf, uLong size) {
    auto* mem = reinterpret_cast<ZipMemBuffer*>(opaque);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    mem->data.insert(mem->data.end(), p, p + size);
    return size;
}

static long ZCALLBACK mem_tell(voidpf opaque, voidpf /*stream*/) {
    auto* mem = reinterpret_cast<ZipMemBuffer*>(opaque);
    return static_cast<long>(mem->data.size());
}

// Для записи ZIP фактически seek практически не нужен,
// оставляем примитивную реализацию.
static long ZCALLBACK mem_seek(voidpf opaque, voidpf /*stream*/, uLong offset, int origin) {
    auto* mem = reinterpret_cast<ZipMemBuffer*>(opaque);
    size_t current = mem->data.size();
    size_t newPos = 0;

    if (origin == ZLIB_FILEFUNC_SEEK_CUR) {
        newPos = current + offset;
    } else if (origin == ZLIB_FILEFUNC_SEEK_END) {
        newPos = current + offset;
    } else if (origin == ZLIB_FILEFUNC_SEEK_SET) {
        newPos = offset;
    }

    if (newPos != current) {
        // minizip при записи обычно двигается только вперёд,
        // если вдруг понадобится — тут нужно будет расширять/обрезать буфер.
        return -1;
    }
    return 0;
}

static int ZCALLBACK mem_close(voidpf, voidpf) { return 0; }
static int ZCALLBACK mem_error(voidpf, voidpf) { return 0; }

// Аналог C# ZipData: один entry в ZIP, все данные подряд.
std::vector<uint8_t> ZipData(const uint8_t* buffer, size_t size) {
    ZipMemBuffer mem;

    zlib_filefunc_def filefunc32 = {};
    filefunc32.zopen_file = mem_open;
    filefunc32.zwrite_file = mem_write;
    filefunc32.ztell_file = mem_tell;
    filefunc32.zseek_file = mem_seek;
    filefunc32.zclose_file = mem_close;
    filefunc32.zerror_file = mem_error;
    filefunc32.opaque = &mem;

    zipFile zf = zipOpen2("dummy", APPEND_STATUS_CREATE, nullptr, &filefunc32);
    if (!zf) {
        throw std::runtime_error("zipOpen2 failed");
    }

    zip_fileinfo zi = {};
    int ret = zipOpenNewFileInZip2(
        zf,
        "zip.txt",        // имя файла внутри архива (как в C#)
        &zi,
        nullptr, 0,
        nullptr, 0,
        nullptr,
        Z_DEFLATED,
        Z_BEST_COMPRESSION,
        0
    );
    if (ret != ZIP_OK) {
        zipClose(zf, nullptr);
        throw std::runtime_error("zipOpenNewFileInZip2 failed");
    }

    ret = zipWriteInFileInZip(zf, buffer, static_cast<uLong>(size));
    if (ret != ZIP_OK) {
        zipCloseFileInZip(zf);
        zipClose(zf, nullptr);
        throw std::runtime_error("zipWriteInFileInZip failed");
    }

    zipCloseFileInZip(zf);
    zipClose(zf, nullptr);

    return mem.data;
}

// ------------------------ Парсинг JSON во вход ----------------------

InputParams ParseInput(const String& json) {
    Aws::Utils::Json::JsonValue root(json);
    if (!root.WasParseSuccessful()) {
        throw std::runtime_error("Failed to parse input JSON");
    }

    auto v = root.View();
    InputParams p;
    p.S3Bucket       = v.GetString("S3Bucket");
    p.VolumeId       = v.GetString("VolumeId");
    p.SnapshotId     = v.GetString("SnapshotId");
    p.BackupId       = v.GetString("BackupId");
    p.SnapshotRegion = v.GetString("SnapshotRegion");
    p.BucketRegion   = v.GetString("BucketRegion");
    if (v.ValueExists("UserData"))      p.UserData      = v.GetString("UserData");
    if (v.ValueExists("EncryptionKey")) p.EncryptionKey = v.GetString("EncryptionKey");
    p.PartId = v.GetInteger("PartId");
    return p;
}

// ----------------------------- AWS helpers --------------------------

BlockHash DownloadEbsBlock(
    Aws::EBS::EBSClient& ebsClient,
    const String& snapshotId,
    const Aws::EBS::Model::Block& block,
    uint8_t* buffer,
    int offset,
    int ebsBlockSize)
{
    Aws::EBS::Model::GetSnapshotBlockRequest req;
    req.SetSnapshotId(snapshotId);
    req.SetBlockIndex(block.GetBlockIndex());
    req.SetBlockToken(block.GetBlockToken());

    auto outcome = ebsClient.GetSnapshotBlock(req);
    if (!outcome.IsSuccess()) {
        throw std::runtime_error("GetSnapshotBlock failed: " +
                outcome.GetError().GetMessage());
    }

    auto result = std::move(outcome.GetResultWithOwnership());
    int dataLen = result.GetDataLength();
    auto& body = result.GetBlockData();

    body.read(reinterpret_cast<char*>(buffer + offset), dataLen);
    if (!body) {
        throw std::runtime_error("Failed to read block data stream");
    }

    BlockHash bh;
    bh.BlockIndex = block.GetBlockIndex();
    bh.Offset     = offset;
    bh.Hash       = result.GetChecksum().c_str();
    return bh;
}

std::unordered_map<int, std::string> DownloadBlockHashData(
    Aws::S3::S3Client& s3,
    const InputParams& input)
{
    std::unordered_map<int, std::string> hashes;

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(input.S3Bucket);
    String key = input.VolumeId + "/metadata/hashes/" + std::to_string(input.PartId + 1);
    req.SetKey(key);

    auto outcome = s3.GetObject(req);
    if (!outcome.IsSuccess()) {
        // как в C#: просто возвращаем пустой словарь при ошибке
        return hashes;
    }

    auto result = outcome.GetResultWithOwnership();
    auto& stream = result.GetBody();

    std::string data((std::istreambuf_iterator<char>(stream)),
                     std::istreambuf_iterator<char>());

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data.data());
    size_t size = data.size();
    size_t pos = 0;

    if (size < sizeof(int)) return hashes;

    int count = 0;
    std::memcpy(&count, ptr + pos, sizeof(int));
    pos += sizeof(int);

    for (int i = 0; i < count; ++i) {
        if (pos + sizeof(int) > size) break;
        int index = 0;
        std::memcpy(&index, ptr + pos, sizeof(int));
        pos += sizeof(int);

        if (pos + sizeof(uint16_t) > size) break;
        uint16_t len = 0;
        std::memcpy(&len, ptr + pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        if (pos + len > size) break;
        std::string hash(reinterpret_cast<const char*>(ptr + pos), len);
        pos += len;

        hashes[index] = hash;
    }

    return hashes;
}

void UploadBlockHashData(
    Aws::S3::S3Client& s3,
    const InputParams& input,
    const std::unordered_map<int, std::string>& blockHashes)
{
    std::string data;
    auto append_int = [&](int value) {
        char buf[sizeof(int)];
        std::memcpy(buf, &value, sizeof(int));
        data.append(buf, sizeof(int));
    };
    auto append_ushort = [&](uint16_t value) {
        char buf[sizeof(uint16_t)];
        std::memcpy(buf, &value, sizeof(uint16_t));
        data.append(buf, sizeof(uint16_t));
    };

    append_int(static_cast<int>(blockHashes.size()));

    for (const auto& kv : blockHashes) {
        append_int(kv.first);

        const std::string& hash = kv.second;
        uint16_t len = static_cast<uint16_t>(hash.size());
        append_ushort(len);
        data.append(hash.data(), hash.size());
    }

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(input.S3Bucket);
    String key = input.VolumeId + "/metadata/hashes/" + std::to_string(input.PartId + 1);
    req.SetKey(key);

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadBlockHashData");
    stream->write(data.data(), data.size());
    req.SetBody(stream);

    auto outcome = s3.PutObject(req);
    if (!outcome.IsSuccess()) {
        throw std::runtime_error("PutObject (hashes) failed: " +
                                 outcome.GetError().GetMessage());
    }
}

void UploadChangedBlockIndices(
    Aws::S3::S3Client& s3,
    const InputParams& input,
    const std::vector<int>& changedBlockIndices)
{
    std::string data;
    auto append_int = [&](int value) {
        char buf[sizeof(int)];
        std::memcpy(buf, &value, sizeof(int));
        data.append(buf, sizeof(int));
    };

    int header = 1;
    append_int(header);
    append_int(static_cast<int>(changedBlockIndices.size()));

    for (int idx : changedBlockIndices) {
        append_int(idx);
    }

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(input.S3Bucket);
    String key = input.VolumeId + "/backups/" + input.BackupId +
                 "/changedblocks/" + std::to_string(input.PartId + 1);
    req.SetKey(key);

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadChangedBlockIndices");
    stream->write(data.data(), data.size());
    req.SetBody(stream);

    auto outcome = s3.PutObject(req);
    if (!outcome.IsSuccess()) {
        throw std::runtime_error("PutObject (changed indices) failed: " +
                                 outcome.GetError().GetMessage());
    }
}

void UploadEbsBlockBatch(
    Aws::S3::S3Client& s3,
    const InputParams& input,
    int blockBatchIndex,
    const std::vector<uint8_t>& buffer)
{
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(input.S3Bucket);

    // VolumeId_BackupId_{batchIndex+1}
    String key = input.VolumeId + "_" + input.BackupId + "_" +
                 std::to_string(blockBatchIndex + 1);
    req.SetKey(key);

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadEbsBlockBatch");
    stream->write(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    req.SetBody(stream);

    // Тег
    Aws::S3::Model::Tag tag;
    String tagKey = input.UserData.empty() ? "calamu" : input.UserData;
    tag.SetKey(tagKey);
    tag.SetValue("");
    Aws::Vector<Aws::S3::Model::Tag> tags;
    tags.push_back(tag);
    Aws::S3::Model::Tagging tagging;
    tagging.SetTagSet(tags);
    req.SetTagging(tagging);

    // SSE-C (как в C# — base64 ключа)
    if (!input.EncryptionKey.empty()) {
        std::string b64 = Base64Encode(input.EncryptionKey.c_str());
        req.SetSSECustomerAlgorithm("AES256");
        req.SetSSECustomerKey(b64.c_str());
    }

    auto outcome = s3.PutObject(req);
    if (!outcome.IsSuccess()) {
        throw std::runtime_error("PutObject (block batch) failed: " +
                                 outcome.GetError().GetMessage());
    }
}

// -------------------- Основная логика (аналог FunctionHandler) ------

void RunFunction(const InputParams& input) {
    using namespace std::chrono;

    const int EbsBlockSize           = 512 * 1024;
    const int EbsBlockBatchSize      = 100;
    const int ListEbsBlocksMaxResult = 1000;
    const int BlockRequestBatchSize  = 20;

    std::vector<uint8_t> buffer(EbsBlockSize * EbsBlockBatchSize);
    std::vector<uint8_t> changedBlockBuffer(EbsBlockSize * EbsBlockBatchSize);

    Aws::Client::ClientConfiguration ebsCfg;
    ebsCfg.region = input.SnapshotRegion;
    Aws::EBS::EBSClient ebsClient(ebsCfg);

    Aws::Client::ClientConfiguration s3Cfg;
    s3Cfg.region = input.BucketRegion;
    Aws::S3::S3Client s3Client(s3Cfg);

    Aws::EBS::Model::ListSnapshotBlocksRequest listReq;
    listReq.SetSnapshotId(input.SnapshotId);
    listReq.SetStartingBlockIndex(input.PartId * ListEbsBlocksMaxResult);
    listReq.SetMaxResults(ListEbsBlocksMaxResult);

    auto listOutcome = ebsClient.ListSnapshotBlocks(listReq);
    if (!listOutcome.IsSuccess()) {
        throw std::runtime_error("ListSnapshotBlocks failed: " +
                                 listOutcome.GetError().GetMessage());
    }
    auto listResp = listOutcome.GetResult();

    std::vector<int> changedBlockIndices;

    auto ebsBlockHashTable = DownloadBlockHashData(s3Client, input);

    auto start = steady_clock::now();

    // 1000 / 100 = 10 батчей по 100 блоков
    const int perPartBatches = ListEbsBlocksMaxResult / EbsBlockBatchSize;

    for (int i = 0; i < perPartBatches; ++i) {
        std::vector<Aws::EBS::Model::Block> blocks;

        int startIndex = listReq.GetStartingBlockIndex() + i * EbsBlockBatchSize;
        int endIndex   = startIndex + EbsBlockBatchSize;

        for (const auto& blk : listResp.GetBlocks()) {
            int bi = blk.GetBlockIndex();
            if (bi >= startIndex && bi < endIndex) {
                blocks.push_back(blk);
            }
        }

        if (blocks.empty())
            continue;

        int count = static_cast<int>(
            (blocks.size() + BlockRequestBatchSize - 1) / BlockRequestBatchSize);

        int offset = 0;
        std::vector<BlockHash> changedBlocks;

        for (int j = 0; j < count; ++j) {
            auto sw1_start = steady_clock::now();

            int beginIdx = j * BlockRequestBatchSize;
            int endIdxB  = std::min<int>(beginIdx + BlockRequestBatchSize,
                                         static_cast<int>(blocks.size()));

            std::vector<std::future<BlockHash>> tasks;
            for (int k = beginIdx; k < endIdxB; ++k) {
                const auto& blk = blocks[k];
                int thisOffset = offset;
                offset += EbsBlockSize;

                tasks.emplace_back(std::async(std::launch::async,
                    [&ebsClient, &input, &blk, thisOffset, &buffer, EbsBlockSize]() {
                        return DownloadEbsBlock(
                            const_cast<Aws::EBS::EBSClient&>(ebsClient),
                            input.SnapshotId,
                            blk,
                            buffer.data(),
                            thisOffset,
                            EbsBlockSize);
                    }
                ));
            }

            for (auto& f : tasks) {
                BlockHash bh = f.get();
                auto it = ebsBlockHashTable.find(bh.BlockIndex);
                if (it == ebsBlockHashTable.end() || it->second != bh.Hash) {
                    ebsBlockHashTable[bh.BlockIndex] = bh.Hash;
                    changedBlocks.push_back(bh);
                    changedBlockIndices.push_back(bh.BlockIndex);
                }
            }

            auto sw1_end = steady_clock::now();
            auto elapsedMs =
                duration_cast<milliseconds>(sw1_end - sw1_start).count();

            if (elapsedMs < 1000) {
                std::this_thread::sleep_for(
                    milliseconds(1000 - elapsedMs)); // аналог Task.Delay
            }
        }

        if (!changedBlocks.empty()) {
            for (size_t j = 0; j < changedBlocks.size(); ++j) {
                std::memcpy(
                    changedBlockBuffer.data() + j * EbsBlockSize,
                    buffer.data() + changedBlocks[j].Offset,
                    EbsBlockSize);
            }

            std::vector<uint8_t> cmpBuffer =
                ZipData(changedBlockBuffer.data(),
                        changedBlocks.size() * EbsBlockSize);

            int blockBatchIndex = perPartBatches * input.PartId + i;

            UploadEbsBlockBatch(s3Client, input, blockBatchIndex, cmpBuffer);
        }
    }

    UploadChangedBlockIndices(s3Client, input, changedBlockIndices);

    if (!changedBlockIndices.empty()) {
        UploadBlockHashData(s3Client, input, ebsBlockHashTable);
    }

    auto end = steady_clock::now();
    auto elapsed = duration_cast<milliseconds>(end - start).count();
    if (elapsed < 1000) {
        std::this_thread::sleep_for(milliseconds(1000 - elapsed));
    }
}

// ----------------------- Lambda handler + main ----------------------

invocation_response handler(invocation_request const& req) {
    try {
        InputParams input = ParseInput(req.payload);
        RunFunction(input);
        return invocation_response::success("", "application/json");
    } catch (const std::exception& ex) {
        return invocation_response::failure(ex.what(), "FunctionError");
    }
}

int main() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        run_handler(handler);
    }
    Aws::ShutdownAPI(options);
    return 0;
}

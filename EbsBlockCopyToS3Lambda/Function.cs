using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Threading.Tasks;
using Amazon;
using Amazon.EBS;
using Amazon.EBS.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Lambda.Core;
using Tag = Amazon.S3.Model.Tag;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace EbsBlockCopyToS3Lambda
{
    public class BlockHash
    {
        public int BlockIndex { get; set; }
        public int Offset { get; set; }
        public string Hash { get; set; }
    }

    public class ChangedBlockArea
    {
        public int StartIndex { get; set; }
        public int EndIndex { get; set; }
    }

    public class InputParams
    {
        public string S3Bucket { get; set; }
        public string VolumeId { get; set; }
        public string SnapshotId { get; set; }
        public string BackupId { get; set; }
        public string SnapshotRegion { get; set; }
        public string BucketRegion { get; set; }
        public string UserData { get; set; }
        public string EncryptionKey { get; set; }
        public int PartId { get; set; }
    }

    public class Function
    {
        public string FunctionHandler(InputParams input, ILambdaContext context)
        {
            var task = Task.Run(async () =>
            {
                var sw = Stopwatch.StartNew();

                const int EbsBlockSize = 512 * 1024;
                const int EbsBlockBatchSize = 100;
                const int ListEbsBlocksMaxResult = 1000;

                byte[] buffer = new byte[EbsBlockSize * EbsBlockBatchSize];
                byte[] changedBlockBuffer = new byte[EbsBlockSize * EbsBlockBatchSize];

                using var ebsClient = new AmazonEBSClient(RegionEndpoint.GetBySystemName(input.SnapshotRegion));
                using var s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(input.BucketRegion));

                var request = new ListSnapshotBlocksRequest 
                { 
                    SnapshotId = input.SnapshotId, 
                    StartingBlockIndex = input.PartId * ListEbsBlocksMaxResult, 
                    MaxResults = ListEbsBlocksMaxResult 
                };

                var listSnapshotBlocksResponse = await ebsClient.ListSnapshotBlocksAsync(request);

                var changedBlockIndices = new List<int>();
                var ebsBlockHashTable = await DownloadBlockHashData(s3Client, input);

                for (int i = 0; i < ListEbsBlocksMaxResult / EbsBlockBatchSize; i++)
                {
                    var blocks = new List<Block>();

                    int startIndex = request.StartingBlockIndex + i * EbsBlockBatchSize;
                    int endIndex = startIndex + EbsBlockBatchSize;

                    foreach (var block in listSnapshotBlocksResponse.Blocks)
                    {
                        if (block.BlockIndex >= startIndex && block.BlockIndex < endIndex)
                        {
                            blocks.Add(block);
                        }
                    }

                    const int blockRequestBatchSize = 20;
                    int count = decimal.ToInt32(Math.Ceiling((decimal)blocks.Count / blockRequestBatchSize));

                    int offset = 0;
                    var changedBlocks = new List<BlockHash>();

                    for (int j = 0; j < count; j++)
                    {
                        var sw1 = Stopwatch.StartNew();

                        var blockBatch = blocks.Skip(j * blockRequestBatchSize).Take(blockRequestBatchSize).ToList();

                        var tasks = new List<Task>();

                        foreach (var block in blockBatch)
                        {
                            tasks.Add(DownloadEbsBlock(ebsClient, input.SnapshotId, block, buffer, offset));
                            offset += EbsBlockSize;
                        }

                        await Task.WhenAll(tasks);

                        foreach (var task in tasks)
                        {
                            var blockHash = ((Task<BlockHash>)task).Result;
                            
                            if (!ebsBlockHashTable.ContainsKey(blockHash.BlockIndex) || blockHash.Hash != ebsBlockHashTable[blockHash.BlockIndex])
                            {
                                ebsBlockHashTable[blockHash.BlockIndex] = blockHash.Hash;
                                changedBlocks.Add(blockHash);
                                changedBlockIndices.Add(blockHash.BlockIndex);
                            }
                        }

                        sw1.Stop();
                        int elapsedMilliseconds = (int)sw1.ElapsedMilliseconds;

                        if (elapsedMilliseconds < 1000)
                        {
                            //Prevent AWS API Rate limit exceptions
                            await Task.Delay(1000 - elapsedMilliseconds);
                        }
                    }

                    if (changedBlocks.Count > 0)
                    {
                        for (int j = 0; j < changedBlocks.Count; j++)
                        {
                            Buffer.BlockCopy(buffer, changedBlocks[j].Offset, changedBlockBuffer, j * EbsBlockSize, EbsBlockSize);
                        }

                        byte[] cmpBuffer = ZipData(changedBlockBuffer, changedBlocks.Count * EbsBlockSize);

                        int blockBatchIndex = (ListEbsBlocksMaxResult / EbsBlockBatchSize) * input.PartId + i;

                        await UploadEbsBlockBatch(s3Client, input, blockBatchIndex, cmpBuffer);
                    }
                }

                await UploadChangedBlockIndices(s3Client, input, changedBlockIndices);

                if (changedBlockIndices.Count > 0)
                {
                    await UploadBlockHashData(s3Client, input, ebsBlockHashTable);
                }

                sw.Stop();
                int elapsed = (int)sw.ElapsedMilliseconds;

                if (elapsed < 1000)
                {
                    //Prevent AWS API Rate limit exceptions
                    await Task.Delay(1000 - elapsed);
                }
            });

            task.Wait();

            return string.Empty;
        }

        private async Task UploadEbsBlockBatch(AmazonS3Client s3, InputParams input, int blockBatchIndex, byte[] buffer)
        {
            using var stream = new MemoryStream(buffer);
            stream.Position = 0;

            var request = new PutObjectRequest
            {
                BucketName = input.S3Bucket,
                Key = $"{input.VolumeId}_{input.BackupId}_{(blockBatchIndex + 1)}",
                InputStream = stream
            };

            var tagKey = string.IsNullOrEmpty(input.UserData) ? "calamu" : input.UserData;
            request.TagSet = new List<Tag>() { new Tag() { Key = tagKey, Value = string.Empty } };

            if (!string.IsNullOrEmpty(input.EncryptionKey))
            {
                request.ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256;
                request.ServerSideEncryptionCustomerProvidedKey = Base64Encode(input.EncryptionKey);
            }

            await s3.PutObjectAsync(request);
        }

        private async Task UploadChangedBlockIndices(AmazonS3Client s3, InputParams input, List<int> changedBlockIndices)
        {
            using var stream = new MemoryStream();
            int header = 1;
            stream.Write(BitConverter.GetBytes(header), 0, sizeof(int));
            stream.Write(BitConverter.GetBytes(changedBlockIndices.Count), 0, sizeof(int));

            foreach (var blockIndex in changedBlockIndices)
            {
                stream.Write(BitConverter.GetBytes(blockIndex), 0, sizeof(int));
            }

            var request = new PutObjectRequest
            {
                BucketName = input.S3Bucket,
                Key = $"{input.VolumeId}/backups/{input.BackupId}/changedblocks/{(input.PartId + 1)}",
                InputStream = stream
            };

            await s3.PutObjectAsync(request);
        }

        private async Task<Dictionary<int, string>> DownloadBlockHashData(AmazonS3Client s3, InputParams input)
        {
            var hashes = new Dictionary<int, string>();

            try
            {
                var objectRequest = new GetObjectRequest
                {
                    BucketName = input.S3Bucket,
                    Key = $"{input.VolumeId}/metadata/hashes/{input.PartId + 1}",
                };

                var response = await s3.GetObjectAsync(objectRequest);

                byte[] buffer = new byte[response.ContentLength];

                using var responseStream = response.ResponseStream;
                using var memoryStream = new MemoryStream();
                await responseStream.CopyToAsync(memoryStream);
                memoryStream.Position = 0;
                memoryStream.Read(buffer, 0, sizeof(int));

                int count = BitConverter.ToInt32(buffer, 0);

                for (int i = 0; i < count; i++)
                {
                    memoryStream.Read(buffer, 0, sizeof(int));
                    int index = BitConverter.ToInt32(buffer, 0);

                    memoryStream.Read(buffer, 0, sizeof(ushort));
                    ushort hashLength = BitConverter.ToUInt16(buffer, 0);
                    byte[] bytes = new byte[hashLength];

                    memoryStream.Read(bytes, 0, hashLength);

                    hashes[index] = new UTF8Encoding().GetString(bytes);
                }
            }
            catch { }

            return hashes;
        }

        private async Task UploadBlockHashData(AmazonS3Client s3, InputParams input, Dictionary<int, string> blockHashes)
        {
            using var stream = new MemoryStream();
            stream.Write(BitConverter.GetBytes(blockHashes.Count), 0, sizeof(int));

            foreach (var blockHash in blockHashes)
            {
                stream.Write(BitConverter.GetBytes(blockHash.Key), 0, sizeof(int));

                ushort len = (ushort)blockHash.Value.Length;
                stream.Write(BitConverter.GetBytes(len), 0, sizeof(ushort));

                byte[] buffer = new UTF8Encoding().GetBytes(blockHash.Value);
                stream.Write(buffer, 0, buffer.Length);
            }

            var request = new PutObjectRequest
            {
                BucketName = input.S3Bucket,
                Key = $"{input.VolumeId}/metadata/hashes/{input.PartId + 1}",
                InputStream = stream
            };

            await s3.PutObjectAsync(request);
        }

        private byte[] ZipData(byte[] buffer, int count)
        {
            byte[] cmpBuffer = null;

            using (MemoryStream memoryStream = new MemoryStream())
            {
                using (var zip = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    var zipItem = zip.CreateEntry("zip.txt");

                    using var sourceMemoryStream = new MemoryStream(buffer, 0, count);
                    using Stream entryStream = zipItem.Open();
                    sourceMemoryStream.CopyTo(entryStream);
                }

                cmpBuffer = memoryStream.ToArray();
            }

            return cmpBuffer;
        }

        private async Task<BlockHash> DownloadEbsBlock(AmazonEBSClient ebsClient, string snapshotId, Block block, byte[] buffer, int offset)
        {
            var request = new GetSnapshotBlockRequest
            {
                SnapshotId = snapshotId,
                BlockIndex = block.BlockIndex,
                BlockToken = block.BlockToken
            };

            var response = await ebsClient.GetSnapshotBlockAsync(request);

            using (var responseStream = response.BlockData)
            {
                using var memoryStream = new MemoryStream();
                await responseStream.CopyToAsync(memoryStream);
                memoryStream.Position = 0;
                memoryStream.Read(buffer, offset, response.DataLength);
            }

            return new BlockHash() { BlockIndex = block.BlockIndex, Hash = response.Checksum, Offset = offset };
        }

        private string Base64Encode(string decodedData)
        {
            var decodedDataBytes = Encoding.UTF8.GetBytes(decodedData);
            var base64EncodedData = Convert.ToBase64String(decodedDataBytes);
            return base64EncodedData;
        }
    }
}
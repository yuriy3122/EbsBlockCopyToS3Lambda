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
    public class Function
    {
        public string FunctionHandler(string input, ILambdaContext context)
        {
            var task = Task.Run(async () =>
            {
                await Task.Delay(1);

                Console.WriteLine(input);
            });

            task.Wait();

            return input;
        }
    }
}
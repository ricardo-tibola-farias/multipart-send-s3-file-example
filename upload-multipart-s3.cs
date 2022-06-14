

private RegionEndpoint BucketRegion => Uri.Region;
private AmazonS3Client _amazonClient;
private AmazonS3Client _s3Client => _amazonClient ?? (_amazonClient = new AmazonS3Client(BucketRegion));

public async Task UploadCompactedFiles(string[] filesKeys, string outputFileKey)
{
    using (var zipStream = new MemoryStream())
    {
        using (var zipArchive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
        {
            foreach (var fileKey in filesKeys)
            {
                var response = await _s3Client.GetObjectAsync([BucketName], fileKey);

                if (!FileFormat.TryParse(response.Headers.ContentType, out var fileFormat))
                {
                    throw new Exception($"Invalid file format: {response?.Headers?.ContentType}");
                }

                var fileName = Path.ChangeExtension(Path.GetFileNameWithoutExtension(fileKey), fileFormat.Extension);
                var zipEntry = zipArchive.CreateEntry(fileName);

                using (var zipEntryStream = zipEntry.Open())
                using (var fileContent = response.ResponseStream)
                {
                    fileContent.CopyTo(zipEntryStream);
                }
            }
        }

       zipStream.Seek(0, SeekOrigin.Begin);

        var contentLength = zipStream.Length;
        var partSize = 104857600;// 100 MB
        var filePosition = 0;

        var uploadId = "";
        var prevTags = "";

        for (var i = 0; filePosition < contentLength; i++)
        {
            bool lastPart = filePosition >= (contentLength - partSize);
            var result = await UploadChunk(outputFileKey, uploadId, i, lastPart, zipStream, prevTags, partSize);

            if (!lastPart)
            {
                var idAndTags = result.Split("|");
                uploadId = idAndTags[0];
                prevTags = idAndTags[1];
            }


            filePosition += partSize;
        };


        //method without use multipart

        //var putObject = new PutObjectRequest
        //{
        //    Key = outputFileKey,
        //    BucketName = [BucketName],
        //    InputStream = zipStream,
        //    ContentType = "application/zip",
        //    CannedACL = S3CannedACL.PublicRead,
        //    TagSet = new List<Tag> { new Tag() { Key = "expireMonth", Value = "1" } }
        //};
        //await FotoGoS3Client.PutObjectAsync(putObject);
    }
}

private async Task<string> UploadChunk(string outputKey, string uploadId, int chunkIndex, bool lastPart, MemoryStream stream, string prevETags, long partSize)
{
    var response = "";

    try
    {
        // Retreiving Previous ETags
        var eTags = new List<PartETag>();
        if (!string.IsNullOrEmpty(prevETags))
        {
            eTags = SetAllETags(prevETags);
        }

        var partNumber = chunkIndex + 1;

        //Step 1: build and send a multi upload request
        if (chunkIndex == 0)
        {
            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = [BucketName],
                Key = outputKey,
                ContentType = "application/zip",
                CannedACL = S3CannedACL.PublicRead,
                TagSet = new List<Tag> { new Tag() { Key = "expireMonth", Value = "1" } }
            };

            var initResponse = await FotoGoS3Client.InitiateMultipartUploadAsync(initiateRequest);
            uploadId = initResponse.UploadId;
        }

        //Step 2: upload each chunk (this is run for every chunk unlike the other steps which are run once)
        var uploadRequest = new UploadPartRequest
        {
            BucketName = BucketNameFotogo,
            Key = outputKey,
            UploadId = uploadId,
            PartNumber = partNumber,
            InputStream = stream,
            IsLastPart = lastPart,
            PartSize = partSize
        };

        var uploadResponse = await FotoGoS3Client.UploadPartAsync(uploadRequest);


        //Step 3: build and send the multipart complete request
        if (lastPart)
        {
            eTags.Add(new PartETag
            {
                PartNumber = partNumber,
                ETag = uploadResponse.ETag
            });

            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = BucketNameFotogo,
                Key = outputKey,
                UploadId = uploadId,
                PartETags = eTags
            };

            var result = await FotoGoS3Client.CompleteMultipartUploadAsync(completeRequest);
        }
        else
        {
            eTags.Add(new PartETag
            {
                PartNumber = partNumber,
                ETag = uploadResponse.ETag
            });

            //Set the uploadId and eTags with the response
            response = uploadRequest.UploadId + "|" + GetAllETags(eTags);
        }
    }
    catch (Exception e)
    {
        throw e;
    }

    return response;
}

private List<PartETag> SetAllETags(string prevETags)
{
    var partETags = new List<PartETag>();
    var splittedPrevETags = prevETags.Split(',');

    for (int i = 0; i < splittedPrevETags.Length; i++)
    {
        partETags.Add(new PartETag
        {
            PartNumber = Int32.Parse(splittedPrevETags[i]),
            ETag = splittedPrevETags[i + 1]
        });

        i = i + 1;
    }

    return partETags;
}

private string GetAllETags(List<PartETag> newETags)
{
    var newPartETags = "";
    var isNotFirstTag = false;

    foreach (var eTag in newETags)
    {
        newPartETags += ((isNotFirstTag) ? "," : "") + (eTag.PartNumber.ToString() + ',' + eTag.ETag);
        isNotFirstTag = true;
    }

    return newPartETags;
}

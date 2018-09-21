using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace COLT
{
    public class s3Manager
    {
        private string _bucketName;
        private string _awsAccessKey;
        private string _awsSecretKey;
        private string _awsServiceUrl;
        private AmazonS3Client _s3Client;

        public s3Manager(string awsAccessKey, string awsSecretKey, string awsServiceUrl, string bucketName)
        {
            _bucketName = bucketName;
            _awsAccessKey = awsAccessKey;
            _awsSecretKey = awsSecretKey;
            _awsServiceUrl = awsServiceUrl;
            AmazonS3Config config = new AmazonS3Config
            {
                ServiceURL = _awsServiceUrl
            };

            _s3Client = new AmazonS3Client(_awsAccessKey, _awsSecretKey, config);
        }
        public string GeneratePreSignedURL(int durationInMinutes, string key)
        {
            string urlString = "";
            try
            {
                GetPreSignedUrlRequest request = new GetPreSignedUrlRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    Expires = DateTime.Now.AddMinutes(durationInMinutes)
                };
                urlString = _s3Client.GetPreSignedURL(request);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            return urlString;
        }


        public async Task UploadFileAsync(byte[] bytes, string name)
        {

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                CannedACL = S3CannedACL.Private,
                Key = string.Format("{0}", name)
            };
            using (var ms = new MemoryStream(bytes))
            {
                request.InputStream = ms;
                await _s3Client.PutObjectAsync(request);
            }
        }

        public async Task<List<string>> GetFileListAsync(string prefix)
        {
            List<string> objects = new List<string>();

            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = _bucketName;
            request.Prefix = prefix;

            do
            {
                ListObjectsResponse response = await _s3Client.ListObjectsAsync(request);

                foreach (var obj in response.S3Objects)
                    objects.Add(obj.Key);


                if (response.IsTruncated)
                {
                    request.Marker = response.NextMarker;
                }
                else
                {
                    request = null;
                }
            } while (request != null);

            return objects;
        }

    }

}


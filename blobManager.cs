using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace COLT
{
    public class blobManager
    {

        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _container;


        public blobManager(string blobStorageAccountConnectionString, string blobContainer)
        {
            _storageAccount = CloudStorageAccount.Parse(blobStorageAccountConnectionString);
            _blobClient = _storageAccount.CreateCloudBlobClient();
            _container = _blobClient.GetContainerReference(blobContainer);
            _container.CreateIfNotExistsAsync();
        }
        public string GeneratePreSignedURL(int durationInMinutes, string key)
        {

            var blob = _container.GetBlobReference(key);

            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddMinutes(durationInMinutes);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

            string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);
            Uri sourceUri = new Uri(blob.Uri + sasBlobToken);


            return sourceUri.ToString();
        }


        public async Task UploadFileAsync(byte[] bytes, string name)
        {

            var blob = _container.GetBlockBlobReference(name);

            await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);


        }

        public async Task<List<string>> GetFileListAsync(string prefix)
        {
            List<string> objects = new List<string>();

            BlobContinuationToken continuationToken = null;
            
            do
            {
                var response = await _container.ListBlobsSegmentedAsync(prefix, continuationToken);
                continuationToken = response.ContinuationToken;
                foreach(CloudBlockBlob blob in response.Results)
                {
                    objects.Add(blob.Name);
                }
            }
            while (continuationToken != null);

            return objects;
        }

    }

}



using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;


namespace COLT
{
    public class gcsManager
    {
        private string _bucketName;
        private string _projectid;
        private StorageClient _gcsClient;
        private GoogleCredential _cred;
        private string _jsonCred;

        public gcsManager(string cloudCredentialJson, string bucketName, string projectid)
        {
            _bucketName = bucketName;
            _projectid = projectid;
            _jsonCred = cloudCredentialJson;
            _cred = GoogleCredential.FromJson(cloudCredentialJson);
            _gcsClient = StorageClient.Create(_cred);


        }
        public string GeneratePreSignedURL(int durationInMinutes, string key)
        {
            string urlString = "";

            var cred = _cred.UnderlyingCredential as ServiceAccountCredential;
            UrlSigner urlSigner = UrlSigner.FromServiceAccountCredential(cred);

            urlString = urlSigner.Sign(
               _bucketName,
               key,
               TimeSpan.FromMinutes(durationInMinutes),
               HttpMethod.Get);

            return urlString;
        }


        public void UploadFile(byte[] bytes, string name)
        {

            _gcsClient.UploadObject(_bucketName, name, "application/octet-stream",  new MemoryStream(bytes));
                       
        }

        public List<string> GetFileList(string prefix)
        {
            List<string> objects = new List<string>();


            foreach (var obj in _gcsClient.ListObjects(_bucketName, prefix))
            {
                objects.Add(obj.Name);
            }


            return objects;
        }

    }

}


using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace COLT
{


    public class IPInfo
    {
        public string ip { get; set; }
        public string city { get; set; }
        public string region { get; set; }
        public string country { get; set; }
        public string loc { get; set; }
        public string postal { get; set; }
        public string org { get; set; }
    }

    public class COLTJob
    {
        public Config config { get; set; }
        public Job[] jobs { get; set; }
    }

    public class Config
    {
        public Cloud[] cloud { get; set; }
        public string output { get; set; }
    }

    public class Cloud
    {
        public string name { get; set; }
        public string type { get; set; }
        public string awsAccessKeySecret { get; set; }
        public string awsAccessKey { get; set; }
        public string awsS3bucket { get; set; }
        public string gcsbucket { get; set; }
        public string awsServiceUrl { get; set; }
        public string blobStorageAccountConnectionString { get; set; }
        public string blobContainer { get; set; }
        public string credential { get; set; }
        public string projectid { get; set; }

    }

    public class Job
    {
        public string name { get; set; }
        public string type { get; set; }
        public string cloud { get; set; }
        public int fileCount { get; set; }
        public int threads { get; set; }
        public int fileSizeInBytes { get; set; }
        public string filePrefix { get; set; }
        public int downloadCount { get; set; }
    }

    public class JobManager
    {
        public COLTJob job;

        public JobManager(string configAsJson)
        {
            job = JsonConvert.DeserializeObject<COLTJob>(configAsJson);
        }
    }
}

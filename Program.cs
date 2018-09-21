using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace COLT
{
    class Program
    {

        //Dictionary of potential connection strings
        private static Dictionary<string, Cloud> _cloudConfig = new Dictionary<string, Cloud>();

        //Timed events
        private static ConcurrentQueue<TimedEvent> timedEvents = new ConcurrentQueue<TimedEvent>();


        static void Main(string[] args)
        {
            //Load JSON file with configuration and job definitions
            string configJson = File.ReadAllText("job.json");
            JobManager manager = new JobManager(configJson);
            foreach (var cloud in manager.job.config.cloud)
            {
                if (!_cloudConfig.ContainsKey(cloud.name))
                    _cloudConfig.Add(cloud.name, cloud);
            }

            Console.WriteLine("Job file contains {0} jobs and {1} storage targets", manager.job.jobs.Length, manager.job.config.cloud.Length);

            //Process each job sequentially
            foreach (var job in manager.job.jobs)
            {
                Console.WriteLine("****JOB STARED****\nJob Name: {0}\nType: {1}\nCloud: {2}\n\n", job.name, job.type, job.cloud);

                switch (job.type)
                {
                    case "download":
                        Download(job).GetAwaiter().GetResult();
                        break;

                    case "upload":
                        Upload(job).GetAwaiter().GetResult();
                        break;

                    default:
                        Console.WriteLine("Not a valid job type");
                        break;
                }

                Console.WriteLine("****JOB FINISHED****\n");
            }

            List<TimedEvent> events = new List<TimedEvent>();
            while(!timedEvents.IsEmpty)
            {
                TimedEvent timed;
                if (timedEvents.TryDequeue(out timed)) {
                    events.Add(timed);
                }
            }

            string output = JsonConvert.SerializeObject(events);
            File.WriteAllText(@"output.json", output);

            var csv = new CsvWriter(File.CreateText(@"output.csv"));            
            csv.WriteRecords(events);

            Console.WriteLine("Wrote {0} timed events to output.json and output.csv", events.Count);
            Console.ReadLine();
        }

        private async static Task Download(Job job)
        {            
            Cloud cloud;
            if (_cloudConfig.TryGetValue(job.cloud, out cloud))
            {
                switch (cloud.type)
                {
                    case "s3":

                        try
                        {
                            using (HttpClient httpClient = new HttpClient())
                            {
                                s3Manager s3 = new s3Manager(cloud.awsAccessKey, cloud.awsAccessKeySecret, cloud.awsServiceUrl, cloud.awsS3bucket);
                                List<string> s3files = await s3.GetFileListAsync(job.filePrefix);

                                for (int i = 0; i < job.downloadCount; i++)
                                {
                                    Random rnd = new Random();
                                    int r = rnd.Next(s3files.Count);



                                    string url = s3.GeneratePreSignedURL(10, s3files[r]);

                                    TimedEvent timer = new TimedEvent();
                                    timer.cloudName = cloud.name;
                                    timer.cloudType = cloud.type;
                                    timer.eventType = job.type;
                                    timer.fileName = s3files[r];                                    
                                    timer.startTime = DateTime.UtcNow;
                                    timer.url = url;

                                    Stopwatch stopwatch = new Stopwatch();
                                    stopwatch.Start();

                                    //download
                                    var response = httpClient.GetAsync(url).Result;

                                    stopwatch.Stop();

                                    timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                    timer.finishTime = DateTime.UtcNow;

                                    var content = await response.Content.ReadAsByteArrayAsync();

                                    timer.fileSizeInBytes = content.Length;
                                    timedEvents.Enqueue(timer);

                                    Console.WriteLine("Downloaded {0} from {1} in {2} ms", timer.fileName, timer.cloudName, timer.elapsedMiliseconds);
                                }
                            }

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in downloading files");
                        }

                        break;

                    case "blob":
                        try
                        {

                            using (HttpClient httpClient = new HttpClient())
                            {
                                blobManager blobClient = new blobManager(cloud.blobStorageAccountConnectionString, cloud.blobContainer);
                                List<string> blobfiles = await blobClient.GetFileListAsync(job.filePrefix);

                                for (int i = 0; i < job.downloadCount; i++)
                                {
                                    Random rnd = new Random();
                                    int r = rnd.Next(blobfiles.Count);

                                    string url = blobClient.GeneratePreSignedURL(10, blobfiles[r]);

                                    TimedEvent timer = new TimedEvent();
                                    timer.cloudName = cloud.name;
                                    timer.cloudType = cloud.type;                                    
                                    timer.eventType = job.type;
                                    timer.fileName = blobfiles[r];
                                    timer.startTime = DateTime.UtcNow;
                                    timer.url = url;

                                    Stopwatch stopwatch = new Stopwatch();
                                    stopwatch.Start();

                                    //download
                                    var response = httpClient.GetAsync(url).Result;

                                    stopwatch.Stop();
                                    timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                    timer.finishTime = DateTime.UtcNow;

                                    var content = await response.Content.ReadAsByteArrayAsync();
                                    timer.fileSizeInBytes = content.Length;
                                    timedEvents.Enqueue(timer);

                                    Console.WriteLine("Downloaded {0} from {1} in {2} ms", timer.fileName, timer.cloudName, timer.elapsedMiliseconds);
                                }
                            }

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files");
                        }
                        break;

                    case "gcs":
     
                        using (HttpClient httpClient = new HttpClient())
                        {
                            gcsManager gcsClient = new gcsManager(Encoding.UTF8.GetString(Convert.FromBase64String(cloud.credential)), cloud.gcsbucket, cloud.projectid);
                            List<string> gcsFiles = gcsClient.GetFileList(job.filePrefix);
                            
                            for (int i = 0; i < job.downloadCount; i++)
                            {
                                Random rnd = new Random();
                                int r = rnd.Next(gcsFiles.Count);

                                string url = gcsClient.GeneratePreSignedURL(10, gcsFiles[r]);

                                TimedEvent timer = new TimedEvent();
                                timer.cloudName = cloud.name;
                                timer.cloudType = cloud.type;
                                timer.eventType = job.type;
                                timer.fileName = gcsFiles[r];
                                timer.startTime = DateTime.UtcNow;
                                timer.url = url;

                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();

                                //download
                                var response = httpClient.GetAsync(url).Result;

                                stopwatch.Stop();
                                timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                timer.finishTime = DateTime.UtcNow;

                                var content = await response.Content.ReadAsByteArrayAsync();
                                timer.fileSizeInBytes = content.Length;
                                timedEvents.Enqueue(timer);

                                Console.WriteLine("Downloaded {0} from {1} in {2} ms", timer.fileName, timer.cloudName, timer.elapsedMiliseconds);
                            }
                        }

                        break;

                    default:
                        Console.WriteLine("Invalid cloud type specificed");
                        break;
                }

            }

        }

        private async static Task Upload(Job job)
        {

            Cloud cloud;
            if (_cloudConfig.TryGetValue(job.cloud, out cloud))
            {
                switch (cloud.type)
                {
                    case "s3":

                        try
                        {
                            s3Manager s3 = new s3Manager(cloud.awsAccessKey, cloud.awsAccessKeySecret, cloud.awsServiceUrl, cloud.awsS3bucket);
                            Parallel.For(0, job.fileCount, (i, state) =>
                           {
                               try
                               {

                                   TimedEvent timer = new TimedEvent();
                                   timer.cloudName = cloud.name;
                                   timer.cloudType = cloud.type;
                                   timer.eventType = job.type;
                                   timer.fileName = job.filePrefix + i;
                                   timer.startTime = DateTime.UtcNow;
                                   
                                   Stopwatch stopwatch = new Stopwatch();
                                   stopwatch.Start();

                                   //Upload
                                   s3.UploadFileAsync(fileManager.generateRandomBytes(job.fileSizeInBytes), job.filePrefix + i).GetAwaiter().GetResult();

                                   stopwatch.Stop();

                                   timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                   timer.finishTime = DateTime.UtcNow;

                                   timer.fileSizeInBytes = job.fileSizeInBytes;
                                   timedEvents.Enqueue(timer);


                                   
                               }
                               catch (Exception ex)
                               {
                                   Console.WriteLine(ex.Message);
                               }
                               Console.WriteLine("{0}{1} uploaded", job.filePrefix, i);
                           });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files");
                        }

                        break;

                    case "gcs":


                        try
                        {
                            gcsManager gcs = new gcsManager(Encoding.UTF8.GetString(Convert.FromBase64String(cloud.credential)), cloud.gcsbucket, cloud.projectid);
                            Parallel.For(0, job.fileCount, (i, state) =>
                            {
                                try
                                {
                                    TimedEvent timer = new TimedEvent();
                                    timer.cloudName = cloud.name;
                                    timer.cloudType = cloud.type;
                                    timer.eventType = job.type;
                                    timer.fileName = job.filePrefix + i;
                                    timer.startTime = DateTime.UtcNow;

                                    Stopwatch stopwatch = new Stopwatch();
                                    stopwatch.Start();

                                    //Upload
                                    gcs.UploadFile(fileManager.generateRandomBytes(job.fileSizeInBytes), job.filePrefix + i);

                                    stopwatch.Stop();

                                    timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                    timer.finishTime = DateTime.UtcNow;

                                    timer.fileSizeInBytes = job.fileSizeInBytes;
                                    timedEvents.Enqueue(timer);

                                    
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.Message);
                                }
                                Console.WriteLine("{0}{1} uploaded", job.filePrefix, i);
                            });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files");
                        }


                        break;


                    case "blob":
                        try
                        {
                            blobManager blobClient = new blobManager(cloud.blobStorageAccountConnectionString, cloud.blobContainer);

                            Parallel.For(0, job.fileCount, (i, state) =>
                            {
                                try
                                {
                                    TimedEvent timer = new TimedEvent();
                                    timer.cloudName = cloud.name;
                                    timer.cloudType = cloud.type;
                                    timer.eventType = job.type;
                                    timer.fileName = job.filePrefix + i;
                                    timer.startTime = DateTime.UtcNow;

                                    Stopwatch stopwatch = new Stopwatch();
                                    stopwatch.Start();

                                    //Upload
                                    blobClient.UploadFileAsync(fileManager.generateRandomBytes(job.fileSizeInBytes), job.filePrefix + i).GetAwaiter().GetResult();

                                    stopwatch.Stop();

                                    timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                    timer.finishTime = DateTime.UtcNow;

                                    timer.fileSizeInBytes = job.fileSizeInBytes;
                                    timedEvents.Enqueue(timer);

                                    
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.Message);
                                }
                                Console.WriteLine("{0}{1} uploaded", job.filePrefix, i);
                            });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files");
                        }
                        break;

                    default:
                        Console.WriteLine("Invalid cloud type specificed");
                        break;
                }

            }



        }
    }
}

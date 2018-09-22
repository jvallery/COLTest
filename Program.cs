using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        private static IPInfo _ip;
        private static HttpClient httpClient = new HttpClient();
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


            //Determine client IP address for tracking source of test data using ipinfo service

            var response = httpClient.GetAsync("http://ipinfo.io/json").Result;
            _ip = JsonConvert.DeserializeObject<IPInfo>(response.Content.ReadAsStringAsync().GetAwaiter().GetResult());


            Console.WriteLine("Job file contains {0} jobs and {1} storage targets", manager.job.jobs.Length, manager.job.config.cloud.Length);

            //Process each job sequentially
            foreach (var job in manager.job.jobs)
            {
                Console.WriteLine("****JOB STARED****\nJob Name: {0}\nType: {1}\nCloud: {2}\n\n", job.name, job.type, job.cloud);

                switch (job.type)
                {
                    case "download":
                        Download(job);
                        break;

                    case "upload":
                        Upload(job);
                        break;

                    default:
                        Console.WriteLine("Not a valid job type");
                        break;
                }

                Console.WriteLine("****JOB FINISHED****\n");
            }

            //Pull out items from timing queue and write to CSV
            List<TimedEvent> events = new List<TimedEvent>();
            while (!timedEvents.IsEmpty)
            {
                TimedEvent timed;
                if (timedEvents.TryDequeue(out timed))
                {
                    events.Add(timed);
                }
            }

            Cloud outputCloud;
            if (_cloudConfig.TryGetValue(manager.job.config.output, out outputCloud))
            {

                using (var memoryStream = new MemoryStream())
                {
                    using (var streamWriter = new StreamWriter(memoryStream))
                    {
                        using (var csvWriter = new CsvWriter(streamWriter))
                        {
                            csvWriter.WriteRecords(events);
                        } // StreamWriter gets flushed here.
                    }

                    string output = JsonConvert.SerializeObject(events);

                    //Write CSV and JSON to blob target
                    blobManager blobClient = new blobManager(outputCloud.blobStorageAccountConnectionString, "results");
                    blobClient.UploadFileAsync(memoryStream.ToArray(), string.Format("{0}.csv", DateTime.UtcNow.ToString("yyyyMMddHHmmss"))).GetAwaiter().GetResult();
                    blobClient.UploadFileAsync(Encoding.UTF8.GetBytes(output), string.Format("{0}.json", DateTime.UtcNow.ToString("yyyyMMddHHmmss"))).GetAwaiter().GetResult();

                }

                Console.WriteLine("Wrote {0} timed events to output.json and output.csv and uploaded to {1}", events.Count, outputCloud.name);
            }


            Console.ReadLine();
        }

        private static void Download(Job job)
        {
            if (job.downloadCount == 0)
                return;

            Cloud cloud;
            if (_cloudConfig.TryGetValue(job.cloud, out cloud))
            {
                List<string> urlsToDownload = new List<string>();

                switch (cloud.type)
                {
                    case "s3":

                        try
                        {
                            s3Manager s3 = new s3Manager(cloud.awsAccessKey, cloud.awsAccessKeySecret, cloud.awsServiceUrl, cloud.awsRegion, cloud.awsS3bucket);
                            List<string> s3files = s3.GetFileListAsync(job.filePrefix).GetAwaiter().GetResult();

                            Parallel.For(0, job.downloadCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                           {
                               Random rnd = new Random();
                               int r = rnd.Next(s3files.Count);
                               string url = s3.GeneratePreSignedURL(10, s3files[r]);
                               urlsToDownload.Add(url);
                           });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in downloading files: {0}", ex.Message);
                        }

                        break;

                    case "blob":
                        try
                        {

                            blobManager blobClient = new blobManager(cloud.blobStorageAccountConnectionString, cloud.blobContainer);
                            List<string> blobFiles = blobClient.GetFileListAsync(job.filePrefix).GetAwaiter().GetResult();

                            string containerSas = blobClient.GetContainerSASRead(20);

                            Parallel.For(0, job.downloadCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                           {
                               Random rnd = new Random();
                               int r = rnd.Next(blobFiles.Count);
                               string url = string.Format("{0}{1}", blobClient.GetBlobURL(blobFiles[r]), containerSas);
                               urlsToDownload.Add(url);
                           });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files: {0}", ex.Message);

                        }
                        break;

                    case "gcs":

                        gcsManager gcsClient = new gcsManager(Encoding.UTF8.GetString(Convert.FromBase64String(cloud.credential)), cloud.gcsbucket, cloud.projectid);
                        List<string> gcsFiles = gcsClient.GetFileList(job.filePrefix);

                        Parallel.For(0, job.downloadCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                       {
                           Random rnd = new Random();
                           int r = rnd.Next(gcsFiles.Count);
                           string url = gcsClient.GeneratePreSignedURL(10, gcsFiles[r]);
                           urlsToDownload.Add(url);
                       });
                        break;

                    default:
                        Console.WriteLine("Invalid cloud type specificed");
                        break;
                }


                List<double> results = new List<double>();
                int downloadCount = 0;
                Parallel.ForEach(urlsToDownload, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, url =>
               {

                   try
                   {
                       TimedEvent timer = new TimedEvent();
                       timer.sourceCity = _ip.city;
                       timer.sourceCountry = _ip.country;
                       timer.sourceIp = _ip.ip;
                       timer.sourceLoc = _ip.loc;
                       timer.sourceOrg = _ip.org;
                       timer.sourcePostal = _ip.postal;
                       timer.sourceRegion = _ip.region;
                       timer.cloudName = cloud.name;
                       timer.cloudType = cloud.type;
                       timer.eventType = job.type;
                       timer.startTime = DateTime.UtcNow;
                       timer.url = url;

                       Stopwatch stopwatch = new Stopwatch();
                       stopwatch.Start();

                       //download
                       var response = httpClient.GetAsync(url).Result;

                       stopwatch.Stop();

                       timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                       timer.finishTime = DateTime.UtcNow;
                       downloadCount++;
                       var content = response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
                       timer.fileSizeInBytes = content.Length;

                       timer.httpStatusCode = (int)response.StatusCode;
                       timedEvents.Enqueue(timer);
                       results.Add(timer.elapsedMiliseconds);
                       Console.WriteLine("{0}: HTTP {1} - Downloaded {2} bytes from {3} in {4} ms", downloadCount, timer.httpStatusCode, timer.fileSizeInBytes, timer.cloudName, timer.elapsedMiliseconds);
                   }
                   catch (Exception ex)
                   {
                       Console.WriteLine("Error downloading: {0}", url);
                   }

               });

                Console.WriteLine("Average latency: {0}, 99th percentile: {1}", Average(results), Percentile(results, 0.99));
            }

        }

        private static void Upload(Job job)
        {
            Cloud cloud;
            if (_cloudConfig.TryGetValue(job.cloud, out cloud))
            {
                List<double> results = new List<double>();

                switch (cloud.type)
                {
                    case "s3":

                        try
                        {
                            s3Manager s3 = new s3Manager(cloud.awsAccessKey, cloud.awsAccessKeySecret, cloud.awsServiceUrl, cloud.awsRegion, cloud.awsS3bucket);


                            Parallel.For(0, job.fileCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                          {

                              try
                              {

                                  TimedEvent timer = new TimedEvent();
                                  timer.sourceCity = _ip.city;
                                  timer.sourceCountry = _ip.country;
                                  timer.sourceIp = _ip.ip;
                                  timer.sourceLoc = _ip.loc;
                                  timer.sourceOrg = _ip.org;
                                  timer.sourcePostal = _ip.postal;
                                  timer.sourceRegion = _ip.region;
                                  timer.cloudName = cloud.name;
                                  timer.cloudType = cloud.type;
                                  timer.eventType = job.type;
                                  timer.fileName = job.filePrefix + i;
                                  timer.startTime = DateTime.UtcNow;


                                  byte[] bytes = fileManager.generateRandomBytes(job.fileSizeInBytes);

                                  Stopwatch stopwatch = new Stopwatch();
                                  stopwatch.Start();

                                  //Upload
                                  s3.UploadFileAsync(bytes, job.filePrefix + i).GetAwaiter().GetResult();

                                  stopwatch.Stop();

                                  bytes = null;
                                  GC.Collect();

                                  timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                  timer.finishTime = DateTime.UtcNow;

                                  timer.fileSizeInBytes = job.fileSizeInBytes;
                                  timedEvents.Enqueue(timer);

                                  results.Add(timer.elapsedMiliseconds);

                              }
                              catch (Exception ex)
                              {
                                  Console.WriteLine(ex.Message);
                              }
                              Console.WriteLine("{0}{1} uploaded to {2}", job.filePrefix, i, cloud.name);
                          });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files: {0}", ex.Message);
                        }

                        break;

                    case "gcs":


                        try
                        {
                            gcsManager gcs = new gcsManager(Encoding.UTF8.GetString(Convert.FromBase64String(cloud.credential)), cloud.gcsbucket, cloud.projectid);

                            Parallel.For(0, job.fileCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                            {

                                try
                                {
                                    TimedEvent timer = new TimedEvent();
                                    timer.sourceCity = _ip.city;
                                    timer.sourceCountry = _ip.country;
                                    timer.sourceIp = _ip.ip;
                                    timer.sourceLoc = _ip.loc;
                                    timer.sourceOrg = _ip.org;
                                    timer.sourcePostal = _ip.postal;
                                    timer.sourceRegion = _ip.region;
                                    timer.cloudName = cloud.name;
                                    timer.cloudType = cloud.type;
                                    timer.eventType = job.type;
                                    timer.fileName = job.filePrefix + i;
                                    timer.startTime = DateTime.UtcNow;



                                    byte[] bytes = fileManager.generateRandomBytes(job.fileSizeInBytes);

                                    Stopwatch stopwatch = new Stopwatch();
                                    stopwatch.Start();

                                    //Upload
                                    gcs.UploadFileAsync(bytes, job.filePrefix + i).GetAwaiter().GetResult();

                                    stopwatch.Stop();

                                    bytes = null;
                                    GC.Collect();


                                    timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                    timer.finishTime = DateTime.UtcNow;

                                    timer.fileSizeInBytes = job.fileSizeInBytes;
                                    timedEvents.Enqueue(timer);
                                    results.Add(timer.elapsedMiliseconds);

                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.Message);
                                }
                                Console.WriteLine("{0}{1} uploaded to {2}", job.filePrefix, i, cloud.name);
                            });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files {0}", ex.Message);
                        }


                        break;


                    case "blob":
                        try
                        {
                            blobManager blobClient = new blobManager(cloud.blobStorageAccountConnectionString, cloud.blobContainer);

                            Parallel.For(0, job.fileCount, new ParallelOptions { MaxDegreeOfParallelism = job.threads }, (i, state) =>
                           {


                               try
                               {
                                   TimedEvent timer = new TimedEvent();
                                   timer.sourceCity = _ip.city;
                                   timer.sourceCountry = _ip.country;
                                   timer.sourceIp = _ip.ip;
                                   timer.sourceLoc = _ip.loc;
                                   timer.sourceOrg = _ip.org;
                                   timer.sourcePostal = _ip.postal;
                                   timer.sourceRegion = _ip.region;
                                   timer.cloudName = cloud.name;
                                   timer.cloudType = cloud.type;
                                   timer.eventType = job.type;
                                   timer.fileName = job.filePrefix + i;
                                   timer.startTime = DateTime.UtcNow;



                                   byte[] bytes = fileManager.generateRandomBytes(job.fileSizeInBytes);

                                   Stopwatch stopwatch = new Stopwatch();
                                   stopwatch.Start();

                                   //Upload
                                   blobClient.UploadFileAsync(bytes, job.filePrefix + i).GetAwaiter().GetResult();

                                   stopwatch.Stop();

                                   bytes = null;
                                   GC.Collect();


                                   timer.elapsedMiliseconds = stopwatch.ElapsedMilliseconds;
                                   timer.finishTime = DateTime.UtcNow;

                                   timer.fileSizeInBytes = job.fileSizeInBytes;
                                   timedEvents.Enqueue(timer);
                                   results.Add(timer.elapsedMiliseconds);

                               }
                               catch (Exception ex)
                               {
                                   Console.WriteLine(ex.Message);
                               }
                               Console.WriteLine("{0}{1} uploaded to {2}", job.filePrefix, i, cloud.name);
                           });

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error in uploading files: {0}", ex.Message);
                        }
                        break;

                    default:
                        Console.WriteLine("Invalid cloud type specificed");
                        break;
                }

                Console.WriteLine("Average latency: {0}, 99th percentile: {1}", Average(results), Percentile(results, 0.99));

            }



        }

        public static double Percentile(IEnumerable<double> seq, double percentile)
        {
            var elements = seq.ToArray();

            if (elements.Length > 0)
            {
                Array.Sort(elements);
                double realIndex = percentile * (elements.Length - 1);
                int index = (int)realIndex;
                double frac = realIndex - index;
                if (index + 1 < elements.Length)
                    return elements[index] * (1 - frac) + elements[index + 1] * frac;
                else
                    return elements[index];
            }
            else
            {
                return 0;
            }
        }

        public static double Average(IEnumerable<double> seq)
        {
            double sum = 0;
            var elements = seq.ToArray();
            if (elements.Length > 0)
            {
                foreach (var val in elements)
                    sum += val;

                double result = sum / elements.Length;

                return result;
            }
            else
            {
                return 0;
            }
        }

    }
}

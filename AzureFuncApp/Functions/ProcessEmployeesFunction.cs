using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureFuncApp.Functions
{
    public static class ProcessEmployeesFunction
    {

        [FunctionName("ProcessEmployees")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequestMessage req, TraceWriter log)
        {
            log.Info("Processing blob input");

            JObject result = new JObject();
            string filename = null;

            // input validation
            if (req.Method.Equals(HttpMethod.Get))
            {
                filename = req.RequestUri.ParseQueryString()["filename"] ?? null;

                if (filename == null)
                {
                    log.Error($"filename is a required input");
                    return req.CreateResponse(HttpStatusCode.BadRequest, "filename is a required input");
                }

                if (Path.GetExtension(filename) != ".json")
                {
                    log.Error($"filename must be a json file");
                    return req.CreateResponse(HttpStatusCode.BadRequest, "filename must be a json file");
                }
            }

            log.Info($"filename: {filename}");

            try
            {

                // get reference to existing blob
                var blobConnectionString = ConfigurationManager.AppSettings["AzBlobStorage"];
                var blobContainerName = ConfigurationManager.AppSettings["AzBlobContainerName"];

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(blobConnectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(blobContainerName);
                var sourceBlob = container.GetBlockBlobReference(filename);

                if (!sourceBlob.Exists())
                {
                    log.Error($"source blob '{filename}' does not exist");
                    result.Add("message", "filename not found");
                    return req.CreateResponse(HttpStatusCode.NotFound, result);
                }

                log.Info($"blob uri: {sourceBlob.Uri.AbsolutePath}");

                // get reference to new blob
                var newblob = container.GetBlockBlobReference($"output/{Path.GetFileNameWithoutExtension(filename)}-{DateTime.Now.ToString("yyyyMMddhhmmss")}.txt");

                using (var readStream = new MemoryStream())
                using (var streamReader = new StreamReader(readStream))
                using (JsonReader jsonReader = new JsonTextReader(streamReader))
                {

                    readStream.Position = 0;
                    JsonSerializer serializer = new JsonSerializer();

                    var blobRequestOptions = new BlobRequestOptions
                    {
                        ServerTimeout = TimeSpan.FromSeconds(300),
                        MaximumExecutionTime = TimeSpan.FromSeconds(900),
                        RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(3), 3),
                    };

                    await sourceBlob.DownloadToStreamAsync(readStream, null, blobRequestOptions, null);
                    readStream.Position = 0;

                    // process the json
                    IList<Dictionary<string, string>> dataItems = serializer.Deserialize<List<Dictionary<string, string>>>(jsonReader);

                    var outputFields = new[] { "id", "first_name", "last_name", "email", "gender", "ssn", "date_of_birth", "date_of_hire", "job_title", "department", "university", "address", "city", "state", "zip_code", "phone" };
                    var dateFields = new[] { "date_of_birth", "date_of_hire" };
                    var addressFields = new[] { "address", "city", "state", "zip_code" };

                    var outputLines = dataItems.ProcessDateFields(dateFields)
                                            .RemoveNewlineCharacters(addressFields)
                                            .ToOutputFormat(outputFields);

                    // write to output stream and upload blob to container
                    int writecount = 0;
                    using (var writeStream = new MemoryStream())
                    using (StreamWriter streamWriter = new StreamWriter(writeStream, Encoding.UTF8))
                    {
                        foreach(var line in outputLines)
                        {
                            streamWriter.WriteLine(line);
                            writecount++;
                        }

                        log.Info($"output records written: {writecount}");

                        streamWriter.Flush();
                        writeStream.Position = 0;
                        newblob.UploadFromStream(writeStream);

                    }

                }

                result.Add("message", "new blob created");
                result.Add("uri", newblob.Uri.AbsolutePath);

                return req.CreateResponse(HttpStatusCode.OK, result, new JsonMediaTypeFormatter());
            }
            catch (Exception ex)
            {
                log.Error("error", ex);
                throw ex;
            }
        }

    }
}

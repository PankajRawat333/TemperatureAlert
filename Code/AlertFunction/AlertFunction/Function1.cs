using CsvHelper;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace AlertFunction
{
    public static class Function1
    {
        private static string databaseEndPoint = "https://stream-poc-db.documents.azure.com:443/";
        private static string databaseKey = "***************************************==";
        private static string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=streamstoragepoc;AccountKey=1********************;";

        private static DocumentClient client = new DocumentClient(new System.Uri(databaseEndPoint), databaseKey,
            new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                // Customize retry options for Throttled requests
                RetryOptions = new RetryOptions()
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 30
                }
            });

        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("temperaturealert", Connection = "connection")]string message, ILogger log)
        {
            log.LogInformation($"processed message: {message}");
            //Insert into Cosmos DB
            JObject jObject = JObject.Parse(message);
            jObject.Add("alertType", "Temperature Alert");
            CreateAsync("alerts-db", "alerts", jObject).GetAwaiter().GetResult();

            //Get last 10 minutes alert from DB
            var alerts = GetDocuments("alerts-db", "alerts", 10);
            var groupAlert = alerts.GroupBy(x => x.DeviceId)
                    .Select(g => new TemperatureAlertDto
                    {
                        DeviceId = g.Key,
                        EventProcessedUtcTime = g.Max(p => p.EventProcessedUtcTime)
                    }).ToList();
            var json = JsonConvert.SerializeObject(groupAlert);
            //Create csv file from alert json
            string csv = jsonToCSV(json, ",");
            //Upload csv file in blob

            UploadFile(csv);
        }

        public static void UploadFile(string csv)
        {
            CloudStorageAccount account;
            CloudStorageAccount.TryParse(storageConnectionString, out account);
            CloudBlobClient cloudBlobClient = account.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference("temperature-alerts");
            cloudBlobContainer.CreateIfNotExistsAsync();
            string firstDirectory = DateTime.UtcNow.ToString("yyyy-MM-dd");
            string secondDirectory = DateTime.UtcNow.ToString("HH-mm");
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference($"{firstDirectory}/{secondDirectory}/temperature-alerts.csv");
            cloudBlockBlob.UploadTextAsync(csv).GetAwaiter().GetResult();
        }

        public static async Task CreateAsync(string databaseId, string collectionId, JObject document)
        {
            await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), document);
        }

        public static List<TemperatureAlertDto> GetDocuments(string databaseId, string collectionId, int minutes)
        {
            TimeSpan t = DateTime.UtcNow.AddMinutes(-minutes) - new DateTime(1970, 1, 1);
            int secondsSinceEpoch = (int)t.TotalSeconds;

            SqlParameterCollection parameters = new SqlParameterCollection();
            parameters.Add(new SqlParameter("@date", secondsSinceEpoch));
            var query = new SqlQuerySpec(
                $"SELECT c.DeviceId,c.EventProcessedUtcTime FROM c where c.alertType='Temperature Alert' and c._ts>=@date", parameters);

            List<TemperatureAlertDto> documents = client.CreateDocumentQuery<TemperatureAlertDto>(
                UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), query,
                new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true }).AsEnumerable().ToList();
            return documents;
        }

        public static DataTable jsonStringToTable(string jsonContent)
        {
            DataTable dt = JsonConvert.DeserializeObject<DataTable>(jsonContent);
            return dt;
        }

        public static string jsonToCSV(string jsonContent, string delimiter)
        {
            StringWriter csvString = new StringWriter();
            using (var csv = new CsvWriter(csvString))
            {
                csv.Configuration.Delimiter = delimiter;

                using (var dt = jsonStringToTable(jsonContent))
                {
                    foreach (DataColumn column in dt.Columns)
                    {
                        csv.WriteField(column.ColumnName);
                    }
                    csv.NextRecord();

                    foreach (DataRow row in dt.Rows)
                    {
                        for (var i = 0; i < dt.Columns.Count; i++)
                        {
                            csv.WriteField(row[i]);
                        }
                        csv.NextRecord();
                    }
                }
            }
            return csvString.ToString();
        }
    }
}
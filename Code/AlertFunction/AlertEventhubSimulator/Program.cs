using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AlertEventhubSimulator
{
    class Program
    {
        private static string eventhubConnectionString = System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];
        private static string eventhubName = System.Configuration.ConfigurationSettings.AppSettings["EventHubName"];

        static void Main(string[] args)
        {
            sendTemperatureAlert();
        }

        private static void sendTemperatureAlert()
        {
            while (true)
            {
                var json = File.ReadAllText("SampleJSON/TemperatureAlert.json");
                JObject jObject = JObject.Parse(json);
                jObject["MessageTime"] = DateTime.UtcNow;

                EventHubClient eventHubClient = EventHubMessageFactory.CreateEventHubClient(eventhubName);
                var eventData = new EventData(new MemoryStream(Encoding.UTF8.GetBytes(jObject.ToString())));
                eventHubClient.Send(eventData);
                Console.WriteLine("temperature alert sent");
                Thread.Sleep(1000 * 60);
            }
            
        }


        private static Lazy<MessagingFactory> lazyEventhubMessagingFactory = new Lazy<MessagingFactory>(() =>
        {
            var eventHubConnection = new ServiceBusConnectionStringBuilder(eventhubConnectionString)
            {
                TransportType = TransportType.Amqp,
            };
            return MessagingFactory.CreateFromConnectionString(eventHubConnection.ToString());
        });

        public static MessagingFactory EventHubMessageFactory
        {
            get
            {
                return lazyEventhubMessagingFactory.Value;
            }
        }
    }
}

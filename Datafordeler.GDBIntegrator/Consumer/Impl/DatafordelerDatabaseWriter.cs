using Datafordeler.DBIntegrator.Config;
using Datafordeler.DBIntegrator.Serialization;
using Datafordeler.GDBIntegrator.Database;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Topos.Config;
using System.Linq;


namespace Datafordeler.DBIntegrator.Consumer
{
    public class DatafordelereDatabaseWriter : IDatafordelerConsumer
    {
        private IDisposable _consumer;
        private readonly ILogger<DatafordelereDatabaseWriter> _logger;
        private readonly KafkaSetting _kafkaSetting;
        private readonly DatabaseSetting _databaseSetting;
        private readonly IDatabaseWriter _databaseWriter;

        public DatafordelereDatabaseWriter(
            ILogger<DatafordelereDatabaseWriter> logger,
            IOptions<KafkaSetting> kafkaSetting,
            IOptions<DatabaseSetting> databaseSetting,
            IDatabaseWriter databaseWriter
            )
        {
            _logger = logger;
            _kafkaSetting = kafkaSetting.Value;
            _databaseSetting = databaseSetting.Value;
            _databaseWriter = databaseWriter;
        }

        public void Start()
        {
            List<JObject> list = new List<JObject>();
            var topics = _kafkaSetting.DatafordelereTopic.Split(",");
            Console.WriteLine("This is one topic" + topics[0]);
            //Console.WriteLine("This is the second topic " + topics[1]);
            foreach (var topic in topics)
            {
                _consumer = Configure
                   .Consumer(_kafkaSetting.DatafordelereTopic, c => c.UseKafka(_kafkaSetting.Server))
                   .Serialization(s => s.DatafordelerEventDeserializer())
                   .Topics(t => t.Subscribe(topic))
                   .Positions(p => p.StoreInFileSystem(_kafkaSetting.PositionFilePath))
                   .Handle(async (messages, context, token) =>
                   {
                       
                           foreach (var message in messages)
                           {
                               if (message.Body is JObject)
                               {
                               //await HandleSubscribedEvent((JObject)message.Body);
                               list.Add((JObject)message.Body);
                                   if (list.Count >= 10000)
                                   {
                                       await (HandleMessages(list, topic));
                                       list.Clear();
                                   }
                               }
                           }
                    
                   }).Start();
            }
        }


        private async Task HandleMessages(List<JObject> list,string topic)
        {
             _logger.LogInformation("Recieved a message");
            _databaseWriter.UpsertData(list,topic);
            //Console.WriteLine("THis is the topic" +_kafkaSetting.DatafordelereTopic);
        }

        public void Dispose()
        {

        }
    }
}

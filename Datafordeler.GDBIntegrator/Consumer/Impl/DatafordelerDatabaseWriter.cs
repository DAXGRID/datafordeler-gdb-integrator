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
            var kafka = _kafkaSetting.Values;
            //Console.WriteLine("This is one topic" + topics[0]);
            //Console.WriteLine("This is the second topic " + topics[1]);
            if (kafka != null)
            {

                foreach (var obj in kafka)
                {
                    var topic = obj.Key;
                    var columns = obj.Value.Split(",");
                    _consumer = Configure
                       .Consumer(topic, c => c.UseKafka(_kafkaSetting.Server))
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
                                   if (list.Count >= 1000)
                                   {
                                       await (HandleMessages(list, topic, columns));
                                       Console.WriteLine("I am here inside the loop");
                                       list.Clear();
                                   }

                               }
                           }
                           Console.WriteLine("This is the number of items " + list.Count); 
                           await (HandleMessages(list, topic, columns));
                           Console.WriteLine("I am here outside the loop");
                           list.Clear();


                       }).Start();
                }
            }

        }


        private async Task HandleMessages(List<JObject> list, string topic, string[] columns)
        {
            //_logger.LogInformation("Recieved a message");
            _databaseWriter.AddToSql(list, topic, columns);
            //Console.WriteLine("THis is the topic" +_kafkaSetting.DatafordelereTopic);
        }

        public void Dispose()
        {

        }
    }
}

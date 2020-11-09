using Datafordeler.DBIntegrator.Config;
using Datafordeler.DBIntegrator.Serialization;
using Datafordeler.GDBIntegrator.Database;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Topos.Config;


namespace Datafordeler.DBIntegrator.Consumer
{
    public class DatafordelereDatabaseWriter : IDatafordelerConsumer
    {
        private IDisposable _consumer;
        private readonly ILogger<DatafordelereDatabaseWriter> _logger;
        private readonly KafkaSetting _kafkaSetting;
        private readonly DatabaseSetting _databaseSetting;
        private List<IDisposable> _consumers = new List<IDisposable>();

        private readonly IDatabaseWriter _databaseWriter;

        private readonly IPostgresWriter _postgresWriter;

        private Dictionary<string, List<JObject>> _topicList = new Dictionary<string, List<JObject>>();

        public DatafordelereDatabaseWriter(
            ILogger<DatafordelereDatabaseWriter> logger,
            IOptions<KafkaSetting> kafkaSetting,
            IOptions<DatabaseSetting> databaseSetting,
            IDatabaseWriter databaseWriter,
            IPostgresWriter postgresWriter
            )
        {
            _logger = logger;
            _kafkaSetting = kafkaSetting.Value;
            _databaseSetting = databaseSetting.Value;
            _databaseWriter = databaseWriter;
            _postgresWriter = postgresWriter;
        }

        public void Start()
        {   
            var list = new List<JObject>();
            var kafka = _kafkaSetting.DatafordelereTopic.Split(",");
            if (kafka != null)
            {

                foreach (var obj in kafka)
                {
                    var topic = obj;
                    var consumer = _consumer = Configure
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
                                   _logger.LogInformation(message.Body.ToString());
                                   if(!_topicList.ContainsKey(topic))
                                   {
                                       _topicList.Add(topic,new List<JObject>());
                                        _topicList[topic].Add((JObject)message.Body);
                                   }
                                   else
                                   {
                                        _topicList[topic].Add((JObject)message.Body);
                                   }
                                   if (_topicList[topic].Count >= 1000)
                                   {
                                       //await (HandleMessages(_topicList[topic], topic, columns));
                                       _topicList[topic].Clear();
                                   }
                               }
                           }
                           //await (HandleMessages(_topicList[topic], topic, columns));
                            _topicList[topic].Clear();
                       }).Start();

                    _consumers.Add(consumer);
                }
            }
        }


        private async Task HandleMessages(List<JObject> list, string topic, string[] columns)
        {
             _postgresWriter.AddToPSQL(list,topic,columns);
            
        }

        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
        }
    }
}

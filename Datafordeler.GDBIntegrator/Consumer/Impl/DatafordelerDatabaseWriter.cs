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
            _consumer = Configure
               .Consumer(_kafkaSetting.DatafordelereTopic, c => c.UseKafka(_kafkaSetting.Server))
               .Serialization(s => s.DatafordelerEventDeserializer())
               .Topics(t => t.Subscribe(_kafkaSetting.DatafordelereTopic))
               .Positions(p => p.StoreInFileSystem(_kafkaSetting.PositionFilePath))
               .Handle(async (messages, context, token) =>
               {
                   foreach (var message in messages)
                   {
                       if (message.Body is JObject)
                       {
                           await HandleSubscribedEvent((JObject)message.Body);
                       }
                   }
               }).Start();

        }

        private async Task HandleSubscribedEvent(JObject eventObject)
        {
            _logger.LogInformation("Recieved a message");
            _databaseWriter.UpsertStuff("blabla");
        }

        public void Dispose()
        {
        }
    }
}

using Newtonsoft.Json.Linq;
using System;
using System.Text;
using Topos.Serialization;

namespace Datafordeler.DBIntegrator.Serialization
{
    public class DatafordelerEventDeserializer : IMessageSerializer
    {
        public ReceivedLogicalMessage Deserialize(ReceivedTransportMessage message)
        {
            if (message is null)
                throw new ArgumentNullException($"{nameof(ReceivedTransportMessage)} is null");

            if (message.Body is null || message.Body.Length == 0)
                throw new ArgumentNullException($"{nameof(ReceivedTransportMessage)} body is null");

            var messageBody = Encoding.UTF8.GetString(message.Body, 0, message.Body.Length);

            var eventObject = JObject.Parse(messageBody);

            return new ReceivedLogicalMessage(message.Headers, eventObject, message.Position);
        }

        public TransportMessage Serialize(LogicalMessage message)
        {
            throw new NotImplementedException();
        }
    }
}

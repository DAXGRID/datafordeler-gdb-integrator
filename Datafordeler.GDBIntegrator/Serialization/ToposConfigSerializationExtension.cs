using System;
using System.Collections.Generic;
using System.Text;
using Topos.Config;
using Topos.Serialization;

namespace Datafordeler.DBIntegrator.Serialization
{
    public static class ToposConfigSerializationExtension
    {
        public static void DatafordelerEventDeserializer(this StandardConfigurer<IMessageSerializer> configurer)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof(configurer));

            StandardConfigurer.Open(configurer)
                .Register(c => new DatafordelerEventDeserializer());
        }
    }
}

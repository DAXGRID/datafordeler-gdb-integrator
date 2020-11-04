using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Npgsql;
namespace Datafordeler.GDBIntegrator.Database
{
    public interface IPostgresWriter
    {
        void AddToPSQL(List<JObject> batch, string topic, string[] columns);

    }

}
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json.Linq;
namespace Datafordeler.GDBIntegrator.Database
{
    public interface IPostgresWriter
    {
        void createTable(string topic, string[] columns);
        bool checkTable(string topic);

        List<JObject> checkLatestDataDuplicates(List<JObject> batch);

        void UpsertData(List<JObject> batch, string topic, string[] columns);
        void AddToPSQL(List<JObject> batch, string topic, string[] columns);

    }

}
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json.Linq;

namespace Datafordeler.GDBIntegrator.Database
{
    public interface IDatabaseWriter
    {
        void UpsertData(List<JObject> batch, string topic,string[] columns);
         void AddToSql(List<JObject> batch, string topic,string[] columns);
        DataTable CreateDataTable(List<JObject> batch, string[] columns );

        List<JObject> checkLatestData(List<JObject> batch);

        List<JObject> checkLatestDataDuplicates(List<JObject> batch);

        void createTable(string topic, string[] columns);
        void createType(string topic, string[] columns);

        void createStoredProcedure(string topic, string[] columns);

        bool checkTable(string topic);

        bool checkType(string topic);
        bool checkProcedure(string topic);
    }
}

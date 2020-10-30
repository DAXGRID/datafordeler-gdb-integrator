using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
namespace Datafordeler.GDBIntegrator.Database
{
    public interface IPostgresWriter
    {
        void createPostgis();
        
        void createTable(string topic, string[] columns);
        
        List<JObject> checkLatestDataDuplicates(List<JObject> batch);
        bool checkTable(string table);

        void UpsertData(List<JObject> batch, string topic, string[] columns);
        void InsertOnConflict(string tempTable,string table, string[] columns);
        void AddToPSQL(List<JObject> batch, string topic, string[] columns);

    }

}
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Npgsql;
namespace Datafordeler.GDBIntegrator.Database
{
    public interface IPostgresWriter
    {
        
        void createTable(string topic, string[] columns, NpgsqlConnection connection);
        void createTemporaryTable(string topic, string[] columns, NpgsqlConnection connection);
        List<JObject> checkLatestDataDuplicates(List<JObject> batch);

        void UpsertData(List<JObject> batch, string topic, string[] columns,NpgsqlConnection connection);
        void InsertOnConflict(string tempTable,string table, string[] columns,NpgsqlConnection connection);
        void AddToPSQL(List<JObject> batch, string topic, string[] columns);

    }

}
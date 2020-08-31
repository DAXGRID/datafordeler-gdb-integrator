using Datafordeler.DBIntegrator.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Datafordeler.GDBIntegrator.Database.Impl
{
    public class MSSQLWriter : IDatabaseWriter
    {
        private readonly ILogger<MSSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;
        private readonly KafkaSetting _kafkaSetting;

        public MSSQLWriter(
            ILogger<MSSQLWriter> logger,
            IOptions<DatabaseSetting> databaseSetting,
            IOptions<KafkaSetting> kafkaSetting
            )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
            _kafkaSetting = kafkaSetting.Value;
        }

        public void UpsertData(List<JObject> batch, string topic, string[] columns)
        {
            /*
            JObject rss = new JObject(new JProperty("id_lokalId","1"), new JProperty("unitAddressDescription","ccc"));
            JObject rss1 = new JObject(new JProperty("id_lokalId","1"), new JProperty("unitAddressDescription","ddd"));
            batch = new List<JObject>();
            batch.Add(rss);
            batch.Add(rss1);
            */
            //_logger.LogDebug("I wrote some stuff: " + data);
            string connectionString;
            SqlConnection cnn;
            SqlCommand command;
            cnn = new SqlConnection(_databaseSetting.ConnectionString);
            var list = checkLatestData(batch);
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command1 = connection.CreateCommand())
                {

                    command1.CommandText = "dbo.Upsert" + topic;
                    command1.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = command1.Parameters.AddWithValue("@UpdateRecords", CreateDataTable(list, columns));
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "dbo.Table" + topic;
                    command1.ExecuteNonQuery();
                }

            }
            cnn.Close();
        }

        public DataTable CreateDataTable(List<JObject> batch, string[] columns)
        {
            DataTable tbl = new DataTable();
            foreach (var column in columns)
            {
                if (column == "registrationFrom" | column == "registrationTo" | column == "effectFrom" | column == "effectTo")
                {
                    tbl.Columns.Add(new DataColumn(column, typeof(DateTime)));
                }
                else
                {
                    tbl.Columns.Add(new DataColumn(column, typeof(string)));
                }

            }

            foreach (var obj in batch)
            {
                DataRow dr = tbl.NewRow();
                foreach (var col in columns)
                {
                    if ((string)obj[col] != "null")
                    {
                        dr[col] = obj[col];
                        Console.WriteLine(obj.ToString());
                    }

                }
                tbl.Rows.Add(dr);
            }

            return tbl;
        }

        public List<JObject> checkLatestData(List<JObject> batch)
        {
            var duplicateKeys = batch.GroupBy(x => x["id_lokalId"])
                  .Where(group => group.Count() > 1)
                  .Select(group =>
                         group.OrderByDescending(x => x["registrationFrom"])
                         .ThenByDescending(x => x["registrationTo"])
                         .ThenByDescending(x => x["effectFrom"])
                         .ThenByDescending(x => x["effectTo"])
                              .FirstOrDefault()).ToList(); 

                 return duplicateKeys;                  
        }
    }
}
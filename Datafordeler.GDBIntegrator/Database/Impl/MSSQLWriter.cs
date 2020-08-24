using Datafordeler.DBIntegrator.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json.Linq;

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
            //_logger.LogDebug("I wrote some stuff: " + data);
            string connectionString;
            SqlConnection cnn;
            SqlCommand command;
            cnn = new SqlConnection(_databaseSetting.ConnectionString);
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command1 = connection.CreateCommand())
                {

                    command1.CommandText = "dbo.Upsert" + topic;
                    command1.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = command1.Parameters.AddWithValue("@UpdateRecords", CreateDataTable(batch, columns));
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
                tbl.Columns.Add(new DataColumn(column, typeof(string)));
            }

            foreach (var obj in batch)
            {
                DataRow dr = tbl.NewRow();
                foreach (var col in columns)
                {
                    dr[col] = obj[col];
                }
                tbl.Rows.Add(dr);
            }
            return tbl;
        }
    }
}
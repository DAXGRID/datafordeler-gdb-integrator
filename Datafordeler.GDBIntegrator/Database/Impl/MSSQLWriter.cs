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

        public MSSQLWriter(
            ILogger<MSSQLWriter> logger,
            IOptions<DatabaseSetting> databaseSetting
            )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
        }

        public void UpsertData(List<JObject> batch)
        {
            //_logger.LogDebug("I wrote some stuff: " + data);
            string connectionString;
            SqlConnection cnn ;
            SqlCommand command;
            cnn = new SqlConnection(_databaseSetting.ConnectionString);
            using(SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();
                using (SqlCommand command1 = connection.CreateCommand())
                {
                    command1.CommandText="dbo.UpsertAdress";
                    command1.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = command1.Parameters.AddWithValue("@UpdateRecords",CreateDataTable(batch));
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "dbo.adressTable1";
                    command1.ExecuteNonQuery();
                }
            }
            cnn.Close();
        }

        public  DataTable CreateDataTable(List<JObject> batch )
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("ID",typeof(string)));
            tbl.Columns.Add(new DataColumn("UnitAdressDescription",typeof(string)));
            tbl.Columns.Add(new DataColumn("Door",typeof(string)));
            tbl.Columns.Add(new DataColumn("Floor",typeof(string)));
            tbl.Columns.Add(new DataColumn("HouseNumber",typeof(string)));

            foreach(var obj in batch )
            {
                DataRow dr = tbl.NewRow();
                dr["ID"] = obj["id_lokalId"];
                dr["UnitAdressDescription"] = obj["unitAddressDescription"];
                dr["Door"] = obj["door"];
                dr["Floor"] = obj["floor"];
                dr["HouseNumber"] = obj["houseNumber"];
                tbl.Rows.Add(dr);
            }

            return tbl;
        }
    }
}
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

        public void AddToSql(List<JObject> batch, string topic, string[] columns)
        {
            if (checkTable(topic) == false)
            {
                createTable(topic, columns);
                createType(topic, columns);
                createStoredProcedure(topic, columns);

                if (columns.Contains("geo"))
                {
                    UpsertData(batch, topic, columns);
                }
                else
                {
                    var list = checkLatestDataDuplicates(batch);
                    UpsertData(list, topic, columns);
                }

            }
            else
            {
                if (columns.Contains("geo"))
                {
                    UpsertData(batch, topic, columns);
                }
                else
                {
                    var list = checkLatestDataDuplicates(batch);
                    UpsertData(list, topic, columns);
                }

            }
        }

        public void UpsertData(List<JObject> batch, string topic, string[] columns)
        {

            try
            {


                using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
                {
                    connection.Open();

                    using (SqlCommand command1 = connection.CreateCommand())
                    {

                        command1.CommandText = "dbo.Upsert" + topic;
                        //command1.CommandText = "[dbo].[BulkInsertFromWKT]";

                        command1.CommandType = CommandType.StoredProcedure;

                        SqlParameter parameter = command1.Parameters.AddWithValue("@UpdateRecords", CreateDataTable(batch, columns));
                        parameter.SqlDbType = SqlDbType.Structured;
                        parameter.TypeName = "dbo.Table" + topic;
                        //parameter.TypeName = "[dbo].[WKT_Example]";
                        command1.ExecuteNonQuery();
                    }
                    _logger.LogInformation(batch.Count + " items were inserted in " + topic);
                    connection.Close();

                }
            }
            catch (System.Data.SqlClient.SqlException e)
            {

                _logger.LogError("Error writing data: {0}.", e.GetType().Name);
                foreach (var c in batch)
                {
                    if (String.IsNullOrEmpty(c["roadRegistrationRoadLine"].ToString()))
                    {
                        _logger.LogInformation(c.ToString());
                    }
                }

            }
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
                    if ((string)obj[col] != "null")
                    {
                        dr[col] = obj[col];
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

        public List<JObject> checkLatestDataDuplicates(List<JObject> batch)
        {
            var dictionary = new Dictionary<string, JObject>();
            var list = new List<JObject>();

            foreach (var currentItem in batch)
            {
                JObject parsedItem;
                if (dictionary.TryGetValue(currentItem["id_lokalId"]?.ToString(), out parsedItem))
                {
                    //Set the time variables
                    var registrationFrom = DateTime.MinValue;
                    var itemRegistrationFrom = DateTime.MinValue;
                    var registrationTo = DateTime.MinValue;
                    var itemRegistrationTo = DateTime.MinValue;
                    var effectFrom = DateTime.MinValue;
                    var itemEffectFrom = DateTime.MinValue;
                    var effectTo = DateTime.MinValue;
                    var itemEffectTo = DateTime.MinValue;

                    bool CheckIfNull(JObject jObject, string key)
                    {
                        return jObject[key]! is null && jObject[key].ToString() != "null";
                    }

                    //Check if it contains the null string
                    // TODO here replace with CheckIfNull
                    if (CheckIfNull(parsedItem, "registrationFrom"))
                    {
                        registrationFrom = DateTime.Parse(parsedItem["registrationFrom"].ToString());
                    }

                    if (currentItem["registrationFrom"]?.ToString() != "null")
                    {
                        itemRegistrationFrom = DateTime.Parse(currentItem["registrationFrom"].ToString());
                    }

                    if (parsedItem["registrationTo"].ToString() != "null")
                    {
                        registrationTo = DateTime.Parse(parsedItem["registrationTo"].ToString());
                    }

                    if (currentItem["registrationTo"].ToString() != "null")
                    {
                        itemRegistrationTo = DateTime.Parse(currentItem["registrationTo"].ToString());
                    }

                    if (parsedItem["effectFrom"].ToString() != "null")
                    {
                        effectFrom = DateTime.Parse(parsedItem["effectFrom"].ToString());
                    }

                    if (currentItem["effectFrom"].ToString() != "null")
                    {
                        itemEffectFrom = DateTime.Parse(currentItem["effectFrom"].ToString());
                    }

                    if (parsedItem["effectTo"].ToString() != "null")
                    {
                        effectTo = DateTime.Parse(parsedItem["effectTo"].ToString());
                    }

                    if (currentItem["effectTo"].ToString() != "null")
                    {
                        itemEffectTo = DateTime.Parse(currentItem["effectTo"].ToString());
                    }

                    var idLokalIdItem = dictionary[currentItem["id_lokalId"]?.ToString()];
                    // Compare the date time values and return only the latest
                    if (registrationFrom < itemRegistrationFrom)
                    {
                        idLokalIdItem = currentItem;
                    }
                    else if (registrationFrom == itemRegistrationFrom)
                    {
                        if (registrationTo < itemRegistrationTo)
                        {
                            idLokalIdItem = currentItem;
                        }
                        else if (registrationTo == itemRegistrationTo)
                        {
                            if (effectFrom < itemEffectFrom)
                            {
                                idLokalIdItem = currentItem;
                            }
                            else if (effectFrom == itemEffectFrom)
                            {
                                if (effectTo < itemEffectTo)
                                {
                                    idLokalIdItem = currentItem;
                                }
                            }
                        }
                    }
                }
                else
                {
                    dictionary[currentItem["id_lokalId"].ToString()] = currentItem;
                }
            }

            //Add in the list only the objects with the latest date
            foreach (var d in dictionary)
            {
                list.Add(d.Value);
            }

            return list;
        }

        public void createTable(string topic, string[] columns)
        {
            using (var connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                StringBuilder mystringBuilder = new StringBuilder();
                string id;

                if (columns.Contains("geo"))
                {
                    id = "gml_id";
                }
                else
                {
                    id = "id_lokalId";
                }

                foreach (var column in columns)
                {
                    if (column == "id_lokalId" | column == "gml_id")
                    {
                        mystringBuilder.Append(column + " varchar(900)" + ",");
                    }
                    else if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo"  | column == "byg404Koordinat" | column == "tek109Koordinat"  )
                    {
                        mystringBuilder.Append(column + " geometry " + ",");
                    }
                    else
                    {
                        mystringBuilder.Append(column + " varchar(max)" + ",");
                    }
                }
                mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
                string tableCommandText = "Create table " + topic + " (" + mystringBuilder + " CONSTRAINT PK_" + topic + " PRIMARY KEY" + " (" + id + ")" + ")";

                using (var command = new SqlCommand(tableCommandText, connection))
                {
                    command.ExecuteNonQuery();
                }

                _logger.LogInformation("Table" + topic + " created");
            }
        }

        public void createType(string topic, string[] columns)
        {
            using (var connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                var mystringBuilder = new StringBuilder();

                foreach (var column in columns)
                {
                    if (column == "id_lokalId" || column == "gml_id")
                    {
                        mystringBuilder.Append(column + " varchar(900)" + ",");
                    }
                    else
                    {
                        mystringBuilder.Append(column + " varchar(max)" + ",");
                    }
                }

                mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
                var commandText = "Create type " + "table" + topic + " As table " + " (" + mystringBuilder + ")";

                connection.Open();

                using (SqlCommand command = new SqlCommand(commandText, connection))
                {
                    command.ExecuteNonQuery();
                }

                _logger.LogInformation("Table type created");

                connection.Close();
            }
        }

        public void createStoredProcedure(string topic, string[] columns)
        {
            using (var connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                var matchedRecordString = new StringBuilder();
                var tableColumns = new StringBuilder();
                var sourceColumns = new StringBuilder();

                string id;

                if (columns.Contains("geo"))
                {
                    id = "gml_id";
                }
                else
                {
                    id = "id_lokalId";
                }

                foreach (var col in columns)
                {
                    if (col == "position" | col == "geo" | col == "roadRegistrationRoadLine")
                    {
                        matchedRecordString.Append("Target." + col + "=" + "geometry::STGeomFromText(Source." + col + ",25832),");
                        sourceColumns.Append("geometry::STGeomFromText(Source." + col + ",25832),");
                        tableColumns.Append(col + ",");
                    }
                    else
                    {
                        matchedRecordString.Append("Target." + col + "=" + "Source." + col + ",");
                        sourceColumns.Append("Source." + col + ",");
                        tableColumns.Append(col + ",");
                    }
                }
                matchedRecordString = matchedRecordString.Remove(matchedRecordString.Length - 1, 1);
                sourceColumns = sourceColumns.Remove(sourceColumns.Length - 1, 1);
                tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);

                // TODO use @ to not concat lines
                // Maybe look into SQL injection issues
                var commandText = "CREATE PROCEDURE " + "dbo.Upsert" + topic
                + " @UpdateRecords " + "dbo.Table" + topic + " READONLY "
                + "AS BEGIN "
                + "MERGE INTO " + topic + " AS Target "
                + "Using @UpdateRecords AS Source "
                + "On Target." + id + "= Source." + id + " WHEN MATCHED THEN "
                + "UPDATE SET " + matchedRecordString
                + " WHEN NOT MATCHED THEN "
                + "INSERT " + "(" + tableColumns + ")"
                + "Values " + "(" + sourceColumns + "); "
                + "END";

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.ExecuteNonQuery();
                }

                _logger.LogInformation("Stored procedure created");

                connection.Close();
            }
        }

        public bool checkTable(string topic)
        {
            using (var connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                string cmdText = @"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_NAME='" + topic + "') SELECT 1 ELSE SELECT 0";
                connection.Open();
                using (SqlCommand command = new SqlCommand(cmdText, connection))
                {
                    bool x = Convert.ToBoolean(command.ExecuteScalar());
                    connection.Close();
                    return x;
                }
            }
        }

        public bool checkType(string topic)
        {
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {

                string cmdText = @"IF EXISTS(SELECT * FROM sys.types
                       WHERE name='" + "table" + topic + "') SELECT 1 ELSE SELECT 0";
                connection.Open();

                using (SqlCommand command = new SqlCommand(cmdText, connection))
                {
                    bool x = Convert.ToBoolean(command.ExecuteScalar());
                    connection.Close();
                    return x;
                }
            }
        }

        public bool checkProcedure(string topic)
        {
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                string cmdText = @"IF EXISTS(SELECT * FROM sys.procedures
                       WHERE name='" + "upsert" + topic + "') SELECT 1 ELSE SELECT 0";

                connection.Open();

                using (SqlCommand command = new SqlCommand(cmdText, connection))
                {
                    bool x = Convert.ToBoolean(command.ExecuteScalar());
                    connection.Close();
                    return x;
                }
            }
        }

    }
}

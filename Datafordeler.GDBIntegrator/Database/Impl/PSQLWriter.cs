using Datafordeler.DBIntegrator.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Linq;
using NetTopologySuite.Geometries;
using Npgsql;

namespace Datafordeler.GDBIntegrator.Database.Impl
{
    public class PSQLWriter : IPostgresWriter
    {
        private readonly ILogger<PSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;
        private readonly KafkaSetting _kafkaSetting;
        private bool postgisExecuted;
        private bool tableCreated;

        private bool dataInserted;

        public PSQLWriter(
           ILogger<PSQLWriter> logger,
           IOptions<DatabaseSetting> databaseSetting,
           IOptions<KafkaSetting> kafkaSetting
           )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
            _kafkaSetting = kafkaSetting.Value;
            postgisExecuted = false;
            tableCreated = false;
            dataInserted = false;

        }

        public void AddToPSQL(List<JObject> batch, string topic, string[] columns)
        {

            if (postgisExecuted == false)
            {
                //createPostgis();
                postgisExecuted = true;
            }

           
            //create temporary table
            if(checkTable(topic+"_temp") == false )
            {
                createTemporaryTable(topic + "_temp", columns);
                createTable(topic, columns);
            }
            else
            {
                var objects = checkLatestDataDuplicates(batch);
                _logger.LogInformation("This is the number of objects " + objects.Count);
                UpsertData(objects, topic +"_temp", columns);
                InsertOnConflict(topic +"_temp", topic, columns);
            }
            

        }

        public void TryMethod(List<JObject> batch, string topic, string tempTable, string[] columns)
        {
            string id;
            var mystringBuilder = new StringBuilder();
            var onConflictColumns = new StringBuilder();

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
                if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                {
                    mystringBuilder.Append("ST_GeomFromText(" + tempTable + "." + column + ",5432),");
                    onConflictColumns.Append(column + " = " + "ST_GeomFromText(EXCLUDED." + column + ",5432),");
                }
                else
                {
                    mystringBuilder.Append(tempTable + "." + column + ",");
                    onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");

                }
            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);

            string insertCommand = "CREATE PROCEDURE" + topic + "insert()"
                + " language sql as $$ "
                + " INSERT INTO " + topic + " SELECT "
                + mystringBuilder
                + " FROM " + tempTable
                + " ON CONFLICT (" + id + ") DO UPDATE "
                + " SET "
                + onConflictColumns + ";"
                + "DROP TABLE " + tempTable + ";";

        }

        public void createTemporaryTable(string topic,string [] columns)
        {
                 using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                StringBuilder mystringBuilder = new StringBuilder();
                string id;
                string tableCommandText;

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
                    mystringBuilder.Append(column + " varchar" + ",");
                    
                }

                mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);

                tableCommandText = "Create table IF NOT EXISTS " + topic + " (" + mystringBuilder + ");";

                using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
                {
                    command.ExecuteNonQuery();

                }

                _logger.LogInformation("Temporary Table " + topic + " created");
            }
   
        }

        public void DropTable(string table)
        {
            using (var conn = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                var commandText = "DROP TABLE " + table + ";";
                using (var command = new NpgsqlCommand(commandText, conn))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        public void InsertOnConflict(string tempTable, string table, string[] columns)
        {
            string id;
            var mystringBuilder = new StringBuilder();
            var onConflictColumns = new StringBuilder();

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
                if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                {
                    mystringBuilder.Append("ST_GeomFromText(" + tempTable + "." + column + ",5432),");
                    onConflictColumns.Append(column + " = " + "ST_GeomFromText(EXCLUDED." + column + ",5432),");
                }
                else
                {
                    mystringBuilder.Append(tempTable + "." + column + ",");
                    onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");

                }
            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);



            using (NpgsqlConnection conn = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                conn.Open();
                string commandText = " INSERT INTO " + table + " SELECT DISTINCT ON (1) "
                + mystringBuilder
                + " FROM " + tempTable
                + " ON CONFLICT (" + id + ") DO UPDATE "
                + " SET "
                + onConflictColumns + ";";

                using (NpgsqlCommand command = new NpgsqlCommand(commandText, conn))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        public void UpsertData(List<JObject> batch, string topic, string[] columns)
        {
            StringBuilder mystringBuilder = new StringBuilder();

            foreach (var column in columns)
            {
                mystringBuilder.Append(column + ",");
            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
            using (var conn = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                conn.Open();
                using (var writer = conn.BeginBinaryImport("COPY " + topic + " (" + mystringBuilder + ") FROM STDIN (FORMAT BINARY) "))
                {
                    foreach (var document in batch)
                    {
                        writer.StartRow();
                        foreach (var column in columns)
                        {

                            writer.Write((string)document[column]);
                        }
                    }
                    writer.Complete();
                    batch.Clear();
                }
            }

        }

        public void createPostgis()
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();
                string tableCommandText = "Create extension postgis";
                using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        public void createTable(string topic, string[] columns)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                StringBuilder mystringBuilder = new StringBuilder();
                string id;
                string tableCommandText;

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
                    if (topic.Contains("temp"))
                    {
                        mystringBuilder.Append(column + " varchar" + ",");
                    }
                    else
                    {
                        if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                        {
                            mystringBuilder.Append(column + " geometry" + ",");
                        }
                        else
                        {
                            mystringBuilder.Append(column + " varchar" + ",");

                        }
                    }
                }

                //mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);

                tableCommandText = "Create table IF NOT EXISTS " + topic + " (" + mystringBuilder + " PRIMARY KEY" + " (" + id + ")" + ");";

                using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
                {
                    command.ExecuteNonQuery();

                }

                _logger.LogInformation("Table " + topic + " created");
            }

        }

        public bool checkTable(string table)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();
                string tableCommandText = " SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = 'public' AND  table_name   = '" + table.ToLower() + "');";
                using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
                {
                    command.ExecuteNonQuery();
                    using (var reader = command.ExecuteReader())
                    {
                        reader.Read();
                        _logger.LogInformation("Is it true that the table exists" + reader.GetValue(0).ToString());
                        return (bool)reader.GetValue(0);
                    }
                }
            }
        }
        public List<JObject> checkLatestDataDuplicates(List<JObject> batch)
        {
            var dictionary = new Dictionary<string, JObject>();
            var list = new List<JObject>();

            foreach (var item in batch)
            {
                JObject jobject;
                if (dictionary.TryGetValue(item["id_lokalId"].ToString(), out jobject))
                {
                    //Set the time variables 
                    DateTime registrationFrom = DateTime.MinValue;
                    DateTime itemRegistrationFrom = DateTime.MinValue;
                    DateTime registrationTo = DateTime.MinValue;
                    DateTime itemRegistrationTo = DateTime.MinValue;
                    DateTime effectFrom = DateTime.MinValue;
                    DateTime itemEffectFrom = DateTime.MinValue;
                    DateTime effectTo = DateTime.MinValue;
                    DateTime itemEffectTo = DateTime.MinValue;

                    //Check if it contains the null string 
                    if (jobject["registrationFrom"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        registrationFrom = DateTime.Parse(jobject["registrationFrom"].ToString());
                    }

                    if (item["registrationFrom"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        itemRegistrationFrom = DateTime.Parse(item["registrationFrom"].ToString());
                    }

                    if (jobject["registrationTo"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        registrationTo = DateTime.Parse(jobject["registrationTo"].ToString());
                    }

                    if (item["registrationTo"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        itemRegistrationTo = DateTime.Parse(item["registrationTo"].ToString());
                    }

                    if (jobject["effectFrom"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        effectFrom = DateTime.Parse(jobject["effectFrom"].ToString());
                    }

                    if (item["effectFrom"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        itemEffectFrom = DateTime.Parse(item["effectFrom"].ToString());
                    }

                    if (jobject["effectTo"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        effectTo = DateTime.Parse(jobject["effectTo"].ToString());
                    }

                    if (item["effectTo"].ToString() != "null") // string.IsNullOrEmpty if on .NET pre 4.0
                    {
                        itemEffectTo = DateTime.Parse(item["effectTo"].ToString());
                    }


                    //Compare the date time values and return only the latest
                    if (registrationFrom < itemRegistrationFrom)
                    {
                        dictionary[item["id_lokalId"].ToString()] = item;

                    }
                    else if (registrationFrom == itemRegistrationFrom)
                    {
                        if (registrationTo < itemRegistrationTo)
                        {
                            dictionary[item["id_lokalId"].ToString()] = item;

                        }
                        else if (registrationTo == itemRegistrationTo)
                        {
                            if (effectFrom < itemEffectFrom)
                            {
                                dictionary[item["id_lokalId"].ToString()] = item;

                            }
                            else if (effectFrom == itemEffectFrom)
                            {
                                if (effectTo < itemEffectTo)
                                {
                                    dictionary[item["id_lokalId"].ToString()] = item;

                                }
                            }
                        }
                    }
                }
                else
                {
                    dictionary[item["id_lokalId"].ToString()] = item;
                }
            }
            //Add in the list only the objects with the latest date
            foreach (var d in dictionary)
            {
                list.Add(d.Value);
            }

            return list;

        }

    }
}

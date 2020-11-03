using Datafordeler.DBIntegrator.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Linq;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;

namespace Datafordeler.GDBIntegrator.Database.Impl
{
    public class PSQLWriter : IPostgresWriter
    {
        private readonly ILogger<PSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;
        private readonly KafkaSetting _kafkaSetting;
        private bool postgisExecuted;

        public PSQLWriter(
           ILogger<PSQLWriter> logger,
           IOptions<DatabaseSetting> databaseSetting,
           IOptions<KafkaSetting> kafkaSetting
           )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
            _kafkaSetting = kafkaSetting.Value;
            NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite();
        }

        public void AddToPSQL(List<JObject> batch, string topic, string[] columns)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();
                createTemporaryTable(topic + "_temp", columns, connection);
                createTable(topic, columns, connection);
                var objects = checkLatestDataDuplicates(batch);
                UpsertData(objects, topic + "_temp", columns, connection);
                InsertOnConflict(topic + "_temp", topic, columns, connection);
            }
        }

        public void createTemporaryTable(string topic, string[] columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();

            foreach (var column in columns)
            {
                if (column == "position" || column == "roadRegistrationRoadLine" || column == "geo")
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }
            }

            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);

            var tableCommandText = "Create temporary table " + topic + " (" + tableColumns + ");";

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation("Temporary Table " + topic + " created");
        }

        public void InsertOnConflict(string tempTable, string table, string[] columns, NpgsqlConnection conn)
        {
            string id;
            var tempColumns = new StringBuilder();
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
                tempColumns.Append(tempTable + "." + column + ",");
                onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");
            }

            tempColumns = tempColumns.Remove(tempColumns.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);

            var commandText = " INSERT INTO " + table + " SELECT DISTINCT ON (1) "
            + tempColumns
            + " FROM " + tempTable
            + " ON CONFLICT (" + id + ") DO UPDATE "
            + " SET "
            + onConflictColumns + ";";

            using (var command = new NpgsqlCommand(commandText, conn))
            {
                command.ExecuteNonQuery();
            }
        }
        public void UpsertData(List<JObject> batch, string topic, string[] columns, NpgsqlConnection conn)
        {
            var tableColumns = new StringBuilder();
            var geometryFactory = new GeometryFactory();
            var rdr = new WKTReader(geometryFactory);

            foreach (var column in columns)
            {

                tableColumns.Append(column + ",");

            }
            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);

            using (var writer = conn.BeginBinaryImport("COPY " + topic + " (" + tableColumns + ") FROM STDIN (FORMAT BINARY) "))
            {
                foreach (var document in batch)
                {
                    writer.StartRow();
                    foreach (var column in columns)
                    {
                        if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                        {
                            // TODO add environment variable
                            rdr.DefaultSRID = 25832;
                            var c = rdr.Read((string)document[column]);
                            writer.Write(c);
                        }
                        else
                        {
                            writer.Write((string)document[column]);
                        }
                    }
                }

                writer.Complete();
                batch.Clear();
            }
        }

        public void createTable(string topic, string[] columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();
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

                if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }

            }

            var tableCommandText = "Create table IF NOT EXISTS " + topic + " (" + tableColumns + " PRIMARY KEY" + " (" + id + ")" + ");";
            _logger.LogInformation(tableCommandText);

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation("Table " + topic + " created");
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

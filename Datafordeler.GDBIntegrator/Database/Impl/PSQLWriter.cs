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
using NetTopologySuite.IO;
using Npgsql;
using Npgsql.NetTopologySuite;

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
            NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite();

        }

        public void AddToPSQL(List<JObject> batch, string topic, string[] columns)
        {

            if (postgisExecuted == false)
            {
                //createPostgis();
                postgisExecuted = true;
            }

            //TryMethod(batch, topic, topic + "_temp", columns);

            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();
                createTemporaryTable(topic+"_temp", columns, connection);
                createTable(topic, columns, connection);
                var objects = checkLatestDataDuplicates(batch);
                UpsertData(objects, topic+"_temp", columns, connection);
                InsertOnConflict(topic+"_temp",topic,columns,connection);
            }

        }

        public void TryMethod(List<JObject> batch, string topic, string tempTable, string[] columns)
        {
            string id;
            var mystringBuilder = new StringBuilder();
            var onConflictColumns = new StringBuilder();
            var createTemporaryTableBuilder = new StringBuilder();
            var createTableBuilder = new StringBuilder();
            var copyBuilder = new StringBuilder();
            GeometryFactory geometryFactory = new GeometryFactory();
            WKTReader rdr = new WKTReader(geometryFactory);

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
                copyBuilder.Append(column + ",");

                if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                {
                    createTableBuilder.Append(column + " geometry" + ",");
                    createTemporaryTableBuilder.Append(column + " geometry" + ",");

                }
                else
                {
                    createTableBuilder.Append(column + " varchar" + ",");
                    createTemporaryTableBuilder.Append(column + " varchar" + ",");

                }


                mystringBuilder.Append(tempTable + "." + column + ",");
                onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");

            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);
            createTemporaryTableBuilder = createTemporaryTableBuilder.Remove(createTableBuilder.Length - 1, 1);
            copyBuilder = copyBuilder.Remove(copyBuilder.Length - 1, 1);

            string insertCommand = " INSERT INTO " + topic + " SELECT DISTINCT ON (1) "
             + mystringBuilder
             + " FROM " + tempTable
             + " ON CONFLICT (" + id + ") DO UPDATE "
             + " SET "
             + onConflictColumns + ";";


            string createTemporaryTable = "Create temporary table " + tempTable + " (" + createTemporaryTableBuilder + " );";
            string createTable = "Create table IF NOT EXISTS " + topic + " (" + createTableBuilder + " PRIMARY KEY" + " (" + id + ")" + ");";



            _logger.LogInformation(createTemporaryTable);
            _logger.LogInformation(createTable);

            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = new NpgsqlCommand(createTemporaryTable, connection))
                {
                    command.ExecuteNonQuery();
                }

                if (checkTable(topic) == false)
                {

                    using (NpgsqlCommand command = new NpgsqlCommand(createTable, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                var objects = checkLatestDataDuplicates(batch);


                using (var writer = connection.BeginBinaryImport("COPY " + tempTable + " (" + copyBuilder + ") FROM STDIN (FORMAT BINARY) "))
                {
                    foreach (var document in objects)
                    {
                        writer.StartRow();
                        foreach (var column in columns)
                        {
                            if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                            {
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
                    objects.Clear();
                }

                using (NpgsqlCommand command = new NpgsqlCommand(insertCommand, connection))
                {
                    command.ExecuteNonQuery();
                }




            }

        }

        public void createTemporaryTable(string topic, string[] columns, NpgsqlConnection connection)
        {


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
                if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                {
                    mystringBuilder.Append(column + " geometry" + ",");
                }
                else
                {
                    mystringBuilder.Append(column + " varchar" + ",");
                }

            }


            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);

            tableCommandText = "Create temporary table " + topic + " (" + mystringBuilder + ");";

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();

            }

            _logger.LogInformation("Temporary Table " + topic + " created");

        }

        public void DropTable(string table)
        {
            using (var conn = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {
                conn.Open();
                var commandText = "DROP TABLE " + table + ";";
                using (var command = new NpgsqlCommand(commandText, conn))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        public void InsertOnConflict(string tempTable, string table, string[] columns, NpgsqlConnection conn)
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

                mystringBuilder.Append(tempTable + "." + column + ",");
                onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");

            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);

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
        public void UpsertData(List<JObject> batch, string topic, string[] columns, NpgsqlConnection conn)
        {
            StringBuilder mystringBuilder = new StringBuilder();
            GeometryFactory geometryFactory = new GeometryFactory();
            WKTReader rdr = new WKTReader(geometryFactory);

            foreach (var column in columns)
            {

                mystringBuilder.Append(column + ",");


            }
            mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1, 1);

            using (var writer = conn.BeginBinaryImport("COPY " + topic + " (" + mystringBuilder + ") FROM STDIN (FORMAT BINARY) "))
            {
                foreach (var document in batch)
                {
                    writer.StartRow();
                    foreach (var column in columns)
                    {
                        if (column == "position" | column == "roadRegistrationRoadLine" | column == "geo")
                        {
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
        public void createTable(string topic, string[] columns, NpgsqlConnection connection)
        {

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
            _logger.LogInformation(tableCommandText);

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();

            }

            _logger.LogInformation("Table " + topic + " created");


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

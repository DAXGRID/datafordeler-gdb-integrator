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

        public void AddToPSQL(List<JObject> batch, string topic, List<string> columns)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_databaseSetting.ConnectionString))
            {

                
                // Add serial id for maping purposes
                columns.Add("gid");
                connection.Open();
                createTemporaryTable(topic + "_temp", columns, connection);
                createTable(topic, columns, connection);
                createSpatialIndex(topic,columns,connection);

                InsertTemporaryData(batch, topic + "_temp", columns, connection);
                InsertOnConflict(topic + "_temp", topic, columns, connection);
               

            }
        }

        private void createTemporaryTable(string topic, List<string> columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();

            foreach (var column in columns)
            {
                if (column == "position" || column == "roadRegistrationRoadLine" || column == "geo" || column == "byg404Koordinat" || column == "tek109Koordinat")
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else if (column == "population")
                {
                    tableColumns.Append(column + " integer" + ",");
                }
                else if (column == "gid")
                {
                    tableColumns.Append(column + " serial" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }
            }

            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);
            var tableColumnsText = tableColumns.ToString();

            var tableCommandText = @$"Create temporary table  {topic}  (  {tableColumns} );";



            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {

                command.ExecuteNonQuery();
            }

            _logger.LogInformation(@$"Temporary Table {topic} created");
        }

        private void InsertOnConflict(string tempTable, string table, List<string> columns, NpgsqlConnection conn)
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

            var commandText = @$" INSERT INTO  {table}  SELECT DISTINCT ON (1)  {tempColumns} FROM   {tempTable}  ON CONFLICT ( {id}  ) DO UPDATE  SET  {onConflictColumns}  ;";

            using (var command = new NpgsqlCommand(commandText, conn))
            {
                command.ExecuteNonQuery();
            }
        }
        private void InsertTemporaryData(List<JObject> batch, string topic, List<string> columns, NpgsqlConnection conn)
        {
            var i = 0;
            var tableColumns = new StringBuilder();
            var geometryFactory = new GeometryFactory();
            var rdr = new WKTReader(geometryFactory);

            foreach (var column in columns)
            {

                tableColumns.Append(column + ",");

            }
            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);

            var comand = @$"COPY  {topic}   (  {tableColumns} ) FROM STDIN (FORMAT BINARY)";

            using (var writer = conn.BeginBinaryImport(comand))
            {
                foreach (var document in batch)
                {  
                    i++;
                    writer.StartRow();
                    foreach (var column in columns)
                    {

                        if (column == "position" || column == "roadRegistrationRoadLine" || column == "geo" || column == "byg404Koordinat" || column == "tek109Koordinat")
                        {
                            // TODO add environment variable
                            rdr.DefaultSRID = _databaseSetting.GeoSRID;

                            if (document[column].Type == JTokenType.Null)
                            {
                                document[column] = "LINESTRING (0 0, 0 0)";
                            }

                            var c = rdr.Read((string)document[column]);
                            writer.Write(c);
                        }
                        else if (column == "population")
                        {
                            if (document[column].Type == JTokenType.Null)
                            {
                                document[column] = 0;
                            }

                            writer.Write((int)document[column]);
                        }
                        else if (column == "gid")
                        {
                            writer.Write(i);
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

        private void createTable(string topic, List<string> columns, NpgsqlConnection connection)
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

                if (column == "position" || column == "roadRegistrationRoadLine" || column == "geo" || column == "byg404Koordinat" || column == "tek109Koordinat")
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else if (column == "population")
                {
                    tableColumns.Append(column + " integer" + ",");
                }
                else if (column == "gid")
                {
                    tableColumns.Append(column + " serial" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }

            }

            var tableCommandText = @$"Create table IF NOT EXISTS   {topic}   (  {tableColumns}  PRIMARY KEY   (  {id} ) );";
            _logger.LogInformation(tableCommandText);

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation(@$"Table {topic} created");
        }

        private void createSpatialIndex(string topic, List<string> columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();
            string geoColumn;

            if(columns.Contains("geo"))
            {
                geoColumn = "geo";
            }
            else if(columns.Contains("position"))
            {
                geoColumn = "position";
            }
            else 
            {
                geoColumn = "roadRegistrationRoadLine";
            }

            var tableCommandText = @$"Create index IF NOT EXISTS  {topic}_index ON {topic} USING GIST({geoColumn});";
            _logger.LogInformation(tableCommandText);

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation(@$"{topic}_index created");

        }

        private List<JObject> checkLatestDataDuplicates(List<JObject> batch)
        {
            var dictionary = new Dictionary<string, JObject>();
            var list = new List<JObject>();

            foreach (var currentItem in batch)
            {
                JObject parsedItem;
                if (dictionary.TryGetValue(currentItem["id_lokalId"].ToString(), out parsedItem))
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
                    //Check if it contains the null string 
                    bool CheckIfNull(JObject jObject, string key)
                    {
                        return jObject[key]! is null && jObject[key].ToString() != "null";
                    }

                    if (CheckIfNull(parsedItem, "registrationFrom"))
                    {
                        registrationFrom = DateTime.Parse(parsedItem["registrationFrom"].ToString());
                    }

                    if (CheckIfNull(currentItem, "registrationFrom"))
                    {
                        itemRegistrationFrom = DateTime.Parse(currentItem["registrationFrom"].ToString());
                    }

                    if (CheckIfNull(parsedItem, "registrationTo"))
                    {
                        registrationTo = DateTime.Parse(parsedItem["registrationTo"].ToString());
                    }

                    if (CheckIfNull(currentItem, "registrationTo"))
                    {
                        itemRegistrationTo = DateTime.Parse(currentItem["registrationTo"].ToString());
                    }

                    if (CheckIfNull(parsedItem, "effectFrom"))
                    {
                        effectFrom = DateTime.Parse(parsedItem["effectFrom"].ToString());
                    }

                    if (CheckIfNull(currentItem, "effectFrom"))
                    {
                        itemEffectFrom = DateTime.Parse(currentItem["effectFrom"].ToString());
                    }

                    if (CheckIfNull(parsedItem, "effectTo"))
                    {
                        effectTo = DateTime.Parse(parsedItem["effectTo"].ToString());
                    }

                    if (CheckIfNull(currentItem, "effectTo"))
                    {
                        itemEffectTo = DateTime.Parse(currentItem["effectTo"].ToString());
                    }

                    var idLokalIdItem = dictionary[currentItem["id_lokalId"]?.ToString()];
                    //Compare the date time values and return only the latest
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
    }
}

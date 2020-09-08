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
using NetTopologySuite.Geometries;

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
            var list = checkLatestDataDuplicates(batch);
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command1 = connection.CreateCommand())
                {

                    command1.CommandText = "dbo.Upsert" + topic;
                    //command1.CommandText = "[dbo].[BulkInsertFromWKT]";
                
                    command1.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = command1.Parameters.AddWithValue("@UpdateRecords", CreateDataTable(list, columns));
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "dbo.Table" + topic;
                    //parameter.TypeName = "[dbo].[WKT_Example]";
                    command1.ExecuteNonQuery();
                }

                connection.Close();

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

            foreach (var item in batch)
            {
                JObject jobject;
                if (dictionary.TryGetValue(item["id_lokalId"].ToString(), out jobject))
                {
                    //Set the time variables 
                    DateTime registrationFrom = DateTime.MinValue;
                    DateTime itemRegistrationFrom= DateTime.MinValue;
                    DateTime registrationTo= DateTime.MinValue;
                    DateTime itemRegistrationTo= DateTime.MinValue;
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

            Console.WriteLine(list.Count);

            return list;

        }

         public void createTable(string topic, string[] columns)
        {
            using (SqlConnection connection = new SqlConnection(_databaseSetting.ConnectionString))
            {
                StringBuilder mystringBuilder = new StringBuilder();


                foreach (var column in columns)
                {
                    if (column == "id_lokalId")
                    {
                        mystringBuilder.Append(column + " varchar(900)" + ",");
                    }
                    else if (column == "position")
                    {
                        mystringBuilder.Append(column + " geometry" + ",");
                    }
                    else
                    {
                        mystringBuilder.Append(column + " varchar(max)" + ",");
                    }
                }
                mystringBuilder = mystringBuilder.Remove(mystringBuilder.Length - 1,1);
                string commandText = "Create table " + topic + "(" + mystringBuilder +  ")";
                SqlCommand command = new SqlCommand(commandText, connection);
                Console.WriteLine(mystringBuilder);
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

    }
}
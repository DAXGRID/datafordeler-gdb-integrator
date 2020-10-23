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
    public class PSQLWriter:IPostgresWriter
    {
        private readonly ILogger<PSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;
        private readonly KafkaSetting _kafkaSetting;

         public PSQLWriter(
            ILogger<PSQLWriter> logger,
            IOptions<DatabaseSetting> databaseSetting,
            IOptions<KafkaSetting> kafkaSetting
            )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
            _kafkaSetting = kafkaSetting.Value;
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
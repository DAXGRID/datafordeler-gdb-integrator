using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json.Linq;

namespace Datafordeler.GDBIntegrator.Database
{
    public interface IDatabaseWriter
    {
        void UpsertData(List<JObject> batch, string topic,string[] columns);
        DataTable CreateDataTable(List<JObject> batch, string[] columns );
    }
}

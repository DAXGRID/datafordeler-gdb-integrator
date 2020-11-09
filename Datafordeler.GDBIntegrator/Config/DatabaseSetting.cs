using System;
using System.Collections.Generic;
using System.Text;

namespace Datafordeler.DBIntegrator.Config
{
    public class DatabaseSetting
    {
        public string DatabaseKind { get; set; }
        public string ConnectionString { get; set; }
        public int GeoSRID {get;set;}
        public Dictionary<string,string> Values 
        {
            get;
            set;
        } 
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace Datafordeler.DBIntegrator.Config
{
    public class KafkaSetting
    {
        public string Server { get; set; }
        public string PositionFilePath { get; set; }
        public string DatafordelereTopic { get; set; }
    }
}

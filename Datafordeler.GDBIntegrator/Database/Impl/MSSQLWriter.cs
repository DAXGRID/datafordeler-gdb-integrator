using Datafordeler.DBIntegrator.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;

namespace Datafordeler.GDBIntegrator.Database.Impl
{
    public class MSSQLWriter : IDatabaseWriter
    {
        private readonly ILogger<MSSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;

        public MSSQLWriter(
            ILogger<MSSQLWriter> logger,
            IOptions<DatabaseSetting> databaseSetting
            )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
        }

        public void UpsertStuff(string data)
        {
            _logger.LogDebug("I wrote some stuff: " + data);
        }
    }
}

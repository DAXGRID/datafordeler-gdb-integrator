using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Datafordeler.DBIntegrator.Config;
using Datafordeler.DBIntegrator.Consumer;
using Datafordeler.GDBIntegrator.Database;
using Datafordeler.GDBIntegrator.Database.Impl;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace Datafordeler.DBIntegrator
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            var hostBuilder = new HostBuilder();

            ConfigureApp(hostBuilder);
            ConfigureLogging(hostBuilder);
            ConfigureServices(hostBuilder);

            return hostBuilder;
        }

        private static void ConfigureApp(IHostBuilder hostBuilder)
        {
            hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.AddJsonFile("appsettings.json", true, true);
                config.AddEnvironmentVariables();                
            });
        }

        private static void ConfigureLogging(IHostBuilder hostBuilder)
        {
            Log.Logger = new LoggerConfiguration()
             .Enrich.FromLogContext()
             .MinimumLevel.Verbose()
             .WriteTo.Console()
             .WriteTo.Debug()
             .CreateLogger();

            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddLogging(loggingBuilder => loggingBuilder.AddSerilog(dispose: true));
            });
        }

        public static void ConfigureServices(IHostBuilder hostBuilder)
        {
            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddOptions();

                services.Configure<KafkaSetting>(kafkaSettings =>
                                                hostContext.Configuration.GetSection("Kafka").Bind(kafkaSettings));

                services.Configure<DatabaseSetting>(databaseSettings =>
                                               hostContext.Configuration.GetSection("Database").Bind(databaseSettings));


                services.AddLogging();
                services.AddSingleton<IDatafordelerConsumer, DatafordelereDatabaseWriter>();
                services.AddSingleton<IDatabaseWriter, MSSQLWriter>();
                services.AddSingleton<IPostgresWriter,PSQLWriter>();
                services.AddHostedService<Worker>();

                var loggingConfiguration = new ConfigurationBuilder()
                   .AddEnvironmentVariables().Build();

            });
        }
    }
}

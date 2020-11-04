using System;
using System.Threading;
using System.Threading.Tasks;
using Datafordeler.DBIntegrator.Consumer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Datafordeler.DBIntegrator
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IDatafordelerConsumer _datafordelerConsumer;

        public Worker(ILogger<Worker> logger, IDatafordelerConsumer datafordelerConsumer)
        {
            _logger = logger;
            _datafordelerConsumer = datafordelerConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting Worker running at: {time}", DateTimeOffset.Now);

            _datafordelerConsumer.Start();

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }

            _datafordelerConsumer.Dispose();
        }
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;
using Gosi.Kafka.Sdk.Consumer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Gosi.Kafka.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly GosiKafkaConsumer<string, string> _consumer;
        private readonly Gosi.Kafka.Sdk.Resilience.IResilienceWrapper<string, string> _resilienceWrapper;
        private readonly ILogger<KafkaConsumerService> _logger;

        public KafkaConsumerService(
            GosiKafkaConsumer<string, string> consumer, 
            Gosi.Kafka.Sdk.Resilience.IResilienceWrapper<string, string> resilienceWrapper,
            ILogger<KafkaConsumerService> logger)
        {
            // Fully auto-configured by the SDK!
            _consumer = consumer;
            _resilienceWrapper = resilienceWrapper;
            _logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => StartConsume(stoppingToken), stoppingToken);
        }

        private async Task StartConsume(CancellationToken cancellationToken)
        {
            _consumer.Topic("hrsd.employee.events.v1")
                     .WithResilience(_resilienceWrapper)
                     .Handler(async record =>
                     {
                         // The log below will automatically include the Trace ID scope!
                         _logger.LogInformation("Processing message details | Key: {Key} | Partition: {Partition} | Offset: {Offset} | Payload: {Payload}",
                                                record.Key, record.Partition, record.Offset, record.Value);
                         
                         if (record.Value != null && record.Value.Contains("FAIL"))
                         {
                             throw new Exception("Simulated processing failure! This will trigger the Resilience Wrapper to retry and route to DLQ.");
                         }
                         
                         await Task.CompletedTask;
                     });

            _logger.LogInformation(".NET Kafka Consumer Service is running and listening...");

            try
            {
                // The SDK handles polling, offset commits, DLQ, etc.
                await _consumer.StartAsync();
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            finally
            {
                _consumer.Dispose();
            }
        }
    }
}

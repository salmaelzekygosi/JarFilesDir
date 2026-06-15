using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Consumer;
using Gosi.Kafka.Sdk.Telemetry;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gosi.Kafka.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly GosiKafkaClientConfig _config;
        private readonly ITelemetryReporter _telemetry;
        private readonly ILogger<GosiKafkaConsumer<string, string>> _logger;

        public KafkaConsumerService(IConfiguration configuration, ILoggerFactory loggerFactory, ILogger<GosiKafkaConsumer<string, string>> logger)
        {
            _config = new GosiKafkaClientConfig.Builder()
                .WithBootstrapServers(configuration["GosiKafka:BootstrapServers"] ?? string.Empty)
                .WithGroupId(configuration["GosiKafka:GroupId"] ?? string.Empty)
                .WithKeyFormat(SerializationFormat.String)
                .WithValueFormat(SerializationFormat.String)
                .Build();
            
            _telemetry = new LoggerTelemetryReporter(loggerFactory.CreateLogger<LoggerTelemetryReporter>());
            _logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => StartConsume(stoppingToken), stoppingToken);
        }

        private async Task StartConsume(CancellationToken cancellationToken)
        {
            using var consumer = new GosiKafkaConsumer<string, string>(_config, _telemetry, _logger);
            
            consumer.Topic("payments.demo-topic.v1")
                    .Handler(async record =>
                    {
                        // The log below will automatically include the Trace ID scope!
                        _logger.LogInformation("Processing message details | Key: {Key} | Partition: {Partition} | Offset: {Offset} | Payload: {Payload}",
                                               record.Key, record.Partition, record.Offset, record.Value);
                        await Task.CompletedTask;
                    });

            _logger.LogInformation(".NET Kafka Consumer Service is running and listening...");

            try
            {
                // The SDK handles polling, offset commits, DLQ, etc.
                await consumer.StartAsync();
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            finally
            {
                consumer.Dispose();
            }
        }
    }
}

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Producer;
using Gosi.Kafka.Sdk.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gosi.Kafka.Services
{
    public class KafkaProducerService : IDisposable
    {
        private readonly GosiKafkaProducer<string, string> _producer;

        public KafkaProducerService(IConfiguration configuration, ILogger<GosiKafkaProducer<string, string>> logger, ILoggerFactory loggerFactory)
        {
            var config = new GosiKafkaClientConfig.Builder()
                .WithBootstrapServers(configuration["GosiKafka:BootstrapServers"] ?? string.Empty)
                .WithClientId(configuration["GosiKafka:ClientId"] ?? string.Empty)
                .WithKeyFormat(SerializationFormat.String)
                .WithValueFormat(SerializationFormat.String)
                .Build();
            
            var telemetry = new LoggerTelemetryReporter(loggerFactory.CreateLogger<LoggerTelemetryReporter>());
            _producer = new GosiKafkaProducer<string, string>(config, telemetry, logger);
        }

        public async Task<DeliveryReport> SendPaymentAsync(Payment payment)
        {
            string jsonPayload = JsonSerializer.Serialize(payment);
            
            string traceId = payment.TraceId ?? Guid.NewGuid().ToString();
            payment.TraceId = traceId;

            string key = payment.Id ?? Guid.NewGuid().ToString();

            var result = await _producer.ProduceAsync("payments.demo-topic.v1", key, jsonPayload);
            return result;
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Producer;
using Microsoft.Extensions.Logging;

namespace Gosi.Kafka.Services
{
    public class KafkaProducerService : IDisposable
    {
        private readonly GosiKafkaProducer<string, string> _producer;

        public KafkaProducerService(GosiKafkaProducer<string, string> producer)
        {
            // Fully auto-configured by the SDK!
            _producer = producer;
        }

        public async Task<Gosi.Kafka.Sdk.Telemetry.DeliveryReport> SendPaymentAsync(Payment payment)
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

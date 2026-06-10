using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace Gosi.Kafka.Services
{
    public class KafkaProducerService
    {
        private readonly ProducerConfig _config;

        public KafkaProducerService(IConfiguration configuration)
        {
            _config = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                SecurityProtocol = Enum.Parse<SecurityProtocol>(configuration["Kafka:SecurityProtocol"] ?? "SaslSsl"),
                SaslMechanism = Enum.Parse<SaslMechanism>(configuration["Kafka:SaslMechanism"] ?? "OAuthBearer"),
                SaslOauthbearerMethod = Enum.Parse<SaslOauthbearerMethod>(configuration["Kafka:SaslOauthbearerMethod"] ?? "Oidc"),
                SaslOauthbearerClientId = configuration["Kafka:SaslOauthbearerClientId"],
                SaslOauthbearerClientSecret = configuration["Kafka:SaslOauthbearerClientSecret"],
                SaslOauthbearerTokenEndpointUrl = configuration["Kafka:SaslOauthbearerTokenEndpointUrl"],
                SaslOauthbearerScope = configuration["Kafka:SaslOauthbearerScope"],
                SslCaLocation = configuration["Kafka:SslCaLocation"],
                Acks = Acks.All
            };
        }

        public async Task<DeliveryResult<string, string>> SendPaymentAsync(Payment payment)
        {
            string jsonPayload = JsonSerializer.Serialize(payment);
            
            string traceId = payment.TraceId ?? Guid.NewGuid().ToString();
            payment.TraceId = traceId;

            using var producer = new ProducerBuilder<string, string>(_config).Build();

            var headers = new Headers();
            // Inject the mandatory trace-id header
            headers.Add("trace-id", Encoding.UTF_8.GetBytes(traceId));

            var message = new Message<string, string>
            {
                Key = payment.Id,
                Value = jsonPayload,
                Headers = headers
            };

            var result = await producer.ProduceAsync("payments.demo-topic.v1", message);
            Console.WriteLine($".NET: Produced payment successfully: {payment.Id} | Partition: {result.Partition.Value} | Offset: {result.Offset.Value}");
            return result;
        }
    }
}

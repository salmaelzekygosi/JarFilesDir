using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;

namespace Gosi.Kafka.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ConsumerConfig _config;

        public KafkaConsumerService(IConfiguration configuration)
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "kafka-demo-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = Enum.Parse<SecurityProtocol>(configuration["Kafka:SecurityProtocol"] ?? "SaslSsl"),
                SaslMechanism = Enum.Parse<SaslMechanism>(configuration["Kafka:SaslMechanism"] ?? "OAuthBearer"),
                SaslOauthbearerMethod = Enum.Parse<SaslOauthbearerMethod>(configuration["Kafka:SaslOauthbearerMethod"] ?? "Oidc"),
                SaslOauthbearerClientId = configuration["Kafka:SaslOauthbearerClientId"],
                SaslOauthbearerClientSecret = configuration["Kafka:SaslOauthbearerClientSecret"],
                SaslOauthbearerTokenEndpointUrl = configuration["Kafka:SaslOauthbearerTokenEndpointUrl"],
                SaslOauthbearerScope = configuration["Kafka:SaslOauthbearerScope"],
                SslCaLocation = configuration["Kafka:SslCaLocation"]
            };
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => StartConsume(stoppingToken), stoppingToken);
        }

        private void StartConsume(CancellationToken cancellationToken)
        {
            using var consumer = new ConsumerBuilder<string, string>(_config).Build();
            consumer.Subscribe("payments.demo-topic.v1");

            Console.WriteLine(".NET Kafka Consumer Service is running and listening...");

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        string key = consumeResult.Message.Key;
                        string payload = consumeResult.Message.Value;
                        long offset = consumeResult.Offset.Value;
                        int partition = consumeResult.Partition.Value;

                        string traceId = "N/A";
                        if (consumeResult.Message.Headers != null)
                        {
                            foreach (var header in consumeResult.Message.Headers)
                            {
                                if (header.Key.Equals("trace-id", StringComparison.OrdinalIgnoreCase))
                                {
                                    traceId = Encoding.UTF_8.GetString(header.GetValueBytes());
                                    break;
                                }
                            }
                        }

                        Console.WriteLine("==================================================");
                        Console.WriteLine(".NET Received Kafka Message:");
                        Console.WriteLine($"  Key:        {key}");
                        Console.WriteLine($"  Partition:  {partition}");
                        Console.WriteLine($"  Offset:     {offset}");
                        Console.WriteLine($"  Trace ID:   {traceId}");
                        Console.WriteLine($"  Payload:    {payload}");
                        Console.WriteLine("==================================================");
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}

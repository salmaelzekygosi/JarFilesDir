using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Auth;
using Gosi.Kafka.Sdk.Producer;
using Gosi.Kafka.Sdk.Consumer;
using Gosi.Kafka.Sdk.Telemetry;

namespace Gosi.Kafka.Sdk.Extensions
{
    public static class GosiKafkaServiceCollectionExtensions
    {
        public static IServiceCollection AddGosiKafka(this IServiceCollection services, IConfiguration configuration)
        {
            var kafkaSection = configuration.GetSection("GosiKafka");
            if (!kafkaSection.Exists())
            {
                throw new InvalidOperationException("Missing 'GosiKafka' section in configuration.");
            }

            // Register Telemetry Reporter
            services.AddSingleton<ITelemetryReporter>(sp => 
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new LoggerTelemetryReporter(loggerFactory.CreateLogger<LoggerTelemetryReporter>());
            });

            // Enforce centralized observability console format
            services.Configure<SimpleConsoleFormatterOptions>(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.SingleLine = true;
            });

            // Register Client Config using SDK Builder
            services.AddSingleton<GosiKafkaClientConfig>(sp =>
            {
                var builder = new GosiKafkaClientConfig.Builder()
                    .WithBootstrapServers(kafkaSection["BootstrapServers"] ?? string.Empty)
                    .WithClientId(kafkaSection["ClientId"] ?? string.Empty)
                    .WithGroupId(kafkaSection["GroupId"] ?? string.Empty)
                    .WithSchemaRegistryUrl(kafkaSection["SchemaRegistryUrl"] ?? string.Empty);

                // Setup Authentication
                string saslMechanism = kafkaSection["SaslMechanism"] ?? string.Empty;
                if ("OAuthBearer".Equals(saslMechanism, StringComparison.OrdinalIgnoreCase))
                {
                    string clientId = kafkaSection["SaslOauthbearerClientId"] ?? string.Empty;
                    string clientSecret = kafkaSection["SaslOauthbearerClientSecret"] ?? string.Empty;
                    string tokenEndpoint = kafkaSection["SaslOauthbearerTokenEndpointUrl"] ?? string.Empty;
                    
                    bool useTls = "SASL_SSL".Equals(kafkaSection["SecurityProtocol"], StringComparison.OrdinalIgnoreCase) 
                               || "SSL".Equals(kafkaSection["SecurityProtocol"], StringComparison.OrdinalIgnoreCase);

                    var authHandler = new OAuthBearerAuthHandler(tokenEndpoint, clientId, clientSecret, useTls);
                    builder.WithAuthenticationHandler(authHandler);
                }

                return builder.Build();
            });

            // Register Open Generics for Producer and Consumer
            services.AddSingleton(typeof(GosiKafkaProducer<,>));
            services.AddTransient(typeof(GosiKafkaConsumer<,>));

            return services;
        }
    }
}

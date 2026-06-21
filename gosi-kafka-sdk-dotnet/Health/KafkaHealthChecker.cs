namespace Gosi.Kafka.Sdk.Health;

using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Config;

/// <summary>
/// Provides proactive connection health checks for the Kafka cluster in .NET.
/// Can be wired into ASP.NET Core Health Checks.
/// </summary>
public class KafkaHealthChecker
{
    private readonly ClientConfig _adminConfig;
    private readonly TimeSpan _timeout;

    public KafkaHealthChecker(GosiKafkaClientConfig clientConfig, TimeSpan? timeout = null)
    {
        _adminConfig = clientConfig.BuildProducerConfig();
        _timeout = timeout ?? TimeSpan.FromSeconds(5);
    }

    public async Task<HealthStatus> CheckHealthAsync()
    {
        long startMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        try
        {
            using var adminClient = new AdminClientBuilder(_adminConfig).Build();
            
            // In Confluent.Kafka, GetMetadata fetches the cluster metadata and requires a round-trip
            var metadata = adminClient.GetMetadata(_timeout);
            
            long latencyMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - startMs;

            return HealthStatus.CreateUp(latencyMs, $"Connected to {metadata.Brokers.Count} brokers.");
        }
        catch (Exception e)
        {
            long latencyMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - startMs;
            return HealthStatus.CreateDown(latencyMs, e.Message);
        }
    }
}

public class HealthStatus
{
    public bool IsUp { get; }
    public long LatencyMs { get; }
    public string Details { get; }

    private HealthStatus(bool isUp, long latencyMs, string details)
    {
        IsUp = isUp;
        LatencyMs = latencyMs;
        Details = details;
    }

    public static HealthStatus CreateUp(long latencyMs, string details) => new HealthStatus(true, latencyMs, details);
    public static HealthStatus CreateDown(long latencyMs, string details) => new HealthStatus(false, latencyMs, details);
}

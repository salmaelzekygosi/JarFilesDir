namespace Gosi.Kafka.Sdk.Consumer;

using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Producer;
using Gosi.Kafka.Sdk.Telemetry;

public class DlqReplayConsumer<TKey, TValue>
    where TKey : class
    where TValue : class
{
    private readonly GosiKafkaConsumer<TKey, TValue> _dlqConsumer;
    private readonly GosiKafkaProducer<TKey, TValue> _targetProducer;
    private readonly ITelemetryReporter _telemetryReporter;

    public DlqReplayConsumer(
        GosiKafkaClientConfig dlqConsumerConfig,
        GosiKafkaProducer<TKey, TValue> targetProducer,
        ITelemetryReporter telemetryReporter,
        Microsoft.Extensions.Logging.ILogger<GosiKafkaConsumer<TKey, TValue>> logger)
    {
        _targetProducer = targetProducer;
        _telemetryReporter = telemetryReporter;
        
        // The DLQ Consumer should not use a ResilienceWrapper to avoid infinite loops
        _dlqConsumer = new GosiKafkaConsumer<TKey, TValue>(dlqConsumerConfig, telemetryReporter, logger);
    }

    public async Task StartReplayAsync(string dlqTopic, string targetTopic)
    {
        _dlqConsumer
            .Topic(dlqTopic)
            .Handler(async record =>
            {
                bool success = false;
                try
                {
                    // Copy original headers but remove the error metadata
                    var replayHeaders = new Headers();
                    foreach (var h in record.Headers)
                    {
                        if (h.Key != "error_code" && h.Key != "stack_trace")
                        {
                            replayHeaders.Add(h.Key, h.GetValueBytes());
                        }
                    }

                    // Add replay marker
                    replayHeaders.Add("replayed", Encoding.UTF8.GetBytes("true"));

                    await _targetProducer.ProduceAsync(targetTopic, record.Key, record.Value, replayHeaders);
                    success = true;
                }
                finally
                {
                    _telemetryReporter.OnReplayAttempt(targetTopic, record.TraceId, success);
                }
            });

        await _dlqConsumer.StartAsync();
    }
}

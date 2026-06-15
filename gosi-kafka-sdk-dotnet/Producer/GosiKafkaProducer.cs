namespace Gosi.Kafka.Sdk.Producer;

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Telemetry;
using Gosi.Kafka.Sdk.Tracing;
using Microsoft.Extensions.Logging;

public class GosiKafkaProducer<TKey, TValue> : IDisposable
    where TKey : class
    where TValue : class
{
    private readonly IProducer<TKey, TValue> _internalProducer;
    private readonly ITelemetryReporter _telemetryReporter;
    private readonly ILogger<GosiKafkaProducer<TKey, TValue>> _logger;

    public GosiKafkaProducer(
        GosiKafkaClientConfig config,
        ITelemetryReporter telemetryReporter,
        ILogger<GosiKafkaProducer<TKey, TValue>> logger)
    {
        _telemetryReporter = telemetryReporter;
        _logger = logger;

        var producerConfig = config.BuildProducerConfig();
        var builder = new ProducerBuilder<TKey, TValue>(producerConfig);

        ISchemaRegistryClient? schemaRegistryClient = null;
        if (config.KeyFormat == SerializationFormat.Avro || config.ValueFormat == SerializationFormat.Avro ||
            config.KeyFormat == SerializationFormat.JsonSchema || config.ValueFormat == SerializationFormat.JsonSchema)
        {
            var schemaConfig = new SchemaRegistryConfig { Url = config.SchemaRegistryUrl };
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaConfig);
        }

        // Configure Serializers
        if (config.KeyFormat == SerializationFormat.Avro)
            builder.SetKeySerializer(new AvroSerializer<TKey>(schemaRegistryClient));
        else if (config.KeyFormat == SerializationFormat.JsonSchema)
            builder.SetKeySerializer(new JsonSerializer<TKey>(schemaRegistryClient));

        if (config.ValueFormat == SerializationFormat.Avro)
            builder.SetValueSerializer(new AvroSerializer<TValue>(schemaRegistryClient));
        else if (config.ValueFormat == SerializationFormat.JsonSchema)
            builder.SetValueSerializer(new JsonSerializer<TValue>(schemaRegistryClient));

        _internalProducer = builder.Build();
    }

    public async Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue value)
    {
        var headers = new Headers();
        
        // Inject trace_id (creates a new one if not present)
        var traceId = TraceContext.InjectIntoHeaders(headers);

        var message = new Message<TKey, TValue>
        {
            Key = key,
            Value = value,
            Headers = headers
        };

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var result = await _internalProducer.ProduceAsync(topic, message);
            stopwatch.Stop();

            var report = Telemetry.DeliveryReport.CreateSuccess(
                topic, result.Partition.Value, result.Offset.Value, result.Timestamp.UnixTimestampMs, traceId, stopwatch.ElapsedMilliseconds);
            
            _telemetryReporter.OnDeliveryReport(report);
            return report;
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            stopwatch.Stop();
            var report = Telemetry.DeliveryReport.CreateFailure(
                topic, traceId, stopwatch.ElapsedMilliseconds, ex);
                
            _telemetryReporter.OnDeliveryReport(report);
            throw;
        }
    }
    
    public void Produce(string topic, TKey key, TValue value)
    {
        var headers = new Headers();
        var traceId = TraceContext.InjectIntoHeaders(headers);

        var message = new Message<TKey, TValue>
        {
            Key = key,
            Value = value,
            Headers = headers
        };

        var stopwatch = Stopwatch.StartNew();

        _internalProducer.Produce(topic, message, deliveryReport =>
        {
            stopwatch.Stop();
            Telemetry.DeliveryReport report;

            if (deliveryReport.Error.IsError)
            {
                var exception = new KafkaException(deliveryReport.Error);
                report = Telemetry.DeliveryReport.CreateFailure(
                    topic, traceId, stopwatch.ElapsedMilliseconds, exception);
            }
            else
            {
                report = Telemetry.DeliveryReport.CreateSuccess(
                    topic, deliveryReport.Partition.Value, deliveryReport.Offset.Value, deliveryReport.Timestamp.UnixTimestampMs, traceId, stopwatch.ElapsedMilliseconds);
            }

            _telemetryReporter.OnDeliveryReport(report);
        });
    }

    public void Flush(TimeSpan timeout)
    {
        _internalProducer.Flush(timeout);
    }

    public void Dispose()
    {
        _internalProducer.Flush(TimeSpan.FromSeconds(10));
        _internalProducer.Dispose();
    }
}

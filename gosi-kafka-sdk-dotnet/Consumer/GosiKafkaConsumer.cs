namespace Gosi.Kafka.Sdk.Consumer;

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Producer;
using Gosi.Kafka.Sdk.Telemetry;
using Gosi.Kafka.Sdk.Tracing;
using Microsoft.Extensions.Logging;
using Confluent.Kafka.SyncOverAsync;

public class GosiKafkaConsumer<TKey, TValue> : IDisposable
    where TKey : class
    where TValue : class
{
    private readonly IConsumer<TKey, TValue> _internalConsumer;
    private readonly ITelemetryReporter _telemetryReporter;
    private readonly ILogger<GosiKafkaConsumer<TKey, TValue>> _logger;
    private readonly CancellationTokenSource _cts = new();

    private string? _topic;
    private Func<GosiRecord<TKey, TValue>, Task>? _handler;
    private string? _dlqTopic;
    private GosiKafkaProducer<TKey, TValue>? _dlqProducer;

    public GosiKafkaConsumer(
        GosiKafkaClientConfig config,
        ITelemetryReporter telemetryReporter,
        ILogger<GosiKafkaConsumer<TKey, TValue>> logger)
    {
        _telemetryReporter = telemetryReporter;
        _logger = logger;

        var consumerConfig = config.BuildConsumerConfig();
        var builder = new ConsumerBuilder<TKey, TValue>(consumerConfig);

        ISchemaRegistryClient? schemaRegistryClient = null;
        if (config.KeyFormat == SerializationFormat.Avro || config.ValueFormat == SerializationFormat.Avro ||
            config.KeyFormat == SerializationFormat.JsonSchema || config.ValueFormat == SerializationFormat.JsonSchema)
        {
            var schemaConfig = new SchemaRegistryConfig { Url = config.SchemaRegistryUrl };
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaConfig);
        }

        // Configure Deserializers
        if (config.KeyFormat == SerializationFormat.Avro)
            builder.SetKeyDeserializer(new AvroDeserializer<TKey>(schemaRegistryClient).AsSyncOverAsync());
        else if (config.KeyFormat == SerializationFormat.JsonSchema)
            builder.SetKeyDeserializer(new JsonDeserializer<TKey>().AsSyncOverAsync());

        if (config.ValueFormat == SerializationFormat.Avro)
            builder.SetValueDeserializer(new AvroDeserializer<TValue>(schemaRegistryClient).AsSyncOverAsync());
        else if (config.ValueFormat == SerializationFormat.JsonSchema)
            builder.SetValueDeserializer(new JsonDeserializer<TValue>().AsSyncOverAsync());

        _internalConsumer = builder.Build();
    }

    public GosiKafkaConsumer<TKey, TValue> Topic(string topic)
    {
        _topic = topic;
        return this;
    }

    public GosiKafkaConsumer<TKey, TValue> Handler(Func<GosiRecord<TKey, TValue>, Task> handler)
    {
        _handler = handler;
        return this;
    }

    public GosiKafkaConsumer<TKey, TValue> WithDlq(string dlqTopic, GosiKafkaProducer<TKey, TValue> dlqProducer)
    {
        _dlqTopic = dlqTopic;
        _dlqProducer = dlqProducer;
        return this;
    }

    public async Task StartAsync()
    {
        if (string.IsNullOrWhiteSpace(_topic) || _handler == null)
        {
            throw new InvalidOperationException("Topic and handler must be configured before starting.");
        }

        _internalConsumer.Subscribe(_topic);
        _logger.LogInformation("Started GosiKafkaConsumer for topic: {Topic}", _topic);

        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _internalConsumer.Consume(_cts.Token);
                    if (consumeResult == null) continue;

                    await ProcessRecordAsync(consumeResult);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Error consuming message");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer cancelled, shutting down");
        }
        finally
        {
            _internalConsumer.Close();
        }
    }

    private async Task ProcessRecordAsync(ConsumeResult<TKey, TValue> result)
    {
        using var scope = TraceContext.InitFromHeaders(_logger, result.Message.Headers, out string traceId);

        var gosiRecord = new GosiRecord<TKey, TValue>(
            result.Message.Key,
            result.Message.Value,
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            traceId,
            result.Message.Headers,
            result.Message.Timestamp.UnixTimestampMs
        );

        try
        {
            await _handler!(gosiRecord);
            CommitOffset(result);
        }
        catch (Exception e)
        {
            if (_dlqTopic != null && _dlqProducer != null)
            {
                await RerouteToDlqAsync(gosiRecord, e);
                CommitOffset(result); // Commit offset after successfully moving to DLQ
            }
            else
            {
                _logger.LogError(e, "Unhandled exception processing record, no DLQ configured.");
                // In production without DLQ, depending on requirements, might want to stop/throw
            }
        }
    }

    private void CommitOffset(ConsumeResult<TKey, TValue> result)
    {
        try
        {
            _internalConsumer.Commit(result);
            _telemetryReporter.OnOffsetCommit(result.Topic, result.Partition.Value, result.Offset.Value, true, null);
        }
        catch (KafkaException e)
        {
            _telemetryReporter.OnOffsetCommit(result.Topic, result.Partition.Value, result.Offset.Value, false, e);
        }
    }

    private async Task RerouteToDlqAsync(GosiRecord<TKey, TValue> record, Exception cause)
    {
        record.Headers.Add("error_code", Encoding.UTF8.GetBytes("500"));
        
        var stackTrace = cause.Message ?? cause.GetType().Name;
        record.Headers.Add("stack_trace", Encoding.UTF8.GetBytes(stackTrace));
        
        // Ensure trace_id is preserved in DLQ message
        TraceContext.InjectIntoHeaders(record.Headers, record.TraceId);

        await _dlqProducer!.ProduceAsync(_dlqTopic!, record.Key, record.Value);
        _telemetryReporter.OnDlqReroute(record.Topic, _dlqTopic!, record.TraceId, cause);
    }

    public void Shutdown()
    {
        _cts.Cancel();
    }

    public void Dispose()
    {
        _cts.Dispose();
        _internalConsumer.Dispose();
    }
}

namespace Gosi.Kafka.Sdk.Consumer;

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Resilience;
using Gosi.Kafka.Sdk.Telemetry;
using Gosi.Kafka.Sdk.Tracing;
using Gosi.Kafka.Sdk.Auth;
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
    private IResilienceWrapper<TKey, TValue>? _resilienceWrapper;

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

    public GosiKafkaConsumer<TKey, TValue> WithResilience(IResilienceWrapper<TKey, TValue> resilienceWrapper)
    {
        _resilienceWrapper = resilienceWrapper;
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

        if (_resilienceWrapper != null)
        {
            _resilienceWrapper.RecordRestart();
            if (_resilienceWrapper.IsInRestartLoop())
            {
                _logger.LogCritical("Consumer is in RESTART LOOP — consider investigating root cause before continuing");
            }
        }

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
                    var authError = AuthErrorClassifier.Classify(e);
                    if (authError == AuthErrorType.AUTHENTICATION_FAILURE || authError == AuthErrorType.AUTHORIZATION_DENIED)
                    {
                        _telemetryReporter.OnAuthError(authError.ToString(), e.Error.Reason);
                    }
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
            if (_resilienceWrapper != null)
            {
                await _resilienceWrapper.ProcessAsync(gosiRecord, _handler!);
            }
            else
            {
                await _handler!(gosiRecord);
            }
            CommitOffset(result);
        }
        catch (Exception e)
        {
            var authError = AuthErrorClassifier.Classify(e);
            if (authError == AuthErrorType.AUTHENTICATION_FAILURE || authError == AuthErrorType.AUTHORIZATION_DENIED)
            {
                _telemetryReporter.OnAuthError(authError.ToString(), e.Message);
                _logger.LogError(e, "{ErrorType} error processing record | topic={Topic} | trace_id={TraceId}",
                        authError, result.Topic, traceId);
                throw AuthErrorClassifier.ClassifyAndWrap(e);
            }

            if (_resilienceWrapper == null)
            {
                _logger.LogError(e, "Unhandled exception processing record, ResilienceWrapper is not configured.");
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

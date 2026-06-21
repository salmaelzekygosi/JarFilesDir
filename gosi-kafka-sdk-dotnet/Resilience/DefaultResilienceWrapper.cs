namespace Gosi.Kafka.Sdk.Resilience;

using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using Gosi.Kafka.Sdk.Auth;
using Gosi.Kafka.Sdk.Consumer;
using Gosi.Kafka.Sdk.Producer;
using Gosi.Kafka.Sdk.Telemetry;
using Gosi.Kafka.Sdk.Tracing;

public class DefaultResilienceWrapper<TKey, TValue> : IResilienceWrapper<TKey, TValue>
    where TKey : class
    where TValue : class
{
    private readonly ResilienceConfig _config;
    private readonly GosiKafkaProducer<TKey, TValue>? _dlqProducer;
    private readonly ITelemetryReporter _telemetryReporter;
    
    private readonly ConcurrentQueue<long> _restartTimestamps = new();
    private long _dlqAccumulationCount = 0;

    public DefaultResilienceWrapper(
        ResilienceConfig config, 
        GosiKafkaProducer<TKey, TValue>? dlqProducer, 
        ITelemetryReporter telemetryReporter)
    {
        _config = config;
        _dlqProducer = dlqProducer;
        _telemetryReporter = telemetryReporter;

        _config.Validate();

        if (_config.ErrorPolicy == ErrorPolicy.CAPTURE_DLQ && _dlqProducer == null)
        {
            throw new ArgumentException("DLQ Producer must be provided when ErrorPolicy is CAPTURE_DLQ");
        }
    }

    public void RecordRestart()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _restartTimestamps.Enqueue(now);

        // Evict old timestamps
        long thresholdTime = now - _config.RestartLoopWindowMs;
        while (_restartTimestamps.TryPeek(out long oldest) && oldest < thresholdTime)
        {
            _restartTimestamps.TryDequeue(out _);
        }

        if (_restartTimestamps.Count >= _config.RestartLoopThreshold)
        {
            _telemetryReporter.OnRestartLoopDetected("unknown-group", _restartTimestamps.Count, _config.RestartLoopWindowMs);
        }
    }

    public bool IsInRestartLoop()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long thresholdTime = now - _config.RestartLoopWindowMs;
        
        int count = 0;
        foreach (var ts in _restartTimestamps)
        {
            if (ts >= thresholdTime) count++;
        }
        
        return count >= _config.RestartLoopThreshold;
    }

    public async Task ProcessAsync(GosiRecord<TKey, TValue> record, Func<GosiRecord<TKey, TValue>, Task> processor)
    {
        int attempt = 0;
        Exception? lastError = null;

        while (attempt <= _config.MaxRetries)
        {
            try
            {
                await processor(record);
                return; // Success!
            }
            catch (Exception e)
            {
                lastError = e;
                
                // Fast-fail auth errors — don't retry them
                var authError = AuthErrorClassifier.Classify(e);
                if (authError != AuthErrorType.UNKNOWN_ERROR)
                {
                    _telemetryReporter.OnAuthError(authError.ToString(), e.Message);
                    throw AuthErrorClassifier.ClassifyAndWrap(e);
                }

                attempt++;
                
                if (attempt <= _config.MaxRetries)
                {
                    // Backoff before next retry
                    await Task.Delay(TimeSpan.FromMilliseconds(_config.RetryBackoffMs));
                }
            }
        }

        // Exhausted retries
        _telemetryReporter.OnRetryExhaustion(record.Topic, _config.Stage ?? "unknown", record.TraceId, attempt - 1, lastError!);

        if (_config.ErrorPolicy == ErrorPolicy.CAPTURE_DLQ)
        {
            await RouteToDlqAsync(record, lastError!);
        }
        else
        {
            throw lastError!;
        }
    }

    private async Task RouteToDlqAsync(GosiRecord<TKey, TValue> record, Exception cause)
    {
        string dlqTopic = _config.GetDlqTopicName();

        // Clear existing error headers
        record.Headers.Remove("error_code");
        record.Headers.Remove("stack_trace");
        record.Headers.Remove("processing_stage");
        record.Headers.Remove("original_topic");

        record.Headers.Add("error_code", Encoding.UTF8.GetBytes("500"));
        record.Headers.Add("stack_trace", Encoding.UTF8.GetBytes(cause.Message ?? cause.GetType().Name));
        record.Headers.Add("processing_stage", Encoding.UTF8.GetBytes(_config.Stage ?? "unknown"));
        record.Headers.Add("original_topic", Encoding.UTF8.GetBytes(record.Topic));

        TraceContext.InjectIntoHeaders(record.Headers, record.TraceId);

        await _dlqProducer!.ProduceAsync(dlqTopic, record.Key, record.Value, record.Headers);
        
        _telemetryReporter.OnDlqReroute(record.Topic, dlqTopic, record.TraceId, cause);

        long currentVol = System.Threading.Interlocked.Increment(ref _dlqAccumulationCount);
        if (currentVol % _config.DlqAccumulationAlertThreshold == 0)
        {
            _telemetryReporter.OnDlqAccumulation(dlqTopic, currentVol, 0.01);
        }
    }
}

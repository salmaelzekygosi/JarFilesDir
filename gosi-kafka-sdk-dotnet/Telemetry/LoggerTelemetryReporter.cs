namespace Gosi.Kafka.Sdk.Telemetry;

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

public class LoggerTelemetryReporter : ITelemetryReporter
{
    private readonly ILogger<LoggerTelemetryReporter> _logger;

    public LoggerTelemetryReporter(ILogger<LoggerTelemetryReporter> logger)
    {
        _logger = logger;
    }

    public void OnDeliveryReport(DeliveryReport report)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "kafka_topic", report.Topic },
            { "latency_ms", report.LatencyMs }
        };

        if (report.TraceId != null)
        {
            scopeState.Add("trace_id", report.TraceId);
        }

        if (report.AuthErrorType != null)
        {
            scopeState.Add("auth_error_type", report.AuthErrorType);
        }

        if (report.Success)
        {
            scopeState.Add("kafka_partition", report.Partition);
            scopeState.Add("kafka_offset", report.Offset);
            using (_logger.BeginScope(scopeState))
            {
                _logger.LogInformation("Successfully delivered message to Kafka");
            }
        }
        else
        {
            using (_logger.BeginScope(scopeState))
            {
                _logger.LogError(report.Error, "Failed to deliver message to Kafka");
            }
        }
    }

    public void OnConsumeLag(string topic, int partition, long lag)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "kafka_topic", topic },
            { "kafka_partition", partition },
            { "kafka_lag", lag }
        };

        using (_logger.BeginScope(scopeState))
        {
            if (lag > 1000)
            {
                _logger.LogWarning("High consumer lag detected: {Lag}", lag);
            }
            else
            {
                _logger.LogDebug("Consumer lag: {Lag}", lag);
            }
        }
    }

    public void OnOffsetCommit(string topic, int partition, long offset, bool success, Exception? error)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "kafka_topic", topic },
            { "kafka_partition", partition },
            { "kafka_offset", offset }
        };

        using (_logger.BeginScope(scopeState))
        {
            if (success)
            {
                _logger.LogDebug("Successfully committed offset");
            }
            else
            {
                _logger.LogError(error, "Failed to commit offset");
            }
        }
    }

    public void OnDlqReroute(string sourceTopic, string dlqTopic, string traceId, Exception cause)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "source_topic", sourceTopic },
            { "dlq_topic", dlqTopic }
        };
        
        if (traceId != null)
        {
            scopeState.Add("trace_id", traceId);
        }

        using (_logger.BeginScope(scopeState))
        {
            _logger.LogWarning(cause, "Message rerouted to DLQ");
        }
    }

    public void OnDlqAccumulation(string dlqTopic, long volume, double ratio)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "dlq_topic", dlqTopic },
            { "dlq_volume", volume },
            { "dlq_ratio", ratio }
        };

        using (_logger.BeginScope(scopeState))
        {
            _logger.LogCritical("CRITICAL: DLQ accumulation alert | topic={DlqTopic} | volume={Volume} | ratio={Ratio:P2}", 
                dlqTopic, volume, ratio);
        }
    }

    public void OnRetryExhaustion(string topic, string stage, string traceId, int retryCount, Exception lastError)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "kafka_topic", topic },
            { "processing_stage", stage },
            { "retry_count", retryCount }
        };

        if (traceId != null) scopeState.Add("trace_id", traceId);

        using (_logger.BeginScope(scopeState))
        {
            _logger.LogError(lastError, "Retry exhausted for stage {Stage} after {Retries} attempts", stage, retryCount);
        }
    }

    public void OnRestartLoopDetected(string consumerGroup, int restartCount, long windowMs)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "consumer_group", consumerGroup },
            { "restart_count", restartCount },
            { "restart_window_ms", windowMs }
        };

        using (_logger.BeginScope(scopeState))
        {
            _logger.LogCritical("CRITICAL: Restart-loop detected (CrashLoopBackOff) for group {Group}. Restarted {Count} times in {WindowMs}ms", 
                consumerGroup, restartCount, windowMs);
        }
    }

    public void OnAuthError(string errorType, string detail)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "auth_error_type", errorType }
        };

        using (_logger.BeginScope(scopeState))
        {
            _logger.LogError("Authentication/Authorization error: {ErrorType} | Detail: {Detail}", errorType, detail);
        }
    }

    public void OnReplayAttempt(string topic, string traceId, bool success)
    {
        var scopeState = new Dictionary<string, object>
        {
            { "kafka_topic", topic },
            { "replay_success", success }
        };

        if (traceId != null) scopeState.Add("trace_id", traceId);

        using (_logger.BeginScope(scopeState))
        {
            if (success)
            {
                _logger.LogInformation("Successfully replayed message");
            }
            else
            {
                _logger.LogError("Failed to replay message");
            }
        }
    }
}

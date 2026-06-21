namespace Gosi.Kafka.Sdk.Telemetry;

using System;

public class DeliveryReport
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
    public string TraceId { get; }
    public long LatencyMs { get; }
    public bool Success { get; }
    public Exception? Error { get; }
    public string? AuthErrorType { get; }

    public DeliveryReport(string topic, int partition, long offset, long timestamp, string traceId, long latencyMs, bool success, Exception? error, string? authErrorType)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        TraceId = traceId;
        LatencyMs = latencyMs;
        Success = success;
        Error = error;
        AuthErrorType = authErrorType;
    }

    public static DeliveryReport CreateSuccess(string topic, int partition, long offset, long timestamp, string traceId, long latencyMs)
    {
        return new DeliveryReport(topic, partition, offset, timestamp, traceId, latencyMs, true, null, null);
    }

    public static DeliveryReport CreateFailure(string topic, string traceId, long latencyMs, Exception error, string? authErrorType = null)
    {
        return new DeliveryReport(topic, -1, -1, -1, traceId, latencyMs, false, error, authErrorType);
    }
}

public interface ITelemetryReporter
{
    void OnDeliveryReport(DeliveryReport report);
    void OnConsumeLag(string topic, int partition, long lag);
    void OnOffsetCommit(string topic, int partition, long offset, bool success, Exception? error);
    void OnDlqReroute(string sourceTopic, string dlqTopic, string traceId, Exception cause);
    
    // New Resilience Hooks
    void OnDlqAccumulation(string dlqTopic, long volume, double ratio);
    void OnRetryExhaustion(string topic, string stage, string traceId, int retryCount, Exception lastError);
    void OnRestartLoopDetected(string consumerGroup, int restartCount, long windowMs);
    void OnAuthError(string errorType, string detail);
    void OnReplayAttempt(string topic, string traceId, bool success);
}

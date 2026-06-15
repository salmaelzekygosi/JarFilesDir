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

    public DeliveryReport(string topic, int partition, long offset, long timestamp, string traceId, long latencyMs, bool success, Exception? error)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        TraceId = traceId;
        LatencyMs = latencyMs;
        Success = success;
        Error = error;
    }

    public static DeliveryReport CreateSuccess(string topic, int partition, long offset, long timestamp, string traceId, long latencyMs)
    {
        return new DeliveryReport(topic, partition, offset, timestamp, traceId, latencyMs, true, null);
    }

    public static DeliveryReport CreateFailure(string topic, string traceId, long latencyMs, Exception error)
    {
        return new DeliveryReport(topic, -1, -1, -1, traceId, latencyMs, false, error);
    }
}

public interface ITelemetryReporter
{
    void OnDeliveryReport(DeliveryReport report);
    void OnConsumeLag(string topic, int partition, long lag);
    void OnOffsetCommit(string topic, int partition, long offset, bool success, Exception? error);
    void OnDlqReroute(string sourceTopic, string dlqTopic, string traceId, Exception cause);
}

package com.gosi.kafka.sdk.telemetry;

/**
 * Captures the result of a Kafka produce operation.
 */
public class DeliveryReport {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String traceId;
    private final long latencyMs;
    private final boolean success;
    private final Exception error;
    private final String authErrorType;

    public DeliveryReport(String topic, int partition, long offset, long timestamp, String traceId, long latencyMs, boolean success, Exception error, String authErrorType) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.traceId = traceId;
        this.latencyMs = latencyMs;
        this.success = success;
        this.error = error;
        this.authErrorType = authErrorType;
    }

    public static DeliveryReport success(String topic, int partition, long offset, long timestamp, String traceId, long latencyMs) {
        return new DeliveryReport(topic, partition, offset, timestamp, traceId, latencyMs, true, null, null);
    }

    public static DeliveryReport failure(String topic, String traceId, long latencyMs, Exception error) {
        return new DeliveryReport(topic, -1, -1, -1, traceId, latencyMs, false, error, null);
    }

    public static DeliveryReport failure(String topic, String traceId, long latencyMs, Exception error, String authErrorType) {
        return new DeliveryReport(topic, -1, -1, -1, traceId, latencyMs, false, error, authErrorType);
    }

    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public long getTimestamp() { return timestamp; }
    public String getTraceId() { return traceId; }
    public long getLatencyMs() { return latencyMs; }
    public boolean isSuccess() { return success; }
    public Exception getError() { return error; }
    public String getAuthErrorType() { return authErrorType; }
}

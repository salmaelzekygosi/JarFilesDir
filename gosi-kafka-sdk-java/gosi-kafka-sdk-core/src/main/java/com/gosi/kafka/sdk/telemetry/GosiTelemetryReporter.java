package com.gosi.kafka.sdk.telemetry;

/**
 * Contract for reporting telemetry metrics.
 * <p>
 * Implementations emit metrics in the shape existing dashboards expect:
 * DLQ volume vs. baseline, DLQ-to-main-topic throughput ratio, error rate
 * per connector/job, retry-exhaustion events, repeated-error-pattern detection,
 * restart-loop alerts, and auth error classification.
 * </p>
 */
public interface GosiTelemetryReporter {
    
    /**
     * Called when a message is successfully delivered or fails.
     */
    void onDeliveryReport(DeliveryReport report);

    /**
     * Called by consumers to report lag on a specific partition.
     */
    void onConsumeLag(String topic, int partition, long lag);

    /**
     * Called when an offset commit succeeds or fails.
     */
    void onOffsetCommit(String topic, int partition, long offset, boolean success, Exception error);

    /**
     * Called when a message is rerouted to a DLQ.
     */
    void onDlqReroute(String sourceTopic, String dlqTopic, String traceId, Exception cause);

    // --- New telemetry hooks for resilience module ---

    /**
     * Called when DLQ volume crosses the accumulation alert threshold.
     */
    void onDlqAccumulation(String dlqTopic, long volume, double ratio);

    /**
     * Called when retry is exhausted and a message is sent to DLQ.
     */
    void onRetryExhaustion(String topic, String stage, String traceId, int retryCount, Exception lastError);

    /**
     * Called when the restart-loop detection threshold is breached.
     */
    void onRestartLoopDetected(String consumerGroup, int restartCount, long windowMs);

    /**
     * Called when an authentication or authorization error is detected.
     *
     * @param errorType "AUTHENTICATION_FAILURE" or "AUTHORIZATION_DENIED"
     * @param detail    human-readable error detail
     */
    void onAuthError(String errorType, String detail);

    /**
     * Called when a DLQ replay is attempted (success or failure).
     */
    void onReplayAttempt(String dlqTopic, String targetTopic, String traceId, boolean success);
}

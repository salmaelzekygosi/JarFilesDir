package com.gosi.kafka.sdk.telemetry;

/**
 * Contract for reporting telemetry metrics like consumer lag and delivery reports.
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
}

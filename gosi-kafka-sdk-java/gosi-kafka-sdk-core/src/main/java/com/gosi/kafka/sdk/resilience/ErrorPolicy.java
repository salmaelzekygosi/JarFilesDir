package com.gosi.kafka.sdk.resilience;

/**
 * Defines the error handling policy for a Kafka integration.
 * <p>
 * Per the Enterprise Event Streaming Framework, this is an explicit per-integration
 * choice — not a single global default.
 * </p>
 */
public enum ErrorPolicy {

    /**
     * Fail-fast: equivalent to {@code errors.tolerance=none}.
     * Any processing error immediately stops consumption and throws.
     * Use for critical/compliance data where silent data loss is unacceptable.
     */
    FAIL_FAST,

    /**
     * Capture to DLQ: route failed records to a per-stage Dead Letter Queue.
     * Use for high-volume non-critical data where throughput is prioritized.
     */
    CAPTURE_DLQ
}

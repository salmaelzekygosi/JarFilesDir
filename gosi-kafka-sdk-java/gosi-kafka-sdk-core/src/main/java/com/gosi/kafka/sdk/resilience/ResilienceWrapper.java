package com.gosi.kafka.sdk.resilience;

import com.gosi.kafka.sdk.consumer.GosiRecord;
import com.gosi.kafka.sdk.consumer.RecordHandler;

/**
 * Core resilience contract for Kafka event processing.
 * <p>
 * Wraps record processing with retry, per-stage DLQ routing, and metrics emission.
 * The processing stage resolves to a DLQ topic following the naming convention:
 * {@code <namespace>.dlq.<stage>.v1}
 * </p>
 * <p>
 * Implementations must:
 * <ul>
 *   <li>Retry according to the configured {@link ResilienceConfig}</li>
 *   <li>Route to per-stage DLQ with standardized headers ({@code stack_trace}, {@code error_code}, {@code trace_id})</li>
 *   <li>Emit metrics matching existing dashboard expectations</li>
 *   <li>Detect restart-loop conditions (3+ restarts in 10 minutes)</li>
 *   <li>Preserve {@code trace_id} across all hops</li>
 * </ul>
 * </p>
 *
 * @param <K> the Kafka record key type
 * @param <V> the Kafka record value type
 */
public interface ResilienceWrapper<K, V> {

    /**
     * Processes a record through the resilience pipeline.
     * <p>
     * On success: the handler completes normally.<br>
     * On failure with CAPTURE_DLQ policy: retries up to maxRetries, then routes to DLQ.<br>
     * On failure with FAIL_FAST policy: throws immediately, no DLQ routing.
     * </p>
     *
     * @param record  the consumed Kafka record
     * @param handler the business logic handler
     * @throws Exception if processing fails and error policy is FAIL_FAST, or if DLQ routing itself fails
     */
    void process(GosiRecord<K, V> record, RecordHandler<K, V> handler) throws Exception;

    /**
     * Returns a snapshot of current resilience metrics.
     * <p>
     * Includes: DLQ volume, DLQ-to-main ratio, error rate per stage,
     * retry-exhaustion events, repeated-error-pattern detection.
     * </p>
     */
    ResilienceMetrics getMetrics();

    /**
     * Returns true if the consumer/connector has restarted more than the configured
     * threshold within the monitoring window (default: 3 restarts in 10 minutes).
     * <p>
     * This maps to the framework's poison-pill/CrashLoopBackOff detection.
     * </p>
     */
    boolean isInRestartLoop();

    /**
     * Records a restart event for restart-loop detection.
     * Should be called by the consumer on each startup.
     */
    void recordRestart();

    /**
     * Returns the resolved DLQ topic name for this wrapper's configured stage.
     */
    String getDlqTopicName();
}

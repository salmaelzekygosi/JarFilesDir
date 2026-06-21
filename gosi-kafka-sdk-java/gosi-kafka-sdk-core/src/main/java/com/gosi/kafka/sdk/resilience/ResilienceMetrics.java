package com.gosi.kafka.sdk.resilience;

import java.util.Map;

/**
 * Snapshot of resilience metrics at a point in time.
 * Emits metrics in the shape existing dashboards expect:
 * DLQ volume, DLQ-to-main-topic throughput ratio, error rate,
 * retry-exhaustion events, and repeated-error-pattern detection.
 */
public class ResilienceMetrics {

    private final long dlqVolume;
    private final long mainTopicVolume;
    private final double dlqToMainRatio;
    private final long retryExhaustionCount;
    private final Map<String, Long> errorPatterns;
    private final int restartCount;
    private final long windowStartMs;
    private final String processingStage;

    public ResilienceMetrics(long dlqVolume, long mainTopicVolume, double dlqToMainRatio,
                             long retryExhaustionCount, Map<String, Long> errorPatterns,
                             int restartCount, long windowStartMs, String processingStage) {
        this.dlqVolume = dlqVolume;
        this.mainTopicVolume = mainTopicVolume;
        this.dlqToMainRatio = dlqToMainRatio;
        this.retryExhaustionCount = retryExhaustionCount;
        this.errorPatterns = errorPatterns;
        this.restartCount = restartCount;
        this.windowStartMs = windowStartMs;
        this.processingStage = processingStage;
    }

    /** Total messages routed to DLQ in the current window. */
    public long getDlqVolume() { return dlqVolume; }

    /** Total messages processed from the main topic in the current window. */
    public long getMainTopicVolume() { return mainTopicVolume; }

    /** Ratio of DLQ messages to main topic throughput. */
    public double getDlqToMainRatio() { return dlqToMainRatio; }

    /** Number of times retry was exhausted (max retries exceeded). */
    public long getRetryExhaustionCount() { return retryExhaustionCount; }

    /** Error code frequency map for repeated-error-pattern detection. */
    public Map<String, Long> getErrorPatterns() { return errorPatterns; }

    /** Number of consumer restarts in the current monitoring window. */
    public int getRestartCount() { return restartCount; }

    /** Start of the current monitoring window (epoch ms). */
    public long getWindowStartMs() { return windowStartMs; }

    /** The processing stage this metric belongs to. */
    public String getProcessingStage() { return processingStage; }
}

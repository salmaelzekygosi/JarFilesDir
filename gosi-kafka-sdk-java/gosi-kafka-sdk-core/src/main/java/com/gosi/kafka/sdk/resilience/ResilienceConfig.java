package com.gosi.kafka.sdk.resilience;

/**
 * Configuration for the {@link ResilienceWrapper}.
 * <p>
 * Encapsulates per-stage DLQ design: each processing stage (ingestion, extraction,
 * enrichment, etc.) resolves to its own DLQ topic following the naming convention
 * {@code <namespace>.dlq.<stage>.v1}.
 * </p>
 * <p>
 * Error policy is an explicit per-integration choice between Fail-Fast
 * (for critical/compliance data) and Capture/DLQ (for high-volume non-critical data).
 * </p>
 */
public class ResilienceConfig {

    private final String namespace;
    private final String processingStage;
    private final ErrorPolicy errorPolicy;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final int restartLoopThreshold;
    private final long restartLoopWindowMs;
    private final long dlqAccumulationAlertThreshold;

    private ResilienceConfig(Builder builder) {
        this.namespace = builder.namespace;
        this.processingStage = builder.processingStage;
        this.errorPolicy = builder.errorPolicy;
        this.maxRetries = builder.maxRetries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.restartLoopThreshold = builder.restartLoopThreshold;
        this.restartLoopWindowMs = builder.restartLoopWindowMs;
        this.dlqAccumulationAlertThreshold = builder.dlqAccumulationAlertThreshold;
    }

    /** Organizational namespace for topic naming (e.g. "hrsd", "medallia"). */
    public String getNamespace() { return namespace; }

    /** Processing stage identifier (e.g. "ingestion", "extraction", "enrichment"). */
    public String getProcessingStage() { return processingStage; }

    /** Error handling policy: FAIL_FAST or CAPTURE_DLQ. */
    public ErrorPolicy getErrorPolicy() { return errorPolicy; }

    /** Maximum retry attempts before routing to DLQ (only used with CAPTURE_DLQ). */
    public int getMaxRetries() { return maxRetries; }

    /** Backoff between retries in milliseconds. */
    public long getRetryBackoffMs() { return retryBackoffMs; }

    /** Number of restarts within the window that triggers restart-loop alert. */
    public int getRestartLoopThreshold() { return restartLoopThreshold; }

    /** Restart-loop detection window in milliseconds. */
    public long getRestartLoopWindowMs() { return restartLoopWindowMs; }

    /** DLQ message count threshold that triggers an accumulation alert. */
    public long getDlqAccumulationAlertThreshold() { return dlqAccumulationAlertThreshold; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String namespace;
        private String processingStage;
        private ErrorPolicy errorPolicy = ErrorPolicy.CAPTURE_DLQ;
        private int maxRetries = 3;
        private long retryBackoffMs = 1000;
        private int restartLoopThreshold = 3;
        private long restartLoopWindowMs = 600_000; // 10 minutes
        private long dlqAccumulationAlertThreshold = 100;

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder processingStage(String processingStage) {
            this.processingStage = processingStage;
            return this;
        }

        public Builder errorPolicy(ErrorPolicy errorPolicy) {
            this.errorPolicy = errorPolicy;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder restartLoopThreshold(int threshold) {
            this.restartLoopThreshold = threshold;
            return this;
        }

        public Builder restartLoopWindowMs(long windowMs) {
            this.restartLoopWindowMs = windowMs;
            return this;
        }

        public Builder dlqAccumulationAlertThreshold(long threshold) {
            this.dlqAccumulationAlertThreshold = threshold;
            return this;
        }

        public ResilienceConfig build() {
            if (namespace == null || namespace.trim().isEmpty()) {
                throw new IllegalArgumentException("namespace is required for ResilienceConfig");
            }
            if (processingStage == null || processingStage.trim().isEmpty()) {
                throw new IllegalArgumentException("processingStage is required for ResilienceConfig");
            }
            if (errorPolicy == null) {
                throw new IllegalArgumentException("errorPolicy is required for ResilienceConfig");
            }
            return new ResilienceConfig(this);
        }
    }
}

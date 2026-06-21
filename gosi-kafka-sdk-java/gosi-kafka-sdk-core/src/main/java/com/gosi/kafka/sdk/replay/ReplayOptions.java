package com.gosi.kafka.sdk.replay;

/**
 * Options for filtering DLQ replay operations.
 * Supports filtering by time range, error code, and trace ID.
 */
public class ReplayOptions {

    private final Long fromTimestampMs;
    private final Long toTimestampMs;
    private final String errorCodeFilter;
    private final String traceIdFilter;
    private final int maxMessages;
    private final String operatorId;

    private ReplayOptions(Builder builder) {
        this.fromTimestampMs = builder.fromTimestampMs;
        this.toTimestampMs = builder.toTimestampMs;
        this.errorCodeFilter = builder.errorCodeFilter;
        this.traceIdFilter = builder.traceIdFilter;
        this.maxMessages = builder.maxMessages;
        this.operatorId = builder.operatorId;
    }

    /** Start of the time window for replay (epoch ms), or null for no lower bound. */
    public Long getFromTimestampMs() { return fromTimestampMs; }

    /** End of the time window for replay (epoch ms), or null for no upper bound. */
    public Long getToTimestampMs() { return toTimestampMs; }

    /** Only replay messages with this error_code header, or null for all. */
    public String getErrorCodeFilter() { return errorCodeFilter; }

    /** Only replay a specific trace_id, or null for all. */
    public String getTraceIdFilter() { return traceIdFilter; }

    /** Maximum number of messages to replay (default 1000). */
    public int getMaxMessages() { return maxMessages; }

    /** Identifier of the operator triggering the replay. */
    public String getOperatorId() { return operatorId; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Long fromTimestampMs;
        private Long toTimestampMs;
        private String errorCodeFilter;
        private String traceIdFilter;
        private int maxMessages = 1000;
        private String operatorId = "system";

        public Builder fromTimestamp(long fromTimestampMs) {
            this.fromTimestampMs = fromTimestampMs;
            return this;
        }

        public Builder toTimestamp(long toTimestampMs) {
            this.toTimestampMs = toTimestampMs;
            return this;
        }

        public Builder errorCodeFilter(String errorCode) {
            this.errorCodeFilter = errorCode;
            return this;
        }

        public Builder traceIdFilter(String traceId) {
            this.traceIdFilter = traceId;
            return this;
        }

        public Builder maxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        public Builder operatorId(String operatorId) {
            this.operatorId = operatorId;
            return this;
        }

        public ReplayOptions build() {
            return new ReplayOptions(this);
        }
    }
}

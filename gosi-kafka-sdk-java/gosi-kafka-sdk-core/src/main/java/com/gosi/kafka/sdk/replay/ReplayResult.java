package com.gosi.kafka.sdk.replay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of a DLQ replay operation.
 * Tracks per-message success/failure and summary statistics.
 */
public class ReplayResult {

    private final String dlqTopic;
    private final String targetTopic;
    private final int totalAttempted;
    private final int totalSucceeded;
    private final int totalFailed;
    private final List<FailedReplay> failures;

    public ReplayResult(String dlqTopic, String targetTopic, int totalAttempted,
                        int totalSucceeded, int totalFailed, List<FailedReplay> failures) {
        this.dlqTopic = dlqTopic;
        this.targetTopic = targetTopic;
        this.totalAttempted = totalAttempted;
        this.totalSucceeded = totalSucceeded;
        this.totalFailed = totalFailed;
        this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
    }

    public String getDlqTopic() { return dlqTopic; }
    public String getTargetTopic() { return targetTopic; }
    public int getTotalAttempted() { return totalAttempted; }
    public int getTotalSucceeded() { return totalSucceeded; }
    public int getTotalFailed() { return totalFailed; }
    public List<FailedReplay> getFailures() { return failures; }
    public boolean isFullySuccessful() { return totalFailed == 0; }

    /**
     * Details of a single failed replay attempt.
     */
    public static class FailedReplay {
        private final String traceId;
        private final long offset;
        private final String errorMessage;

        public FailedReplay(String traceId, long offset, String errorMessage) {
            this.traceId = traceId;
            this.offset = offset;
            this.errorMessage = errorMessage;
        }

        public String getTraceId() { return traceId; }
        public long getOffset() { return offset; }
        public String getErrorMessage() { return errorMessage; }
    }
}

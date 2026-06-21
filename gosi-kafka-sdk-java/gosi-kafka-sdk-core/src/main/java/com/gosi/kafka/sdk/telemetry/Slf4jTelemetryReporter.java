package com.gosi.kafka.sdk.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Implements structured logging for telemetry using SLF4J MDC.
 * <p>
 * Log fields align with the centralized Splunk pipeline that already aggregates
 * broker, Flink, Connect worker/task, Schema Registry, REST proxy, and auth/authz events.
 * </p>
 */
public class Slf4jTelemetryReporter implements GosiTelemetryReporter {

    private static final Logger LOG = LoggerFactory.getLogger(Slf4jTelemetryReporter.class);

    private static final String KAFKA_TOPIC = "kafka_topic";
    private static final String KAFKA_PARTITION = "kafka_partition";
    private static final String KAFKA_OFFSET = "kafka_offset";

    @Override
    public void onDeliveryReport(DeliveryReport report) {
        if (report.getTraceId() != null) {
            MDC.put("trace_id", report.getTraceId());
        }
        MDC.put(KAFKA_TOPIC, report.getTopic());
        MDC.put("latency_ms", String.valueOf(report.getLatencyMs()));

        try {
            if (report.isSuccess()) {
                MDC.put(KAFKA_PARTITION, String.valueOf(report.getPartition()));
                MDC.put(KAFKA_OFFSET, String.valueOf(report.getOffset()));
                LOG.info("Successfully delivered message to Kafka");
            } else {
                LOG.error("Failed to deliver message to Kafka", report.getError());
            }
        } finally {
            MDC.remove(KAFKA_TOPIC);
            MDC.remove(KAFKA_PARTITION);
            MDC.remove(KAFKA_OFFSET);
            MDC.remove("latency_ms");
            // We do not clear trace_id here as it belongs to the calling thread's context
        }
    }

    @Override
    public void onConsumeLag(String topic, int partition, long lag) {
        MDC.put(KAFKA_TOPIC, topic);
        MDC.put(KAFKA_PARTITION, String.valueOf(partition));
        MDC.put("kafka_lag", String.valueOf(lag));
        
        try {
            if (lag > 1000) {
                LOG.warn("High consumer lag detected: {}", lag);
            } else {
                LOG.debug("Consumer lag: {}", lag);
            }
        } finally {
            MDC.remove(KAFKA_TOPIC);
            MDC.remove(KAFKA_PARTITION);
            MDC.remove("kafka_lag");
        }
    }

    @Override
    public void onOffsetCommit(String topic, int partition, long offset, boolean success, Exception error) {
        MDC.put(KAFKA_TOPIC, topic);
        MDC.put(KAFKA_PARTITION, String.valueOf(partition));
        MDC.put(KAFKA_OFFSET, String.valueOf(offset));
        
        try {
            if (success) {
                LOG.debug("Successfully committed offset");
            } else {
                LOG.error("Failed to commit offset", error);
            }
        } finally {
            MDC.remove(KAFKA_TOPIC);
            MDC.remove(KAFKA_PARTITION);
            MDC.remove(KAFKA_OFFSET);
        }
    }

    @Override
    public void onDlqReroute(String sourceTopic, String dlqTopic, String traceId, Exception cause) {
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        MDC.put("source_topic", sourceTopic);
        MDC.put("dlq_topic", dlqTopic);
        
        String errorCode = "PROCESSING_ERROR";
        String stackTraceMsg = getStackTrace(cause);
        MDC.put("error_code", errorCode);
        MDC.put("stack_trace", stackTraceMsg);
        
        try {
            LOG.warn("Message rerouted to DLQ | trace_id: {} | error_code: {} | stack_trace: {}", 
                     traceId, errorCode, stackTraceMsg, cause);
        } finally {
            MDC.remove("source_topic");
            MDC.remove("dlq_topic");
            MDC.remove("error_code");
            MDC.remove("stack_trace");
        }
    }

    @Override
    public void onDlqAccumulation(String dlqTopic, long volume, double ratio) {
        MDC.put("dlq_topic", dlqTopic);
        MDC.put("dlq_volume", String.valueOf(volume));
        MDC.put("dlq_ratio", String.format("%.4f", ratio));

        try {
            LOG.warn("DLQ accumulation alert | dlq_topic={} | volume={} | ratio={}",
                    dlqTopic, volume, String.format("%.4f", ratio));
        } finally {
            MDC.remove("dlq_topic");
            MDC.remove("dlq_volume");
            MDC.remove("dlq_ratio");
        }
    }

    @Override
    public void onRetryExhaustion(String topic, String stage, String traceId, int retryCount, Exception lastError) {
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        MDC.put(KAFKA_TOPIC, topic);
        MDC.put("processing_stage", stage);
        MDC.put("retry_count", String.valueOf(retryCount));

        try {
            LOG.warn("Retry exhausted | topic={} | stage={} | trace_id={} | retries={} | error={}",
                    topic, stage, traceId, retryCount,
                    lastError != null ? lastError.getMessage() : "unknown", lastError);
        } finally {
            MDC.remove(KAFKA_TOPIC);
            MDC.remove("processing_stage");
            MDC.remove("retry_count");
        }
    }

    @Override
    public void onRestartLoopDetected(String consumerGroup, int restartCount, long windowMs) {
        MDC.put("consumer_group", consumerGroup);
        MDC.put("restart_count", String.valueOf(restartCount));
        MDC.put("restart_window_ms", String.valueOf(windowMs));

        try {
            LOG.error("RESTART LOOP DETECTED | consumer_group={} | restarts={} | window_ms={} | " +
                            "This exceeds the framework threshold for a poison-pill/CrashLoopBackOff condition",
                    consumerGroup, restartCount, windowMs);
        } finally {
            MDC.remove("consumer_group");
            MDC.remove("restart_count");
            MDC.remove("restart_window_ms");
        }
    }

    @Override
    public void onAuthError(String errorType, String detail) {
        MDC.put("auth_error_type", errorType);

        try {
            if ("AUTHORIZATION_DENIED".equals(errorType)) {
                LOG.error("Authorization DENIED (ACL/RBAC) | type={} | detail={}", errorType, detail);
            } else {
                LOG.error("Authentication FAILURE | type={} | detail={}", errorType, detail);
            }
        } finally {
            MDC.remove("auth_error_type");
        }
    }

    @Override
    public void onReplayAttempt(String dlqTopic, String targetTopic, String traceId, boolean success) {
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        MDC.put("dlq_topic", dlqTopic);
        MDC.put("replay_target_topic", targetTopic);

        try {
            if (success) {
                LOG.info("DLQ replay succeeded | dlq={} | target={} | trace_id={}", dlqTopic, targetTopic, traceId);
            } else {
                LOG.warn("DLQ replay failed | dlq={} | target={} | trace_id={}", dlqTopic, targetTopic, traceId);
            }
        } finally {
            MDC.remove("dlq_topic");
            MDC.remove("replay_target_topic");
        }
    }

    private String getStackTrace(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}

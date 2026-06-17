package com.gosi.kafka.sdk.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Implements structured logging for telemetry using SLF4J MDC.
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
        
        String errorCode = "500";
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

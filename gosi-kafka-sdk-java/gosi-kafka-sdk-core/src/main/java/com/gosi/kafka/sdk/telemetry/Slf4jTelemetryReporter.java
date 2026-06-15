package com.gosi.kafka.sdk.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Implements structured logging for telemetry using SLF4J MDC.
 */
public class Slf4jTelemetryReporter implements GosiTelemetryReporter {

    private static final Logger LOG = LoggerFactory.getLogger(Slf4jTelemetryReporter.class);

    @Override
    public void onDeliveryReport(DeliveryReport report) {
        if (report.getTraceId() != null) {
            MDC.put("trace_id", report.getTraceId());
        }
        MDC.put("kafka_topic", report.getTopic());
        MDC.put("latency_ms", String.valueOf(report.getLatencyMs()));

        try {
            if (report.isSuccess()) {
                MDC.put("kafka_partition", String.valueOf(report.getPartition()));
                MDC.put("kafka_offset", String.valueOf(report.getOffset()));
                LOG.info("Successfully delivered message to Kafka");
            } else {
                LOG.error("Failed to deliver message to Kafka", report.getError());
            }
        } finally {
            MDC.remove("kafka_topic");
            MDC.remove("kafka_partition");
            MDC.remove("kafka_offset");
            MDC.remove("latency_ms");
            // We do not clear trace_id here as it belongs to the calling thread's context
        }
    }

    @Override
    public void onConsumeLag(String topic, int partition, long lag) {
        MDC.put("kafka_topic", topic);
        MDC.put("kafka_partition", String.valueOf(partition));
        MDC.put("kafka_lag", String.valueOf(lag));
        
        try {
            if (lag > 1000) {
                LOG.warn("High consumer lag detected: {}", lag);
            } else {
                LOG.debug("Consumer lag: {}", lag);
            }
        } finally {
            MDC.remove("kafka_topic");
            MDC.remove("kafka_partition");
            MDC.remove("kafka_lag");
        }
    }

    @Override
    public void onOffsetCommit(String topic, int partition, long offset, boolean success, Exception error) {
        MDC.put("kafka_topic", topic);
        MDC.put("kafka_partition", String.valueOf(partition));
        MDC.put("kafka_offset", String.valueOf(offset));
        
        try {
            if (success) {
                LOG.debug("Successfully committed offset");
            } else {
                LOG.error("Failed to commit offset", error);
            }
        } finally {
            MDC.remove("kafka_topic");
            MDC.remove("kafka_partition");
            MDC.remove("kafka_offset");
        }
    }

    @Override
    public void onDlqReroute(String sourceTopic, String dlqTopic, String traceId, Exception cause) {
        if (traceId != null) {
            MDC.put("trace_id", traceId);
        }
        MDC.put("source_topic", sourceTopic);
        MDC.put("dlq_topic", dlqTopic);
        
        try {
            LOG.warn("Message rerouted to DLQ", cause);
        } finally {
            MDC.remove("source_topic");
            MDC.remove("dlq_topic");
        }
    }
}

package com.gosi.kafka.sdk.replay;

import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Semi-automated, operator-triggered DLQ replay utility.
 * <p>
 * Reads from a DLQ topic and re-injects events to a target topic, preserving
 * the original {@code trace_id} and appending replay metadata to headers.
 * </p>
 * <p>
 * This is NOT an always-running consumer. It must be explicitly invoked by an
 * operator — human judgment is required before re-injection to avoid
 * re-poisoning the pipeline.
 * </p>
 * <p>
 * Per the LLDs: failed events must be "isolated, traceable, and reprocessable"
 * without silent data loss.
 * </p>
 *
 * @param <K> Kafka record key type
 * @param <V> Kafka record value type
 */
public class DlqReplayConsumer<K, V> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DlqReplayConsumer.class);

    private final GosiKafkaClientConfig config;
    private final GosiKafkaProducer<K, V> producer;
    private final GosiTelemetryReporter telemetryReporter;

    public DlqReplayConsumer(GosiKafkaClientConfig config,
                              GosiKafkaProducer<K, V> producer,
                              GosiTelemetryReporter telemetryReporter) {
        this.config = config;
        this.producer = producer;
        this.telemetryReporter = telemetryReporter;
    }

    /**
     * Replays messages from a DLQ topic to a target topic.
     * <p>
     * Each replayed message preserves its original {@code trace_id} and receives
     * additional headers: {@code replay_timestamp}, {@code replay_source_dlq},
     * {@code replay_operator}.
     * </p>
     *
     * @param dlqTopic    the DLQ topic to read from
     * @param targetTopic the target topic to re-inject into
     * @param options     filtering and control options
     * @return a ReplayResult summarizing the operation
     */
    public ReplayResult replay(String dlqTopic, String targetTopic, ReplayOptions options) {
        LOG.info("Starting DLQ replay | dlq={} | target={} | operator={} | maxMessages={}",
                dlqTopic, targetTopic, options.getOperatorId(), options.getMaxMessages());

        Map<String, Object> consumerProps = config.buildConsumerProperties();
        // Use a unique group ID for replay to avoid interfering with active consumers
        consumerProps.put("group.id", "gosi-sdk-replay-" + System.currentTimeMillis());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        int totalAttempted = 0;
        int totalSucceeded = 0;
        int totalFailed = 0;
        List<ReplayResult.FailedReplay> failures = new ArrayList<>();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(dlqTopic));

            int emptyPollCount = 0;
            while (totalAttempted < options.getMaxMessages() && emptyPollCount < 3) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    continue;
                }
                emptyPollCount = 0;

                for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                    if (totalAttempted < options.getMaxMessages() && matchesFilters(consumerRecord, options)) {

                        totalAttempted++;
                        String traceId = extractHeaderValue(consumerRecord, "trace_id");

                        try {
                            // Add replay metadata headers
                            consumerRecord.headers().add("replay_timestamp",
                                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
                            consumerRecord.headers().add("replay_source_dlq",
                                    dlqTopic.getBytes(StandardCharsets.UTF_8));
                            consumerRecord.headers().add("replay_operator",
                                    options.getOperatorId().getBytes(StandardCharsets.UTF_8));

                            // Remove DLQ-specific error headers before re-injection
                            consumerRecord.headers().remove("error_code");
                            consumerRecord.headers().remove("stack_trace");
                            consumerRecord.headers().remove("retry_count");
                            consumerRecord.headers().remove("failure_timestamp");

                            @SuppressWarnings("unchecked")
                            GosiKafkaProducer<Object, Object> rawProducer = (GosiKafkaProducer<Object, Object>) (GosiKafkaProducer<?, ?>) producer;
                            rawProducer.sendAsync(targetTopic, consumerRecord.key(), consumerRecord.value(), consumerRecord.headers());

                            totalSucceeded++;
                            telemetryReporter.onReplayAttempt(dlqTopic, targetTopic, traceId, true);

                            LOG.debug("Replayed message | trace_id={} | dlq={} | target={}",
                                    traceId, dlqTopic, targetTopic);

                        } catch (Exception e) {
                            totalFailed++;
                            failures.add(new ReplayResult.FailedReplay(traceId, consumerRecord.offset(), e.getMessage()));
                            telemetryReporter.onReplayAttempt(dlqTopic, targetTopic, traceId, false);

                            LOG.error("Failed to replay message | trace_id={} | offset={} | error={}",
                                    traceId, consumerRecord.offset(), e.getMessage());
                        }
                    }
                }

                consumer.commitSync();
            }
        }

        ReplayResult result = new ReplayResult(dlqTopic, targetTopic, totalAttempted,
                totalSucceeded, totalFailed, failures);

        LOG.info("DLQ replay complete | dlq={} | target={} | attempted={} | succeeded={} | failed={}",
                dlqTopic, targetTopic, totalAttempted, totalSucceeded, totalFailed);

        return result;
    }

    private boolean matchesFilters(ConsumerRecord<byte[], byte[]> consumerRecord, ReplayOptions options) {
        // Time range filter
        if (options.getFromTimestampMs() != null && consumerRecord.timestamp() < options.getFromTimestampMs()) {
            return false;
        }
        if (options.getToTimestampMs() != null && consumerRecord.timestamp() > options.getToTimestampMs()) {
            return false;
        }

        // Error code filter
        if (options.getErrorCodeFilter() != null) {
            String errorCode = extractHeaderValue(consumerRecord, "error_code");
            if (!options.getErrorCodeFilter().equals(errorCode)) {
                return false;
            }
        }

        // Trace ID filter
        if (options.getTraceIdFilter() != null) {
            String traceId = extractHeaderValue(consumerRecord, "trace_id");
            if (!options.getTraceIdFilter().equals(traceId)) {
                return false;
            }
        }

        return true;
    }

    private String extractHeaderValue(ConsumerRecord<byte[], byte[]> consumerRecord, String key) {
        Header header = consumerRecord.headers().lastHeader(key);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public void close() {
        // No persistent resources to close
    }
}

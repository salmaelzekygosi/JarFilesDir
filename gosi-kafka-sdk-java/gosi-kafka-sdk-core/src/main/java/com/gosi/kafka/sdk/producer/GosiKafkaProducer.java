package com.gosi.kafka.sdk.producer;

import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.telemetry.DeliveryReport;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import com.gosi.kafka.sdk.telemetry.Slf4jTelemetryReporter;
import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulated producer for GOSI applications.
 * Enforces trace_id injection and telemetry reporting.
 */
public class GosiKafkaProducer<K, V> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GosiKafkaProducer.class);

    private final KafkaProducer<K, V> internalProducer;
    private final GosiTelemetryReporter telemetryReporter;

    public GosiKafkaProducer(GosiKafkaClientConfig config) {
        this(config, new Slf4jTelemetryReporter());
    }

    public GosiKafkaProducer(GosiKafkaClientConfig config, GosiTelemetryReporter telemetryReporter) {
        Map<String, Object> props = config.buildProducerProperties();
        
        // Setup serializers based on config
        configureSerializers(props, config);

        this.internalProducer = new KafkaProducer<>(props);
        this.telemetryReporter = telemetryReporter;
    }

    private void configureSerializers(Map<String, Object> props, GosiKafkaClientConfig config) {
        switch (config.getKeyFormat()) {
            case AVRO:
                props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
                props.put("schema.registry.url", config.getSchemaRegistryUrl());
                break;
            case JSON_SCHEMA:
                props.put("key.serializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
                props.put("schema.registry.url", config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                break;
        }

        switch (config.getValueFormat()) {
            case AVRO:
                props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
                props.put("schema.registry.url", config.getSchemaRegistryUrl());
                break;
            case JSON_SCHEMA:
                props.put("value.serializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
                props.put("schema.registry.url", config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                break;
        }
    }

    /**
     * Sends a record to Kafka asynchronously, returning a CompletableFuture.
     * Extracts trace_id from MDC or generates one if missing, and injects into headers.
     */
    public java.util.concurrent.CompletableFuture<DeliveryReport> send(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        
        // Inject trace_id from MDC into headers
        String traceId = TraceContext.injectIntoHeaders(record.headers());
        
        long startMs = System.currentTimeMillis();
        java.util.concurrent.CompletableFuture<DeliveryReport> future = new java.util.concurrent.CompletableFuture<>();

        internalProducer.send(record, (metadata, exception) -> {
            long latencyMs = System.currentTimeMillis() - startMs;
            DeliveryReport report;
            if (exception == null) {
                report = DeliveryReport.success(topic, metadata.partition(), metadata.offset(), metadata.timestamp(), traceId, latencyMs);
                telemetryReporter.onDeliveryReport(report);
                future.complete(report);
            } else {
                report = DeliveryReport.failure(topic, traceId, latencyMs, exception);
                telemetryReporter.onDeliveryReport(report);
                future.completeExceptionally(exception);
            }
        });
        
        return future;
    }
    
    // Proper wrapper method
    public void sendAsync(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        String traceId = TraceContext.injectIntoHeaders(record.headers());
        long startMs = System.currentTimeMillis();

        internalProducer.send(record, (metadata, exception) -> {
            long latencyMs = System.currentTimeMillis() - startMs;
            DeliveryReport report;
            if (exception == null) {
                report = DeliveryReport.success(topic, metadata.partition(), metadata.offset(), metadata.timestamp(), traceId, latencyMs);
            } else {
                report = DeliveryReport.failure(topic, traceId, latencyMs, exception);
            }
            telemetryReporter.onDeliveryReport(report);
        });
    }

    @Override
    public void close() {
        LOG.info("Closing GosiKafkaProducer");
        internalProducer.close(java.time.Duration.ofSeconds(30));
    }
}

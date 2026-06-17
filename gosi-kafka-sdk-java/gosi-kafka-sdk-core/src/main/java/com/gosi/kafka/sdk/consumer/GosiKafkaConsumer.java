package com.gosi.kafka.sdk.consumer;

import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import com.gosi.kafka.sdk.telemetry.Slf4jTelemetryReporter;
import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulated Kafka Consumer that manages the polling loop,
 * MDC trace_id extraction, consumer lag telemetry, and automatic offset commits.
 */
public class GosiKafkaConsumer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(GosiKafkaConsumer.class);

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private final KafkaConsumer<K, V> internalConsumer;
    private final GosiTelemetryReporter telemetryReporter;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private String topic;
    private RecordHandler<K, V> handler;
    private String dlqTopic;
    private com.gosi.kafka.sdk.producer.GosiKafkaProducer<K, V> dlqProducer;

    public GosiKafkaConsumer(GosiKafkaClientConfig config) {
        this(config, new Slf4jTelemetryReporter());
    }

    public GosiKafkaConsumer(GosiKafkaClientConfig config, GosiTelemetryReporter telemetryReporter) {
        Map<String, Object> props = config.buildConsumerProperties();
        configureDeserializers(props, config);
        this.internalConsumer = new KafkaConsumer<>(props);
        this.telemetryReporter = telemetryReporter;
    }

    private void configureDeserializers(Map<String, Object> props, GosiKafkaClientConfig config) {
        if (config.getSchemaRegistryUrl() != null && !config.getSchemaRegistryUrl().isEmpty()) {
            propagateSslPropertiesToSchemaRegistry(props);
        }

        switch (config.getKeyFormat()) {
            case AVRO:
                props.put(KEY_DESERIALIZER, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                props.put("specific.avro.reader", "true");
                break;
            case JSON_SCHEMA:
                props.put(KEY_DESERIALIZER, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
                break;
        }

        switch (config.getValueFormat()) {
            case AVRO:
                props.put(VALUE_DESERIALIZER, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                props.put("specific.avro.reader", "true");
                break;
            case JSON_SCHEMA:
                props.put(VALUE_DESERIALIZER, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
                break;
        }
    }

    private void propagateSslPropertiesToSchemaRegistry(Map<String, Object> props) {
        copyProperty(props, "ssl.truststore.location", "schema.registry.ssl.truststore.location");
        copyProperty(props, "ssl.truststore.password", "schema.registry.ssl.truststore.password");
        copyProperty(props, "ssl.truststore.type", "schema.registry.ssl.truststore.type");

        copyProperty(props, "basic.auth.credentials.source", "schema.registry.basic.auth.credentials.source");
        copyProperty(props, "basic.auth.user.info", "schema.registry.basic.auth.user.info");
        copyProperty(props, "schema.registry.basic.auth.credentials.source", "basic.auth.credentials.source");
        copyProperty(props, "schema.registry.basic.auth.user.info", "basic.auth.user.info");

        copyProperty(props, "bearer.auth.credentials.source", "schema.registry.bearer.auth.credentials.source");
        copyProperty(props, "bearer.auth.issuer.endpoint.url", "schema.registry.bearer.auth.issuer.endpoint.url");
        copyProperty(props, "bearer.auth.client.id", "schema.registry.bearer.auth.client.id");
        copyProperty(props, "bearer.auth.client.secret", "schema.registry.bearer.auth.client.secret");
        copyProperty(props, "bearer.auth.scope", "schema.registry.bearer.auth.scope");

        copyProperty(props, "schema.registry.bearer.auth.credentials.source", "bearer.auth.credentials.source");
        copyProperty(props, "schema.registry.bearer.auth.issuer.endpoint.url", "bearer.auth.issuer.endpoint.url");
        copyProperty(props, "schema.registry.bearer.auth.client.id", "bearer.auth.client.id");
        copyProperty(props, "schema.registry.bearer.auth.client.secret", "bearer.auth.client.secret");
        copyProperty(props, "schema.registry.bearer.auth.scope", "bearer.auth.scope");
    }

    private void copyProperty(Map<String, Object> props, String sourceKey, String targetKey) {
        if (props.containsKey(sourceKey) && props.get(sourceKey) != null) {
            props.put(targetKey, props.get(sourceKey));
        }
    }

    public GosiKafkaConsumer<K, V> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public GosiKafkaConsumer<K, V> handler(RecordHandler<K, V> handler) {
        this.handler = handler;
        return this;
    }

    public GosiKafkaConsumer<K, V> withDlq(String dlqTopic, com.gosi.kafka.sdk.producer.GosiKafkaProducer<K, V> dlqProducer) {
        this.dlqTopic = dlqTopic;
        this.dlqProducer = dlqProducer;
        return this;
    }

    /**
     * Starts the consumer polling loop on the current thread.
     * This method blocks until shutdown() is called.
     */
    public void start() {
        if (topic == null || handler == null) {
            throw new IllegalStateException("Topic and handler must be configured before starting.");
        }

        internalConsumer.subscribe(Collections.singletonList(topic));
        running.set(true);
        LOG.info("Started GosiKafkaConsumer for topic: {}", topic);

        try {
            while (running.get()) {
                ConsumerRecords<K, V> records = internalConsumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<K, V> consumerRecord : records) {
                    processRecord(consumerRecord);
                }
                
                reportConsumerLag();
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            internalConsumer.close();
            LOG.info("Closed GosiKafkaConsumer for topic: {}", topic);
        }
    }

    private void processRecord(ConsumerRecord<K, V> consumerRecord) {
        String traceId = TraceContext.initFromHeaders(consumerRecord.headers());
        
        GosiRecord<K, V> gosiRecord = new GosiRecord<>(
            consumerRecord.key(),
            consumerRecord.value(),
            consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.offset(),
            traceId,
            consumerRecord.headers(),
            consumerRecord.timestamp()
        );

        try {
            handler.handle(gosiRecord);
            commitOffset(consumerRecord);
        } catch (Exception e) {
            if (dlqTopic != null && dlqProducer != null) {
                rerouteToDlq(gosiRecord, e);
                commitOffset(consumerRecord); // Commit offset after successfully moving to DLQ
            } else {
                LOG.error("Unhandled exception processing record, no DLQ configured.", e);
                // In production without a DLQ, you might want to pause/stop to avoid data loss
            }
        } finally {
            TraceContext.clear();
        }
    }

    private void commitOffset(ConsumerRecord<K, V> consumerRecord) {
        TopicPartition partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(consumerRecord.offset() + 1);
        Map<TopicPartition, OffsetAndMetadata> currentOffset = Collections.singletonMap(partition, offsetMeta);
        
        internalConsumer.commitAsync(currentOffset, (offsets, exception) -> {
            boolean success = exception == null;
            telemetryReporter.onOffsetCommit(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), success, exception);
        });
    }

    private void rerouteToDlq(GosiRecord<K, V> gosiRecord, Exception cause) {
        // Clear any existing error headers to avoid duplicate/stale metadata
        gosiRecord.getHeaders().remove("error_code");
        gosiRecord.getHeaders().remove("stack_trace");

        gosiRecord.getHeaders().add("error_code", "500".getBytes(StandardCharsets.UTF_8));
        
        String stackTrace = getStackTrace(cause);
        gosiRecord.getHeaders().add("stack_trace", stackTrace.getBytes(StandardCharsets.UTF_8));
        
        // Ensure trace_id is preserved
        TraceContext.injectIntoHeaders(gosiRecord.getHeaders());
        
        dlqProducer.sendAsync(dlqTopic, gosiRecord.getKey(), gosiRecord.getValue(), gosiRecord.getHeaders());
        telemetryReporter.onDlqReroute(gosiRecord.getTopic(), dlqTopic, gosiRecord.getTraceId(), cause);
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

    private void reportConsumerLag() {
        // Simplified lag reporting placeholder
    }

    /**
     * Signals the polling loop to stop.
     */
    public void shutdown() {
        running.set(false);
        internalConsumer.wakeup();
    }
}

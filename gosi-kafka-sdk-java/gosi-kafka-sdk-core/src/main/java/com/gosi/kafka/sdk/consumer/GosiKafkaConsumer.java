package com.gosi.kafka.sdk.consumer;

import com.gosi.kafka.sdk.auth.AuthErrorClassifier;
import com.gosi.kafka.sdk.auth.AuthErrorType;
import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.resilience.ResilienceWrapper;
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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulated Kafka Consumer that manages the polling loop,
 * MDC trace_id extraction, consumer lag telemetry, and automatic offset commits.
 * <p>
 * Supports two DLQ routing modes:
 * <ul>
 *   <li>{@link #withResilience(ResilienceWrapper)} — per-stage DLQ with retry, metrics, restart-loop detection</li>
 *   <li>{@link #withDlq(String, com.gosi.kafka.sdk.producer.GosiKafkaProducer)} — legacy single-topic DLQ (deprecated)</li>
 * </ul>
 * </p>
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
    private ResilienceWrapper<K, V> resilienceWrapper;

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

    /**
     * Configures per-stage resilience with retry, DLQ routing, and metrics.
     * This is the preferred API over {@link #withDlq(String, com.gosi.kafka.sdk.producer.GosiKafkaProducer)}.
     *
     * @param resilienceWrapper the resilience wrapper to use
     * @return this consumer for chaining
     */
    public GosiKafkaConsumer<K, V> withResilience(ResilienceWrapper<K, V> resilienceWrapper) {
        this.resilienceWrapper = resilienceWrapper;
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

        // Record a restart event for restart-loop detection
        if (resilienceWrapper != null) {
            resilienceWrapper.recordRestart();
            if (resilienceWrapper.isInRestartLoop()) {
                LOG.error("Consumer is in RESTART LOOP — consider investigating root cause before continuing");
            }
        }

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
        } catch (Exception e) {
            // Classify auth/authz errors before propagating
            AuthErrorType errorType = AuthErrorClassifier.classify(e);
            if (errorType == AuthErrorType.AUTHENTICATION_FAILURE || errorType == AuthErrorType.AUTHORIZATION_DENIED) {
                telemetryReporter.onAuthError(errorType.name(), e.getMessage());
                LOG.error("{} error during consumption | topic={}", errorType, topic, e);
            }
            throw e;
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
            if (resilienceWrapper != null) {
                // Per-stage resilience: retry + DLQ routing handled by wrapper
                resilienceWrapper.process(gosiRecord, handler);
            } else {
                // Direct handler invocation (legacy path)
                handler.handle(gosiRecord);
            }
            commitOffset(consumerRecord);
        } catch (Exception e) {
            // Classify auth errors distinctly
            AuthErrorType errorType = AuthErrorClassifier.classify(e);
            if (errorType == AuthErrorType.AUTHENTICATION_FAILURE || errorType == AuthErrorType.AUTHORIZATION_DENIED) {
                telemetryReporter.onAuthError(errorType.name(), e.getMessage());
                LOG.error("{} error processing record | topic={} | trace_id={}",
                        errorType, consumerRecord.topic(), traceId, e);
                throw new org.apache.kafka.common.KafkaException(AuthErrorClassifier.classifyAndWrap(e));
            }

            if (resilienceWrapper == null) {
                LOG.error("Unhandled exception processing record, ResilienceWrapper is not configured.", e);
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

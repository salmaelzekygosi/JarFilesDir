package com.gosi.kafka.sdk.producer;

import com.gosi.kafka.sdk.auth.AuthErrorClassifier;
import com.gosi.kafka.sdk.auth.AuthErrorType;
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

/**
 * Encapsulated producer for GOSI applications.
 * Enforces trace_id injection and telemetry reporting.
 */
public class GosiKafkaProducer<K, V> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GosiKafkaProducer.class);
    
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

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
        if (config.getSchemaRegistryUrl() != null && !config.getSchemaRegistryUrl().isEmpty()) {
            propagateSslPropertiesToSchemaRegistry(props);
        }

        switch (config.getKeyFormat()) {
            case AVRO:
                props.put(KEY_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case JSON_SCHEMA:
                props.put(KEY_SERIALIZER, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
                break;
        }

        switch (config.getValueFormat()) {
            case AVRO:
                props.put(VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case JSON_SCHEMA:
                props.put(VALUE_SERIALIZER, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
                props.put(SCHEMA_REGISTRY_URL, config.getSchemaRegistryUrl());
                break;
            case STRING:
            default:
                props.put(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
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

    /**
     * Sends a record to Kafka asynchronously, returning a CompletableFuture.
     * Extracts trace_id from MDC or generates one if missing, and injects into headers.
     */
    public java.util.concurrent.CompletableFuture<DeliveryReport> send(String topic, K key, V value) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, value);
        
        // Inject trace_id from MDC into headers
        String traceId = TraceContext.injectIntoHeaders(producerRecord.headers());
        
        long startMs = System.currentTimeMillis();
        java.util.concurrent.CompletableFuture<DeliveryReport> future = new java.util.concurrent.CompletableFuture<>();

        internalProducer.send(producerRecord, (metadata, exception) -> {
            long latencyMs = System.currentTimeMillis() - startMs;
            DeliveryReport report;
            if (exception == null) {
                report = DeliveryReport.success(topic, metadata.partition(), metadata.offset(), metadata.timestamp(), traceId, latencyMs);
                telemetryReporter.onDeliveryReport(report);
                future.complete(report);
            } else {
                AuthErrorType authErrorType = AuthErrorClassifier.classify(exception);
                String authErrorStr = null;
                if (authErrorType == AuthErrorType.AUTHENTICATION_FAILURE || authErrorType == AuthErrorType.AUTHORIZATION_DENIED) {
                    authErrorStr = authErrorType.name();
                    telemetryReporter.onAuthError(authErrorStr, exception.getMessage());
                }
                
                report = DeliveryReport.failure(topic, traceId, latencyMs, exception, authErrorStr);
                telemetryReporter.onDeliveryReport(report);
                future.completeExceptionally(exception);
            }
        });
        
        return future;
    }
    
    // Proper wrapper method
    public void sendAsync(String topic, K key, V value) {
        sendAsync(topic, key, value, null);
    }
    
    public void sendAsync(String topic, K key, V value, org.apache.kafka.common.header.Headers headers) {
        ProducerRecord<K, V> producerRecord;
        if (headers != null) {
            producerRecord = new ProducerRecord<>(topic, null, null, key, value, headers);
        } else {
            producerRecord = new ProducerRecord<>(topic, key, value);
        }
        String traceId = TraceContext.injectIntoHeaders(producerRecord.headers());
        long startMs = System.currentTimeMillis();

        internalProducer.send(producerRecord, (metadata, exception) -> {
            long latencyMs = System.currentTimeMillis() - startMs;
            DeliveryReport report;
            if (exception == null) {
                report = DeliveryReport.success(topic, metadata.partition(), metadata.offset(), metadata.timestamp(), traceId, latencyMs);
            } else {
                AuthErrorType authErrorType = AuthErrorClassifier.classify(exception);
                String authErrorStr = null;
                if (authErrorType == AuthErrorType.AUTHENTICATION_FAILURE || authErrorType == AuthErrorType.AUTHORIZATION_DENIED) {
                    authErrorStr = authErrorType.name();
                    telemetryReporter.onAuthError(authErrorStr, exception.getMessage());
                }
                report = DeliveryReport.failure(topic, traceId, latencyMs, exception, authErrorStr);
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

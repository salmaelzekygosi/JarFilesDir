package com.gosi.kafka.sdk.config;

import com.gosi.kafka.sdk.auth.AuthenticationHandler;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Immutable configuration class for GOSI Kafka clients.
 * Enforces organizational standards and required properties.
 */
public class GosiKafkaClientConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final AuthenticationHandler authenticationHandler;
    private final String clientId;
    private final String groupId;
    private final SerializationFormat keyFormat;
    private final SerializationFormat valueFormat;
    
    // Tuning parameters
    private final int maxInFlightRequests;
    private final int retries;
    private final String acks;
    private final boolean enableIdempotence;
    private final String autoOffsetReset;

    private GosiKafkaClientConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.schemaRegistryUrl = builder.schemaRegistryUrl;
        this.authenticationHandler = builder.authenticationHandler;
        this.clientId = builder.clientId;
        this.groupId = builder.groupId;
        this.keyFormat = builder.keyFormat;
        this.valueFormat = builder.valueFormat;
        this.maxInFlightRequests = builder.maxInFlightRequests;
        this.retries = builder.retries;
        this.acks = builder.acks;
        this.enableIdempotence = builder.enableIdempotence;
        this.autoOffsetReset = builder.autoOffsetReset;
    }

    /**
     * Builds the properties map for a Kafka Producer, injecting auth and defaults.
     */
    public Map<String, Object> buildProducerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        if (clientId != null && !clientId.isEmpty()) {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        }

        // Organizational Producer Defaults
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);

        // Apply authentication
        if (authenticationHandler != null) {
            authenticationHandler.configure(props);
        }

        return props;
    }

    /**
     * Builds the properties map for a Kafka Consumer, injecting auth and defaults.
     */
    public Map<String, Object> buildConsumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        if (clientId != null && !clientId.isEmpty()) {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        }
        if (groupId != null && !groupId.isEmpty()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        // Organizational Consumer Defaults
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // SDK manages commits

        // Apply authentication
        if (authenticationHandler != null) {
            authenticationHandler.configure(props);
        }

        return props;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public SerializationFormat getKeyFormat() {
        return keyFormat;
    }

    public SerializationFormat getValueFormat() {
        return valueFormat;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String bootstrapServers;
        private String schemaRegistryUrl;
        private AuthenticationHandler authenticationHandler;
        private String clientId;
        private String groupId;
        private SerializationFormat keyFormat = SerializationFormat.STRING;
        private SerializationFormat valueFormat = SerializationFormat.AVRO;
        
        // Safe Organizational Defaults
        private int maxInFlightRequests = 5;
        private int retries = Integer.MAX_VALUE;
        private String acks = "all";
        private boolean enableIdempotence = true;
        private String autoOffsetReset = "earliest";

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder schemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        public Builder authenticationHandler(AuthenticationHandler handler) {
            this.authenticationHandler = handler;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder keyFormat(SerializationFormat format) {
            this.keyFormat = format;
            return this;
        }

        public Builder valueFormat(SerializationFormat format) {
            this.valueFormat = format;
            return this;
        }

        public GosiKafkaClientConfig build() {
            if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers is required");
            }
            if ((valueFormat == SerializationFormat.AVRO || valueFormat == SerializationFormat.JSON_SCHEMA) 
                    && (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty())) {
                throw new IllegalArgumentException("schemaRegistryUrl is required when using AVRO or JSON_SCHEMA");
            }
            return new GosiKafkaClientConfig(this);
        }
    }
}

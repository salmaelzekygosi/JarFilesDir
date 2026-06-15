package com.gosi.kafka.sdk.auth;

import java.util.Map;

/**
 * Strategy interface for configuring Kafka authentication.
 * Implementations are responsible for injecting the correct properties
 * (like security.protocol, sasl.mechanism, jaas config) into the properties map.
 */
public interface AuthenticationHandler {
    
    /**
     * Applies authentication configuration to the given properties map.
     * 
     * @param kafkaProperties the properties map that will be passed to the Kafka client
     */
    void configure(Map<String, Object> kafkaProperties);
}

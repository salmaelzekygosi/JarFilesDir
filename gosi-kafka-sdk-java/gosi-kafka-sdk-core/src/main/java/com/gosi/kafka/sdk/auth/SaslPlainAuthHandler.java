package com.gosi.kafka.sdk.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Map;

/**
 * Configures SASL/PLAIN authentication.
 * Used by most legacy connectors and Flink jobs.
 */
public class SaslPlainAuthHandler implements AuthenticationHandler {

    private final String username;
    private final String password;
    private final boolean useTls;

    public SaslPlainAuthHandler(String username, String password, boolean useTls) {
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username cannot be empty");
        }
        if (password == null || password.trim().isEmpty()) {
            throw new IllegalArgumentException("Password cannot be empty");
        }
        this.username = username;
        this.password = password;
        this.useTls = useTls;
    }

    @Override
    public void configure(Map<String, Object> kafkaProperties) {
        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, useTls ? "SASL_SSL" : "SASL_PLAINTEXT");
        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        kafkaProperties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, username, password));
    }
}

package com.gosi.kafka.sdk.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;

/**
 * Configures Mutual TLS (mTLS) authentication.
 */
public class MutualTlsAuthHandler implements AuthenticationHandler {

    private final String keystoreLocation;
    private final String keystorePassword;
    private final String keyPassword;
    private final String truststoreLocation;
    private final String truststorePassword;

    public MutualTlsAuthHandler(String keystoreLocation, String keystorePassword, String keyPassword,
                                String truststoreLocation, String truststorePassword) {
        if (keystoreLocation == null || keystoreLocation.trim().isEmpty()) {
            throw new IllegalArgumentException("Keystore location cannot be empty");
        }
        this.keystoreLocation = keystoreLocation;
        this.keystorePassword = keystorePassword;
        this.keyPassword = keyPassword != null ? keyPassword : keystorePassword;
        this.truststoreLocation = truststoreLocation;
        this.truststorePassword = truststorePassword;
    }

    @Override
    public void configure(Map<String, Object> kafkaProperties) {
        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        
        kafkaProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        if (keystorePassword != null) {
            kafkaProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        }
        if (keyPassword != null) {
            kafkaProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }
        
        if (truststoreLocation != null && !truststoreLocation.trim().isEmpty()) {
            kafkaProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
            if (truststorePassword != null) {
                kafkaProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
            }
        }
    }
}

package com.gosi.kafka.sdk.schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates Confluent Schema Registry configuration including authentication and TLS.
 */
public class GosiSchemaRegistryConfig {

    private final String url;
    private final String basicAuthUserInfo;
    private final String truststoreLocation;
    private final String truststorePassword;

    public GosiSchemaRegistryConfig(String url, String basicAuthUserInfo, String truststoreLocation, String truststorePassword) {
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema Registry URL cannot be empty");
        }
        this.url = url;
        this.basicAuthUserInfo = basicAuthUserInfo;
        this.truststoreLocation = truststoreLocation;
        this.truststorePassword = truststorePassword;
    }

    /**
     * Builds the properties map needed for Kafka Avro/JsonSchema serializers.
     */
    public Map<String, Object> buildProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", url);
        
        if (basicAuthUserInfo != null && !basicAuthUserInfo.trim().isEmpty()) {
            props.put("basic.auth.credentials.source", "USER_INFO");
            props.put("basic.auth.user.info", basicAuthUserInfo);
        }
        
        if (truststoreLocation != null && !truststoreLocation.trim().isEmpty()) {
            props.put("schema.registry.ssl.truststore.location", truststoreLocation);
            if (truststorePassword != null) {
                props.put("schema.registry.ssl.truststore.password", truststorePassword);
            }
        }
        
        return props;
    }

    public String getUrl() {
        return url;
    }
}
